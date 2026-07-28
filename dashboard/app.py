"""
Live transaction & fraud dashboard — Phase 5 of the Real-Time Transaction
Analytics (BigQuery) portfolio project.

Polls the dbt marts in BigQuery on an interval and shows rolling transaction
volume alongside the live fraud-flag rate.

Run:
    streamlit run dashboard/app.py

Reads the same GCP_PROJECT_ID env var as the generator, and authenticates with
application-default credentials:
    gcloud auth application-default login
"""

import os
from datetime import datetime, timezone

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "payment-analytics-portfolio")
MARTS = f"{PROJECT_ID}.payments_analytics_marts"

# How long a query result is reused before BigQuery is hit again. The dashboard
# reads pre-aggregated marts (a few KB each), so this is about avoiding
# pointless round-trips rather than cost — but the sandbox has a 1 TB/month
# query allowance and an uncached auto-refreshing dashboard is a good way to
# spend it on nothing.
CACHE_TTL_SECONDS = 30

# The generator loads a micro-batch every BATCH_INTERVAL_SEC (45s). Data older
# than a few multiples of that means the live loop is not running.
STALE_AFTER_MINUTES = 5

st.set_page_config(
    page_title="Payment fraud monitor",
    page_icon="💳",
    layout="wide",
)

# ---- Palette -------------------------------------------------------------
# Two steps of the same hues, chosen for the light and dark chart surfaces
# rather than flipped automatically.
PALETTE = {
    "light": {
        "volume": "#2a78d6",   # categorical slot 1, blue
        "alert": "#e34948",    # categorical slot 8, red
        "muted": "#898781",
        "grid": "#e1e0d9",
    },
    "dark": {
        "volume": "#3987e5",
        "alert": "#e66767",
        "muted": "#898781",
        "grid": "#2c2c2a",
    },
}


def theme_colors() -> dict:
    """Pick the palette step matching the viewer's Streamlit theme."""
    try:
        mode = st.context.theme.type          # "light" | "dark"
    except Exception:
        mode = "light"
    return PALETTE.get(mode, PALETTE["light"])


@st.cache_resource
def get_client() -> bigquery.Client:
    """One BigQuery client for the whole session — cache_resource, not
    cache_data, because a client is a live connection, not a value."""
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def load_hourly() -> pd.DataFrame:
    return run_query(f"""
        SELECT transacted_hour, txn_count, active_users, total_amount_eur,
               suspicious_count, suspicious_amount_eur, suspicious_rate,
               velocity_count, geo_count, amount_count, multi_rule_count,
               actual_fraud_count
        FROM `{MARTS}.agg_suspicious_hourly`
        ORDER BY transacted_hour
    """)


def load_rule_performance() -> pd.DataFrame:
    return run_query(f"""
        SELECT rule_name, total_flagged, precision_score,
               targeted_recall_score, f1_score
        FROM `{MARTS}.agg_anomaly_performance`
        ORDER BY CASE rule_name WHEN 'velocity' THEN 1 WHEN 'geo' THEN 2
                                WHEN 'amount' THEN 3 ELSE 4 END
    """)


def load_recent_suspicious(limit: int) -> pd.DataFrame:
    # Filtered on transacted_date first so the partition filter prunes before
    # the ORDER BY touches anything.
    return run_query(f"""
        SELECT transacted_at, user_id, merchant_id, merchant_category,
               country, user_home_country, ROUND(amount_eur, 2) AS amount_eur,
               rules_triggered, flag_velocity, flag_geo, flag_amount
        FROM `{MARTS}.fct_transaction_anomalies`
        WHERE is_suspicious
          AND transacted_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        ORDER BY transacted_at DESC
        LIMIT {int(limit)}
    """)


def line_chart(df: pd.DataFrame, y: str, title: str, color: str,
               y_title: str, c: dict, percent: bool = False):
    """Single-series time line. No legend — the title names the series."""
    y_axis = alt.Axis(title=y_title, format="%" if percent else "~s",
                      labelColor=c["muted"], titleColor=c["muted"],
                      gridColor=c["grid"], domain=False, tickSize=0)
    return (
        alt.Chart(df, title=title)
        .mark_line(
            color=color,
            strokeWidth=2,                    # thin marks
            point=alt.OverlayMarkDef(color=color, size=30),
            interpolate="monotone",
        )
        .encode(
            x=alt.X("transacted_hour:T",
                    axis=alt.Axis(title=None, labelColor=c["muted"],
                                  gridColor=c["grid"], domain=False,
                                  tickSize=0, format="%b %d %H:%M")),
            y=alt.Y(f"{y}:Q", axis=y_axis,
                    scale=alt.Scale(domainMin=0, zero=True)),
            tooltip=[
                alt.Tooltip("transacted_hour:T", title="Hour",
                            format="%b %d %H:%M"),
                alt.Tooltip(f"{y}:Q", title=y_title,
                            format=".2%" if percent else ",.0f"),
            ],
        )
        .properties(height=260)
        .configure_view(stroke=None)
        .configure_axis(labelFontSize=11, titleFontSize=11)
    )


# ---- Page ----------------------------------------------------------------

st.title("💳 Payment fraud monitor")
st.caption(
    f"Rule-based flagging over BigQuery marts · project `{PROJECT_ID}` · "
    "models built with dbt"
)

with st.sidebar:
    st.header("Controls")
    auto_refresh = st.toggle("Auto-refresh", value=True)
    refresh_seconds = st.select_slider(
        "Refresh every", options=[15, 30, 60, 120, 300], value=30,
        format_func=lambda s: f"{s}s",
        disabled=not auto_refresh,
    )
    row_limit = st.slider("Recent alerts to show", 10, 100, 25, step=5)
    if st.button("Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption(
        f"Results cached {CACHE_TTL_SECONDS}s. The dashboard reads "
        "pre-aggregated marts, so a refresh scans kilobytes, not the "
        "115k-row fact table."
    )


def render() -> None:
    c = theme_colors()
    hourly = load_hourly()

    if hourly.empty:
        st.warning(
            "No data in `agg_suspicious_hourly`. Run the generator, then "
            "`dbt build`, and refresh."
        )
        return

    hourly["transacted_hour"] = pd.to_datetime(hourly["transacted_hour"],
                                               utc=True)
    latest = hourly["transacted_hour"].max()
    age_min = (datetime.now(timezone.utc) - latest).total_seconds() / 60

    # ---- Freshness -------------------------------------------------------
    # This dashboard shows whatever is in BigQuery. Without the generator's
    # live loop running, that is a static backfill — say so plainly rather
    # than letting a stale chart imply live traffic.
    if age_min > STALE_AFTER_MINUTES:
        st.warning(
            f"**Data is {age_min/60:.1f} h old** (latest hour "
            f"{latest:%Y-%m-%d %H:%M} UTC). The generator's live loop is not "
            "running, so this is a static snapshot. Start it with "
            "`python synthetic_transaction_generator.py`, then `dbt build` to "
            "refresh the marts.",
            icon="⏸️",
        )
    else:
        st.success(
            f"Live — latest hour {latest:%H:%M} UTC "
            f"({age_min:.0f} min old)", icon="🟢",
        )

    # ---- KPI row ---------------------------------------------------------
    # Headline numbers are stat tiles, not a one-bar chart.
    window = hourly.tail(24)
    txns = int(window["txn_count"].sum())
    flagged = int(window["suspicious_count"].sum())
    volume = float(window["total_amount_eur"].sum())
    flagged_value = float(window["suspicious_amount_eur"].sum())
    rate = flagged / txns if txns else 0.0
    multi = int(window["multi_rule_count"].sum())

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Transactions (24 h)", f"{txns:,}")
    k2.metric("Volume (24 h)", f"€{volume:,.0f}")
    k3.metric("Flagged suspicious", f"{flagged:,}", f"{rate:.2%} of traffic")
    k4.metric("Value flagged", f"€{flagged_value:,.0f}")
    k5.metric("Multi-rule hits", f"{multi:,}", help=(
        "Transactions tripping 2+ rules. Historically 100% precision — the "
        "natural auto-block tier."
    ))

    st.divider()

    # ---- Time series -----------------------------------------------------
    # Two separate charts, never a dual axis: volume and a rate share no scale.
    left, right = st.columns(2)
    with left:
        st.altair_chart(
            line_chart(hourly, "txn_count", "Transaction volume per hour",
                       c["volume"], "Transactions", c),
            width="stretch",
        )
    with right:
        st.altair_chart(
            line_chart(hourly, "suspicious_rate", "Suspicious rate per hour",
                       c["alert"], "Flagged share", c, percent=True),
            width="stretch",
        )

    # ---- Which rule is firing -------------------------------------------
    st.subheader("Alerts by rule")
    rules_long = hourly.melt(
        id_vars="transacted_hour",
        value_vars=["velocity_count", "geo_count", "amount_count"],
        var_name="rule", value_name="alerts",
    )
    rules_long["rule"] = rules_long["rule"].str.replace("_count", "",
                                                        regex=False)
    st.altair_chart(
        alt.Chart(rules_long)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("transacted_hour:T", axis=alt.Axis(
                title=None, labelColor=c["muted"], gridColor=c["grid"],
                domain=False, tickSize=0, format="%b %d %H:%M")),
            y=alt.Y("alerts:Q", axis=alt.Axis(
                title="Alerts", labelColor=c["muted"], titleColor=c["muted"],
                gridColor=c["grid"], domain=False, tickSize=0)),
            # 3 series: categorical, fixed order, never cycled.
            color=alt.Color("rule:N", scale=alt.Scale(
                domain=["velocity", "geo", "amount"],
                range=[c["volume"], "#eb6834", "#1baf7a"]),
                legend=alt.Legend(title=None, orient="top",
                                  labelColor=c["muted"])),
            tooltip=[
                alt.Tooltip("transacted_hour:T", title="Hour",
                            format="%b %d %H:%M"),
                alt.Tooltip("rule:N", title="Rule"),
                alt.Tooltip("alerts:Q", title="Alerts", format=",.0f"),
            ],
        )
        .properties(height=240)
        .configure_view(stroke=None),
        width="stretch",
    )
    st.caption(
        "Rules overlap — one transaction can trip several, so these bars sum "
        "to more than the flagged total."
    )

    # ---- Accuracy + recent alerts ---------------------------------------
    perf_col, alert_col = st.columns([1, 2])

    with perf_col:
        st.subheader("Rule accuracy")
        perf = load_rule_performance()
        st.dataframe(
            perf, hide_index=True, width="stretch",
            column_config={
                "rule_name": "Rule",
                "total_flagged": st.column_config.NumberColumn("Flagged",
                                                               format="%d"),
                "precision_score": st.column_config.NumberColumn(
                    "Precision", format="%.3f"),
                "targeted_recall_score": st.column_config.NumberColumn(
                    "Recall*", format="%.3f"),
                "f1_score": st.column_config.NumberColumn("F1",
                                                          format="%.3f"),
            },
        )
        st.caption(
            "\\* Recall against the pattern each rule targets. Scored against "
            "synthetic ground truth — available here only because the data is "
            "generated; production would score against confirmed chargebacks."
        )

    with alert_col:
        st.subheader("Most recent alerts")
        recent = load_recent_suspicious(row_limit)
        if recent.empty:
            st.info("No flagged transactions in the last 2 days.")
        else:
            recent = recent.copy()
            recent["rules"] = recent.apply(
                lambda r: " ".join(filter(None, [
                    "velocity" if r["flag_velocity"] else "",
                    "geo" if r["flag_geo"] else "",
                    "amount" if r["flag_amount"] else "",
                ])), axis=1,
            )
            st.dataframe(
                recent[["transacted_at", "user_id", "merchant_category",
                        "country", "user_home_country", "amount_eur",
                        "rules_triggered", "rules"]],
                hide_index=True, width="stretch",
                column_config={
                    "transacted_at": st.column_config.DatetimeColumn(
                        "Time (UTC)", format="MMM DD HH:mm:ss"),
                    "user_id": "User",
                    "merchant_category": "Category",
                    "country": "Country",
                    "user_home_country": "Home",
                    "amount_eur": st.column_config.NumberColumn(
                        "Amount (€)", format="%.2f"),
                    "rules_triggered": st.column_config.NumberColumn(
                        "Rules", format="%d"),
                    "rules": "Triggered",
                },
            )


# st.fragment re-runs only this function on the interval, leaving the sidebar
# and page chrome alone — so the controls do not flicker on every poll.
if auto_refresh:
    render = st.fragment(run_every=f"{refresh_seconds}s")(render)

render()
