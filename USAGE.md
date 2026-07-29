# Usage guide

Operating instructions for the pipeline: first-time setup, day-to-day running,
tuning, and the failure modes that are specific to a BigQuery sandbox.

See [README.md](README.md) for what the project is and why it is built this way.

---

## 1. Prerequisites

| Requirement | Notes |
|---|---|
| **Python 3.12** | 3.9 is EOL and `google-auth` warns on every run. `brew install python@3.12` |
| **A GCP project** | Sandbox (no billing) is fine — see [§6](#6-sandbox-limits) for what that rules out |
| **gcloud CLI** | For application-default credentials |
| **A BigQuery dataset** | Default `payments_synthetic`. Note its **region** — you need it exactly |

---

## 2. One-time setup

```bash
git clone https://github.com/MaryaPanova/payment-analytics-portfolio.git
cd payment-analytics-portfolio

python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

gcloud auth application-default login
```

Point the project at your own GCP project (optional — defaults to mine):

```bash
export GCP_PROJECT_ID=your-project-id
export BQ_DATASET_ID=payments_synthetic     # optional
export BQ_TABLE_ID=raw_transactions         # optional
```

Create the dbt profile:

```bash
cp dbt_project/profiles.yml.example dbt_project/profiles.yml
```

Then edit `dbt_project/profiles.yml` and set `project:` and `location:`.

> **`location:` must match your dataset's region exactly.** `EU` is a
> multi-region and is **not** the same place as `europe-west10`. A mismatch
> fails at *query* time, not at connection time, so `dbt debug` will pass and
> `dbt run` will not. Check yours with:
>
> ```bash
> .venv/bin/python -c "from google.cloud import bigquery; \
>   print(bigquery.Client().get_dataset('payments_synthetic').location)"
> ```

Verify the connection:

```bash
.venv/bin/dbt debug --project-dir dbt_project --profiles-dir dbt_project
```

---

## 3. Running it

### Seed history

The dbt models compute over rolling windows, so they need history before they
show anything useful. 48 hours is the tested default (~115k rows, ~10 minutes):

```bash
.venv/bin/python synthetic_transaction_generator.py --backfill-hours 48 --no-live
```

Drop `--no-live` to continue into the live loop once the backfill finishes.

### Keep data flowing

```bash
.venv/bin/python synthetic_transaction_generator.py
```

Loads a 30-row micro-batch every 45 seconds. `Ctrl+C` to stop. Leave this
running in its own terminal if you want the dashboard to look live.

### Build the models

```bash
.venv/bin/dbt build --project-dir dbt_project --profiles-dir dbt_project
```

`build` runs models *and* tests in dependency order — 11 models, 72 tests.
Expect `PASS=84`. Useful variants:

```bash
# One model and everything downstream of it
.venv/bin/dbt build --project-dir dbt_project --profiles-dir dbt_project \
  --select fct_transactions+

# Tests only
.venv/bin/dbt test --project-dir dbt_project --profiles-dir dbt_project
```

New data does **not** appear in the marts until you rebuild — they are tables,
not views. The dashboard reads the marts, so the loop is: generate → `dbt
build` → refresh.

### Watch it

```bash
.venv/bin/streamlit run dashboard/app.py
```

Opens on <http://localhost:8501>. The sidebar controls auto-refresh interval and
alert row count.

> If the banner says **"Data is N h old"**, that is correct behaviour, not a
> bug: the generator's live loop is not running, so the marts hold a static
> snapshot. The dashboard says so rather than implying live traffic.

---

## 4. Tuning the detection rules

Thresholds are dbt vars — no SQL edits needed:

| Var | Default | What it does |
|---|---|---|
| `velocity_min_txns` | 4 | Transactions inside the window before it reads as a burst |
| `velocity_window_seconds` | 120 | That window |
| `geo_max_gap_seconds` | 3600 | How fast a country change must be to look impossible |
| `amount_zscore_threshold` | 3.0 | Std devs above the user's trailing mean |
| `user_rolling_window_seconds` | 86400 | Length of the trailing amount baseline |
| `min_rolling_observations` | 10 | Below this, the z-score is null and the rule declines to judge |

```bash
.venv/bin/dbt build --project-dir dbt_project --profiles-dir dbt_project \
  --vars '{amount_zscore_threshold: 4, velocity_min_txns: 3}'
```

Then read the effect straight out of the scoring model:

```sql
SELECT rule_name, precision_score, targeted_recall_score, f1_score
FROM `your-project.payments_analytics_marts.agg_anomaly_performance`;
```

Raising a threshold trades recall for precision. `targeted_recall_score` is the
per-rule number to watch — `recall_score` is capped near one third by design,
because each rule targets one of three fraud patterns.

### Changing the fraud rate

Edit `TARGET_FRAUD_RATE` in `synthetic_transaction_generator.py`. Branch
probabilities are solved from it, so the realised rate follows the constant —
do **not** hand-edit the individual branch probabilities, which is what caused
the documented rate and the real one to drift apart by 3× previously.

Requires regenerating data ([§5](#5-resetting-the-data)).

---

## 5. Resetting the data

DML (`TRUNCATE`, `DELETE`) is **blocked without billing**. Delete the table
instead — the next load job recreates it:

```bash
.venv/bin/python -c "
from google.cloud import bigquery
c = bigquery.Client()
c.delete_table('your-project.payments_synthetic.raw_transactions')
print('deleted')
"
```

Then re-run the backfill. This is also **required after any generator schema
change** (a new column), since load jobs will not reconcile against the old
schema.

---

## 6. Sandbox limits

A no-billing BigQuery sandbox constrains this project in four ways. All are
documented here because each cost real debugging time.

| Limit | Effect | Workaround in this repo |
|---|---|---|
| **No DML** | `TRUNCATE`/`DELETE`/`UPDATE` fail with `billingNotEnabled` | Delete and recreate the table ([§5](#5-resetting-the-data)) |
| **No streaming inserts** | True real-time ingestion unavailable | Load jobs every 45s — honest near-real-time |
| **No scheduled queries** | Data Transfer Service needs billing, so Looker Studio auto-refresh is out | Streamlit polls instead |
| **60-day table expiry** | Tables silently vanish | Re-run the backfill; check `default_table_expiration_ms` on the dataset |

Confirm your own billing state:

```bash
gcloud beta billing projects describe $GCP_PROJECT_ID
```

> **Load jobs are capped at ~1,500 per table per day.** The backfill batches one
> load job per simulated hour (48 jobs for 48h) rather than one per micro-batch
> (which would be 1,920 and would fail). If you extend the backfill well beyond
> 48 hours, keep that ceiling in mind.

---

## 7. Troubleshooting

**`dbt debug` passes but `dbt run` fails with "Not found: Dataset"**
Region mismatch. See the callout in [§2](#2-one-time-setup).

**`403 billingNotEnabled` on a DELETE or TRUNCATE**
Expected on a sandbox. Use [§5](#5-resetting-the-data).

**Dashboard shows "Data is N h old"**
The live loop is not running. Start the generator, then `dbt build`.

**Dashboard numbers do not change after new data lands**
The marts are tables. Re-run `dbt build`. Results are also cached in the app
for 30 seconds — use **Refresh now** in the sidebar to clear it.

**`dbt: bad interpreter: .../python: no such file or directory`**
The venv was moved or renamed. Console scripts hardcode absolute paths — you
cannot relocate a venv. Delete `.venv` and rebuild it ([§2](#2-one-time-setup)).

**Load job fails on schema mismatch after editing the generator**
Adding a column changes the table schema. Delete the table ([§5](#5-resetting-the-data)).

**Tests fail on `amount_eur` being null**
A currency is missing from `dbt_project/seeds/fx_rates_to_eur.csv`. The FX join
is a LEFT join deliberately, so an unseeded currency nulls `amount_eur` and
trips the test rather than silently dropping the transaction. Add the rate and
re-run `dbt seed`.

---

## 8. Cost

Everything here fits the BigQuery free tier: 10 GB storage and 1 TB of query
processing per month.

- 48h of data ≈ **14 MB** stored (115,200 rows).
- A full `dbt build` scans on the order of a few hundred MB — the model builds
  alone are ≈ 70 MB and the 72 tests add the rest. dbt prints bytes processed
  per model as it runs, so you can measure your own rather than trust this.
- The dashboard polls `agg_suspicious_hourly` (49 rows, a few KB), so refreshes
  are effectively free — deliberately, so an auto-refreshing dashboard cannot
  quietly consume the monthly allowance. Querying `fct_transaction_anomalies`
  directly every 30s would scan ~2.7 GB/day instead.
