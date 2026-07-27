"""
Synthetic payment transaction generator — Phase 1 & 2 of the
Real-Time Transaction Analytics (BigQuery) portfolio project.

Generates fake but realistic payment events with built-in fraud-like
patterns (velocity spikes, geo mismatches, amount outliers), then loads
them into BigQuery in micro-batches to simulate near-real-time ingestion.

Usage:
    python synthetic_transaction_generator.py --backfill-hours 48
    python synthetic_transaction_generator.py                       # live loop only

Target table defaults to payment-analytics-portfolio.payments_synthetic.raw_transactions;
override with the GCP_PROJECT_ID / BQ_DATASET_ID / BQ_TABLE_ID env vars.
"""

import argparse
import os
import time
import uuid
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker
from google.cloud import bigquery

# ---- Config: override via env vars, or edit the defaults ----
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "payment-analytics-portfolio")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "payments_synthetic")
TABLE_ID = os.environ.get("BQ_TABLE_ID", "raw_transactions")

BATCH_SIZE = 30          # transactions per micro-batch
BATCH_INTERVAL_SEC = 45  # how often to load a new batch in live mode

fake = Faker()
random.seed(42)

# ---- Synthetic reference data: fixed pools of users & merchants ----
COUNTRIES = ["DE", "FR", "NL", "ES", "IT", "PL", "PT", "BR", "US", "AE"]
DEVICES = ["ios", "android", "web"]
CURRENCIES = {"DE": "EUR", "FR": "EUR", "NL": "EUR", "ES": "EUR", "IT": "EUR",
              "PL": "PLN", "PT": "EUR", "BR": "BRL", "US": "USD", "AE": "AED"}

N_USERS = 500
N_MERCHANTS = 60

USERS = [
    {"user_id": f"u_{i:04d}", "home_country": random.choice(COUNTRIES),
     "home_device": random.choice(DEVICES), "avg_amount": round(random.uniform(15, 250), 2)}
    for i in range(N_USERS)
]
MERCHANTS = [
    {"merchant_id": f"m_{i:03d}", "category": random.choice(
        ["retail", "travel", "food_delivery", "gaming", "subscriptions", "electronics"])}
    for i in range(N_MERCHANTS)
]


def _base_transaction(ts: datetime, user: dict, fraud_flag: bool = False) -> dict:
    merchant = random.choice(MERCHANTS)
    country = user["home_country"]
    amount = round(max(1.0, random.gauss(user["avg_amount"], user["avg_amount"] * 0.3)), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user["user_id"],
        "merchant_id": merchant["merchant_id"],
        "merchant_category": merchant["category"],
        "amount": amount,
        "currency": CURRENCIES[country],
        "country": country,
        "device": user["home_device"],
        "timestamp": ts.isoformat(),
        "is_fraud_synthetic": fraud_flag,  # ground truth, for later evaluating your flagging logic
    }


def generate_batch(n: int, as_of: datetime) -> list[dict]:
    """Generate n transactions ending at `as_of`, with ~4-5% injected fraud-like patterns."""
    rows = []
    i = 0
    while i < n:
        roll = random.random()

        if roll < 0.015 and i + 6 < n:
            # Velocity spike: same user, several transactions within ~90 seconds
            user = random.choice(USERS)
            burst_start = as_of - timedelta(seconds=random.randint(0, 120))
            burst_size = random.randint(5, 8)
            for j in range(burst_size):
                ts = burst_start + timedelta(seconds=j * random.randint(5, 15))
                rows.append(_base_transaction(ts, user, fraud_flag=True))
            i += burst_size

        elif roll < 0.03:
            # Geo mismatch: transaction from a country far from the user's home country,
            # timestamped implausibly close to a "normal" one
            user = random.choice(USERS)
            ts = as_of - timedelta(seconds=random.randint(0, 300))
            row = _base_transaction(ts, user, fraud_flag=True)
            other_countries = [c for c in COUNTRIES if c != user["home_country"]]
            row["country"] = random.choice(other_countries)
            row["currency"] = CURRENCIES[row["country"]]
            rows.append(row)
            i += 1

        elif roll < 0.045:
            # Amount outlier: 8-15x this user's normal amount
            user = random.choice(USERS)
            ts = as_of - timedelta(seconds=random.randint(0, 300))
            row = _base_transaction(ts, user, fraud_flag=True)
            row["amount"] = round(user["avg_amount"] * random.uniform(8, 15), 2)
            rows.append(row)
            i += 1

        else:
            # Normal transaction
            user = random.choice(USERS)
            ts = as_of - timedelta(seconds=random.randint(0, 300))
            rows.append(_base_transaction(ts, user, fraud_flag=False))
            i += 1

    return rows


def load_batch(client: bigquery.Client, table_ref: str, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()  # wait for the load job to finish
    print(f"[{datetime.now(timezone.utc).isoformat()}] loaded {len(rows)} rows -> {table_ref}")


def backfill(client: bigquery.Client, table_ref: str, hours: int) -> None:
    """Seed historical data so dbt rolling-window models have something to aggregate
    before the live loop starts producing real-time data.

    Generates data at the same BATCH_INTERVAL_SEC granularity as the live loop
    (so patterns/timestamps look identical to real ingestion), but flushes to
    BigQuery once per simulated hour instead of once per micro-batch. This keeps
    the backfill well under BigQuery's ~1,500-load-jobs-per-table-per-day limit
    (24h backfill = 24 load jobs, not 1,920)."""
    now = datetime.now(timezone.utc)
    batches_per_hour = 3600 // BATCH_INTERVAL_SEC
    print(f"Backfilling ~{hours}h of history in {hours} load jobs "
          f"({batches_per_hour} micro-batches each)...")
    for h in range(hours):
        hour_rows = []
        for b in range(batches_per_hour):
            offset_sec = h * 3600 + b * BATCH_INTERVAL_SEC
            as_of = now - timedelta(seconds=offset_sec)
            hour_rows.extend(generate_batch(BATCH_SIZE, as_of))
        load_batch(client, table_ref, hour_rows)


def live_loop(client: bigquery.Client, table_ref: str) -> None:
    print("Starting live micro-batch loop. Ctrl+C to stop.")
    try:
        while True:
            rows = generate_batch(BATCH_SIZE, datetime.now(timezone.utc))
            load_batch(client, table_ref, rows)
            time.sleep(BATCH_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("Stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill-hours", type=int, default=0,
                         help="Hours of synthetic history to seed before starting the live loop")
    parser.add_argument("--no-live", action="store_true",
                         help="Only backfill, skip the live loop")
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    if args.backfill_hours:
        backfill(client, table_ref, args.backfill_hours)

    if not args.no_live:
        live_loop(client, table_ref)
