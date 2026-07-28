-- Rolling transactions-per-minute per merchant, at transaction grain.
--
-- A merchant's throughput spiking is itself a fraud signal (card testing runs
-- a stolen card list through one merchant), and it is the series the Phase 5
-- dashboard plots per merchant.
--
-- Trailing window including the current row: unlike the user amount baseline
-- this is a *rate observation*, not a baseline to compare the current row
-- against, so the current transaction legitimately counts toward it.

select
    transaction_id,
    merchant_id,
    transacted_at,

    count(*) over (
        partition by merchant_id
        order by unix_seconds(transacted_at)
        range between {{ var('merchant_rate_window_seconds') }} preceding
                  and current row
    ) as merchant_txns_per_minute,

    -- Minute bucket, so the dashboard can group without recomputing windows.
    timestamp_trunc(transacted_at, minute) as transacted_minute

from {{ ref('stg_transactions') }}
