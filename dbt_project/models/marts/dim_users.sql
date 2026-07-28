{{ config(materialized='table') }}

-- User dimension: one row per user, holding the stable profile attributes
-- that describe who they are rather than what any single payment did.
--
-- On lookahead: these aggregate over the user's whole history, which is
-- deliberate and different from the amount baseline in
-- int_user_rolling_amounts. A dimension is a periodically rebuilt profile —
-- in production this table would be refreshed nightly from data up to the
-- previous day, so scoring today against it uses only the past. The amount
-- z-score could not be handled that way because it has to react within the
-- same window the fraud occurs in, which is why that one is a trailing
-- window computed per transaction.

select
    user_id,

    -- Established home base. Used by the geo rule as a stable profile
    -- attribute, not as a same-transaction lookup.
    approx_top_count(country, 1)[offset(0)].value  as home_country,
    approx_top_count(device, 1)[offset(0)].value   as primary_device,

    count(*)                                       as lifetime_txn_count,
    count(distinct merchant_id)                    as lifetime_distinct_merchants,
    sum(amount_eur)                                as lifetime_amount_eur,
    avg(amount_eur)                                as lifetime_avg_amount_eur,
    stddev(amount_eur)                             as lifetime_stddev_amount_eur,

    min(transacted_at)                             as first_seen_at,
    max(transacted_at)                             as last_seen_at

from {{ ref('stg_transactions') }}
group by user_id
