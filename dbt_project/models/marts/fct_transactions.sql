{{
    config(
        materialized='table',
        partition_by={'field': 'transacted_date', 'data_type': 'date'},
        cluster_by=['merchant_category', 'country']
    )
}}

-- Central fact of the star schema: one row per transaction, carrying foreign
-- keys to dim_users and dim_merchants plus the per-transaction features the
-- Phase 4 rules threshold.
--
-- merchant_category is kept on the fact rather than left to a dim_merchants
-- join, because it is the clustering key — pruning has to happen without a
-- join. That denormalisation is a storage-for-scan-cost trade, not an
-- oversight; the dimension remains the source of truth and the relationships
-- tests keep the two consistent.
--
-- The per-user amount baseline comes from int_user_rolling_amounts, which
-- uses a TRAILING window. An earlier version averaged each user's entire
-- history here, meaning a transaction was scored against transactions that
-- had not happened yet.

with txns as (

    select * from {{ ref('stg_transactions') }}

),

user_rolling as (

    select * from {{ ref('int_user_rolling_amounts') }}

),

merchant_rates as (

    select * from {{ ref('int_merchant_txn_rates') }}

),

user_profile as (

    select user_id, home_country from {{ ref('dim_users') }}

)

select
    txns.transaction_id,

    -- Foreign keys into the dimensions.
    txns.user_id,
    txns.merchant_id,

    txns.merchant_category,
    txns.country,
    txns.device,
    txns.currency,
    txns.amount_original,
    txns.amount_eur,
    txns.transacted_at,
    txns.transacted_hour,
    txns.transacted_date,
    merchant_rates.transacted_minute,

    -- Trailing per-user baseline.
    user_rolling.user_rolling_avg_amount_eur,
    user_rolling.user_rolling_stddev_amount_eur,
    user_rolling.user_rolling_txn_count,

    -- Rolling merchant throughput.
    merchant_rates.merchant_txns_per_minute,

    -- Stable profile attribute, from the dimension.
    user_profile.home_country as user_home_country,

    -- Deviation from the user's trailing mean, in std devs.
    --
    -- Null — not zero — when the window holds too little history to judge.
    -- Zero would assert "perfectly average", which is a claim the data does
    -- not support; null says "cannot say yet" and downstream declines to flag.
    case
        when user_rolling.user_rolling_txn_count
             < {{ var('min_rolling_observations') }} then null
        when coalesce(user_rolling.user_rolling_stddev_amount_eur, 0) = 0 then 0
        else (txns.amount_eur - user_rolling.user_rolling_avg_amount_eur)
             / user_rolling.user_rolling_stddev_amount_eur
    end as amount_zscore_vs_user,

    -- Velocity signal: gap to this user's previous transaction. Null for a
    -- user's first transaction, which is correctly "no velocity evidence".
    timestamp_diff(
        txns.transacted_at,
        lag(txns.transacted_at) over (
            partition by txns.user_id order by txns.transacted_at
        ),
        second
    ) as seconds_since_user_prev_txn,

    lag(txns.country) over (
        partition by txns.user_id order by txns.transacted_at
    ) as user_prev_country,

    txns.is_fraud_synthetic,
    txns.fraud_pattern

from txns
left join user_rolling
    on txns.transaction_id = user_rolling.transaction_id
left join merchant_rates
    on txns.transaction_id = merchant_rates.transaction_id
left join user_profile
    on txns.user_id = user_profile.user_id
