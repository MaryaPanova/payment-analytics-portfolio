{{
    config(
        materialized='table',
        partition_by={'field': 'transacted_date', 'data_type': 'date'},
        cluster_by=['merchant_category', 'country']
    )
}}

-- Transaction-grain fact with per-user context attached.
--
-- The user-level windows here (deviation from the user's own average, seconds
-- since their previous transaction) are the raw material Phase 4's anomaly
-- rules will threshold. They are computed once, here, so the flagging logic
-- stays declarative.
--
-- Partitioned on transacted_date and clustered on the two columns the marts
-- filter by, so a "last 7 days, one category" query scans a slice, not the
-- whole table.

with txns as (

    select * from {{ ref('stg_transactions') }}

),

user_stats as (

    select
        user_id,
        avg(amount_eur)     as user_avg_amount_eur,
        stddev(amount_eur)  as user_stddev_amount_eur,

        -- The country this user transacts from most often — their established
        -- home base. Derived purely from observed behaviour, so it is available
        -- on unlabelled production data too.
        approx_top_count(country, 1)[offset(0)].value as user_modal_country

    from txns
    group by user_id

)

select
    txns.transaction_id,
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
    txns.is_fraud_synthetic,
    txns.fraud_pattern,

    user_stats.user_avg_amount_eur,
    user_stats.user_modal_country,

    -- How far this transaction sits from the user's own norm, in std devs.
    -- Null-safe: a user with one transaction has no stddev, and a user whose
    -- amounts are all identical has stddev 0 — neither is an outlier.
    case
        when coalesce(user_stats.user_stddev_amount_eur, 0) = 0 then 0
        else (txns.amount_eur - user_stats.user_avg_amount_eur)
             / user_stats.user_stddev_amount_eur
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

    -- Geo signal: did this user's country change from their last transaction?
    lag(txns.country) over (
        partition by txns.user_id order by txns.transacted_at
    ) as user_prev_country

from txns
left join user_stats
    on txns.user_id = user_stats.user_id
