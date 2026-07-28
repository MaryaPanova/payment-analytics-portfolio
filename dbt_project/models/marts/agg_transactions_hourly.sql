{{
    config(
        materialized='table',
        partition_by={'field': 'transacted_date', 'data_type': 'date'}
    )
}}

-- Hourly platform-level metrics. This is the table the Phase 5 dashboard
-- reads for its headline time series.

select
    transacted_hour,
    date(transacted_hour)                       as transacted_date,

    count(*)                                    as txn_count,
    count(distinct user_id)                     as active_users,
    count(distinct merchant_id)                 as active_merchants,

    sum(amount_eur)                             as total_amount_eur,
    avg(amount_eur)                             as avg_amount_eur,
    approx_quantiles(amount_eur, 100)[offset(50)] as median_amount_eur,
    max(amount_eur)                             as max_amount_eur,

    countif(is_fraud_synthetic)                 as fraud_txn_count,
    sum(if(is_fraud_synthetic, amount_eur, 0))  as fraud_amount_eur,
    -- Guarded against a zero-row hour, which can't happen with a group by but
    -- would if this ever became a left join onto a date spine.
    safe_divide(countif(is_fraud_synthetic), count(*)) as fraud_rate

from {{ ref('fct_transactions') }}
group by transacted_hour
