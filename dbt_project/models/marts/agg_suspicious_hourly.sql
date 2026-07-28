{{
    config(
        materialized='table',
        partition_by={'field': 'transacted_date', 'data_type': 'date'}
    )
}}

-- Hourly flagging summary — the table the Phase 5 dashboard polls.
--
-- It exists so the dashboard never queries fct_transaction_anomalies directly.
-- That fact is ~23 MiB; a dashboard refreshing every 30s against it would scan
-- ~2.7 GB/day against a 1 TB free-tier query allowance, and would get slower as
-- history grows. This rolls the same numbers into one row per hour — a few KB —
-- so refresh cost is effectively constant no matter how much data accumulates.

select
    transacted_hour,
    date(transacted_hour)                           as transacted_date,

    count(*)                                        as txn_count,
    count(distinct user_id)                         as active_users,
    sum(amount_eur)                                 as total_amount_eur,

    countif(is_suspicious)                          as suspicious_count,
    sum(if(is_suspicious, amount_eur, 0))           as suspicious_amount_eur,
    safe_divide(countif(is_suspicious), count(*))   as suspicious_rate,

    -- Per-rule counts, so the dashboard can show which rule is driving alerts
    -- without a second query. These overlap: one transaction can trip several.
    countif(flag_velocity)                          as velocity_count,
    countif(flag_geo)                               as geo_count,
    countif(flag_amount)                            as amount_count,

    countif(rules_triggered >= 2)                   as multi_rule_count,

    -- Ground truth, for the accuracy strip. Synthetic-data only.
    countif(is_fraud_synthetic)                     as actual_fraud_count

from {{ ref('fct_transaction_anomalies') }}
group by transacted_hour
