{{
    config(
        materialized='table',
        partition_by={'field': 'transacted_date', 'data_type': 'date'},
        cluster_by=['merchant_category']
    )
}}

-- Daily per-merchant rollup: which merchants carry volume, and which carry
-- disproportionate fraud. Feeds the merchant breakdown on the dashboard.

select
    transacted_date,
    merchant_id,
    merchant_category,

    count(*)                                    as txn_count,
    count(distinct user_id)                     as unique_users,

    sum(amount_eur)                             as total_amount_eur,
    avg(amount_eur)                             as avg_amount_eur,
    max(amount_eur)                             as max_amount_eur,

    countif(is_fraud_synthetic)                 as fraud_txn_count,
    sum(if(is_fraud_synthetic, amount_eur, 0))  as fraud_amount_eur,
    safe_divide(countif(is_fraud_synthetic), count(*)) as fraud_rate

from {{ ref('fct_transactions') }}
group by transacted_date, merchant_id, merchant_category
