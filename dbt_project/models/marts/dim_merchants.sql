{{ config(materialized='table') }}

-- Merchant dimension: one row per merchant.
--
-- merchant_category is fixed per merchant in the source, so any_value is
-- correct and cheaper than a grouping key. If a merchant could ever change
-- category this would need to become a slowly-changing dimension.

select
    merchant_id,
    any_value(merchant_category)                   as merchant_category,

    count(*)                                       as lifetime_txn_count,
    count(distinct user_id)                        as lifetime_distinct_users,
    sum(amount_eur)                                as lifetime_amount_eur,
    avg(amount_eur)                                as lifetime_avg_amount_eur,
    max(amount_eur)                                as lifetime_max_amount_eur,

    min(transacted_at)                             as first_seen_at,
    max(transacted_at)                             as last_seen_at

from {{ ref('stg_transactions') }}
group by merchant_id
