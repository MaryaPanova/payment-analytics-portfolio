{{ config(materialized='view') }}

-- Cleaned transaction grain: one row per transaction_id.
--
-- Two things happen here that everything downstream depends on:
--   1. `timestamp` arrives as ISO-8601 STRING (the generator loads via a
--      pandas DataFrame, which infers STRING). Cast once, here, so no
--      downstream model has to remember to.
--   2. `amount` is denominated in the row's own currency, so summing it
--      raw across countries is meaningless. amount_eur is the only
--      figure marts should aggregate.

with raw as (

    select * from {{ source('payments_synthetic', 'raw_transactions') }}

),

fx as (

    select * from {{ ref('fx_rates_to_eur') }}

)

select
    raw.transaction_id,
    raw.user_id,
    raw.merchant_id,
    raw.merchant_category,
    raw.country,
    raw.device,
    raw.currency,

    cast(raw.amount as numeric)                          as amount_original,
    cast(raw.amount as numeric) * fx.rate_to_eur         as amount_eur,

    cast(raw.timestamp as timestamp)                     as transacted_at,
    timestamp_trunc(cast(raw.timestamp as timestamp), hour) as transacted_hour,
    date(cast(raw.timestamp as timestamp))               as transacted_date,

    raw.is_fraud_synthetic

from raw
-- Deliberately a LEFT join. An inner join would silently drop transactions in
-- any currency missing from the seed — losing real money from the totals with
-- no signal. Left join keeps the row and nulls amount_eur, which trips the
-- not_null test on amount_eur and fails the build loudly instead.
left join fx
    on raw.currency = fx.currency
