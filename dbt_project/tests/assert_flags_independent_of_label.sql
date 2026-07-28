-- Guards against label leakage.
--
-- Every rule must be a pure function of observable columns, so two
-- transactions with identical rule inputs must always receive identical
-- flags — regardless of what is_fraud_synthetic says. If someone ever
-- "improves" a rule by peeking at the label, the same input combination
-- would start producing different flags depending on the label, and this
-- test fails.
--
-- Returns rows only on failure.

with grouped as (

    select
        user_txns_in_window,
        country,
        user_prev_country,
        seconds_since_user_prev_txn,
        amount_zscore_vs_user,

        count(distinct cast(flag_velocity as string)) as distinct_velocity_flags,
        count(distinct cast(flag_geo as string))      as distinct_geo_flags,
        count(distinct cast(flag_amount as string))   as distinct_amount_flags

    from {{ ref('fct_transaction_anomalies') }}
    group by 1, 2, 3, 4, 5

)

select *
from grouped
where distinct_velocity_flags > 1
   or distinct_geo_flags > 1
   or distinct_amount_flags > 1
