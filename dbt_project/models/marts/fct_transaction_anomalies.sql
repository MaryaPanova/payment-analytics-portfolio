{{
    config(
        materialized='table',
        partition_by={'field': 'transacted_date', 'data_type': 'date'},
        cluster_by=['is_flagged', 'merchant_category']
    )
}}

-- Rule-based fraud flagging, one row per transaction.
--
-- is_fraud_synthetic is carried through this model but is NEVER an input to
-- any rule — it exists only so agg_anomaly_performance can score the rules
-- against it. Every predicate below uses columns that would be available on
-- real, unlabelled data.
--
-- Thresholds come from dbt vars (see dbt_project.yml) so they can be tuned
-- from the command line without editing SQL.

with txns as (

    select * from {{ ref('fct_transactions') }}

),

windowed as (

    select
        txns.*,

        -- Burst detection. A plain "gap to previous transaction" test is not
        -- enough here: with 500 users over 48h the average gap is ~750s, so
        -- ~11% of perfectly normal transactions land within 90s of the user's
        -- previous one by chance alone. Counting transactions across a window
        -- separates a real burst (5-8 in ~90s) from that background noise.
        count(*) over (
            partition by txns.user_id
            order by unix_seconds(txns.transacted_at)
            range between {{ var('velocity_window_seconds') }} preceding and current row
        ) as user_txns_in_window

    from txns

),

flagged as (

    select
        windowed.*,

        user_txns_in_window >= {{ var('velocity_min_txns') }}
            as flag_velocity,

        -- "Impossible travel": the country changed, and it changed faster than
        -- anyone could physically move between them.
        (
            user_prev_country is not null
            and user_prev_country != country
            and seconds_since_user_prev_txn <= {{ var('geo_max_gap_seconds') }}
        ) as flag_geo,

        -- One-sided on purpose. An unusually *small* amount is not fraud in
        -- this dataset, so only the upper tail counts.
        amount_zscore_vs_user >= {{ var('amount_zscore_threshold') }}
            as flag_amount

    from windowed

)

select
    * except (is_fraud_synthetic),

    cast(flag_velocity as int64)
      + cast(flag_geo as int64)
      + cast(flag_amount as int64)          as rules_triggered,

    (flag_velocity or flag_geo or flag_amount) as is_flagged,

    -- Kept last, and deliberately after the flags, to make it visually obvious
    -- in the output that it plays no part in producing them.
    is_fraud_synthetic

from flagged
