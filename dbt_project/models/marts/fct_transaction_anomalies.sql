{{
    config(
        materialized='table',
        partition_by={'field': 'transacted_date', 'data_type': 'date'},
        cluster_by=['is_suspicious', 'merchant_category']
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

        -- Away from the user's established home base, and too soon after their
        -- last transaction for the trip to be real.
        --
        -- Compares against the user's home country from dim_users, not their
        -- previous country. Comparing to the previous country flagged the
        -- return trip as well as the departure: fraud moves the user abroad,
        -- their next legitimate transaction moves them back, and both look
        -- like a change. That produced 1,705 false positives and held geo
        -- precision at 0.51. A home base is stable, so only the genuine
        -- excursion trips it.
        (
            country != user_home_country
            and coalesce(seconds_since_user_prev_txn, 0)
                <= {{ var('geo_max_gap_seconds') }}
        ) as flag_geo,

        -- One-sided on purpose. An unusually *small* amount is not fraud in
        -- this dataset, so only the upper tail counts.
        --
        -- coalesce to false because amount_zscore_vs_user is null when the
        -- trailing window holds too little history. Null there means "no
        -- basis to judge", and no basis to judge is not grounds to flag.
        coalesce(
            amount_zscore_vs_user >= {{ var('amount_zscore_threshold') }},
            false
        ) as flag_amount

    from windowed

)

select
    * except (is_fraud_synthetic, fraud_pattern),

    cast(flag_velocity as int64)
      + cast(flag_geo as int64)
      + cast(flag_amount as int64)          as rules_triggered,

    (flag_velocity or flag_geo or flag_amount) as is_suspicious,

    -- Kept last, and deliberately after the flags, to make it visually obvious
    -- in the output that they play no part in producing them.
    is_fraud_synthetic,
    fraud_pattern

from flagged
