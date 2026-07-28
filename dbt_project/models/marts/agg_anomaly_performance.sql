{{ config(materialized='table') }}

-- Scores each rule against the synthetic ground truth: one row per rule, plus
-- an "any_rule" row for the combined engine.
--
-- This is the model that makes the flagging honest. Without it, "we flag
-- suspicious transactions" is unfalsifiable — this says how often the flags
-- are right, and what they miss.
--
-- Only possible because the data is synthetic and carries a label. On real
-- data this table would be rebuilt from confirmed chargebacks instead.

with anomalies as (

    select * from {{ ref('fct_transaction_anomalies') }}

),

-- Unpivot to one row per (transaction, rule) so every rule is scored by the
-- same arithmetic rather than three near-identical copies of it.
by_rule as (

    select 'velocity' as rule_name, flag_velocity as flagged, is_fraud_synthetic from anomalies
    union all
    select 'geo',      flag_geo,      is_fraud_synthetic from anomalies
    union all
    select 'amount',   flag_amount,   is_fraud_synthetic from anomalies
    union all
    select 'any_rule', is_flagged,    is_fraud_synthetic from anomalies

),

counts as (

    select
        rule_name,
        count(*)                                        as txns_scored,
        countif(flagged and is_fraud_synthetic)         as true_positives,
        countif(flagged and not is_fraud_synthetic)     as false_positives,
        countif(not flagged and is_fraud_synthetic)     as false_negatives,
        countif(not flagged and not is_fraud_synthetic) as true_negatives
    from by_rule
    group by rule_name

)

select
    rule_name,
    txns_scored,
    true_positives,
    false_positives,
    false_negatives,
    true_negatives,

    true_positives + false_positives as total_flagged,
    true_positives + false_negatives as total_actual_fraud,

    -- safe_divide throughout: a rule that fires on nothing has undefined
    -- precision, and that should surface as null rather than a divide error.
    round(safe_divide(true_positives, true_positives + false_positives), 4)
        as precision_score,
    round(safe_divide(true_positives, true_positives + false_negatives), 4)
        as recall_score,
    round(
        safe_divide(
            2 * true_positives,
            2 * true_positives + false_positives + false_negatives
        ), 4
    ) as f1_score,

    -- Of everything the rule did not flag, how much was actually fraud. This
    -- is the number an ops team cares about: what is slipping through.
    round(safe_divide(false_negatives, false_negatives + true_negatives), 4)
        as false_omission_rate

from counts
order by rule_name
