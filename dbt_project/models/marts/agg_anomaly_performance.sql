{{ config(materialized='table') }}

-- Scores each rule against the synthetic ground truth: one row per rule, plus
-- an "any_rule" row for the combined engine.
--
-- This is the model that makes the flagging honest. Without it, "we flag
-- suspicious transactions" is unfalsifiable — this says how often the flags
-- are right, and what they miss.
--
-- Two recalls are reported, and the distinction matters:
--
--   recall_score          - of ALL fraud, how much this rule caught
--   targeted_recall_score - of the fraud pattern this rule actually TARGETS,
--                           how much it caught
--
-- Each rule detects one of three patterns, so recall_score is bounded at
-- roughly one third by construction and says more about the mix of fraud in
-- the data than about the rule. targeted_recall_score is the number that
-- describes whether the rule works. Only the any_rule row, which has no single
-- target, should be read on recall_score.
--
-- Only possible because the data is synthetic and carries a pattern label. On
-- real data this would be rebuilt from confirmed chargebacks instead.

with anomalies as (

    select * from {{ ref('fct_transaction_anomalies') }}

),

-- Unpivot to one row per (transaction, rule) so every rule is scored by the
-- same arithmetic rather than four near-identical copies of it.
by_rule as (

    select 'velocity' as rule_name, 'velocity' as target_pattern,
           flag_velocity as flagged, is_fraud_synthetic, fraud_pattern
    from anomalies
    union all
    select 'geo', 'geo', flag_geo, is_fraud_synthetic, fraud_pattern
    from anomalies
    union all
    select 'amount', 'amount', flag_amount, is_fraud_synthetic, fraud_pattern
    from anomalies
    union all
    -- The combined engine targets every pattern, so its "target population" is
    -- all fraud. Null target_pattern selects that branch below.
    select 'any_rule', null, is_suspicious, is_fraud_synthetic, fraud_pattern
    from anomalies

),

counts as (

    select
        rule_name,
        target_pattern,
        count(*)                                        as txns_scored,
        countif(flagged and is_fraud_synthetic)         as true_positives,
        countif(flagged and not is_fraud_synthetic)     as false_positives,
        countif(not flagged and is_fraud_synthetic)     as false_negatives,
        countif(not flagged and not is_fraud_synthetic) as true_negatives,

        countif(
            case when target_pattern is null then is_fraud_synthetic
                 else fraud_pattern = target_pattern end
        ) as target_population,

        countif(
            flagged and case when target_pattern is null then is_fraud_synthetic
                             else fraud_pattern = target_pattern end
        ) as target_caught

    from by_rule
    group by rule_name, target_pattern

)

select
    rule_name,
    target_pattern,
    txns_scored,
    true_positives,
    false_positives,
    false_negatives,
    true_negatives,

    true_positives + false_positives as total_flagged,
    true_positives + false_negatives as total_actual_fraud,
    target_population,
    target_caught,

    -- safe_divide throughout: a rule that fires on nothing has undefined
    -- precision, and that should surface as null rather than a divide error.
    round(safe_divide(true_positives, true_positives + false_positives), 4)
        as precision_score,
    round(safe_divide(true_positives, true_positives + false_negatives), 4)
        as recall_score,
    round(safe_divide(target_caught, target_population), 4)
        as targeted_recall_score,
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
