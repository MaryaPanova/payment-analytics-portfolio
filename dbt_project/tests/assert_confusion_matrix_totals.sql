-- The four confusion-matrix cells must account for every scored transaction,
-- and the flagged/actual subtotals must agree with them. If a null ever creeps
-- into flagged or is_fraud_synthetic, the countif predicates would silently
-- drop the row and every rate computed from them would be quietly wrong.
--
-- Returns rows only on failure, which is what dbt treats as a failing test.

select
    rule_name,
    txns_scored,
    true_positives + false_positives + false_negatives + true_negatives
        as cells_total
from {{ ref('agg_anomaly_performance') }}
where true_positives + false_positives + false_negatives + true_negatives
      != txns_scored
   or true_positives + false_positives != total_flagged
   or true_positives + false_negatives != total_actual_fraud
