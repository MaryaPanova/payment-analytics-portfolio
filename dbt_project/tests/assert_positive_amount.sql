-- Every transaction amount must be strictly positive, in both the original
-- currency and the EUR conversion.
--
-- Zero is a failure, not just negatives: a zero-amount payment is either a
-- generator bug or an FX rate of 0 in the seed, and both would quietly drag
-- down every average and total downstream rather than announcing themselves.
--
-- Returns rows only on failure.

select
    transaction_id,
    currency,
    amount_original,
    amount_eur
from {{ ref('stg_transactions') }}
where amount_original is null
   or amount_original <= 0
   or amount_eur is null
   or amount_eur <= 0
