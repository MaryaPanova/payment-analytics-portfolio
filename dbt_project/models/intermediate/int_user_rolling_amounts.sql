-- Rolling per-user amount baseline, at transaction grain.
--
-- The window is TRAILING and excludes the current row:
--
--     range between N preceding and 1 preceding
--
-- Both halves of that matter.
--
--   * Trailing, because the earlier version averaged over the user's *entire*
--     history — including transactions that happened after the one being
--     scored. That is lookahead bias. A system scoring a payment at 10:00
--     cannot consult 14:00's data, so any accuracy it bought was fictional.
--
--   * Excluding the current row, because a large fraudulent amount would
--     otherwise pull up the very average it is being compared against,
--     shrinking its own z-score.
--
-- Note this is RANGE, not ROWS: the bounds are seconds, not row counts. "1
-- preceding" therefore means "up to 1 second before this transaction", which
-- also drops anything sharing the same second — concurrent activity, not
-- history, so that is the behaviour we want.

select
    transaction_id,
    user_id,
    transacted_at,
    amount_eur,

    avg(amount_eur)    over user_trailing as user_rolling_avg_amount_eur,
    stddev(amount_eur) over user_trailing as user_rolling_stddev_amount_eur,

    -- How much history the window actually saw. A baseline built on three
    -- transactions is not a baseline; downstream uses this to decline to
    -- judge rather than emit a confident number from nothing.
    count(*)           over user_trailing as user_rolling_txn_count

from {{ ref('stg_transactions') }}

window user_trailing as (
    partition by user_id
    order by unix_seconds(transacted_at)
    range between {{ var('user_rolling_window_seconds') }} preceding and 1 preceding
)
