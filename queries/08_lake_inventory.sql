-- Q8: What is actually in the lake, per partition?
-- Operational rather than analytical: it drives the compaction decision and
-- makes the small-file problem a number instead of an intuition.
SELECT
    exchange,
    symbol,
    date,
    hour,
    count(*)                    AS rows,
    min(window_start)           AS first_window,
    max(window_start)           AS last_window,
    round(epoch(max(window_start)) - epoch(min(window_start)), 1) AS span_seconds
FROM book_features
GROUP BY exchange, symbol, date, hour
ORDER BY exchange, symbol, date, hour;
