-- Q1: How does the bid-ask spread vary by hour of day, per venue?
-- Spread is the most direct measure of liquidity, and it is strongly
-- time-of-day dependent in crypto despite the market never closing.
SELECT
    exchange,
    symbol,
    hour,
    count(*)                                   AS windows,
    round(avg(avg_relative_spread_bps), 4)     AS mean_spread_bps,
    round(median(avg_relative_spread_bps), 4)  AS median_spread_bps,
    round(quantile_cont(avg_relative_spread_bps, 0.95), 4) AS p95_spread_bps,
    round(max(avg_relative_spread_bps), 4)     AS max_spread_bps
FROM book_features
WHERE avg_relative_spread_bps IS NOT NULL
GROUP BY exchange, symbol, hour
ORDER BY exchange, symbol, hour;
