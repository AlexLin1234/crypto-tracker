-- Q2: How is order book imbalance distributed?
-- Imbalance in [-1, 1] is a headline microstructure feature for Milestone 7.
-- A distribution tightly centred on zero means little predictive content; a
-- persistent skew means either real directional pressure or a venue quirk, and
-- it matters which before anything is modelled on it.
SELECT
    exchange,
    symbol,
    count(*)                                AS windows,
    round(avg(book_imbalance), 4)           AS mean_imbalance,
    round(median(book_imbalance), 4)        AS median_imbalance,
    round(stddev_samp(book_imbalance), 4)   AS stddev_imbalance,
    round(quantile_cont(book_imbalance, 0.05), 4) AS p05,
    round(quantile_cont(book_imbalance, 0.95), 4) AS p95,
    sum(CASE WHEN book_imbalance > 0 THEN 1 ELSE 0 END)::DOUBLE / count(*) AS frac_bid_heavy
FROM book_features
WHERE book_imbalance IS NOT NULL
GROUP BY exchange, symbol
ORDER BY exchange, symbol;
