-- Q4: Does volatility cluster -- are high-dispersion windows followed by more?
-- Volatility clustering is one of the few genuinely robust stylized facts in
-- finance. If it does not appear at all, that is evidence the sample is too
-- short or too quiet to support any modelling, which is worth knowing before
-- Milestone 7 rather than after.
WITH seq AS (
    SELECT
        exchange, symbol, window_start,
        price_stddev,
        lag(price_stddev) OVER (
            PARTITION BY exchange, symbol ORDER BY window_start
        ) AS prev_stddev
    FROM candles_1s
    WHERE price_stddev IS NOT NULL
)
SELECT
    exchange,
    symbol,
    count(*) AS pairs,
    -- Positive autocorrelation of dispersion is the clustering signature.
    round(corr(price_stddev, prev_stddev), 4) AS lag1_autocorrelation
FROM seq
WHERE prev_stddev IS NOT NULL
GROUP BY exchange, symbol
HAVING count(*) > 2
ORDER BY exchange, symbol;
