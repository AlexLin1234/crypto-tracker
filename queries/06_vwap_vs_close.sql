-- Q6: How far does VWAP sit from the closing price of the same window?
-- A sanity check on the aggregation as much as an analytical result: VWAP must
-- lie within the window's high-low range, and a systematic gap between VWAP and
-- close indicates volume concentrated at one end of the window.
SELECT
    exchange,
    symbol,
    count(*) AS windows,
    round(avg(vwap - close), 8)                       AS mean_vwap_minus_close,
    round(avg(abs(vwap - close) / nullif(close, 0)) * 10000, 4) AS mean_abs_gap_bps,
    -- Must be zero. VWAP outside [low, high] is arithmetically impossible.
    sum(CASE WHEN vwap > high OR vwap < low THEN 1 ELSE 0 END) AS impossible_vwap_rows
FROM candles_1s
WHERE vwap IS NOT NULL
GROUP BY exchange, symbol
ORDER BY exchange, symbol;
