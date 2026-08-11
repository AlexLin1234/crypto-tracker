-- Q5: Is aggressor flow balanced between buys and sells?
-- Depends on the aggressor side normalized at ingestion -- Binance encodes it
-- by implication (`m` = buyer is maker), so a sign error here would show up as
-- a persistent one-sided imbalance rather than as an obvious failure.
SELECT
    exchange,
    symbol,
    count(*)                      AS windows,
    sum(trade_count)              AS trades,
    round(sum(buy_volume), 8)     AS buy_volume,
    round(sum(sell_volume), 8)    AS sell_volume,
    round(
        (sum(buy_volume) - sum(sell_volume)) / nullif(sum(volume), 0), 4
    )                             AS net_flow_imbalance,
    round(avg(volume_imbalance), 4) AS mean_window_imbalance
FROM candles_1s
GROUP BY exchange, symbol
ORDER BY exchange, symbol;
