-- Q3: End-to-end ingest latency, venue timestamp to ours.
-- Feeds the Milestone 6 benchmark table. Percentiles rather than a mean because
-- latency distributions are right-skewed: the mean hides the tail that actually
-- causes late events and watermark drops.
--
-- IMPORTANT: this measure is only meaningful for data ingested LIVE. When a
-- captured fixture is replayed, the ingest timestamp is "now" while the event
-- timestamp is whenever the capture happened, so the "latency" is really the
-- age of the fixture -- days, not milliseconds. The `measurement_valid` column
-- makes that explicit rather than letting an absurd number be read as a real
-- one. See D-024.
SELECT
    exchange,
    symbol,
    count(*)                                             AS windows,
    round(avg(avg_ingest_latency_ms), 2)                 AS mean_ms,
    round(quantile_cont(avg_ingest_latency_ms, 0.50), 2) AS p50_ms,
    round(quantile_cont(avg_ingest_latency_ms, 0.95), 2) AS p95_ms,
    round(quantile_cont(avg_ingest_latency_ms, 0.99), 2) AS p99_ms,
    round(max(avg_ingest_latency_ms), 2)                 AS max_ms,
    -- Negative latency is not a bug: it means the venue clock leads ours.
    sum(CASE WHEN avg_ingest_latency_ms < 0 THEN 1 ELSE 0 END) AS negative_latency_windows,
    -- Anything past a minute is not network latency, it is replayed data.
    CASE
        WHEN max(avg_ingest_latency_ms) > 60000
            THEN 'NO - replayed fixture, not live ingest'
        ELSE 'yes'
    END                                                  AS measurement_valid
FROM book_features
WHERE avg_ingest_latency_ms IS NOT NULL
GROUP BY exchange, symbol
ORDER BY exchange, symbol;
