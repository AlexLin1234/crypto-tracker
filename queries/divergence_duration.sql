-- Divergence episodes: how long each excursion lasted, and whether any of them
-- were even arguably worth acting on.
--
-- Duration is computed here rather than in the streaming job on purpose.
-- Sessionizing consecutive windows is a gaps-and-islands problem, which in
-- Structured Streaming means custom state handling that has to be right across
-- restarts and late data. In SQL over the Parquet lake it is a window function,
-- it is re-runnable, and it is trivially inspectable. Streaming does detection;
-- batch does episode assembly. See D-022.
--
--   duckdb -c ".read queries/divergence_duration.sql"

WITH flagged AS (
    SELECT
        symbol,
        exchange_a,
        exchange_b,
        window_start,
        divergence_magnitude_bps,
        net_edge_bps,
        survives_costs,
        richer_venue,
        exceeds_threshold
    FROM read_parquet('data/lake/divergence/**/*.parquet', hive_partitioning = true)
),
numbered AS (
    -- Islands: consecutive flagged windows share (row_number - dense_rank).
    SELECT
        *,
        row_number() OVER (
            PARTITION BY symbol, exchange_a, exchange_b ORDER BY window_start
        )
        - row_number() OVER (
            PARTITION BY symbol, exchange_a, exchange_b, exceeds_threshold
            ORDER BY window_start
        ) AS episode_id
    FROM flagged
),
episodes AS (
    SELECT
        symbol,
        exchange_a,
        exchange_b,
        episode_id,
        min(window_start) AS started_at,
        max(window_start) AS ended_at,
        count(*) AS windows,
        max(divergence_magnitude_bps) AS peak_bps,
        avg(divergence_magnitude_bps) AS mean_bps,
        max(net_edge_bps) AS best_net_edge_bps,
        bool_or(survives_costs) AS ever_survived_costs,
        any_value(richer_venue) AS richer_venue
    FROM numbered
    WHERE exceeds_threshold
    GROUP BY symbol, exchange_a, exchange_b, episode_id
)
SELECT
    symbol,
    exchange_a || ' vs ' || exchange_b AS venue_pair,
    started_at,
    windows AS duration_windows,
    round(peak_bps, 3) AS peak_bps,
    round(mean_bps, 3) AS mean_bps,
    round(best_net_edge_bps, 3) AS best_net_edge_bps,
    -- The column that matters. If this is false for every episode -- which is
    -- the expected outcome on liquid pairs -- then the honest summary of the
    -- whole detector is "it finds excursions, and none of them were tradeable".
    ever_survived_costs
FROM episodes
ORDER BY peak_bps DESC;
