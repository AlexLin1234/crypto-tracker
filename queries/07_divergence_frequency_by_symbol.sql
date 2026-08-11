-- Q7: How often do venues diverge, and does any of it clear the cost floor?
-- The headline Milestone 4 question. `tradeable_windows` is the one that
-- matters: detecting divergence is easy, and the expected answer is that
-- essentially none of it survives fees and spread. See D-022.
SELECT
    symbol,
    exchange_a || ' vs ' || exchange_b AS venue_pair,
    count(*)                                        AS joined_windows,
    sum(CASE WHEN exceeds_threshold THEN 1 ELSE 0 END) AS flagged_windows,
    sum(CASE WHEN survives_costs THEN 1 ELSE 0 END) AS tradeable_windows,
    round(avg(divergence_magnitude_bps), 4)         AS mean_divergence_bps,
    round(quantile_cont(divergence_magnitude_bps, 0.99), 4) AS p99_divergence_bps,
    round(max(divergence_magnitude_bps), 4)         AS max_divergence_bps,
    round(avg(cost_floor_bps), 4)                   AS mean_cost_floor_bps,
    round(max(net_edge_bps), 4)                     AS best_net_edge_bps,
    round(avg(abs(skew_ms)), 2)                     AS mean_abs_clock_skew_ms
FROM divergence
GROUP BY symbol, exchange_a, exchange_b
ORDER BY symbol;
