"""Cross-exchange alignment: join two venues on event time, quantify divergence.

    uv run python -m xstream.processing.divergence_job --brokers localhost:9092

Milestone 4, the differentiator. Two independent venue streams, joined on
`(symbol, event_time_window)`, with the genuinely hard parts handled explicitly
rather than assumed away.

**Different update frequencies** are handled by aligning both sides to a common
event-time grid *before* joining. Kraken and Binance emit at completely
different rates; a raw event-to-event join would either explode combinatorially
or require picking an arbitrary "nearest" match. Aggregating each venue into
fixed windows first makes the join a clean equi-join on the window key, and
makes "the price at time T" mean the same thing on both sides.

**Clock skew** is why the window cannot be arbitrarily small. Venue timestamps
come from the venues' own clocks, which are not synchronized with each other.
If skew exceeds the window width, the same market instant lands in different
windows and the join silently compares the wrong pairs. The window must
therefore be chosen larger than the expected skew, and the observed skew is
measured and emitted per row (`skew_ms`) so the assumption is checkable rather
than assumed. See D-020.

**One exchange dropping out** produces no rows, by design: this is an inner
join. An outer join would emit rows with a null on one side, and a "divergence"
computed against a missing venue is not a divergence, it is an outage. Outages
belong in the data-quality checks of Milestone 5, not in the divergence table
where they would be indistinguishable from real signal.
"""

from __future__ import annotations

import argparse
import os
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..analysis.economics import DEFAULT_TAKER_FEE_BPS
from .common import (
    add_partition_columns,
    build_session,
    parse_snapshots,
    read_kafka,
    write_parquet,
)

SNAPSHOT_TOPIC = "orderbook.snapshots"
DIVERGENCE_TOPIC = "divergence.events"

#: Must exceed expected inter-venue clock skew. See D-020.
DEFAULT_WINDOW = "1 second"
#: Wider than the per-venue jobs': a stream-stream join has to hold both sides'
#: state until it can be sure no further match will arrive.
DEFAULT_WATERMARK = "30 seconds"
#: Gross divergence, in bps, above which a row is flagged. Deliberately well
#: below the cost floor so the honesty layer has something to reject.
DEFAULT_THRESHOLD_BPS = 5.0


def per_venue_grid(snapshots: DataFrame, window: str, watermark: str) -> DataFrame:
    """Align one venue's snapshots onto a shared event-time grid."""
    return (
        snapshots.withWatermark("event_time", watermark)
        .groupBy(
            F.window("event_time", window).alias("w"),
            F.col("exchange"),
            F.col("symbol"),
        )
        .agg(
            F.last("mid_price", ignorenulls=True).alias("mid_price"),
            F.last("best_bid", ignorenulls=True).alias("best_bid"),
            F.last("best_ask", ignorenulls=True).alias("best_ask"),
            F.last("relative_spread_bps", ignorenulls=True).alias("spread_bps"),
            F.count("*").alias("snapshot_count"),
            F.max("event_time").alias("last_event_time"),
            F.avg("ingest_latency_ms").alias("avg_ingest_latency_ms"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "exchange",
            "symbol",
            "mid_price",
            "best_bid",
            "best_ask",
            "spread_bps",
            "snapshot_count",
            "last_event_time",
            "avg_ingest_latency_ms",
        )
    )


def join_venues(grid: DataFrame, threshold_bps: float) -> DataFrame:
    """Self-join the venue grid to produce one row per venue *pair* per window.

    `a.exchange < b.exchange` gives each unordered pair exactly once. Without
    it the join yields both (kraken, binance) and (binance, kraken), which would
    double-count every divergence and, worse, report it with opposite signs.
    """
    left = grid.alias("a")
    right = grid.alias("b")

    joined = left.join(
        right,
        (F.col("a.symbol") == F.col("b.symbol"))
        & (F.col("a.window_start") == F.col("b.window_start"))
        & (F.col("a.exchange") < F.col("b.exchange")),
        how="inner",
    )

    mid_a, mid_b = F.col("a.mid_price"), F.col("b.mid_price")
    # Reference is the midpoint of the two venues' mids, so the measure does not
    # arbitrarily privilege one venue as "correct".
    reference = (mid_a + mid_b) / 2

    return (
        joined.select(
            F.col("a.window_start").alias("window_start"),
            F.col("a.window_end").alias("window_end"),
            F.col("a.symbol").alias("symbol"),
            F.col("a.exchange").alias("exchange_a"),
            F.col("b.exchange").alias("exchange_b"),
            mid_a.alias("mid_a"),
            mid_b.alias("mid_b"),
            F.col("a.spread_bps").alias("spread_a_bps"),
            F.col("b.spread_bps").alias("spread_b_bps"),
            F.col("a.snapshot_count").alias("updates_a"),
            F.col("b.snapshot_count").alias("updates_b"),
            # Observed inter-venue clock skew within the window: the assumption
            # that the window is wider than the skew, made checkable.
            (
                F.col("b.last_event_time").cast("double")
                - F.col("a.last_event_time").cast("double")
            ).alias("skew_seconds"),
            reference.alias("reference_mid"),
        )
        .withColumn("divergence_abs", F.col("mid_b") - F.col("mid_a"))
        .withColumn(
            "divergence_bps",
            F.when(
                F.col("reference_mid") > 0,
                F.col("divergence_abs") / F.col("reference_mid") * 10000,
            ),
        )
        .withColumn(
            # Which venue is richer. Named rather than left as a sign so the
            # table reads without having to remember the subtraction order.
            "richer_venue",
            F.when(F.col("divergence_abs") > 0, F.col("exchange_b"))
            .when(F.col("divergence_abs") < 0, F.col("exchange_a"))
            .otherwise(F.lit(None)),
        )
        .withColumn("divergence_magnitude_bps", F.abs(F.col("divergence_bps")))
        .withColumn("skew_ms", F.col("skew_seconds") * 1000)
        .drop("skew_seconds")
        .withColumn(
            "exceeds_threshold", F.col("divergence_magnitude_bps") > F.lit(threshold_bps)
        )
    )


def with_cost_floor(divergences: DataFrame, fees: dict[str, Decimal]) -> DataFrame:
    """Attach the net-of-costs view. This is the honesty layer.

    A gross divergence is not an opportunity. Subtracting two taker fees and the
    half-spread crossed on each venue gives a cost floor; `net_edge_bps` below
    zero means the divergence is definitively not exploitable. Above zero means
    only that this crude, deliberately favourable accounting has not ruled it
    out -- latency, queue position, inventory and displayed size all subtract
    further and are not modelled here.

    The fee lookup is built as a Spark map rather than a Python dict lookup
    because `exchange_a`/`exchange_b` are column values, not constants.
    """
    fee_map = F.create_map(
        *[x for k, v in fees.items() for x in (F.lit(k), F.lit(float(v)))]
    )
    fee_a = F.coalesce(fee_map[F.col("exchange_a")], F.lit(None).cast("double"))
    fee_b = F.coalesce(fee_map[F.col("exchange_b")], F.lit(None).cast("double"))

    return (
        divergences.withColumn("fee_a_bps", fee_a)
        .withColumn("fee_b_bps", fee_b)
        .withColumn(
            "cost_floor_bps",
            F.col("fee_a_bps")
            + F.col("fee_b_bps")
            + 0.5 * (F.col("spread_a_bps") + F.col("spread_b_bps")),
        )
        .withColumn(
            "net_edge_bps", F.col("divergence_magnitude_bps") - F.col("cost_floor_bps")
        )
        .withColumn("survives_costs", F.col("net_edge_bps") > 0)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Cross-exchange divergence detection")
    parser.add_argument("--brokers", default=os.environ.get("REDPANDA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=SNAPSHOT_TOPIC)
    parser.add_argument("--output", default=os.environ.get("PARQUET_ROOT", "./data/lake"))
    parser.add_argument("--checkpoint", default=os.environ.get("CHECKPOINT_ROOT", "./checkpoints"))
    parser.add_argument("--window", default=DEFAULT_WINDOW)
    parser.add_argument("--watermark", default=DEFAULT_WATERMARK)
    parser.add_argument("--threshold-bps", type=float, default=DEFAULT_THRESHOLD_BPS)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--await-seconds", type=int, default=0)
    args = parser.parse_args(argv)

    spark = build_session("xstream-divergence")
    snapshots = parse_snapshots(read_kafka(spark, args.topic, args.brokers))
    grid = per_venue_grid(snapshots, args.window, args.watermark)
    divergences = with_cost_floor(
        join_venues(grid, args.threshold_bps), DEFAULT_TAKER_FEE_BPS
    )

    # Parquet carries every joined window, not only the flagged ones: the
    # distribution of divergence is the analytical result, and keeping only
    # exceedances would make it impossible to say how unusual an exceedance is.
    parquet_query = write_parquet(
        add_partition_columns(divergences.withColumn("exchange", F.col("exchange_a"))),
        path=f"{args.output}/divergence",
        checkpoint=f"{args.checkpoint}/divergence",
        trigger_seconds=args.trigger_seconds,
        query_name="divergence_parquet",
    )

    # The topic carries only exceedances: it is an alerting stream, and alerting
    # on every window would make it useless.
    events = divergences.filter(F.col("exceeds_threshold")).select(
        F.col("symbol").cast("string").alias("key"),
        F.to_json(F.struct("*")).alias("value"),
    )
    kafka_query = (
        events.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.brokers)
        .option("topic", DIVERGENCE_TOPIC)
        .option("checkpointLocation", f"{args.checkpoint}/divergence_events")
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("divergence_events")
        .start()
    )

    if args.await_seconds:
        import time

        deadline = time.monotonic() + args.await_seconds
        while time.monotonic() < deadline and (
            parquet_query.isActive or kafka_query.isActive
        ):
            time.sleep(1)
        parquet_query.stop()
        kafka_query.stop()
    else:
        parquet_query.awaitTermination()
        kafka_query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
