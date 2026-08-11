"""Windowed order book features: spread, depth, imbalance.

    uv run python -m xstream.orderbook.snapshotter --brokers localhost:9092 &
    uv run python -m xstream.processing.book_job    --brokers localhost:9092

Milestone 3's second job. It reads *reconstructed* top-of-book snapshots from
`orderbook.snapshots` and collapses them into fixed intervals.

The first version of this job read `orderbook.raw` and derived spread and
imbalance from the delta messages directly. That was wrong: both venues send
only the levels that changed, so a delta's first bid is an arbitrary moved
level rather than the best bid. The output showed it plainly -- Binance BTC-USD
averaged a ~$216 spread against Kraken's $0.10 -- and the fix was to put the
verified `xstream.orderbook` reconstruction in front of Spark rather than to
patch the arithmetic here. See D-017.

What remains here is what Spark is genuinely good at: windowing, watermarking,
aggregation, and partitioned columnar output.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import (
    add_partition_columns,
    build_session,
    parse_snapshots,
    read_kafka,
    write_parquet,
)

DEFAULT_WATERMARK = "10 seconds"
DEFAULT_INTERVAL = "1 second"
SNAPSHOT_TOPIC = "orderbook.snapshots"


def sample_into_windows(snapshots: DataFrame, interval: str, watermark: str) -> DataFrame:
    """Collapse the snapshot stream into fixed intervals.

    `last(...)` gives the state at the end of the interval, which is the honest
    reading of "the book at time T". The averages sit alongside because a single
    end-of-window sample is noisy at 1s resolution, and Milestone 7 needs to
    know whether a feature is a point sample or a mean.

    The shuffle is here, on (window, exchange, symbol). It is the only one in
    the job.
    """
    return (
        snapshots.withWatermark("event_time", watermark)
        .groupBy(
            F.window("event_time", interval).alias("w"),
            F.col("exchange"),
            F.col("symbol"),
        )
        .agg(
            F.last("best_bid", ignorenulls=True).alias("best_bid"),
            F.last("best_ask", ignorenulls=True).alias("best_ask"),
            F.last("mid_price", ignorenulls=True).alias("mid_price"),
            F.last("spread", ignorenulls=True).alias("spread"),
            F.avg("spread").alias("avg_spread"),
            F.avg("relative_spread_bps").alias("avg_relative_spread_bps"),
            F.last("bid_depth", ignorenulls=True).alias("bid_depth"),
            F.last("ask_depth", ignorenulls=True).alias("ask_depth"),
            F.last("book_imbalance", ignorenulls=True).alias("book_imbalance"),
            F.avg("book_imbalance").alias("avg_book_imbalance"),
            F.count("*").alias("snapshot_count"),
            # Carried through so data quality is queryable alongside the
            # features rather than living only in a log somewhere.
            F.max("applied_updates").alias("applied_updates"),
            F.max("gap_count").alias("gap_count"),
            F.max("checksum_failures").alias("checksum_failures"),
            F.avg("ingest_latency_ms").alias("avg_ingest_latency_ms"),
        )
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Windowed order book features")
    parser.add_argument("--brokers", default=os.environ.get("REDPANDA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=SNAPSHOT_TOPIC)
    parser.add_argument("--output", default=os.environ.get("PARQUET_ROOT", "./data/lake"))
    parser.add_argument("--checkpoint", default=os.environ.get("CHECKPOINT_ROOT", "./checkpoints"))
    parser.add_argument("--watermark", default=DEFAULT_WATERMARK)
    parser.add_argument("--interval", default=DEFAULT_INTERVAL)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--await-seconds", type=int, default=0)
    args = parser.parse_args(argv)

    spark = build_session("xstream-book")
    snapshots = parse_snapshots(read_kafka(spark, args.topic, args.brokers))
    features = add_partition_columns(
        sample_into_windows(snapshots, args.interval, args.watermark)
    )

    query = write_parquet(
        features,
        path=f"{args.output}/book_features",
        checkpoint=f"{args.checkpoint}/book_features",
        trigger_seconds=args.trigger_seconds,
        query_name="book_features",
    )

    query.awaitTermination(args.await_seconds or None)
    if args.await_seconds:
        query.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
