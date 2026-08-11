"""Windowed trade aggregates: OHLCV, VWAP, realized volatility, imbalance.

    uv run python -m xstream.processing.trades_job --brokers localhost:9092

Event-time windowing with an explicit watermark. The watermark policy and its
consequences are in DECISIONS.md D-015; the short version is that a watermark
is a decision about *how wrong you are willing to be*, and pretending otherwise
by leaving it out just moves the decision somewhere less visible.
"""

from __future__ import annotations

import argparse
import os
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import (
    add_partition_columns,
    build_session,
    parse_trades,
    read_kafka,
    write_parquet,
)

#: Windows to emit. Sub-second market structure is the point of the project, so
#: 1s is the finest; 1m gives something human-readable to sanity-check against.
WINDOW_SPECS = {"1s": "1 second", "10s": "10 seconds", "1m": "1 minute"}

#: How late an event may arrive and still be counted. See D-015.
DEFAULT_WATERMARK = "10 seconds"


def aggregate(trades: DataFrame, window: str, watermark: str) -> DataFrame:
    """OHLCV plus microstructure features for one window size.

    The shuffle lives here: `groupBy` on (window, exchange, symbol) forces a
    repartition by that key. It is the only shuffle in the job, and its width is
    set by `spark.sql.shuffle.partitions`.
    """
    windowed = (
        trades.withWatermark("event_time", watermark)
        .groupBy(
            F.window("event_time", window).alias("w"),
            F.col("exchange"),
            F.col("symbol"),
        )
        .agg(
            # OHLC. first/last are ordered within the window by Spark's
            # arrival order, which is why `ignoreNulls` matters more than it
            # looks: a null would otherwise silently become the open.
            F.first("price", ignorenulls=True).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price", ignorenulls=True).alias("close"),
            F.sum("qty").alias("volume"),
            F.count("*").alias("trade_count"),
            # VWAP = sum(price*qty) / sum(qty). Kept as the numerator here and
            # divided after, so the division happens once on aggregated values
            # rather than per row.
            F.sum(F.col("price") * F.col("qty")).alias("notional"),
            # Signed volume, for the buy/sell imbalance. `side` is the aggressor
            # side, normalized at ingestion -- Binance encodes it by implication
            # and getting it backwards would flip this feature's sign.
            F.sum(F.when(F.col("side") == "buy", F.col("qty")).otherwise(0)).alias("buy_volume"),
            F.sum(F.when(F.col("side") == "sell", F.col("qty")).otherwise(0)).alias("sell_volume"),
            # Realized volatility proxy: stddev of trade prices in the window.
            # Not annualized, and not a returns-based estimator -- see D-016 for
            # why this is deliberately the cruder of the two options.
            F.stddev("price").alias("price_stddev"),
            F.avg("ingest_latency_ms").alias("avg_ingest_latency_ms"),
        )
    )

    return (
        windowed.withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
        .withColumn(
            "vwap",
            F.when(F.col("volume") > 0, F.col("notional") / F.col("volume")),
        )
        .withColumn(
            # (buy - sell) / (buy + sell), in [-1, 1].
            "volume_imbalance",
            F.when(
                F.col("volume") > 0,
                (F.col("buy_volume") - F.col("sell_volume")) / F.col("volume"),
            ),
        )
        .withColumn("window_size", F.lit(window))
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Windowed trade aggregates")
    parser.add_argument("--brokers", default=os.environ.get("REDPANDA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default="trades.raw")
    parser.add_argument("--output", default=os.environ.get("PARQUET_ROOT", "./data/lake"))
    parser.add_argument("--checkpoint", default=os.environ.get("CHECKPOINT_ROOT", "./checkpoints"))
    parser.add_argument("--watermark", default=DEFAULT_WATERMARK)
    parser.add_argument("--windows", default="1s,10s,1m")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument(
        "--await-seconds",
        type=int,
        default=0,
        help="stop after N seconds instead of running forever (for testing)",
    )
    args = parser.parse_args(argv)

    spark = build_session("xstream-trades")
    trades = parse_trades(read_kafka(spark, args.topic, args.brokers))

    queries = []
    for label in [w.strip() for w in args.windows.split(",") if w.strip()]:
        window = WINDOW_SPECS[label]
        df = add_partition_columns(aggregate(trades, window, args.watermark))
        queries.append(
            write_parquet(
                df,
                path=f"{args.output}/candles_{label}",
                # One checkpoint per query, never shared: a checkpoint encodes
                # that query's offsets and state, so sharing one silently
                # corrupts both.
                checkpoint=f"{args.checkpoint}/candles_{label}",
                trigger_seconds=args.trigger_seconds,
                query_name=f"candles_{label}",
            )
        )

    if args.await_seconds:
        # One shared deadline, not one per query: awaiting each in turn would
        # make the total wait the sum of the timeouts rather than the deadline.
        deadline = time.monotonic() + args.await_seconds
        while time.monotonic() < deadline and any(q.isActive for q in queries):
            time.sleep(1)
        for query in queries:
            query.stop()
    else:
        for query in queries:
            query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
