"""Shared Spark setup: session, schemas, Kafka source, Parquet sink.

Two schema decisions carry over from ingestion and are worth stating here,
because this is where they stop being obvious.

**Prices arrive as strings and are cast to `DECIMAL`, not `DOUBLE`.** Ingestion
went to some trouble to keep prices exact (D-003); parsing them here as
`DoubleType` would throw that away at the first step of the analytical layer.
Spark's `DECIMAL(38,18)` is exact and its arithmetic is well defined.

**Event time is the exchange timestamp, never the ingest timestamp.** Windowing
on ingest time would silently paper over exactly the lateness and clock skew
this project is supposed to measure.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

#: Kafka's Spark connector is not bundled with PySpark; it is resolved from
#: Maven at session start. Pinned to the running Spark version, since a mismatch
#: fails at query start with an unhelpful message.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:{version}"

PRICE_TYPE = "DECIMAL(38,18)"

TRADE_SCHEMA = StructType(
    [
        StructField("exchange", StringType()),
        StructField("symbol", StringType()),
        StructField("price", StringType()),
        StructField("qty", StringType()),
        StructField("side", StringType()),
        StructField("trade_id", StringType()),
        StructField("exchange_timestamp", StringType()),
        StructField("ingest_timestamp", StringType()),
    ]
)

#: Emitted by xstream.orderbook.snapshotter from *reconstructed* books, not
#: from raw deltas. Deriving top of book from a delta is wrong -- both venues
#: send only changed levels -- so this schema is the one Spark consumes for
#: book features. See D-017.
SNAPSHOT_SCHEMA = StructType(
    [
        StructField("exchange", StringType()),
        StructField("symbol", StringType()),
        StructField("state", StringType()),
        StructField("best_bid", StringType()),
        StructField("bid_qty", StringType()),
        StructField("best_ask", StringType()),
        StructField("ask_qty", StringType()),
        StructField("spread", StringType()),
        StructField("mid_price", StringType()),
        StructField("bid_depth", StringType()),
        StructField("ask_depth", StringType()),
        StructField("book_imbalance", StringType()),
        StructField("applied_updates", LongType()),
        StructField("gap_count", LongType()),
        StructField("checksum_failures", LongType()),
        StructField("exchange_timestamp", StringType()),
        StructField("ingest_timestamp", StringType()),
    ]
)


def build_session(app_name: str, *, shuffle_partitions: int = 8) -> SparkSession:
    """A local Spark session with the Kafka connector resolved from Maven.

    `shuffle.partitions` defaults to 200, which is absurd for a local job over
    two symbols: it produces 200 mostly-empty tasks per shuffle and dominates
    the runtime. Lowering it is the single highest-impact local tuning knob.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    version = SparkSession.getActiveSession().version if SparkSession.getActiveSession() else None
    if version is None:
        import pyspark

        version = pyspark.__version__
    builder = builder.config("spark.jars.packages", KAFKA_PACKAGE.format(version=version))
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def read_kafka(
    spark: SparkSession, topic: str, brokers: str, *, starting: str = "earliest"
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        # Without this, a topic that loses data (retention, or a recreated
        # topic) kills the stream instead of continuing from what exists.
        .option("failOnDataLoss", "false")
        .load()
    )


def with_ingest_latency(df: DataFrame) -> DataFrame:
    """Milliseconds between the venue's timestamp and ours.

    Casting the timestamps to double rather than using `unix_timestamp` is the
    whole point: `unix_timestamp` returns whole seconds, which would quantize
    every latency to a multiple of 1000 ms and make the Milestone 6 percentiles
    meaningless. The double cast keeps sub-second precision.

    Negative values are possible and are not scrubbed -- they mean the venue's
    clock is ahead of ours, which is exactly the cross-venue clock skew
    Milestone 4 has to reckon with. Hiding it here would hide it everywhere.
    """
    return df.withColumn(
        "ingest_latency_ms",
        (F.col("ingest_time").cast("double") - F.col("event_time").cast("double")) * 1000,
    )


def parse_trades(raw: DataFrame) -> DataFrame:
    """Kafka bytes -> typed trade rows with event time and ingest latency."""
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t")
    ).select("t.*")

    return parsed.select(
        F.col("exchange"),
        F.col("symbol"),
        F.col("price").cast(PRICE_TYPE).alias("price"),
        F.col("qty").cast(PRICE_TYPE).alias("qty"),
        F.col("side"),
        F.col("trade_id"),
        F.to_timestamp("exchange_timestamp").alias("event_time"),
        F.to_timestamp("ingest_timestamp").alias("ingest_time"),
    ).transform(with_ingest_latency)


def parse_snapshots(raw: DataFrame) -> DataFrame:
    """Kafka bytes -> typed rows of reconstructed top-of-book state.

    These rows come from `xstream.orderbook.snapshotter`, which applies deltas
    to real, checksum-verified books. Spark does not attempt to derive top of
    book from deltas itself -- that was tried, and it is wrong, because both
    venues send only changed levels. See D-017.
    """
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), SNAPSHOT_SCHEMA).alias("s")
    ).select("s.*")

    numeric = ["best_bid", "bid_qty", "best_ask", "ask_qty", "spread",
               "mid_price", "bid_depth", "ask_depth", "book_imbalance"]
    out = parsed.select(
        F.col("exchange"),
        F.col("symbol"),
        F.col("state"),
        *[F.col(c).cast(PRICE_TYPE).alias(c) for c in numeric],
        F.col("applied_updates"),
        F.col("gap_count"),
        F.col("checksum_failures"),
        F.to_timestamp("exchange_timestamp").alias("event_time"),
        F.to_timestamp("ingest_timestamp").alias("ingest_time"),
    )
    return out.withColumn(
        "relative_spread_bps",
        F.when(F.col("mid_price") > 0, F.col("spread") / F.col("mid_price") * 10000),
    ).transform(with_ingest_latency)


def add_partition_columns(df: DataFrame, time_col: str = "window_start") -> DataFrame:
    """date/hour partition columns, derived from event time.

    Deriving them from event time rather than processing time means a late
    event lands in the partition it belongs to, which is what makes the Parquet
    layout queryable by market time rather than by when the job happened to run.
    """
    return df.withColumn("date", F.to_date(time_col)).withColumn(
        "hour", F.hour(time_col)
    )


def write_parquet(
    df: DataFrame,
    path: str,
    checkpoint: str,
    *,
    trigger_seconds: int = 10,
    output_mode: str = "append",
    query_name: str | None = None,
):
    """Windowed aggregates -> partitioned Parquet.

    Partitioned `date/hour/exchange/symbol`: coarsest first so a query bounded
    by time prunes whole directories before touching a file, and exchange/symbol
    last because nearly every analytical query filters on them.
    """
    writer = (
        df.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("date", "hour", "exchange", "symbol")
        .outputMode(output_mode)
        .trigger(processingTime=f"{trigger_seconds} seconds")
    )
    if query_name:
        writer = writer.queryName(query_name)
    return writer.start()
