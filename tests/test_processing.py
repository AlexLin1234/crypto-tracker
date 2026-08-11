"""Spark aggregation tests.

These run a local Spark session, so they are slow relative to the rest of the
suite (a few seconds of JVM startup). They are worth it: the aggregation logic
is where a silently wrong number would enter the feature store, and the
arithmetic here -- VWAP, signed volume imbalance, decimal handling -- is exactly
the kind that looks right and isn't.

The session deliberately omits the Kafka connector: none of these tests read
Kafka, and resolving the package from Maven would make them slower still.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from xstream.processing.common import with_ingest_latency  # noqa: E402
from xstream.processing.trades_job import aggregate  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("xstream-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def ts(second: int, micro: int = 0) -> dt.datetime:
    return dt.datetime(2026, 8, 2, 5, 43, second, micro, tzinfo=dt.timezone.utc)


@pytest.fixture
def trades(spark):
    """Four trades in one second, with known aggregates."""
    rows = [
        # exchange, symbol, price, qty, side, trade_id, event_time, ingest_time
        ("kraken", "BTC-USD", Decimal("100"), Decimal("2"), "buy", "1", ts(1), ts(1, 250_000)),
        ("kraken", "BTC-USD", Decimal("110"), Decimal("1"), "sell", "2", ts(1), ts(1, 250_000)),
        ("kraken", "BTC-USD", Decimal("90"), Decimal("1"), "buy", "3", ts(1), ts(1, 250_000)),
        ("kraken", "BTC-USD", Decimal("105"), Decimal("4"), "sell", "4", ts(1), ts(1, 250_000)),
    ]
    df = spark.createDataFrame(
        rows,
        "exchange string, symbol string, price decimal(38,18), qty decimal(38,18), "
        "side string, trade_id string, event_time timestamp, ingest_time timestamp",
    )
    return with_ingest_latency(df)


def one_row(df):
    collected = df.collect()
    assert len(collected) == 1, f"expected a single window, got {len(collected)}"
    return collected[0]


def test_ohlcv_is_computed_over_the_window(trades) -> None:
    row = one_row(aggregate(trades, "1 second", "0 seconds"))
    assert row["high"] == Decimal("110")
    assert row["low"] == Decimal("90")
    assert row["volume"] == Decimal("8")
    assert row["trade_count"] == 4


def test_vwap_is_volume_weighted_not_a_plain_mean(trades) -> None:
    """(100*2 + 110*1 + 90*1 + 105*4) / 8 = 820/8 = 102.5, vs mean 101.25."""
    row = one_row(aggregate(trades, "1 second", "0 seconds"))
    assert row["vwap"] == Decimal("102.5")
    assert row["vwap"] != Decimal("101.25"), "this is the unweighted mean"


def test_volume_imbalance_uses_signed_aggressor_volume(trades) -> None:
    """buys 3, sells 5 -> (3 - 5) / 8 = -0.25."""
    row = one_row(aggregate(trades, "1 second", "0 seconds"))
    assert row["buy_volume"] == Decimal("3")
    assert row["sell_volume"] == Decimal("5")
    assert row["volume_imbalance"] == Decimal("-0.25")


def test_volume_imbalance_is_bounded_by_one_sided_flow(spark) -> None:
    rows = [
        ("kraken", "BTC-USD", Decimal("100"), Decimal("2"), "buy", "1", ts(1), ts(1)),
        ("kraken", "BTC-USD", Decimal("101"), Decimal("3"), "buy", "2", ts(1), ts(1)),
    ]
    df = with_ingest_latency(
        spark.createDataFrame(
            rows,
            "exchange string, symbol string, price decimal(38,18), qty decimal(38,18), "
            "side string, trade_id string, event_time timestamp, ingest_time timestamp",
        )
    )
    assert one_row(aggregate(df, "1 second", "0 seconds"))["volume_imbalance"] == Decimal("1")


def test_windows_split_on_event_time_boundaries(trades, spark) -> None:
    extra = spark.createDataFrame(
        [("kraken", "BTC-USD", Decimal("200"), Decimal("1"), "buy", "5", ts(2), ts(2))],
        "exchange string, symbol string, price decimal(38,18), qty decimal(38,18), "
        "side string, trade_id string, event_time timestamp, ingest_time timestamp",
    )
    combined = trades.unionByName(with_ingest_latency(extra))
    result = aggregate(combined, "1 second", "0 seconds").collect()
    assert len(result) == 2, "trades one second apart belong to different windows"


def test_exchanges_and_symbols_aggregate_separately(trades, spark) -> None:
    other = spark.createDataFrame(
        [("binance_us", "ETH-USD", Decimal("50"), Decimal("1"), "buy", "9", ts(1), ts(1))],
        "exchange string, symbol string, price decimal(38,18), qty decimal(38,18), "
        "side string, trade_id string, event_time timestamp, ingest_time timestamp",
    )
    combined = trades.unionByName(with_ingest_latency(other))
    result = aggregate(combined, "1 second", "0 seconds").collect()
    assert len(result) == 2
    assert {r["exchange"] for r in result} == {"kraken", "binance_us"}


def test_prices_stay_decimal_through_aggregation(trades) -> None:
    """D-003 all the way into the analytical layer."""
    row = one_row(aggregate(trades, "1 second", "0 seconds"))
    for field in ("open", "high", "low", "close", "volume", "vwap"):
        assert isinstance(row[field], Decimal), f"{field} is {type(row[field])}"


def test_ingest_latency_keeps_sub_second_precision(trades) -> None:
    """unix_timestamp would round this to 0 or 1000; the double cast keeps 250."""
    latencies = {r["ingest_latency_ms"] for r in trades.collect()}
    assert latencies == {250.0}


def test_ingest_latency_can_be_negative(spark) -> None:
    """A venue clock ahead of ours is real signal, not something to clamp."""
    df = spark.createDataFrame(
        [(ts(5), ts(4))], "event_time timestamp, ingest_time timestamp"
    )
    assert with_ingest_latency(df).collect()[0]["ingest_latency_ms"] == -1000.0


def test_stddev_is_null_for_a_single_trade(spark) -> None:
    """Realized volatility is undefined on one observation, and says so."""
    df = with_ingest_latency(
        spark.createDataFrame(
            [("kraken", "BTC-USD", Decimal("100"), Decimal("1"), "buy", "1", ts(1), ts(1))],
            "exchange string, symbol string, price decimal(38,18), qty decimal(38,18), "
            "side string, trade_id string, event_time timestamp, ingest_time timestamp",
        )
    )
    assert one_row(aggregate(df, "1 second", "0 seconds"))["price_stddev"] is None
