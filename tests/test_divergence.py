"""Cross-exchange divergence: cost model and join semantics.

The Spark tests here run on **synthetic** rows, deliberately and with no
apology. The captured fixture has zero cross-venue overlap (Kraken traded only
BTC-USD, Binance.US only ETH-USD, and only Kraken's books are seedable), so
real data cannot exercise a join at all. Synthetic input is the right tool for
testing join *semantics* -- pair ordering, sign conventions, dropout handling --
because those are properties of the code, not of the market.

What synthetic data cannot do is tell us anything about real divergence
magnitudes, and nothing here claims otherwise. See D-021.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from xstream.analysis.economics import (
    DEFAULT_TAKER_FEE_BPS,
    EXPLOITABILITY_CAVEATS,
    CostModel,
)

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from xstream.processing.divergence_job import (  # noqa: E402
    join_venues,
    with_cost_floor,
)


# --- cost model (no Spark) ---------------------------------------------------


def test_round_trip_cost_includes_both_fees_and_both_half_spreads() -> None:
    model = CostModel(taker_fee_bps={"a": Decimal("10"), "b": Decimal("20")})
    # 10 + 20 + 0.5 * (4 + 6) = 35
    assert model.round_trip_cost_bps("a", "b", Decimal("4"), Decimal("6")) == Decimal("35")


def test_net_edge_uses_magnitude_so_direction_does_not_matter() -> None:
    model = CostModel(taker_fee_bps={"a": Decimal("1"), "b": Decimal("1")})
    up = model.net_edge_bps(Decimal("10"), "a", "b", Decimal("0"), Decimal("0"))
    down = model.net_edge_bps(Decimal("-10"), "a", "b", Decimal("0"), Decimal("0"))
    assert up == down == Decimal("8")


def test_a_divergence_below_the_cost_floor_does_not_survive() -> None:
    model = CostModel(taker_fee_bps={"a": Decimal("26"), "b": Decimal("40")})
    assert not model.survives_costs(Decimal("5"), "a", "b", Decimal("1"), Decimal("1"))


def test_a_large_divergence_can_clear_the_floor() -> None:
    model = CostModel(taker_fee_bps={"a": Decimal("26"), "b": Decimal("40")})
    assert model.survives_costs(Decimal("100"), "a", "b", Decimal("1"), Decimal("1"))


def test_real_venue_pair_breakeven_is_large_against_observed_spreads() -> None:
    """The headline honesty number.

    Kraken + Binance.US published taker fees, with the spreads actually measured
    in Milestone 3 (0.02 and 0.14 bps), put breakeven at ~66 bps. Liquid-pair
    cross-venue divergences are typically a few bps, so this is the arithmetic
    behind "most detected divergence is not exploitable".
    """
    model = CostModel()
    breakeven = model.breakeven_divergence_bps(
        "kraken", "binance_us", Decimal("0.02"), Decimal("0.14")
    )
    assert breakeven > Decimal("60")
    assert not model.survives_costs(
        Decimal("10"), "kraken", "binance_us", Decimal("0.02"), Decimal("0.14")
    )


def test_unknown_exchange_is_an_error_not_a_silent_zero() -> None:
    """A missing fee must not quietly become free trading."""
    with pytest.raises(KeyError):
        CostModel().fee_bps("not_a_venue")


def test_caveats_are_present_for_documentation_to_reference() -> None:
    assert len(EXPLOITABILITY_CAVEATS) >= 5
    assert any("Latency" in c for c in EXPLOITABILITY_CAVEATS)


def test_default_fees_cover_the_chosen_venues() -> None:
    assert {"kraken", "binance_us"} <= set(DEFAULT_TAKER_FEE_BPS)


# --- join semantics (Spark, synthetic rows) ----------------------------------


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("xstream-divergence-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


GRID_SCHEMA = (
    "window_start timestamp, window_end timestamp, exchange string, symbol string, "
    "mid_price decimal(38,18), best_bid decimal(38,18), best_ask decimal(38,18), "
    "spread_bps decimal(38,18), snapshot_count long, last_event_time timestamp, "
    "avg_ingest_latency_ms double"
)


def w(second: int) -> dt.datetime:
    return dt.datetime(2026, 8, 2, 5, 43, second, tzinfo=dt.timezone.utc)


def grid_row(exchange, symbol, mid, second=1, spread_bps="1", last_event=None):
    return (
        w(second), w(second + 1), exchange, symbol,
        Decimal(mid), Decimal(mid) - 1, Decimal(mid) + 1, Decimal(spread_bps),
        10, last_event or w(second), 100.0,
    )


def make_grid(spark, rows):
    return spark.createDataFrame(rows, GRID_SCHEMA)


def test_each_venue_pair_appears_once_not_twice(spark) -> None:
    """Without the a.exchange < b.exchange guard this yields two mirrored rows."""
    grid = make_grid(spark, [
        grid_row("kraken", "BTC-USD", "100"),
        grid_row("binance_us", "BTC-USD", "101"),
    ])
    result = join_venues(grid, 5.0).collect()
    assert len(result) == 1
    assert (result[0]["exchange_a"], result[0]["exchange_b"]) == ("binance_us", "kraken")


def test_divergence_bps_is_relative_to_the_two_venue_midpoint(spark) -> None:
    grid = make_grid(spark, [
        grid_row("binance_us", "BTC-USD", "100"),
        grid_row("kraken", "BTC-USD", "102"),
    ])
    row = join_venues(grid, 5.0).collect()[0]
    assert row["divergence_abs"] == Decimal("2")
    # 2 / 101 * 10000 ~= 198.02 bps
    assert abs(float(row["divergence_bps"]) - 198.0198) < 0.01


def test_richer_venue_is_named_not_left_as_a_sign(spark) -> None:
    grid = make_grid(spark, [
        grid_row("binance_us", "BTC-USD", "100"),
        grid_row("kraken", "BTC-USD", "102"),
    ])
    assert join_venues(grid, 5.0).collect()[0]["richer_venue"] == "kraken"

    grid2 = make_grid(spark, [
        grid_row("binance_us", "BTC-USD", "105"),
        grid_row("kraken", "BTC-USD", "102"),
    ])
    assert join_venues(grid2, 5.0).collect()[0]["richer_venue"] == "binance_us"


def test_a_venue_dropping_out_produces_no_row(spark) -> None:
    """Inner join by design: a divergence against a missing venue is an outage."""
    grid = make_grid(spark, [grid_row("kraken", "BTC-USD", "100")])
    assert join_venues(grid, 5.0).count() == 0


def test_different_symbols_never_join(spark) -> None:
    """The captured fixture's exact situation: no overlapping symbol."""
    grid = make_grid(spark, [
        grid_row("kraken", "BTC-USD", "100"),
        grid_row("binance_us", "ETH-USD", "100"),
    ])
    assert join_venues(grid, 5.0).count() == 0


def test_different_windows_never_join(spark) -> None:
    grid = make_grid(spark, [
        grid_row("kraken", "BTC-USD", "100", second=1),
        grid_row("binance_us", "BTC-USD", "101", second=5),
    ])
    assert join_venues(grid, 5.0).count() == 0


def test_clock_skew_within_a_window_is_measured(spark) -> None:
    """The window-wider-than-skew assumption, made checkable per row."""
    grid = make_grid(spark, [
        grid_row("binance_us", "BTC-USD", "100", last_event=w(1)),
        grid_row("kraken", "BTC-USD", "100", last_event=w(1) + dt.timedelta(milliseconds=300)),
    ])
    # Tolerance is 0.01 ms, not exact equality: skew is derived by casting
    # timestamps to double, and a float64 holding ~1.8e9 epoch seconds has only
    # sub-microsecond resolution left. That is far below any real clock skew, so
    # it is a precision floor rather than a defect.
    assert abs(join_venues(grid, 5.0).collect()[0]["skew_ms"] - 300.0) < 0.01


def test_threshold_flag_tracks_magnitude_in_both_directions(spark) -> None:
    small = make_grid(spark, [
        grid_row("binance_us", "BTC-USD", "100.00"),
        grid_row("kraken", "BTC-USD", "100.01"),
    ])
    assert join_venues(small, 5.0).collect()[0]["exceeds_threshold"] is False

    big = make_grid(spark, [
        grid_row("binance_us", "BTC-USD", "100"),
        grid_row("kraken", "BTC-USD", "90"),
    ])
    row = join_venues(big, 5.0).collect()[0]
    assert row["exceeds_threshold"] is True
    assert row["divergence_magnitude_bps"] > 0


def test_cost_floor_rejects_a_flagged_but_unexploitable_divergence(spark) -> None:
    """The honesty layer doing its job: flagged, but not worth acting on."""
    grid = make_grid(spark, [
        grid_row("binance_us", "BTC-USD", "100.00", spread_bps="0.14"),
        grid_row("kraken", "BTC-USD", "100.10", spread_bps="0.02"),
    ])
    row = with_cost_floor(join_venues(grid, 5.0), DEFAULT_TAKER_FEE_BPS).collect()[0]

    assert row["exceeds_threshold"] is True, "10 bps clears the alerting threshold"
    assert row["survives_costs"] is False, "but not the 66 bps cost floor"
    assert row["net_edge_bps"] < 0


def test_cost_floor_admits_a_genuinely_large_divergence(spark) -> None:
    grid = make_grid(spark, [
        grid_row("binance_us", "BTC-USD", "100", spread_bps="0.1"),
        grid_row("kraken", "BTC-USD", "110", spread_bps="0.1"),
    ])
    row = with_cost_floor(join_venues(grid, 5.0), DEFAULT_TAKER_FEE_BPS).collect()[0]
    assert row["survives_costs"] is True
    assert row["net_edge_bps"] > 0
