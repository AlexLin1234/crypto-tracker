"""Benchmark harness tests.

The harness is measurement code, so the risk is not that it crashes but that it
reports a plausible wrong number. These pin the arithmetic and the definitions
-- rates, units, and the market-time span that the load test's speed multiple
is computed from.
"""

from __future__ import annotations

import pytest

from xstream.bench import stages
from xstream.bench.loadtest import fixture_messages


def test_rate_arithmetic_is_items_over_seconds() -> None:
    result = stages.StageResult("s", "frames", 1000, 0.5, 1)
    assert result.per_second == 2000.0
    assert result.micros_each == 500.0


def test_zero_duration_does_not_divide_by_zero() -> None:
    assert stages.StageResult("s", "frames", 10, 0.0, 1).per_second == 0.0


def test_zero_items_does_not_divide_by_zero() -> None:
    assert stages.StageResult("s", "frames", 0, 1.0, 1).micros_each == 0.0


def test_summarize_picks_the_slowest_stage() -> None:
    results = [
        stages.StageResult("fast", "frames", 1000, 0.1, 1),   # 10,000/s
        stages.StageResult("slow", "frames", 1000, 1.0, 1),   # 1,000/s
    ]
    summary = stages.summarize(results)
    assert summary["slowest_stage"] == "slow"
    assert summary["slowest_rate"] == pytest.approx(1000.0)


def test_decimal_overhead_is_a_ratio_of_rates() -> None:
    results = [
        stages.StageResult("json parse (float)", "frames", 1000, 0.5, 1),    # 2000/s
        stages.StageResult("json parse (Decimal)", "frames", 1000, 1.0, 1),  # 1000/s
    ]
    assert stages.decimal_overhead(results) == pytest.approx(2.0)


def test_decimal_overhead_is_none_when_stages_are_absent() -> None:
    assert stages.decimal_overhead([]) is None


def test_fixture_span_is_market_time_not_wall_time() -> None:
    """The load test's speed multiple is meaningless if this is wrong."""
    payloads, span = fixture_messages("kraken")
    assert payloads, "fixture must yield messages"
    # The Kraken capture covers a few minutes of market time, not days.
    assert 1.0 < span < 86400.0


def test_fixture_messages_are_key_value_bytes() -> None:
    payloads, _ = fixture_messages("kraken")
    key, value = payloads[0]
    assert isinstance(key, bytes) and isinstance(value, bytes)
    assert key in {b"BTC-USD", b"ETH-USD"}


@pytest.mark.parametrize("exchange", ["kraken", "binance_us"])
def test_raw_lines_are_available_for_both_venues(exchange: str) -> None:
    assert len(stages.raw_lines(exchange)) == 500


def test_cpu_stages_run_and_report_positive_rates() -> None:
    """A smoke test over the real fixture; one repeat to stay fast."""
    for result in (
        stages.bench_json_parse_decimal("kraken", repeats=1),
        stages.bench_normalize("kraken", repeats=1),
        stages.bench_encode("kraken", repeats=1),
        stages.bench_book_apply(repeats=1),
    ):
        assert result.items > 0
        assert result.per_second > 0


def test_checksum_verification_is_measurably_more_expensive() -> None:
    """The 8x figure in BENCHMARKS.md is a claim; this keeps it honest.

    Asserts only the direction and a conservative floor, not the exact ratio --
    a benchmark assertion tight enough to be precise would be flaky on a shared
    machine.
    """
    with_checksum = stages.bench_book_apply(repeats=3)
    without = stages.bench_book_apply_no_checksum(repeats=3)
    assert without.per_second > with_checksum.per_second * 2
