"""Assertions about the captured sample fixtures.

These pin the Milestone 0 findings that later milestones depend on, so that the
claims in docs/SCHEMAS.md and DECISIONS.md cannot drift away from the data
without a test failing. They are about the *fixtures*, not about live venues --
a venue changing its API is a real event that these tests will not catch until
someone re-captures.
"""

from __future__ import annotations

import json
import pathlib

import pytest

SAMPLES = pathlib.Path(__file__).resolve().parents[1] / "docs" / "samples"


def load(name: str) -> list:
    lines = (SAMPLES / f"{name}.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.mark.parametrize(
    "name,expected", [("kraken", 500), ("binance_us", 500), ("coinbase", 4)]
)
def test_fixture_frame_counts(name: str, expected: int) -> None:
    assert len(load(name)) == expected


@pytest.mark.parametrize("name", ["kraken", "binance_us", "coinbase"])
def test_no_error_frames_in_fixtures(name: str) -> None:
    """A fixture containing error frames is not market data (see D-000)."""
    import sys

    sys.path.insert(0, str(SAMPLES.parents[1] / "scripts" / "recon"))
    from _common import looks_like_error

    assert [f for f in load(name) if looks_like_error(f)] == []


# --- D-004: Coinbase has no gap-detection mechanism --------------------------


def test_coinbase_l2update_has_no_sequence_or_checksum() -> None:
    """The finding that drove the exchange choice (D-004, D-005).

    If Coinbase ever adds a sequence number to l2update, this test fails and the
    exchange decision deserves revisiting -- which is the point.
    """
    updates = [f for f in load("coinbase") if f.get("type") == "l2update"]
    assert updates, "fixture must contain at least one l2update"
    for frame in updates:
        assert set(frame) == {"changes", "product_id", "time", "type"}


def test_coinbase_sequence_exists_only_on_the_trade_channel() -> None:
    trades = [f for f in load("coinbase") if f.get("type") == "last_match"]
    assert trades and all("sequence" in f for f in trades)


# --- D-004: Kraken verifies by checksum --------------------------------------


def test_kraken_book_frames_all_carry_a_checksum() -> None:
    books = [f for f in load("kraken") if f.get("channel") == "book"]
    assert books
    for frame in books:
        for entry in frame["data"]:
            assert isinstance(entry["checksum"], int)


def test_kraken_book_has_no_sequence_number() -> None:
    """Kraken verifies state, it does not number messages. Shapes the M2 design."""
    books = [f for f in load("kraken") if f.get("channel") == "book"]
    for frame in books:
        for entry in frame["data"]:
            assert "sequence" not in entry


# --- D-003: Kraken sends prices as JSON numbers ------------------------------


def test_kraken_prices_are_json_numbers_not_strings() -> None:
    """Why the connector must parse with parse_float=Decimal (D-003)."""
    snapshot = next(
        f for f in load("kraken") if f.get("channel") == "book" and f.get("type") == "snapshot"
    )
    level = snapshot["data"][0]["bids"][0]
    assert isinstance(level["price"], float)
    assert isinstance(level["qty"], float)


def test_decimal_parsing_preserves_kraken_precision() -> None:
    """Demonstrates the D-003 mitigation actually works on the real fixture."""
    from decimal import Decimal

    raw = (SAMPLES / "kraken.jsonl").read_text().splitlines()
    frame = next(
        f
        for f in (json.loads(line, parse_float=Decimal) for line in raw)
        if f.get("channel") == "book"
    )
    for entry in frame["data"]:
        for level in entry["bids"] + entry["asks"]:
            assert isinstance(level["price"], Decimal)


# --- D-004: Binance carries usable sequence numbers --------------------------


def test_binance_depth_updates_are_sequence_contiguous() -> None:
    """The gap-detection rule for Milestone 2: U == previous_u + 1.

    Verified against the real capture rather than asserted from documentation.
    """
    last: dict[str, int] = {}
    checked = 0
    for frame in load("binance_us"):
        if not frame["stream"].endswith("depth"):
            continue
        data = frame["data"]
        symbol = data["s"]
        if symbol in last:
            assert data["U"] == last[symbol] + 1, f"gap in {symbol}"
            checked += 1
        last[symbol] = data["u"]
    assert checked > 100, "fixture should exercise the rule on many updates"


def test_binance_frames_are_wrapped_in_a_combined_stream_envelope() -> None:
    for frame in load("binance_us"):
        assert set(frame) == {"stream", "data"}
