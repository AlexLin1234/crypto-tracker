"""Order book reconstruction tests.

The centrepiece is `test_kraken_replay_verifies_every_checksum`: replaying the
whole captured session and checking the venue's own CRC32 after every update is
a far stronger statement than any hand-written assertion. If the book drifts by
one level, one price, or one quantity, the checksum stops matching immediately.

Gap detection is tested by injection rather than by waiting for a real gap:
the capture contains no discontinuities (verified in Milestone 0), so a gap has
to be manufactured to prove the detector fires.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from decimal import Decimal

import pytest

from xstream.ingest import BinanceUSConnector, KrakenConnector
from xstream.ingest.schema import BookDelta, Level, parse_decimal_json
from xstream.orderbook import ApplyOutcome, BookManager, BookState, OrderBook

SAMPLES = pathlib.Path(__file__).resolve().parents[1] / "docs" / "samples"
TS = dt.datetime(2026, 8, 2, 12, 0, tzinfo=dt.timezone.utc)


def deltas(exchange: str) -> list[BookDelta]:
    connector = {"kraken": KrakenConnector, "binance_us": BinanceUSConnector}[exchange](
        ["BTC-USD", "ETH-USD"]
    )
    out = []
    for line in (SAMPLES / f"{exchange}.jsonl").read_text().splitlines():
        for message in connector.normalize(parse_decimal_json(line), TS):
            if isinstance(message, BookDelta):
                out.append(message)
    return out


def make_delta(**kw) -> BookDelta:
    base = dict(
        exchange="binance_us",
        symbol="BTC-USD",
        is_snapshot=False,
        bids=(),
        asks=(),
        exchange_timestamp=TS,
        ingest_timestamp=TS,
    )
    base.update(kw)
    return BookDelta(**base)


def lv(price: str, qty: str) -> Level:
    return Level(price=Decimal(price), qty=Decimal(qty))


# --- the full-replay verification -------------------------------------------


def test_kraken_replay_verifies_every_checksum() -> None:
    """Snapshot + 472 diffs, with the venue's CRC32 checked after each one."""
    manager = BookManager()
    outcomes: dict[str, int] = {}
    for delta in deltas("kraken"):
        result = manager.apply(delta)
        outcomes[result.outcome.name] = outcomes.get(result.outcome.name, 0) + 1

    assert outcomes == {"SNAPSHOT_APPLIED": 2, "APPLIED": 472}
    summary = manager.summary()
    assert summary["checksum_failures"] == 0
    assert summary["gaps"] == 0
    assert summary["stale"] == 0


def test_kraken_replay_holds_book_invariants_throughout() -> None:
    """Property test: invariants must hold after every single update."""
    manager = BookManager()
    checked = 0
    for delta in deltas("kraken"):
        if not manager.apply(delta).ok:
            continue
        book = manager.book_for(delta.exchange, delta.symbol)
        bids, asks = book.bids(50), book.asks(50)

        assert not book.is_crossed(), "best bid crossed best ask"
        assert [l.price for l in bids] == sorted((l.price for l in bids), reverse=True)
        assert [l.price for l in asks] == sorted(l.price for l in asks)
        assert all(l.qty > 0 for l in (*bids, *asks)), "zero-qty level retained"
        assert len(bids) <= 10 and len(asks) <= 10, "depth not truncated"
        checked += 1

    assert checked == 474


def test_binance_real_updates_stay_sequence_contiguous() -> None:
    """Real Binance data, applied through the book's own gap detector."""
    book = OrderBook("binance_us", "BTC-USD", depth=None)
    book.apply(make_delta(is_snapshot=True, bids=(lv("1", "1"),), asks=(lv("2", "1"),),
                          last_sequence=None))
    applied = 0
    for delta in deltas("binance_us"):
        if delta.symbol != "BTC-USD":
            continue
        if book.last_sequence is None:
            book.last_sequence = delta.first_sequence - 1
        result = book.apply(delta)
        assert result.outcome is ApplyOutcome.APPLIED, result.detail
        applied += 1
    assert applied > 150
    assert book.gap_count == 0


# --- gap detection ----------------------------------------------------------


def test_injected_gap_triggers_resync() -> None:
    book = OrderBook("binance_us", "BTC-USD")
    book.apply(make_delta(is_snapshot=True, bids=(lv("100", "1"),), asks=(lv("101", "1"),),
                          last_sequence=10))

    ok = book.apply(make_delta(first_sequence=11, last_sequence=12, bids=(lv("100", "2"),)))
    assert ok.outcome is ApplyOutcome.APPLIED

    gapped = book.apply(make_delta(first_sequence=99, last_sequence=100))
    assert gapped.outcome is ApplyOutcome.GAP_DETECTED
    assert gapped.outcome.needs_resync
    assert "expected sequence 13" in gapped.detail
    assert book.state is BookState.STALE
    assert book.gap_count == 1


def test_gap_leaves_the_last_good_book_unmutated() -> None:
    """A detected gap must not half-apply the message that revealed it."""
    book = OrderBook("binance_us", "BTC-USD")
    book.apply(make_delta(is_snapshot=True, bids=(lv("100", "1"),), asks=(lv("101", "1"),),
                          last_sequence=10))

    book.apply(make_delta(first_sequence=99, last_sequence=100, bids=(lv("100", "9"),)))
    assert book.best_bid == (Decimal("100"), Decimal("1")), "gapped update was applied"


def test_stale_book_rejects_updates_until_a_snapshot() -> None:
    book = OrderBook("binance_us", "BTC-USD")
    book.apply(make_delta(is_snapshot=True, bids=(lv("100", "1"),), asks=(lv("101", "1"),),
                          last_sequence=10))
    book.apply(make_delta(first_sequence=99, last_sequence=100))
    assert book.state is BookState.STALE

    rejected = book.apply(make_delta(first_sequence=101, last_sequence=102))
    assert rejected.outcome is ApplyOutcome.REJECTED_STALE

    recovered = book.apply(
        make_delta(is_snapshot=True, bids=(lv("200", "5"),), asks=(lv("201", "5"),),
                   last_sequence=500)
    )
    assert recovered.outcome is ApplyOutcome.SNAPSHOT_APPLIED
    assert book.state is BookState.READY
    assert book.best_bid == (Decimal("200"), Decimal("5"))


def test_update_before_any_snapshot_is_rejected() -> None:
    book = OrderBook("binance_us", "BTC-USD")
    result = book.apply(make_delta(first_sequence=1, last_sequence=2, bids=(lv("100", "1"),)))
    assert result.outcome is ApplyOutcome.REJECTED_NOT_SEEDED
    assert result.outcome.needs_resync
    assert len(book) == 0


def test_checksum_mismatch_marks_the_book_stale() -> None:
    book = OrderBook("kraken", "BTC-USD", depth=10, price_precision=1)
    book.apply(make_delta(exchange="kraken", is_snapshot=True,
                          bids=(lv("100.0", "1"),), asks=(lv("101.0", "1"),)))
    result = book.apply(
        make_delta(exchange="kraken", bids=(lv("100.0", "2"),), checksum=1)
    )
    assert result.outcome is ApplyOutcome.CHECKSUM_MISMATCH
    assert book.state is BookState.STALE
    assert book.checksum_failures == 1


def test_manager_records_resync_events_but_not_unseeded_noise() -> None:
    manager = BookManager()
    manager.apply(make_delta(first_sequence=1, last_sequence=2))   # unseeded
    assert manager.resyncs == []

    manager.apply(make_delta(is_snapshot=True, bids=(lv("100", "1"),), last_sequence=10))
    manager.apply(make_delta(first_sequence=99, last_sequence=100))
    assert len(manager.resyncs) == 1
    assert manager.resyncs[0].outcome is ApplyOutcome.GAP_DETECTED


# --- level mechanics --------------------------------------------------------


def test_zero_quantity_removes_a_level() -> None:
    book = OrderBook("binance_us", "BTC-USD")
    book.apply(make_delta(is_snapshot=True,
                          bids=(lv("100", "1"), lv("99", "2")),
                          asks=(lv("101", "1"),), last_sequence=1))
    assert len(book.bids(10)) == 2

    book.apply(make_delta(first_sequence=2, last_sequence=2, bids=(lv("99", "0"),)))
    assert [l.price for l in book.bids(10)] == [Decimal("100")]


def test_removing_a_missing_level_is_not_an_error() -> None:
    book = OrderBook("binance_us", "BTC-USD")
    book.apply(make_delta(is_snapshot=True, bids=(lv("100", "1"),), last_sequence=1))
    result = book.apply(make_delta(first_sequence=2, last_sequence=2, bids=(lv("42", "0"),)))
    assert result.outcome is ApplyOutcome.APPLIED


def test_snapshot_levels_with_zero_quantity_are_dropped() -> None:
    book = OrderBook("binance_us", "BTC-USD")
    book.apply(make_delta(is_snapshot=True, bids=(lv("100", "1"), lv("99", "0")),
                          last_sequence=1))
    assert len(book.bids(10)) == 1


def test_depth_truncation_keeps_the_best_levels() -> None:
    book = OrderBook("kraken", "BTC-USD", depth=2, price_precision=1)
    book.apply(make_delta(exchange="kraken", is_snapshot=True,
                          bids=(lv("100", "1"), lv("99", "1"), lv("98", "1")),
                          asks=(lv("101", "1"), lv("102", "1"), lv("103", "1"))))
    assert [l.price for l in book.bids(10)] == [Decimal("100"), Decimal("99")]
    assert [l.price for l in book.asks(10)] == [Decimal("101"), Decimal("102")]


def test_prices_stay_decimal_through_reconstruction() -> None:
    """D-003 end to end: a float key here would silently split levels."""
    manager = BookManager()
    for delta in deltas("kraken")[:50]:
        manager.apply(delta)
    book = manager.book_for("kraken", "BTC-USD")
    for level in (*book.bids(10), *book.asks(10)):
        assert isinstance(level.price, Decimal)
        assert isinstance(level.qty, Decimal)


# --- derived reads ----------------------------------------------------------


def test_derived_metrics_on_a_known_book() -> None:
    book = OrderBook("binance_us", "BTC-USD")
    book.apply(make_delta(is_snapshot=True,
                          bids=(lv("100", "3"), lv("99", "1")),
                          asks=(lv("102", "1"), lv("103", "1")), last_sequence=1))

    assert book.best_bid == (Decimal("100"), Decimal("3"))
    assert book.best_ask == (Decimal("102"), Decimal("1"))
    assert book.spread == Decimal("2")
    assert book.mid_price == Decimal("101")
    assert book.volume_at_depth(2) == (Decimal("4"), Decimal("2"))
    # (bid - ask) / (bid + ask) == (4 - 2) / 6
    assert book.imbalance(2) == Decimal("2") / Decimal("6")


def test_reads_are_none_on_an_empty_book() -> None:
    book = OrderBook("binance_us", "BTC-USD")
    assert book.best_bid is None and book.best_ask is None
    assert book.spread is None and book.mid_price is None
    assert book.imbalance() is None


@pytest.mark.parametrize("exchange,symbol", [("kraken", "BTC-USD"), ("kraken", "ETH-USD")])
def test_replayed_books_expose_sane_market_state(exchange, symbol) -> None:
    manager = BookManager()
    for delta in deltas("kraken"):
        manager.apply(delta)
    book = manager.book_for(exchange, symbol)

    assert book.state is BookState.READY
    assert book.spread > 0
    assert -1 <= book.imbalance(10) <= 1
    bid_vol, ask_vol = book.volume_at_depth(10)
    assert bid_vol > 0 and ask_vol > 0
