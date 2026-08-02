"""Normalization tests, driven by the real captured fixtures.

Every assertion here runs against frames the venues actually sent
(docs/samples/*.jsonl), not hand-written input. Hand-written input would only
prove the normalizer agrees with my idea of the schema, which is precisely the
thing Milestone 0 existed to stop guessing about.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
from decimal import Decimal

import pytest

from xstream.ingest import (
    TOPIC_ORDERBOOK,
    TOPIC_TRADES,
    BinanceUSConnector,
    BookDelta,
    InMemorySink,
    KrakenConnector,
    Trade,
    partition_key,
    topic_for,
)
from xstream.ingest.schema import encode, parse_decimal_json

SAMPLES = pathlib.Path(__file__).resolve().parents[1] / "docs" / "samples"
INGEST_TS = dt.datetime(2026, 8, 2, 12, 0, tzinfo=dt.timezone.utc)


def frames(name: str) -> list:
    lines = (SAMPLES / f"{name}.jsonl").read_text().splitlines()
    return [parse_decimal_json(line) for line in lines]


def normalize_all(connector, name: str) -> list:
    out = []
    for frame in frames(name):
        out.extend(connector.normalize(frame, INGEST_TS))
    return out


@pytest.fixture
def kraken() -> KrakenConnector:
    return KrakenConnector(["BTC-USD", "ETH-USD"])


@pytest.fixture
def binance() -> BinanceUSConnector:
    return BinanceUSConnector(["BTC-USD", "ETH-USD"])


# --- whole-fixture invariants ------------------------------------------------


def test_every_kraken_frame_normalizes_without_error(kraken) -> None:
    messages = normalize_all(kraken, "kraken")
    # 472 book updates + 2 snapshots + 1 trade; heartbeats, acks and the status
    # frame carry no market data and normalize to nothing.
    assert len(messages) == 475


def test_every_binance_frame_normalizes_without_error(binance) -> None:
    messages = normalize_all(binance, "binance_us")
    assert len(messages) == 500  # 493 depth + 7 trades, nothing ignorable


@pytest.mark.parametrize("name", ["kraken", "binance_us"])
def test_all_symbols_are_canonical(name, kraken, binance) -> None:
    connector = kraken if name == "kraken" else binance
    for message in normalize_all(connector, name):
        assert message.symbol in {"BTC-USD", "ETH-USD"}, message.symbol


@pytest.mark.parametrize("name", ["kraken", "binance_us"])
def test_prices_are_decimal_never_float(name, kraken, binance) -> None:
    """D-003: floats must not reach the order book, which keys on exact prices."""
    connector = kraken if name == "kraken" else binance
    for message in normalize_all(connector, name):
        if isinstance(message, Trade):
            assert isinstance(message.price, Decimal)
            assert isinstance(message.qty, Decimal)
        else:
            for level in message.bids + message.asks:
                assert isinstance(level.price, Decimal)
                assert isinstance(level.qty, Decimal)


@pytest.mark.parametrize("name", ["kraken", "binance_us"])
def test_both_timestamps_are_populated_and_utc(name, kraken, binance) -> None:
    connector = kraken if name == "kraken" else binance
    for message in normalize_all(connector, name):
        assert message.exchange_timestamp.tzinfo is not None
        assert message.ingest_timestamp == INGEST_TS
        # The latency analysis in Milestone 6 subtracts these, so a venue clock
        # far in the future would silently produce negative latencies.
        assert message.exchange_timestamp.year == 2026


def test_ignorable_kraken_frames_produce_nothing(kraken) -> None:
    ignorable = [
        {"channel": "heartbeat"},
        {"method": "subscribe", "success": True, "result": {}},
        {"channel": "status", "type": "update", "data": []},
    ]
    for frame in ignorable:
        assert kraken.normalize(frame, INGEST_TS) == []


# --- Kraken specifics --------------------------------------------------------


def test_kraken_book_carries_checksum_and_no_sequence(kraken) -> None:
    """D-004: Kraken verifies state; it does not number messages."""
    books = [m for m in normalize_all(kraken, "kraken") if isinstance(m, BookDelta)]
    assert books
    for book in books:
        assert isinstance(book.checksum, int)
        assert book.first_sequence is None
        assert book.last_sequence is None


def test_kraken_snapshot_flag_matches_frame_type(kraken) -> None:
    books = [m for m in normalize_all(kraken, "kraken") if isinstance(m, BookDelta)]
    assert sum(b.is_snapshot for b in books) == 2


def test_kraken_decimal_precision_survives_normalization(kraken) -> None:
    """A scientific-notation qty from the real capture must stay exact."""
    raw = json.loads(
        '{"channel":"book","type":"update","data":[{"symbol":"BTC/USD",'
        '"bids":[{"price":63347.8,"qty":5.1e-05}],"asks":[],'
        '"checksum":1,"timestamp":"2026-08-02T05:43:31.066741Z"}]}',
        parse_float=Decimal,
    )
    (book,) = kraken.normalize(raw, INGEST_TS)
    assert book.bids[0].qty == Decimal("0.000051")
    assert book.bids[0].price == Decimal("63347.8")


def test_kraken_symbol_mapping_round_trips(kraken) -> None:
    assert kraken.venue_symbols == ("BTC/USD", "ETH/USD")
    assert kraken.to_canonical("BTC/USD") == "BTC-USD"


def test_kraken_subscribes_by_frame(kraken) -> None:
    frames_ = kraken.subscribe_frames()
    assert [f["params"]["channel"] for f in frames_] == ["book", "trade"]


# --- Binance specifics -------------------------------------------------------


def test_binance_book_carries_sequence_and_no_checksum(binance) -> None:
    books = [m for m in normalize_all(binance, "binance_us") if isinstance(m, BookDelta)]
    assert books
    for book in books:
        assert isinstance(book.first_sequence, int)
        assert isinstance(book.last_sequence, int)
        assert book.checksum is None


def test_binance_sequences_stay_contiguous_after_normalization(binance) -> None:
    """The Milestone 2 gap rule, checked through the normalizer this time."""
    last: dict[str, int] = {}
    checked = 0
    for book in normalize_all(binance, "binance_us"):
        if not isinstance(book, BookDelta):
            continue
        if book.symbol in last:
            assert book.first_sequence == last[book.symbol] + 1
            checked += 1
        last[book.symbol] = book.last_sequence
    assert checked > 100


def test_binance_aggressor_side_is_inverted_from_maker_flag(binance) -> None:
    """`m: true` means the buyer was passive, so the trade was a sell aggression.

    Getting this backwards would silently flip the volume imbalance feature in
    Milestone 3 with nothing downstream to catch it.
    """
    base = {
        "e": "trade", "E": 1785649534887, "s": "BTCUSD", "t": 1,
        "p": "60000.00", "q": "0.5", "b": 1, "a": 2, "T": 1785649534887, "M": True,
    }
    (sell,) = binance.normalize({"stream": "btcusd@trade", "data": {**base, "m": True}}, INGEST_TS)
    (buy,) = binance.normalize({"stream": "btcusd@trade", "data": {**base, "m": False}}, INGEST_TS)
    assert sell.side == "sell"
    assert buy.side == "buy"


def test_binance_uses_trade_time_not_event_time(binance) -> None:
    data = {
        "e": "trade", "E": 1785649599999, "s": "BTCUSD", "t": 1, "p": "1", "q": "1",
        "b": 1, "a": 2, "T": 1785649534887, "m": False, "M": True,
    }
    (trade,) = binance.normalize({"stream": "btcusd@trade", "data": data}, INGEST_TS)
    assert trade.exchange_timestamp == dt.datetime.fromtimestamp(
        1785649534887 / 1000, tz=dt.timezone.utc
    )


def test_binance_subscribes_by_url_with_no_frames(binance) -> None:
    assert binance.subscribe_frames() == []
    url = binance.stream_url()
    assert "btcusd@depth" in url and "ethusd@trade" in url


def test_binance_depth_updates_are_never_snapshots(binance) -> None:
    books = [m for m in normalize_all(binance, "binance_us") if isinstance(m, BookDelta)]
    assert not any(b.is_snapshot for b in books)


# --- routing, keying, serialization -----------------------------------------


def test_messages_route_to_the_right_topics(kraken, binance) -> None:
    messages = normalize_all(kraken, "kraken") + normalize_all(binance, "binance_us")
    for message in messages:
        expected = TOPIC_TRADES if isinstance(message, Trade) else TOPIC_ORDERBOOK
        assert topic_for(message) == expected


def test_partition_key_is_the_canonical_symbol(kraken, binance) -> None:
    """D-008: both venues' BTC-USD land in the same partition, by design."""
    messages = normalize_all(kraken, "kraken") + normalize_all(binance, "binance_us")
    keys = {partition_key(m) for m in messages}
    assert keys == {b"BTC-USD", b"ETH-USD"}


def test_encoded_payload_keeps_decimals_as_strings(kraken) -> None:
    """Emitting JSON numbers would undo D-003 at the broker boundary."""
    trade = next(m for m in normalize_all(kraken, "kraken") if isinstance(m, Trade))
    payload = json.loads(encode(trade))
    assert isinstance(payload["price"], str)
    assert isinstance(payload["qty"], str)
    assert Decimal(payload["price"]) == trade.price


def test_encoded_payload_carries_both_timestamps(kraken) -> None:
    trade = next(m for m in normalize_all(kraken, "kraken") if isinstance(m, Trade))
    payload = json.loads(encode(trade))
    assert "exchange_timestamp" in payload and "ingest_timestamp" in payload


def test_sink_collects_every_message(kraken) -> None:
    sink = InMemorySink()
    messages = normalize_all(kraken, "kraken")
    for message in messages:
        sink.send(message)
    assert len(sink.messages) == len(messages)
    assert sink.topic_counts()[TOPIC_ORDERBOOK] == 474


def test_unsupported_symbol_is_rejected_early() -> None:
    with pytest.raises(ValueError, match="unsupported symbols"):
        KrakenConnector(["DOGE-USD"])
