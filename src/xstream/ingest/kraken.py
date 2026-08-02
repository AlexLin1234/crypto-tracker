"""Kraken WebSocket v2 connector.

Schema reference: docs/SCHEMAS.md. Every branch below is exercised by
tests/test_normalization.py against docs/samples/kraken.jsonl.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

from .base import Connector
from .schema import BookDelta, Level, Trade

#: Kraken maintains the book at this depth, and the checksum is computed over
#: the top N levels, so this value and the checksum verification in Milestone 2
#: have to agree.
BOOK_DEPTH = 10


class KrakenConnector(Connector):
    name = "kraken"
    SYMBOL_MAP = {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"}

    def stream_url(self) -> str:
        return "wss://ws.kraken.com/v2"

    def subscribe_frames(self) -> list[dict[str, Any]]:
        symbols = list(self.venue_symbols)
        return [
            {
                "method": "subscribe",
                "params": {"channel": "book", "symbol": symbols, "depth": BOOK_DEPTH},
            },
            {
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": symbols},
            },
        ]

    def normalize(
        self, frame: Any, ingest_timestamp: dt.datetime
    ) -> list[Trade | BookDelta]:
        if not isinstance(frame, dict):
            return []

        channel = frame.get("channel")
        if channel == "book":
            return self._book(frame, ingest_timestamp)
        if channel == "trade":
            return self._trades(frame, ingest_timestamp)

        # heartbeat, status, and subscribe acks carry no market data.
        return []

    def _book(
        self, frame: dict[str, Any], ingest_timestamp: dt.datetime
    ) -> list[Trade | BookDelta]:
        is_snapshot = frame.get("type") == "snapshot"
        out: list[Trade | BookDelta] = []
        for entry in frame.get("data", []):
            out.append(
                BookDelta(
                    exchange=self.name,
                    symbol=self.to_canonical(entry["symbol"]),
                    is_snapshot=is_snapshot,
                    bids=tuple(_levels(entry.get("bids", []))),
                    asks=tuple(_levels(entry.get("asks", []))),
                    exchange_timestamp=_ts(entry["timestamp"]),
                    ingest_timestamp=ingest_timestamp,
                    # Kraken verifies state rather than numbering messages, so
                    # the sequence fields stay None. See DECISIONS.md D-004.
                    checksum=entry.get("checksum"),
                )
            )
        return out

    def _trades(
        self, frame: dict[str, Any], ingest_timestamp: dt.datetime
    ) -> list[Trade | BookDelta]:
        out: list[Trade | BookDelta] = []
        for entry in frame.get("data", []):
            out.append(
                Trade(
                    exchange=self.name,
                    symbol=self.to_canonical(entry["symbol"]),
                    price=_dec(entry["price"]),
                    qty=_dec(entry["qty"]),
                    # Kraken states the aggressor side directly.
                    side=entry["side"],
                    trade_id=str(entry["trade_id"]),
                    exchange_timestamp=_ts(entry["timestamp"]),
                    ingest_timestamp=ingest_timestamp,
                )
            )
        return out


def _levels(raw: list[dict[str, Any]]) -> list[Level]:
    # Kraken sends levels as objects, unlike the [price, size] pairs the other
    # venues use.
    return [Level(price=_dec(lv["price"]), qty=_dec(lv["qty"])) for lv in raw]


def _dec(value: Any) -> Decimal:
    """Coerce to Decimal without ever routing through float.

    When the frame was parsed with `parse_decimal_json`, Kraken's JSON numbers
    already arrive as Decimal and this is a no-op. The `str()` fallback exists
    for the case where a caller parsed with plain `json.loads`; converting via
    `str` keeps the shortest repr rather than the full binary expansion, which
    is the least-wrong option once precision has already been lost.
    """
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _ts(value: str) -> dt.datetime:
    # Kraken timestamps are RFC 3339 with a trailing Z, which fromisoformat
    # only accepts natively from Python 3.11 onward.
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
