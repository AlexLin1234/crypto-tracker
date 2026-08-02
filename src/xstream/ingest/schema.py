"""The common internal schema every exchange is normalized into.

This module is the central design artifact of the ingestion layer. Kraken and
Binance.US disagree on essentially every representational choice (see
docs/SCHEMAS.md): envelope shape, number encoding, timestamp format, symbol
spelling, level structure, and -- most importantly -- how book integrity is
verified at all. Everything downstream of ingestion sees only the types defined
here, so those disagreements are resolved exactly once, in one place, and are
unit-tested against real captured frames.

Two decisions here are load-bearing:

1. Prices and quantities are `Decimal`, never `float`. Kraken sends prices as
   JSON numbers, so precision is lost inside `json.loads` unless it is told
   otherwise (DECISIONS.md D-003). Order books key on exact price equality, so
   a float that is one ULP off is a different level.

2. `BookDelta` carries *both* integrity mechanisms as optional fields, because
   neither venue has both and the pipeline must not pretend they are the same
   thing. Kraken verifies state with a checksum; Binance numbers messages with
   sequence IDs. Collapsing these into one field would force a lie in one
   direction or the other (D-004).
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
from decimal import Decimal
from typing import Any, Literal

Side = Literal["buy", "sell"]

# Canonical symbols. Each connector maps its venue's spelling onto these, so no
# code outside a connector ever sees "BTC/USD" or "BTCUSD".
CANONICAL_SYMBOLS = ("BTC-USD", "ETH-USD")


@dataclasses.dataclass(frozen=True, slots=True)
class Level:
    """One price level. `qty` of zero means the level was removed."""

    price: Decimal
    qty: Decimal


@dataclasses.dataclass(frozen=True, slots=True)
class Trade:
    """A single executed trade, normalized across venues.

    `side` is the *aggressor* side: which party crossed the spread. Venues
    encode this differently and one of them encodes it by implication rather
    than directly, so it is normalized here rather than downstream, where
    getting it backwards would silently invert the volume-imbalance feature.
    """

    exchange: str
    symbol: str
    price: Decimal
    qty: Decimal
    side: Side
    trade_id: str
    exchange_timestamp: dt.datetime
    ingest_timestamp: dt.datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            # Decimals are serialized as strings, deliberately. Emitting them as
            # JSON numbers would undo D-003 at the broker boundary: the consumer
            # would parse them straight back into binary floats.
            "price": str(self.price),
            "qty": str(self.qty),
            "side": self.side,
            "trade_id": self.trade_id,
            "exchange_timestamp": _iso(self.exchange_timestamp),
            "ingest_timestamp": _iso(self.ingest_timestamp),
        }


@dataclasses.dataclass(frozen=True, slots=True)
class BookDelta:
    """A snapshot of, or an incremental change to, an L2 order book.

    Snapshots and updates share a type because both venues also share a shape
    for them; `is_snapshot` distinguishes. Milestone 2 consumes this type.

    Integrity fields, at most one family of which is populated per venue:

    - `checksum`: Kraken's CRC32 over the top-N book state. Verifies the book
      is correct; says nothing about how many messages were missed.
    - `first_sequence` / `last_sequence`: Binance's `U`/`u` update IDs. The gap
      rule is `first_sequence == previous last_sequence + 1`.

    A venue with neither (Coinbase, were it ever added) can only be resynced on
    a timer, and the `None`s here make that visible rather than implicit.
    """

    exchange: str
    symbol: str
    is_snapshot: bool
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    exchange_timestamp: dt.datetime
    ingest_timestamp: dt.datetime
    checksum: int | None = None
    first_sequence: int | None = None
    last_sequence: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "is_snapshot": self.is_snapshot,
            "bids": [[str(lv.price), str(lv.qty)] for lv in self.bids],
            "asks": [[str(lv.price), str(lv.qty)] for lv in self.asks],
            "exchange_timestamp": _iso(self.exchange_timestamp),
            "ingest_timestamp": _iso(self.ingest_timestamp),
            "checksum": self.checksum,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
        }


def _iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat()


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def encode(message: Trade | BookDelta) -> bytes:
    """Serialize a normalized message for the broker."""
    return json.dumps(message.to_dict(), separators=(",", ":")).encode()


def parse_decimal_json(raw: str | bytes) -> Any:
    """Parse JSON with numbers preserved as Decimal.

    Used by every connector. Kraken needs it for correctness (D-003); the others
    send strings and are unaffected, so applying it uniformly costs nothing and
    removes a per-venue footgun.
    """
    return json.loads(raw, parse_float=Decimal, parse_int=int)
