"""Binance.US combined-stream connector.

Schema reference: docs/SCHEMAS.md. Every branch below is exercised by
tests/test_normalization.py against docs/samples/binance_us.jsonl.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

from .base import Connector
from .schema import BookDelta, Level, Trade


class BinanceUSConnector(Connector):
    """Binance.US rather than binance.com, which geo-blocks US IPs."""

    name = "binance_us"
    SYMBOL_MAP = {"BTC-USD": "BTCUSD", "ETH-USD": "ETHUSD"}

    def stream_url(self) -> str:
        # The subscription lives in the URL: there is no subscribe frame and no
        # ack. Stream names must be lowercase.
        streams = []
        for venue_symbol in self.venue_symbols:
            streams.append(f"{venue_symbol.lower()}@depth")
            streams.append(f"{venue_symbol.lower()}@trade")
        return "wss://stream.binance.us:9443/stream?streams=" + "/".join(streams)

    def normalize(
        self, frame: Any, ingest_timestamp: dt.datetime
    ) -> list[Trade | BookDelta]:
        if not isinstance(frame, dict) or "data" not in frame:
            return []

        data = frame["data"]
        event = data.get("e")

        if event == "depthUpdate":
            return [
                BookDelta(
                    exchange=self.name,
                    symbol=self.to_canonical(data["s"]),
                    # The @depth stream is diffs only; the REST snapshot needed
                    # to seed the book is a Milestone 2 concern.
                    is_snapshot=False,
                    bids=tuple(_levels(data.get("b", []))),
                    asks=tuple(_levels(data.get("a", []))),
                    exchange_timestamp=_ts_ms(data["E"]),
                    ingest_timestamp=ingest_timestamp,
                    # Binance numbers messages rather than checksumming state,
                    # so `checksum` stays None. See DECISIONS.md D-004.
                    first_sequence=data["U"],
                    last_sequence=data["u"],
                )
            ]

        if event == "trade":
            return [
                Trade(
                    exchange=self.name,
                    symbol=self.to_canonical(data["s"]),
                    price=_dec(data["p"]),
                    qty=_dec(data["q"]),
                    side=_aggressor_side(data["m"]),
                    trade_id=str(data["t"]),
                    # `T` is trade time, `E` is event time. Trade time is the
                    # one that belongs on the trade.
                    exchange_timestamp=_ts_ms(data["T"]),
                    ingest_timestamp=ingest_timestamp,
                )
            ]

        return []


def _aggressor_side(buyer_is_maker: bool) -> str:
    """Translate Binance's `m` flag into an aggressor side.

    `m` is "the buyer is the market maker", i.e. the buyer was resting passively
    and the *seller* crossed the spread. So `m == True` is a sell aggression.

    This inversion is the single easiest thing to get backwards in the whole
    connector, and nothing downstream would flag it: the buy/sell volume
    imbalance feature in Milestone 3 would simply carry the wrong sign forever.
    Hence a named function and a dedicated test rather than an inline `not`.
    """
    return "sell" if buyer_is_maker else "buy"


def _levels(raw: list[list[str]]) -> list[Level]:
    return [Level(price=_dec(price), qty=_dec(qty)) for price, qty in raw]


def _dec(value: Any) -> Decimal:
    # Binance sends decimal strings, so this is exact.
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _ts_ms(value: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc)
