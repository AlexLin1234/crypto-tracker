"""The shared connector interface.

The interface is shaped by a real asymmetry found during reconnaissance rather
than by guesswork: Kraken subscribes by sending frames after connecting and
acks each (channel, symbol) pair, while Binance.US encodes the subscription in
the URL path and never acks. A design that assumed "connect, then send a
subscribe message" would not fit Binance, and one that assumed "everything is
in the URL" would not fit Kraken.

So a connector answers three questions and nothing more:

- what URL do I connect to for these symbols?      `stream_url`
- what do I send once connected, if anything?      `subscribe_frames`
- what do these bytes mean?                        `normalize`

Everything else -- connecting, reconnecting with backoff, metrics, shutdown --
is shared machinery in `runner.py`, because none of it differs per venue.
`normalize` is a pure function of (frame, ingest timestamp), which is what
makes it directly testable against the captured fixtures with no network and no
broker.
"""

from __future__ import annotations

import abc
import datetime as dt
from typing import Any, Iterable

from .schema import BookDelta, Trade


class Connector(abc.ABC):
    """One exchange's dialect of the market data feed."""

    #: Short venue identifier, used as the `exchange` field and in metrics.
    name: str

    def __init__(self, symbols: Iterable[str]) -> None:
        self.symbols = tuple(symbols)
        unknown = [s for s in self.symbols if s not in self.SYMBOL_MAP]
        if unknown:
            raise ValueError(f"{self.name}: unsupported symbols {unknown}")

    #: canonical symbol -> venue symbol. Explicit rather than derived: Binance
    #: spells BTC-USD as "BTCUSD", which cannot be split back into base/quote
    #: unambiguously without a table.
    SYMBOL_MAP: dict[str, str] = {}

    @property
    def venue_symbols(self) -> tuple[str, ...]:
        return tuple(self.SYMBOL_MAP[s] for s in self.symbols)

    def to_canonical(self, venue_symbol: str) -> str:
        for canonical, venue in self.SYMBOL_MAP.items():
            if venue == venue_symbol:
                return canonical
        raise KeyError(f"{self.name}: unknown venue symbol {venue_symbol!r}")

    @abc.abstractmethod
    def stream_url(self) -> str:
        """The websocket URL to connect to."""

    def subscribe_frames(self) -> list[dict[str, Any]]:
        """Frames to send after connecting. Empty when the URL carries it."""
        return []

    @abc.abstractmethod
    def normalize(
        self, frame: Any, ingest_timestamp: dt.datetime
    ) -> list[Trade | BookDelta]:
        """Convert one decoded venue frame into normalized messages.

        Returns a list because a single frame may carry several logical
        messages (Kraken batches multiple symbols' book entries in one frame),
        and returns an empty list for frames that carry no market data at all
        -- heartbeats, subscription acks, status messages. Callers must treat
        an empty result as normal, not as an error.
        """
