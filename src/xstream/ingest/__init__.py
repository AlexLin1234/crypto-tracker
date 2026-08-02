"""Ingestion: exchange websockets in, normalized messages into Redpanda."""

from .base import Connector
from .binance_us import BinanceUSConnector
from .kraken import KrakenConnector
from .producer import (
    TOPIC_ORDERBOOK,
    TOPIC_TRADES,
    InMemorySink,
    MessageSink,
    RedpandaSink,
    build_sink,
    partition_key,
    topic_for,
)
from .schema import BookDelta, Level, Trade

__all__ = [
    "BinanceUSConnector",
    "BookDelta",
    "Connector",
    "InMemorySink",
    "KrakenConnector",
    "Level",
    "MessageSink",
    "RedpandaSink",
    "TOPIC_ORDERBOOK",
    "TOPIC_TRADES",
    "Trade",
    "build_sink",
    "partition_key",
    "topic_for",
]

CONNECTORS: dict[str, type[Connector]] = {
    KrakenConnector.name: KrakenConnector,
    BinanceUSConnector.name: BinanceUSConnector,
}
