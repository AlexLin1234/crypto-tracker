"""Message sinks: where normalized messages go.

The sink is an interface with two implementations so that the entire ingestion
path can be exercised without a broker. That is not a testing convenience bolted
on afterwards -- it is what makes the pipeline verifiable in environments where
Redpanda cannot run, and it is what the fixture replay in `replay.py` uses.

Partitioning is by canonical symbol; the reasoning is in DECISIONS.md D-008.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Protocol

from .schema import BookDelta, Trade, encode

#: Topic names. Kept here rather than inline so the replay harness, the topic
#: creation script, and the runner cannot drift apart.
TOPIC_TRADES = "trades.raw"
TOPIC_ORDERBOOK = "orderbook.raw"


def topic_for(message: Trade | BookDelta) -> str:
    return TOPIC_TRADES if isinstance(message, Trade) else TOPIC_ORDERBOOK


def partition_key(message: Trade | BookDelta) -> bytes:
    """Partition by canonical symbol, deliberately *not* by (exchange, symbol).

    Kafka guarantees ordering within a partition only. Keying by symbol alone
    puts both venues' BTC-USD in the same partition, which:

    - preserves per-venue ordering, since each venue's messages retain their
      relative order within that partition, which is what Milestone 2's book
      reconstruction actually requires; and
    - co-locates the two venues' streams for the same symbol, which is exactly
      what Milestone 4's cross-exchange join consumes.

    The cost is capped parallelism: with two symbols, only two partitions ever
    receive data no matter how many exist. That is a real and deliberate skew
    (D-008), and Milestone 6 is where it gets measured rather than hand-waved.
    """
    return message.symbol.encode()


class MessageSink(Protocol):
    def send(self, message: Trade | BookDelta) -> None: ...
    def flush(self, timeout: float = 10.0) -> int: ...
    def close(self) -> None: ...


@dataclasses.dataclass
class InMemorySink:
    """Collects messages in memory. Used by tests and by fixture replay."""

    messages: list[tuple[str, bytes, bytes]] = dataclasses.field(default_factory=list)

    def send(self, message: Trade | BookDelta) -> None:
        self.messages.append((topic_for(message), partition_key(message), encode(message)))

    def flush(self, timeout: float = 10.0) -> int:
        return 0

    def close(self) -> None:
        return None

    def topic_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for topic, _, _ in self.messages:
            counts[topic] = counts.get(topic, 0) + 1
        return counts


class RedpandaSink:
    """confluent-kafka producer aimed at Redpanda.

    Delivery is at-least-once: the producer retries, and a retry after an
    ambiguous failure can duplicate. Exactly-once would need the idempotent
    producer plus transactional consumers downstream, which is not free and is
    not warranted for market data where duplicates are detectable from the
    venue's own sequence numbers and trade IDs. Stated plainly in D-009 rather
    than left for a reader to infer.
    """

    def __init__(self, brokers: str, *, on_delivery_error=None) -> None:
        from confluent_kafka import Producer

        self._delivery_errors = 0
        self._on_delivery_error = on_delivery_error
        self._producer = Producer(
            {
                "bootstrap.servers": brokers,
                # Retry on transient failures; market data is worth a few
                # hundred ms of retry rather than a hole in the stream.
                "retries": 5,
                "retry.backoff.ms": 200,
                # Small linger buys meaningful batching at these message rates
                # without adding latency that would show up in Milestone 6.
                "linger.ms": 5,
                "compression.type": "lz4",
                "enable.idempotence": False,
            }
        )

    @property
    def delivery_errors(self) -> int:
        return self._delivery_errors

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            self._delivery_errors += 1
            if self._on_delivery_error:
                self._on_delivery_error(err, msg)

    def send(self, message: Trade | BookDelta) -> None:
        from confluent_kafka import KafkaException

        try:
            self._producer.produce(
                topic=topic_for(message),
                key=partition_key(message),
                value=encode(message),
                on_delivery=self._delivery_callback,
            )
        except BufferError:
            # The local queue is full: the broker is not keeping up. Block
            # briefly to apply backpressure to the websocket reader rather than
            # dropping the message silently.
            self._producer.poll(1.0)
            self._producer.produce(
                topic=topic_for(message),
                key=partition_key(message),
                value=encode(message),
                on_delivery=self._delivery_callback,
            )
        except KafkaException:
            self._delivery_errors += 1
            raise
        # Serve delivery callbacks without blocking.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def close(self) -> None:
        self.flush()


def build_sink(brokers: str | None) -> MessageSink:
    """Return a real sink, or an in-memory one when no broker is configured."""
    if not brokers:
        return InMemorySink()
    return RedpandaSink(brokers)
