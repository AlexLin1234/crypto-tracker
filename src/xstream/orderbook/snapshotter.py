"""Reconstruct books from the raw delta stream and emit periodic snapshots.

    uv run python -m xstream.orderbook.snapshotter --brokers localhost:9092

This exists because of a mistake worth recording. The first version of the
Milestone 3 book job computed spread and imbalance directly from the raw delta
messages in Spark, on the assumption that each message carried the top of book.
It does not: **both venues send only the levels that changed.** A diff's first
bid is whatever level happened to move, not the best bid.

The output made that obvious rather than subtle -- Binance BTC-USD showed an
average spread of ~$216 (35 bps) against Kraken's $0.10 (0.02 bps). A plausible
but wrong number would have been far worse; this one was absurd enough to
investigate.

Top of book is only knowable from *reconstructed* state, which is stateful and
already exists, verified against 474 real checksums, in `xstream.orderbook`.
So this component sits between the raw topic and Spark: consume deltas, apply
them to real `OrderBook` instances, and publish the derived top-of-book state
on a fixed interval. Spark then aggregates rows that are already correct.

Keeping reconstruction here rather than in Spark is deliberate (D-017): it is
inherently sequential per (exchange, symbol), it needs the checksum
verification that only the Python implementation has, and moving it into Spark
would mean either a stateful reimplementation or shipping book state through a
shuffle.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import sys
import time
from decimal import Decimal

from ..ingest.producer import TOPIC_ORDERBOOK
from ..ingest.schema import BookDelta, Level, parse_decimal_json
from .book import BookState
from .manager import BookManager

TOPIC_SNAPSHOTS = "orderbook.snapshots"
DEFAULT_INTERVAL_MS = 250
DEPTH = 10


def _delta_from_payload(payload: dict) -> BookDelta:
    """Rebuild a BookDelta from its wire form.

    Prices come back as strings, deliberately -- they were serialized that way
    so the broker boundary could not quietly turn them into floats (D-003).
    """
    return BookDelta(
        exchange=payload["exchange"],
        symbol=payload["symbol"],
        is_snapshot=payload["is_snapshot"],
        bids=tuple(Level(Decimal(p), Decimal(q)) for p, q in payload["bids"]),
        asks=tuple(Level(Decimal(p), Decimal(q)) for p, q in payload["asks"]),
        exchange_timestamp=dt.datetime.fromisoformat(payload["exchange_timestamp"]),
        ingest_timestamp=dt.datetime.fromisoformat(payload["ingest_timestamp"]),
        checksum=payload.get("checksum"),
        first_sequence=payload.get("first_sequence"),
        last_sequence=payload.get("last_sequence"),
    )


def snapshot_payload(book, event_time: dt.datetime, ingest_time: dt.datetime) -> dict:
    """Top-of-book state plus depth features, as a flat row for Spark."""
    bid, ask = book.best_bid, book.best_ask
    bid_depth, ask_depth = book.volume_at_depth(DEPTH)
    imbalance = book.imbalance(DEPTH)
    return {
        "exchange": book.exchange,
        "symbol": book.symbol,
        "state": book.state.value,
        "best_bid": str(bid[0]) if bid else None,
        "bid_qty": str(bid[1]) if bid else None,
        "best_ask": str(ask[0]) if ask else None,
        "ask_qty": str(ask[1]) if ask else None,
        "spread": str(book.spread) if book.spread is not None else None,
        "mid_price": str(book.mid_price) if book.mid_price is not None else None,
        "bid_depth": str(bid_depth),
        "ask_depth": str(ask_depth),
        "book_imbalance": str(imbalance) if imbalance is not None else None,
        "applied_updates": book.applied_updates,
        "gap_count": book.gap_count,
        "checksum_failures": book.checksum_failures,
        "exchange_timestamp": event_time.isoformat(),
        "ingest_timestamp": ingest_time.isoformat(),
    }


def run(
    brokers: str,
    *,
    interval_ms: int = DEFAULT_INTERVAL_MS,
    max_seconds: float | None = None,
    source_topic: str = TOPIC_ORDERBOOK,
    sink_topic: str = TOPIC_SNAPSHOTS,
    group_id: str = "xstream-snapshotter",
) -> dict:
    from confluent_kafka import Consumer, Producer

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([source_topic])
    producer = Producer({"bootstrap.servers": brokers, "linger.ms": 5})

    manager = BookManager()
    stats = {"consumed": 0, "applied": 0, "rejected": 0, "snapshots": 0}
    stop = {"now": False}

    def _stop(*_):
        stop["now"] = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _stop)

    deadline = time.monotonic() + max_seconds if max_seconds else None
    idle_polls = 0
    interval = dt.timedelta(milliseconds=interval_ms)
    #: Last emitted *event* time per book, not wall-clock time.
    last_emitted: dict[tuple[str, str], dt.datetime] = {}

    try:
        while not stop["now"]:
            if deadline and time.monotonic() > deadline:
                break

            message = consumer.poll(0.2)
            if message is None or message.error():
                idle_polls += 1
                if idle_polls > 50:
                    break  # source drained
                continue

            idle_polls = 0
            stats["consumed"] += 1
            delta = _delta_from_payload(parse_decimal_json(message.value()))
            result = manager.apply(delta)
            stats["applied" if result.ok else "rejected"] += 1

            # Sample on event time, not wall-clock. Wall-clock sampling makes a
            # replay produce a different series than a live run: 40 seconds of
            # market data is consumed in about two seconds, so a 250 ms
            # wall-clock timer fires a handful of times and then repeats a
            # frozen book forever. Event-time sampling yields one row per 250 ms
            # of *market* time either way, which is both what the feature series
            # should mean and what makes replay deterministic.
            book = manager.book_for(delta.exchange, delta.symbol)
            if book.state is not BookState.READY or book.last_update_time is None:
                continue
            key = (book.exchange, book.symbol)
            previous = last_emitted.get(key)
            if previous is None or book.last_update_time - previous >= interval:
                last_emitted[key] = book.last_update_time
                stats["snapshots"] += _emit_one(producer, book, sink_topic)
    finally:
        producer.flush(10)
        consumer.close()

    stats["books"] = len(manager.books)
    stats["manager"] = manager.summary()
    return stats


def _emit_one(producer, book, topic: str) -> int:
    """Publish one snapshot row for a book in a trustworthy state.

    Callers only reach here for READY books. STALE and EMPTY books are never
    published, rather than published with null prices: emitting them would put
    rows into the feature store that look like data but describe a book the
    pipeline has explicitly said it cannot vouch for (D-013).
    """
    payload = snapshot_payload(book, book.last_update_time, dt.datetime.now(dt.timezone.utc))
    producer.produce(
        topic=topic,
        key=book.symbol.encode(),
        value=json.dumps(payload, separators=(",", ":")).encode(),
    )
    producer.poll(0)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Order book snapshotter")
    parser.add_argument("--brokers", default=os.environ.get("REDPANDA_BROKERS", "localhost:9092"))
    parser.add_argument("--interval-ms", type=int, default=DEFAULT_INTERVAL_MS)
    parser.add_argument("--max-seconds", type=float, default=None)
    parser.add_argument("--group-id", default="xstream-snapshotter")
    args = parser.parse_args(argv)

    stats = run(
        args.brokers,
        interval_ms=args.interval_ms,
        max_seconds=args.max_seconds,
        group_id=args.group_id,
    )
    json.dump(stats, sys.stdout, indent=2, default=str)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
