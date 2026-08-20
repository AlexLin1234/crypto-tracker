"""Measure consumer drain rate and the lag it leaves behind.

    uv run python -m xstream.bench.consumer_lag --brokers localhost:9092

Consumer lag is the number of messages sitting between a group's committed
offset and the log end. It is the single most useful operational signal a Kafka
pipeline has: rising lag means the consumer is slower than the producer, and
the gap grows without bound until something is fixed.

This measures it directly rather than reasoning about it -- produce a burst,
drain for a fixed window, and report both the achieved drain rate and the lag
still outstanding.
"""

from __future__ import annotations

import argparse
import json
import sys
import time


def measure(brokers: str, topic: str, group: str, seconds: float) -> dict:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])

    consumed = 0
    idle_batches = 0
    start = time.perf_counter()
    last_message_at = start
    # Stop once the backlog is drained rather than burning the whole window:
    # a fixed window that includes idle time after catch-up understates the
    # drain rate, which is the number this exists to measure.
    while time.perf_counter() - start < seconds:
        batch = consumer.consume(num_messages=1000, timeout=0.5)
        received = sum(1 for m in batch if m and not m.error())
        consumed += received
        if received:
            idle_batches = 0
            last_message_at = time.perf_counter()
        else:
            idle_batches += 1
            if consumed and idle_batches >= 3:
                break
    elapsed = last_message_at - start

    # Lag per partition: log end offset minus the group's current position.
    metadata = consumer.list_topics(topic, timeout=15).topics[topic]
    partitions = [TopicPartition(topic, p) for p in metadata.partitions]
    committed = consumer.committed(partitions, timeout=15)

    total_lag = 0
    total_end = 0
    for tp in committed:
        low, high = consumer.get_watermark_offsets(tp, timeout=15, cached=False)
        total_end += high
        position = tp.offset if tp.offset and tp.offset >= 0 else low
        total_lag += max(0, high - position)
    consumer.close()

    return {
        "consumed": consumed,
        "seconds": round(elapsed, 3),
        "drain_rate_per_sec": round(consumed / elapsed, 1) if elapsed else 0.0,
        "note": "elapsed measures time to drain the backlog, excluding idle time after catch-up",
        "log_end_total": total_end,
        "remaining_lag": total_lag,
        "caught_up": total_lag == 0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Consumer drain rate and lag")
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="bench.load")
    parser.add_argument("--group", default="bench-lag")
    parser.add_argument("--seconds", type=float, default=10.0)
    args = parser.parse_args(argv)

    result = measure(args.brokers, args.topic, args.group, args.seconds)
    json.dump(result, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
