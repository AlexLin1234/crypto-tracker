"""Replay the fixture at a multiple of real time and find where it breaks.

    uv run python -m xstream.bench.loadtest --speed 10 --speed 100 --speed 0

`--speed 0` means unthrottled: go as fast as the machine allows. That is the
run that actually finds the ceiling; 10x and 100x only demonstrate that the
pipeline keeps up comfortably below it.

**How "speed" is defined.** The fixture spans a known amount of *market* time.
Replaying at Nx means compressing that span into `span / N` seconds. The
achieved rate is measured rather than assumed, because a pipeline that cannot
keep up will silently fall behind the schedule -- and falling behind is exactly
the finding worth reporting.

The failure mode this looks for is not a crash. It is the producer's local
queue filling, which surfaces as `BufferError` and forces the caller to block.
That backpressure is the healthy behaviour: the alternative is dropping market
data silently. Counting how often it happens is how backpressure gets measured
instead of assumed.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import pathlib
import statistics
import sys
import time

from ..ingest import KrakenConnector, BinanceUSConnector
from ..ingest.schema import encode, now_utc, parse_decimal_json

SAMPLES = pathlib.Path(__file__).resolve().parents[3] / "docs" / "samples"


@dataclasses.dataclass
class LoadResult:
    speed: str
    target_rate: float
    achieved_rate: float
    messages: int
    seconds: float
    backpressure_events: int
    max_block_ms: float
    kept_up: bool
    detail: str = ""


def fixture_messages(exchange: str) -> tuple[list[tuple[bytes, bytes]], float]:
    """Normalized payloads plus the market-time span they cover, in seconds."""
    connector = {"kraken": KrakenConnector, "binance_us": BinanceUSConnector}[exchange](
        ["BTC-USD", "ETH-USD"]
    )
    ts = now_utc()
    payloads: list[tuple[bytes, bytes]] = []
    times: list[float] = []
    for line in (SAMPLES / f"{exchange}.jsonl").read_text().splitlines():
        if not line.strip():
            continue
        for message in connector.normalize(parse_decimal_json(line), ts):
            payloads.append((message.symbol.encode(), encode(message)))
            times.append(message.exchange_timestamp.timestamp())
    span = (max(times) - min(times)) if len(times) > 1 else 1.0
    return payloads, max(span, 0.001)


def run_load(
    brokers: str,
    payloads: list[tuple[bytes, bytes]],
    span_seconds: float,
    speed: float,
    topic: str,
    queue_size: int = 20000,
) -> LoadResult:
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "linger.ms": 5,
            "compression.type": "lz4",
            # Configurable so backpressure can be induced on demand. At the
            # default the broker drains faster than this process can enqueue,
            # so the queue never fills and BufferError never fires -- which is
            # itself the finding at normal settings. Shrinking it forces the
            # path to be exercised and its behaviour observed.
            "queue.buffering.max.messages": queue_size,
        }
    )

    target_rate = float("inf") if speed == 0 else len(payloads) * speed / span_seconds
    interval = 0.0 if speed == 0 else 1.0 / target_rate

    backpressure = 0
    blocks: list[float] = []
    start = time.perf_counter()
    next_send = start

    for key, value in payloads:
        if interval:
            now = time.perf_counter()
            if now < next_send:
                time.sleep(next_send - now)
            next_send += interval
        while True:
            try:
                producer.produce(topic=topic, key=key, value=value)
                break
            except BufferError:
                backpressure += 1
                block_start = time.perf_counter()
                producer.poll(0.5)
                blocks.append((time.perf_counter() - block_start) * 1000)
        producer.poll(0)

    producer.flush(120)
    seconds = time.perf_counter() - start
    achieved = len(payloads) / seconds if seconds else 0.0

    # "Kept up" means the achieved rate is within 5% of the schedule. Below
    # that the replay silently stretched, which is the failure being hunted.
    kept_up = True if speed == 0 else achieved >= target_rate * 0.95

    return LoadResult(
        speed="unthrottled" if speed == 0 else f"{speed:g}x",
        target_rate=0.0 if speed == 0 else target_rate,
        achieved_rate=achieved,
        messages=len(payloads),
        seconds=seconds,
        backpressure_events=backpressure,
        max_block_ms=max(blocks) if blocks else 0.0,
        kept_up=kept_up,
        detail="" if kept_up else "fell behind schedule",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay load test")
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="bench.load")
    parser.add_argument("--exchange", default="kraken")
    parser.add_argument(
        "--speed", action="append", type=float, default=None,
        help="replay speed multiple; 0 = unthrottled. Repeatable.",
    )
    parser.add_argument("--repeat-fixture", type=int, default=1,
                        help="concatenate the fixture N times for a longer run")
    parser.add_argument("--queue-size", type=int, default=20000,
                        help="producer queue depth; shrink to force backpressure")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    speeds = args.speed or [10.0, 100.0, 0.0]
    payloads, span = fixture_messages(args.exchange)
    payloads = payloads * args.repeat_fixture
    span = span * args.repeat_fixture

    print(
        f"fixture: {len(payloads):,} messages spanning {span:.1f}s of market time "
        f"({len(payloads)/span:,.0f} msg/s at real time)",
        file=sys.stderr,
    )

    results = []
    for speed in speeds:
        result = run_load(
            args.brokers, payloads, span, speed, args.topic, args.queue_size
        )
        results.append(result)
        print(
            f"  {result.speed:>12}  target {result.target_rate:>10,.0f}  "
            f"achieved {result.achieved_rate:>10,.0f} msg/s  "
            f"backpressure {result.backpressure_events:>4}  "
            f"{'kept up' if result.kept_up else 'FELL BEHIND'}",
            file=sys.stderr,
        )

    if args.json:
        json.dump([dataclasses.asdict(r) for r in results], sys.stdout, indent=2)
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
