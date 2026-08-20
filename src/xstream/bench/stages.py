"""Per-stage throughput measurement.

Each function measures one stage of the pipeline in isolation, so the numbers
compose into a picture of where the time actually goes rather than a single
end-to-end figure that hides the bottleneck.

Two measurement rules, both of which matter more than they look:

**The fixture is looped, not re-read.** Frames are parsed once into memory and
then replayed from there, so disk I/O and JSON-file overhead do not contaminate
a measurement that is supposed to be about the pipeline.

**Every stage reports messages/sec of its own input unit.** Parsing counts
frames; normalization counts frames in and messages out; the book counts
deltas. Comparing "messages/sec" across stages that mean different things by
"message" is the easiest way to draw a wrong conclusion from a benchmark table,
so the unit is stated per stage.

What this module deliberately does **not** measure is end-to-end latency. On
replayed fixtures that number is the age of the capture, not a network
property -- see DECISIONS.md D-024.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
import statistics
import time
from decimal import Decimal

from ..ingest import BinanceUSConnector, KrakenConnector
from ..ingest.schema import BookDelta, encode, now_utc, parse_decimal_json
from ..orderbook import BookManager

SAMPLES = pathlib.Path(__file__).resolve().parents[3] / "docs" / "samples"


@dataclasses.dataclass
class StageResult:
    stage: str
    unit: str
    items: int
    seconds: float
    repeats: int
    notes: str = ""

    @property
    def per_second(self) -> float:
        return self.items / self.seconds if self.seconds > 0 else 0.0

    @property
    def micros_each(self) -> float:
        return self.seconds / self.items * 1e6 if self.items else 0.0

    def row(self) -> list[str]:
        return [
            self.stage,
            self.unit,
            f"{self.items:,}",
            f"{self.seconds:.3f}",
            f"{self.per_second:,.0f}",
            f"{self.micros_each:.2f}",
            self.notes,
        ]


HEADERS = ["stage", "unit", "items", "seconds", "per_second", "micros_each", "notes"]


def raw_lines(exchange: str) -> list[str]:
    return [
        line
        for line in (SAMPLES / f"{exchange}.jsonl").read_text().splitlines()
        if line.strip()
    ]


def _timed(fn, repeats: int) -> tuple[float, int]:
    """Run `fn` `repeats` times, returning (best-of seconds, items).

    Best-of rather than mean: the slowest runs are dominated by scheduler noise
    and GC on a shared machine, and the fastest run is the closest available
    estimate of the work itself. Reported alongside the repeat count so the
    reader can judge.
    """
    best = None
    items = 0
    for _ in range(repeats):
        start = time.perf_counter()
        items = fn()
        elapsed = time.perf_counter() - start
        best = elapsed if best is None or elapsed < best else best
    return best or 0.0, items


def bench_json_parse_float(exchange: str, repeats: int = 5) -> StageResult:
    lines = raw_lines(exchange)

    def run() -> int:
        for line in lines:
            json.loads(line)
        return len(lines)

    seconds, items = _timed(run, repeats)
    return StageResult(
        "json parse (float)", "frames", items, seconds, repeats,
        "stdlib json.loads; loses price precision",
    )


def bench_json_parse_decimal(exchange: str, repeats: int = 5) -> StageResult:
    lines = raw_lines(exchange)

    def run() -> int:
        for line in lines:
            parse_decimal_json(line)
        return len(lines)

    seconds, items = _timed(run, repeats)
    return StageResult(
        "json parse (Decimal)", "frames", items, seconds, repeats,
        "parse_float=Decimal; required for correctness (D-003)",
    )


def bench_normalize(exchange: str, repeats: int = 5) -> StageResult:
    connector = {"kraken": KrakenConnector, "binance_us": BinanceUSConnector}[exchange](
        ["BTC-USD", "ETH-USD"]
    )
    frames = [parse_decimal_json(line) for line in raw_lines(exchange)]
    ts = now_utc()

    def run() -> int:
        produced = 0
        for frame in frames:
            produced += len(connector.normalize(frame, ts))
        return produced

    seconds, items = _timed(run, repeats)
    return StageResult(
        "normalize", "messages out", items, seconds, repeats,
        f"{len(frames)} frames in, pre-parsed",
    )


def bench_encode(exchange: str, repeats: int = 5) -> StageResult:
    connector = {"kraken": KrakenConnector, "binance_us": BinanceUSConnector}[exchange](
        ["BTC-USD", "ETH-USD"]
    )
    ts = now_utc()
    messages = []
    for line in raw_lines(exchange):
        messages.extend(connector.normalize(parse_decimal_json(line), ts))

    def run() -> int:
        for message in messages:
            encode(message)
        return len(messages)

    seconds, items = _timed(run, repeats)
    return StageResult(
        "encode (JSON bytes)", "messages", items, seconds, repeats,
        "Decimal -> string, preserves precision",
    )


def bench_book_apply(repeats: int = 5) -> StageResult:
    """Book reconstruction including CRC32 verification on every update."""
    connector = KrakenConnector(["BTC-USD", "ETH-USD"])
    ts = now_utc()
    deltas = []
    for line in raw_lines("kraken"):
        deltas.extend(
            m for m in connector.normalize(parse_decimal_json(line), ts)
            if isinstance(m, BookDelta)
        )

    def run() -> int:
        manager = BookManager()
        for delta in deltas:
            manager.apply(delta)
        return len(deltas)

    seconds, items = _timed(run, repeats)
    return StageResult(
        "book apply + checksum", "deltas", items, seconds, repeats,
        "CRC32 recomputed over top-10 on every update",
    )


def bench_book_apply_no_checksum(repeats: int = 5) -> StageResult:
    """The same work with verification disabled, to price the checksum."""
    connector = KrakenConnector(["BTC-USD", "ETH-USD"])
    ts = now_utc()
    deltas = []
    for line in raw_lines("kraken"):
        for m in connector.normalize(parse_decimal_json(line), ts):
            if isinstance(m, BookDelta):
                deltas.append(dataclasses.replace(m, checksum=None))

    def run() -> int:
        manager = BookManager()
        for delta in deltas:
            manager.apply(delta)
        return len(deltas)

    seconds, items = _timed(run, repeats)
    return StageResult(
        "book apply (no checksum)", "deltas", items, seconds, repeats,
        "same path, verification skipped",
    )


def bench_produce(brokers: str, topic: str = "bench.raw", batches: int = 5) -> StageResult:
    """Producer throughput against a real broker, including the final flush.

    The flush is inside the timing deliberately: `produce()` only enqueues, so a
    measurement that stopped before flushing would report the speed of filling a
    buffer rather than the speed of delivering messages.
    """
    from confluent_kafka import Producer

    connector = KrakenConnector(["BTC-USD", "ETH-USD"])
    ts = now_utc()
    payloads = []
    for line in raw_lines("kraken"):
        for message in connector.normalize(parse_decimal_json(line), ts):
            payloads.append((message.symbol.encode(), encode(message)))

    producer = Producer(
        {"bootstrap.servers": brokers, "linger.ms": 5, "compression.type": "lz4"}
    )

    start = time.perf_counter()
    sent = 0
    for _ in range(batches):
        for key, value in payloads:
            while True:
                try:
                    producer.produce(topic=topic, key=key, value=value)
                    break
                except BufferError:
                    # Local queue full: the broker is the constraint here, and
                    # blocking is the honest way to measure it.
                    producer.poll(0.1)
            sent += 1
        producer.poll(0)
    producer.flush(60)
    seconds = time.perf_counter() - start

    return StageResult(
        "produce to broker", "messages", sent, seconds, 1,
        f"{batches} passes over the fixture, flush included",
    )


def summarize(results: list[StageResult]) -> dict:
    rates = {r.stage: r.per_second for r in results}
    slowest = min(rates.items(), key=lambda kv: kv[1]) if rates else ("n/a", 0)
    return {"slowest_stage": slowest[0], "slowest_rate": slowest[1], "rates": rates}


def decimal_overhead(results: list[StageResult]) -> float | None:
    """How much the Decimal correctness guarantee costs, as a ratio."""
    by_stage = {r.stage: r for r in results}
    fast = by_stage.get("json parse (float)")
    exact = by_stage.get("json parse (Decimal)")
    if not fast or not exact or exact.per_second == 0:
        return None
    return fast.per_second / exact.per_second
