"""Replay captured fixtures through the real ingestion path.

    uv run python -m xstream.ingest.replay                      # in-memory
    uv run python -m xstream.ingest.replay --brokers localhost:19092

This exists for three reasons, in order of importance:

1. It makes the pipeline verifiable without a live feed. The frames are real
   captured venue output and they travel through the same connector,
   normalizer, schema and sink that production uses -- only the transport
   differs. That is a much stronger check than a unit test with hand-written
   input, and it is the only end-to-end check available in an environment that
   cannot reach the exchanges.
2. Milestone 2 needs deterministic replay to test order book reconstruction.
3. Milestone 6 needs to replay at accelerated rates to find the breaking point.

It does *not* verify the websocket transport or the venues' live behaviour.
Those need a real connection.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

from . import CONNECTORS
from .producer import InMemorySink, build_sink
from .schema import BookDelta, Trade, now_utc, parse_decimal_json

SAMPLES = pathlib.Path(__file__).resolve().parents[3] / "docs" / "samples"


def replay_file(exchange: str, path: pathlib.Path, sink, symbols=None) -> dict[str, int]:
    """Feed one fixture file through its connector into `sink`."""
    connector = CONNECTORS[exchange](symbols or ["BTC-USD", "ETH-USD"])
    stats = {"frames": 0, "produced": 0, "ignored": 0, "errors": 0}

    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        stats["frames"] += 1
        try:
            frame = parse_decimal_json(line)
            messages = connector.normalize(frame, now_utc())
        except (ValueError, KeyError, TypeError) as exc:
            stats["errors"] += 1
            print(f"  ! frame {stats['frames']}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue

        if not messages:
            stats["ignored"] += 1
            continue
        for message in messages:
            sink.send(message)
        stats["produced"] += len(messages)

    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay captured fixtures")
    parser.add_argument("--brokers", default=None, help="produce to Redpanda if set")
    parser.add_argument("--exchange", default=None, choices=sorted(CONNECTORS))
    args = parser.parse_args(argv)

    sink = build_sink(args.brokers)
    exchanges = [args.exchange] if args.exchange else sorted(CONNECTORS)

    totals = {"frames": 0, "produced": 0, "ignored": 0, "errors": 0}
    for exchange in exchanges:
        path = SAMPLES / f"{exchange}.jsonl"
        if not path.exists():
            print(f"{exchange}: no fixture at {path}", file=sys.stderr)
            continue
        stats = replay_file(exchange, path, sink)
        print(
            f"{exchange:<12} frames={stats['frames']:<5} produced={stats['produced']:<5} "
            f"ignored={stats['ignored']:<4} errors={stats['errors']}"
        )
        for key in totals:
            totals[key] += stats[key]

    sink.flush()
    print(
        f"{'TOTAL':<12} frames={totals['frames']:<5} produced={totals['produced']:<5} "
        f"ignored={totals['ignored']:<4} errors={totals['errors']}"
    )

    if isinstance(sink, InMemorySink):
        print("\nper-topic:", json.dumps(sink.topic_counts()))
        keys = sorted({key.decode() for _, key, _ in sink.messages})
        print("partition keys:", keys)
    sink.close()

    return 1 if totals["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
