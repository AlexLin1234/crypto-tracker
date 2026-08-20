"""Run the per-stage benchmark suite.

    uv run python -m xstream.bench --brokers localhost:9092
"""

from __future__ import annotations

import argparse
import json
import sys

from . import stages


def render(results: list[stages.StageResult]) -> None:
    rows = [r.row() for r in results]
    widths = [len(h) for h in stages.HEADERS]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    print("  ".join(h.ljust(widths[i]) for i, h in enumerate(stages.HEADERS)))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Per-stage throughput benchmark")
    parser.add_argument("--brokers", default=None, help="include the producer stage")
    parser.add_argument("--exchange", default="kraken")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    results = [
        stages.bench_json_parse_float(args.exchange, args.repeats),
        stages.bench_json_parse_decimal(args.exchange, args.repeats),
        stages.bench_normalize(args.exchange, args.repeats),
        stages.bench_encode(args.exchange, args.repeats),
        stages.bench_book_apply(args.repeats),
        stages.bench_book_apply_no_checksum(args.repeats),
    ]
    if args.brokers:
        results.append(stages.bench_produce(args.brokers))

    if args.json:
        json.dump(
            {
                "results": [r.__dict__ | {"per_second": r.per_second} for r in results],
                "summary": stages.summarize(results),
                "decimal_overhead_ratio": stages.decimal_overhead(results),
            },
            sys.stdout, indent=2,
        )
        print()
        return 0

    render(results)
    summary = stages.summarize(results)
    print(f"\nslowest stage: {summary['slowest_stage']} "
          f"({summary['slowest_rate']:,.0f}/s)")
    overhead = stages.decimal_overhead(results)
    if overhead:
        print(f"Decimal parsing costs {overhead:.2f}x versus float parsing (D-003).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
