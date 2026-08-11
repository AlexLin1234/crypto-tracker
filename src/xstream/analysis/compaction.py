"""Compact many small streaming files into few large ones.

    uv run python -m xstream.analysis.compaction --dry-run
    uv run python -m xstream.analysis.compaction

**Why this is not optional polish.** A streaming job with a 10 second trigger
writes a new file per partition per trigger: six per minute, 360 per hour, per
(date, hour, exchange, symbol) combination. Each carries Parquet footer and
row-group overhead, and every query pays a per-file open. Columnar formats want
files in the hundreds of megabytes; the current lake averages a few kilobytes.
The gap is roughly four orders of magnitude, and it gets worse linearly with
uptime.

**The safety rule that shapes the design: only compact closed partitions.** A
streaming query may still be appending to the current hour, so rewriting it
races the writer -- and because compaction deletes its inputs, losing that race
loses data. Partitions are therefore only compacted once their hour is strictly
in the past, which is checkable from the partition path alone without asking
the writer anything.

Compaction writes to a temporary file and only deletes the originals after the
replacement is durably in place, so an interruption leaves either the original
files or both, never neither.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import pathlib
import re

import duckdb

from .lake import DEFAULT_LAKE, DATASETS

#: date=YYYY-MM-DD/hour=H somewhere in the path.
_DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")
_HOUR_RE = re.compile(r"hour=(\d{1,2})")

COMPACTED_PREFIX = "compacted-"


@dataclasses.dataclass
class CompactionResult:
    partition: pathlib.Path
    files_before: int
    files_after: int
    bytes_before: int
    bytes_after: int
    rows: int
    skipped_reason: str | None = None

    @property
    def compacted(self) -> bool:
        return self.skipped_reason is None


def partition_hour(path: pathlib.Path) -> dt.datetime | None:
    """The event-time hour a partition directory represents."""
    text = str(path)
    date_match, hour_match = _DATE_RE.search(text), _HOUR_RE.search(text)
    if not date_match or not hour_match:
        return None
    day = dt.date.fromisoformat(date_match.group(1))
    return dt.datetime(
        day.year, day.month, day.day, int(hour_match.group(1)), tzinfo=dt.timezone.utc
    )


def is_closed(path: pathlib.Path, now: dt.datetime | None = None) -> bool:
    """True when no writer can still be appending to this partition.

    Strictly-in-the-past hours only. The current hour is excluded even if it
    looks idle, because "no file has appeared recently" is not evidence that the
    stream has moved on.
    """
    hour = partition_hour(path)
    if hour is None:
        return False
    now = now or dt.datetime.now(dt.timezone.utc)
    return hour < now.replace(minute=0, second=0, microsecond=0)


def leaf_partitions(dataset_root: pathlib.Path) -> list[pathlib.Path]:
    """Directories that directly contain Parquet files."""
    if not dataset_root.exists():
        return []
    return sorted({p.parent for p in dataset_root.rglob("*.parquet")})


def compact_partition(
    partition: pathlib.Path,
    *,
    min_files: int = 2,
    dry_run: bool = False,
    now: dt.datetime | None = None,
    force: bool = False,
) -> CompactionResult:
    files = sorted(partition.glob("*.parquet"))
    before_bytes = sum(f.stat().st_size for f in files)
    result = CompactionResult(partition, len(files), len(files), before_bytes, before_bytes, 0)

    if len(files) < min_files:
        result.skipped_reason = f"only {len(files)} file(s)"
        return result
    if not force and not is_closed(partition, now):
        result.skipped_reason = "partition still open (current hour)"
        return result

    con = duckdb.connect()
    rows = con.execute(
        f"SELECT count(*) FROM read_parquet('{partition}/*.parquet')"
    ).fetchone()[0]
    result.rows = rows

    if dry_run:
        return result

    target = partition / f"{COMPACTED_PREFIX}{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}.parquet"
    temp = target.with_suffix(".parquet.tmp")

    # Read every input before writing anything, then move into place, then
    # delete. An interruption at any point leaves the originals intact or leaves
    # both copies -- never a partition with neither.
    con.execute(
        f"""
        COPY (SELECT * FROM read_parquet('{partition}/*.parquet'))
        TO '{temp}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    temp.replace(target)
    for path in files:
        path.unlink()

    result.files_after = 1
    result.bytes_after = target.stat().st_size
    return result


def compact_lake(
    lake_root: pathlib.Path = DEFAULT_LAKE,
    *,
    dry_run: bool = False,
    now: dt.datetime | None = None,
    force: bool = False,
) -> list[CompactionResult]:
    results = []
    for dataset in DATASETS:
        for partition in leaf_partitions(lake_root / dataset):
            results.append(
                compact_partition(partition, dry_run=dry_run, now=now, force=force)
            )
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compact small Parquet files")
    parser.add_argument("--lake", type=pathlib.Path, default=DEFAULT_LAKE)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="compact open partitions too. Unsafe while a stream is writing.",
    )
    args = parser.parse_args(argv)

    results = compact_lake(args.lake, dry_run=args.dry_run, force=args.force)
    done = [r for r in results if r.compacted]
    skipped = [r for r in results if not r.compacted]

    for r in done:
        rel = r.partition.relative_to(args.lake)
        print(
            f"{'would compact' if args.dry_run else 'compacted'} {rel}: "
            f"{r.files_before} -> {r.files_after} files, "
            f"{r.bytes_before:,} -> {r.bytes_after:,} bytes, {r.rows:,} rows"
        )
    for r in skipped:
        print(f"skipped {r.partition.relative_to(args.lake)}: {r.skipped_reason}")

    if done and not args.dry_run:
        before = sum(r.bytes_before for r in done)
        after = sum(r.bytes_after for r in done)
        print(
            f"\n{len(done)} partition(s): "
            f"{sum(r.files_before for r in done)} -> {sum(r.files_after for r in done)} files, "
            f"{before:,} -> {after:,} bytes "
            f"({100 * (1 - after / before):.1f}% smaller)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
