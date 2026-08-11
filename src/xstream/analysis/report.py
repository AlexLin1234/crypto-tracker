"""One command that summarizes everything in the lake.

    uv run python -m xstream.analysis.report

Milestone 5's done-criterion. It prints, in order: what exists on disk, the
data quality checks, and the output of every query in `queries/`.

The design rule throughout is that **absent data is reported, not hidden and
not fatal**. Several datasets can legitimately be empty -- Milestone 4's
divergence join produces nothing without two venues (D-021) -- and a report
that crashed on the first missing dataset would be useless exactly when it is
most needed. Equally, a report that silently omitted them would let a reader
assume coverage that does not exist. Empty is a finding.
"""

from __future__ import annotations

import argparse
import pathlib

from .lake import DEFAULT_LAKE, connect, discover, run_query_file
from .quality import run_all

QUERY_DIR = pathlib.Path("queries")
RULE = "=" * 78


def _heading(title: str) -> None:
    print(f"\n{RULE}\n{title}\n{RULE}")


def _table(rows: list, headers: list[str], limit: int = 25) -> None:
    if not rows:
        print("  (no rows)")
        return
    widths = [len(h) for h in headers]
    shown = rows[:limit]
    rendered = [[("" if v is None else str(v)) for v in row] for row in shown]
    for row in rendered:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(cell))
    print("  " + "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    print("  " + "  ".join("-" * w for w in widths))
    for row in rendered:
        print("  " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row[: len(widths)])))
    if len(rows) > limit:
        print(f"  ... {len(rows) - limit} more row(s)")


def inventory(lake_root: pathlib.Path) -> int:
    _heading("LAKE INVENTORY")
    datasets = discover(lake_root)
    rows = [
        [
            name,
            "yes" if info.present else "NO",
            info.files,
            f"{info.bytes:,}",
            f"{info.avg_file_bytes:,.0f}",
        ]
        for name, info in datasets.items()
    ]
    _table(rows, ["dataset", "present", "files", "bytes", "avg_file_bytes"])

    present = [i for i in datasets.values() if i.present]
    total_files = sum(i.files for i in present)
    total_bytes = sum(i.bytes for i in present)
    if total_files:
        avg = total_bytes / total_files
        print(f"\n  {total_files} file(s), {total_bytes:,} bytes, {avg:,.0f} bytes/file average.")
        if avg < 1_000_000:
            print(
                "  Small-file problem: columnar formats want files in the hundreds of MB.\n"
                "  Run `python -m xstream.analysis.compaction` on closed partitions."
            )
    missing = [n for n, i in datasets.items() if not i.present]
    if missing:
        print(f"\n  Absent datasets: {', '.join(missing)}")
        print("  Absent is a finding, not an error -- see DECISIONS.md D-021.")
    return total_files


def quality(lake_root: pathlib.Path) -> list:
    _heading("DATA QUALITY")
    con = connect(lake_root)
    results = run_all(con, lake_root)
    for result in results:
        print(f"  {result}")
    warnings = [r for r in results if not r.passed]
    print(f"\n  {len(results) - len(warnings)} passed, {len(warnings)} warning(s).")
    return results


def queries(lake_root: pathlib.Path, query_dir: pathlib.Path) -> None:
    _heading("ANALYTICAL QUERIES")
    con = connect(lake_root)
    files = sorted(p for p in query_dir.glob("*.sql"))
    if not files:
        print(f"  (no .sql files in {query_dir})")
        return

    for path in files:
        print(f"\n-- {path.name} " + "-" * max(0, 60 - len(path.name)))
        first_line = path.read_text().splitlines()[0].lstrip("- ").strip()
        print(f"   {first_line}")
        ok, message, rows = run_query_file(con, path)
        if not ok:
            print(f"   {message}")
            continue
        headers = [d[0] for d in con.description] if con.description else []
        _table(rows, headers)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summary report over the Parquet lake")
    parser.add_argument("--lake", type=pathlib.Path, default=DEFAULT_LAKE)
    parser.add_argument("--queries", type=pathlib.Path, default=QUERY_DIR)
    parser.add_argument("--skip-queries", action="store_true")
    args = parser.parse_args(argv)

    print(f"xstream lake report — {args.lake.resolve()}")
    total_files = inventory(args.lake)
    if total_files == 0:
        print("\nNothing in the lake yet. Run the ingestion and processing jobs first.")
        return 0

    quality(args.lake)
    if not args.skip_queries:
        queries(args.lake, args.queries)

    # Always exit 0: quality warnings describe the data, not a broken report.
    # A CI gate should read the checks, not this process's exit status.
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
