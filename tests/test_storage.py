"""Storage layer: lake discovery, compaction safety, quality checks.

No Spark here -- these build tiny Parquet files with DuckDB directly, so the
whole module runs in well under a second. The compaction tests matter most:
compaction *deletes its inputs*, so a bug here loses data permanently, and the
safety rules are the thing worth pinning.
"""

from __future__ import annotations

import datetime as dt
import pathlib

import duckdb
import pytest

from xstream.analysis.compaction import (
    compact_lake,
    compact_partition,
    is_closed,
    partition_hour,
)
from xstream.analysis.lake import connect, discover, run_query_file
from xstream.analysis.quality import (
    check_null_rates,
    check_price_outliers,
    check_schema_drift,
    check_time_gaps,
)

NOW = dt.datetime(2026, 8, 2, 12, 0, tzinfo=dt.timezone.utc)


def write_parquet(path: pathlib.Path, rows: list[tuple], columns: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t ({columns})")
    con.executemany(
        f"INSERT INTO t VALUES ({', '.join('?' * len(rows[0]))})", rows
    )
    con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


def make_book_features(root: pathlib.Path, *, files: int = 3, hour: int = 5, seconds=None):
    """A book_features partition split across `files` small Parquet files."""
    partition = root / "book_features" / "date=2026-08-02" / f"hour={hour}" / "exchange=kraken" / "symbol=BTC-USD"
    seconds = seconds if seconds is not None else list(range(files))
    for i in range(files):
        second = seconds[i] if i < len(seconds) else i
        write_parquet(
            partition / f"part-{i:05d}.parquet",
            [(dt.datetime(2026, 8, 2, hour, 0, second, tzinfo=dt.timezone.utc),
              100.0 + i, 0.1, 0.05, 10.0)],
            "window_start TIMESTAMP, mid_price DOUBLE, spread DOUBLE, "
            "book_imbalance DOUBLE, avg_ingest_latency_ms DOUBLE",
        )
    return partition


# --- lake discovery ----------------------------------------------------------


def test_discover_reports_absent_datasets_without_raising(tmp_path) -> None:
    make_book_features(tmp_path, files=2)
    found = discover(tmp_path)
    assert found["book_features"].present
    assert not found["divergence"].present
    assert found["divergence"].files == 0


def test_connect_creates_views_only_for_present_datasets(tmp_path) -> None:
    make_book_features(tmp_path, files=1)
    con = connect(tmp_path)
    assert con.execute("SELECT count(*) FROM book_features").fetchone()[0] == 1
    with pytest.raises(duckdb.CatalogException):
        con.execute("SELECT * FROM divergence")


def test_hive_partition_columns_are_recovered_from_the_path(tmp_path) -> None:
    """Without hive_partitioning these columns do not exist at all."""
    make_book_features(tmp_path, files=1)
    row = connect(tmp_path).execute(
        "SELECT exchange, symbol, date, hour FROM book_features"
    ).fetchone()
    assert row[0] == "kraken" and row[1] == "BTC-USD"


def test_a_query_over_a_missing_dataset_is_skipped_not_failed(tmp_path) -> None:
    make_book_features(tmp_path, files=1)
    sql = tmp_path / "q.sql"
    sql.write_text("SELECT * FROM divergence;")
    ok, message, _ = run_query_file(connect(tmp_path), sql)
    assert not ok and "skipped" in message


# --- compaction safety -------------------------------------------------------


def test_partition_hour_is_parsed_from_the_path() -> None:
    path = pathlib.Path("data/lake/book_features/date=2026-08-02/hour=5/exchange=kraken/symbol=BTC-USD")
    assert partition_hour(path) == dt.datetime(2026, 8, 2, 5, tzinfo=dt.timezone.utc)


def test_the_current_hour_is_never_considered_closed() -> None:
    """The safety rule: a writer may still be appending."""
    current = pathlib.Path("date=2026-08-02/hour=12/exchange=kraken/symbol=BTC-USD")
    past = pathlib.Path("date=2026-08-02/hour=11/exchange=kraken/symbol=BTC-USD")
    assert not is_closed(current, NOW)
    assert is_closed(past, NOW)


def test_an_open_partition_is_skipped(tmp_path) -> None:
    partition = make_book_features(tmp_path, files=3, hour=12)
    result = compact_partition(partition, now=NOW)
    assert not result.compacted
    assert "still open" in result.skipped_reason
    assert len(list(partition.glob("*.parquet"))) == 3, "inputs must be untouched"


def test_a_single_file_partition_is_left_alone(tmp_path) -> None:
    partition = make_book_features(tmp_path, files=1, hour=5)
    result = compact_partition(partition, now=NOW)
    assert not result.compacted and "only 1 file" in result.skipped_reason


def test_compaction_merges_files_and_preserves_every_row(tmp_path) -> None:
    partition = make_book_features(tmp_path, files=4, hour=5)
    before = duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{partition}/*.parquet')"
    ).fetchone()[0]

    result = compact_partition(partition, now=NOW)

    assert result.compacted
    assert result.files_before == 4 and result.files_after == 1
    after = duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{partition}/*.parquet')"
    ).fetchone()[0]
    assert after == before == 4, "compaction must not lose rows"
    assert len(list(partition.glob("*.parquet"))) == 1


def test_compaction_preserves_values_not_just_row_counts(tmp_path) -> None:
    partition = make_book_features(tmp_path, files=3, hour=5)
    query = "SELECT mid_price FROM read_parquet('{p}/*.parquet') ORDER BY mid_price"
    before = duckdb.connect().execute(query.format(p=partition)).fetchall()
    compact_partition(partition, now=NOW)
    after = duckdb.connect().execute(query.format(p=partition)).fetchall()
    assert before == after


def test_dry_run_changes_nothing(tmp_path) -> None:
    partition = make_book_features(tmp_path, files=3, hour=5)
    result = compact_partition(partition, dry_run=True, now=NOW)
    assert result.rows == 3
    assert len(list(partition.glob("*.parquet"))) == 3


def test_compacted_output_stays_queryable_through_the_lake_views(tmp_path) -> None:
    """Compaction deletes its inputs, so this is the check that matters."""
    make_book_features(tmp_path, files=3, hour=5)
    before = connect(tmp_path).execute("SELECT count(*) FROM book_features").fetchone()[0]
    compact_lake(tmp_path, now=NOW)
    after = connect(tmp_path).execute("SELECT count(*) FROM book_features").fetchone()[0]
    assert after == before == 3


# --- quality checks ----------------------------------------------------------


def test_time_gap_check_finds_a_hole(tmp_path) -> None:
    make_book_features(tmp_path, files=3, hour=5, seconds=[0, 1, 30])
    result = check_time_gaps(connect(tmp_path), "book_features", 1.0)
    assert not result.passed and "gap" in result.detail


def test_time_gap_check_passes_on_a_regular_series(tmp_path) -> None:
    make_book_features(tmp_path, files=3, hour=5, seconds=[0, 1, 2])
    assert check_time_gaps(connect(tmp_path), "book_features", 1.0).passed


def test_null_rate_check_flags_a_mostly_null_column(tmp_path) -> None:
    partition = tmp_path / "book_features/date=2026-08-02/hour=5/exchange=kraken/symbol=BTC-USD"
    write_parquet(
        partition / "part-0.parquet",
        [(dt.datetime(2026, 8, 2, 5, 0, i, tzinfo=dt.timezone.utc), 100.0, None, 0.0, 1.0)
         for i in range(5)],
        "window_start TIMESTAMP, mid_price DOUBLE, spread DOUBLE, "
        "book_imbalance DOUBLE, avg_ingest_latency_ms DOUBLE",
    )
    result = check_null_rates(connect(tmp_path), "book_features")
    assert not result.passed and "spread" in result.detail


def test_schema_drift_is_detected_across_files(tmp_path) -> None:
    partition = tmp_path / "book_features/date=2026-08-02/hour=5/exchange=kraken/symbol=BTC-USD"
    write_parquet(partition / "a.parquet", [(1.0, 2.0)], "mid_price DOUBLE, spread DOUBLE")
    write_parquet(partition / "b.parquet", [(1, 2)], "mid_price INTEGER, spread INTEGER")
    result = check_schema_drift(tmp_path, "book_features")
    assert not result.passed and "distinct schemas" in result.detail


def test_schema_drift_passes_when_all_files_agree(tmp_path) -> None:
    make_book_features(tmp_path, files=3)
    assert check_schema_drift(tmp_path, "book_features").passed


def test_outlier_check_flags_an_extreme_price(tmp_path) -> None:
    partition = tmp_path / "book_features/date=2026-08-02/hour=5/exchange=kraken/symbol=BTC-USD"
    rows = [(dt.datetime(2026, 8, 2, 5, 0, i, tzinfo=dt.timezone.utc), 100.0, 0.1, 0.0, 1.0)
            for i in range(30)]
    rows.append((dt.datetime(2026, 8, 2, 5, 1, tzinfo=dt.timezone.utc), 99999.0, 0.1, 0.0, 1.0))
    write_parquet(
        partition / "part-0.parquet", rows,
        "window_start TIMESTAMP, mid_price DOUBLE, spread DOUBLE, "
        "book_imbalance DOUBLE, avg_ingest_latency_ms DOUBLE",
    )
    result = check_price_outliers(connect(tmp_path), "book_features", "mid_price")
    assert not result.passed and "modified-z" in result.detail


def test_outlier_check_passes_on_stable_prices(tmp_path) -> None:
    make_book_features(tmp_path, files=5)
    assert check_price_outliers(connect(tmp_path), "book_features", "mid_price").passed
