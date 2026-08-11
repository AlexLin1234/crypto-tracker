"""DuckDB access layer over the Parquet lake.

DuckDB reads Parquet directly, so there is no load step and no second copy of
the data: the lake *is* the database. That is the whole reason this layer is
thin — its job is to name the datasets and recover the Hive partition columns,
not to own the data.

Every dataset is optional. A pipeline that has not run a given job yet, or one
whose job legitimately produced nothing (Milestone 4's divergence join emits no
rows without two venues, D-021), must not make the report crash. Missing and
empty are normal states here, and are reported as such rather than raised.
"""

from __future__ import annotations

import dataclasses
import pathlib

import duckdb

DEFAULT_LAKE = pathlib.Path("data/lake")

#: dataset name -> the time column that orders it. Used by the gap checks and
#: by compaction to decide what a "closed" partition is.
DATASETS: dict[str, str] = {
    "candles_1s": "window_start",
    "candles_10s": "window_start",
    "candles_1m": "window_start",
    "book_features": "window_start",
    "divergence": "window_start",
}


@dataclasses.dataclass(frozen=True)
class DatasetInfo:
    name: str
    path: pathlib.Path
    files: int
    bytes: int

    @property
    def present(self) -> bool:
        return self.files > 0

    @property
    def avg_file_bytes(self) -> float:
        return self.bytes / self.files if self.files else 0.0


def discover(lake_root: pathlib.Path = DEFAULT_LAKE) -> dict[str, DatasetInfo]:
    """What actually exists on disk, regardless of what was expected."""
    found = {}
    for name in DATASETS:
        path = lake_root / name
        files = sorted(path.rglob("*.parquet")) if path.exists() else []
        found[name] = DatasetInfo(
            name=name,
            path=path,
            files=len(files),
            bytes=sum(f.stat().st_size for f in files),
        )
    return found


def connect(lake_root: pathlib.Path = DEFAULT_LAKE) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with one view per present dataset.

    `hive_partitioning=true` is what recovers `date`, `hour`, `exchange` and
    `symbol` from the directory names. Without it those columns simply do not
    exist in the scan -- they were never written into the files, only into the
    paths -- and every query that groups by exchange silently fails.
    """
    con = duckdb.connect()
    for name, info in discover(lake_root).items():
        if not info.present:
            continue
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {name} AS
            SELECT * FROM read_parquet('{info.path}/**/*.parquet',
                                       hive_partitioning = true)
            """
        )
    return con


def available_views(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'"
    ).fetchall()
    return {r[0] for r in rows}


def run_query_file(
    con: duckdb.DuckDBPyConnection, path: pathlib.Path
) -> tuple[bool, str, list]:
    """Execute a .sql file, tolerating datasets it needs but that are absent.

    Returns (ok, message, rows). A query over a dataset that does not exist is
    reported as skipped rather than failed: it is a statement about the data
    available, not about the query being wrong.
    """
    sql = path.read_text()
    try:
        result = con.execute(sql).fetchall()
        return True, f"{len(result)} rows", result
    except (duckdb.CatalogException, duckdb.IOException) as exc:
        # CatalogException: the view was never created because the dataset is
        # absent. IOException: a glob in the SQL itself matched no files. Both
        # mean "this dataset does not exist yet", which is a statement about the
        # data, not about the query.
        return False, f"skipped (missing dataset): {str(exc).splitlines()[0][:80]}", []
    except duckdb.Error as exc:
        return False, f"failed: {str(exc).splitlines()[0]}", []
