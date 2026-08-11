"""Data quality checks over the lake.

Four checks, each aimed at a failure this pipeline can actually produce rather
than at a generic checklist:

- **Time-series gaps.** Windowed aggregates should form a regular grid. A hole
  means the stream stopped, the watermark dropped events, or a venue went away
  -- all invisible in a query that only ever averages.
- **Null rates.** A feature that is 40% null is not a feature, and a model will
  happily learn the null pattern instead of the signal.
- **Schema drift.** Files written by different job versions land in the same
  dataset. Parquet will not complain until a query hits a column that changed
  type underneath it.
- **Outlier prices.** A fat-fingered print or a bad parse shows up as a price
  far from its neighbours. Measured in median-absolute-deviation units rather
  than standard deviations, because an outlier inflates the standard deviation
  it would be judged against. Catching it here is much cheaper than explaining
  a strange backtest later.

Every check returns a result rather than raising, and severity is separated
from failure: a gap in the data is a fact about the world, not necessarily a
bug, and the report should say which is which rather than exiting non-zero on
the first surprise.
"""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Any

import duckdb

from .lake import DATASETS, available_views


@dataclasses.dataclass
class CheckResult:
    check: str
    dataset: str
    passed: bool
    detail: str
    rows: list[Any] = dataclasses.field(default_factory=list)

    def __str__(self) -> str:
        return f"[{'PASS' if self.passed else 'WARN'}] {self.dataset}.{self.check}: {self.detail}"


def check_time_gaps(
    con: duckdb.DuckDBPyConnection, dataset: str, expected_seconds: float
) -> CheckResult:
    """Find intervals between consecutive windows that exceed the cadence.

    Grouped per (exchange, symbol) because a gap on one venue is a different
    event from a gap everywhere, and averaging across venues hides it.
    """
    time_col = DATASETS[dataset]
    rows = con.execute(
        f"""
        WITH ordered AS (
            SELECT exchange, symbol, {time_col} AS t,
                   lag({time_col}) OVER (
                       PARTITION BY exchange, symbol ORDER BY {time_col}
                   ) AS prev_t
            FROM {dataset}
        )
        SELECT exchange, symbol, prev_t, t,
               epoch(t) - epoch(prev_t) AS gap_seconds
        FROM ordered
        WHERE prev_t IS NOT NULL
          AND epoch(t) - epoch(prev_t) > {expected_seconds * 1.5}
        ORDER BY gap_seconds DESC
        LIMIT 20
        """
    ).fetchall()
    if not rows:
        return CheckResult("time_gaps", dataset, True, "no gaps beyond 1.5x cadence")
    worst = rows[0]
    return CheckResult(
        "time_gaps",
        dataset,
        False,
        f"{len(rows)} gap(s); largest {worst[4]:.1f}s on {worst[0]}/{worst[1]}",
        rows,
    )


def check_null_rates(
    con: duckdb.DuckDBPyConnection, dataset: str, threshold: float = 0.2
) -> CheckResult:
    """Null fraction per column, flagging any above the threshold."""
    columns = [
        (r[0], r[1])
        for r in con.execute(f"DESCRIBE {dataset}").fetchall()
        # Partition columns are recovered from the path and never null.
        if r[0] not in {"date", "hour", "exchange", "symbol"}
    ]
    if not columns:
        return CheckResult("null_rates", dataset, True, "no columns to check")

    total = con.execute(f"SELECT count(*) FROM {dataset}").fetchone()[0]
    if total == 0:
        return CheckResult("null_rates", dataset, True, "dataset is empty")

    parts = [
        f"sum(CASE WHEN {name} IS NULL THEN 1 ELSE 0 END)::DOUBLE / {total} AS \"{name}\""
        for name, _ in columns
    ]
    row = con.execute(f"SELECT {', '.join(parts)} FROM {dataset}").fetchone()
    offenders = [
        (name, rate) for (name, _), rate in zip(columns, row) if rate and rate > threshold
    ]
    if not offenders:
        return CheckResult("null_rates", dataset, True, f"all columns below {threshold:.0%} null")
    worst = ", ".join(f"{n} {r:.0%}" for n, r in sorted(offenders, key=lambda x: -x[1])[:5])
    return CheckResult(
        "null_rates", dataset, False, f"{len(offenders)} column(s) above threshold: {worst}",
        offenders,
    )


def check_schema_drift(lake_root: pathlib.Path, dataset: str) -> CheckResult:
    """Compare each file's schema against the most common one.

    Files in one dataset are written by whatever job version was running at the
    time. Parquet tolerates that until a query touches a column whose type moved
    underneath it, which surfaces far from the cause.
    """
    path = lake_root / dataset
    files = sorted(path.rglob("*.parquet"))
    if len(files) < 2:
        return CheckResult("schema_drift", dataset, True, f"{len(files)} file(s), nothing to compare")

    con = duckdb.connect()
    schemas: dict[tuple, list[pathlib.Path]] = {}
    for file in files:
        cols = tuple(
            (r[0], r[1])
            for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file}')").fetchall()
        )
        schemas.setdefault(cols, []).append(file)

    if len(schemas) == 1:
        return CheckResult("schema_drift", dataset, True, f"{len(files)} file(s), one schema")

    majority = max(schemas.values(), key=len)
    return CheckResult(
        "schema_drift",
        dataset,
        False,
        f"{len(schemas)} distinct schemas across {len(files)} files "
        f"(majority covers {len(majority)})",
        [[str(f) for f in group] for group in schemas.values()],
    )


#: Iglewicz-Hoaglin modified z-score constant: 0.6745 makes MAD a consistent
#: estimator of the standard deviation for normally distributed data.
_MAD_SCALE = 0.6745

#: Iglewicz-Hoaglin fallback constant, used when MAD collapses to zero.
_MEAN_AD_SCALE = 1.253314


def check_price_outliers(
    con: duckdb.DuckDBPyConnection, dataset: str, column: str, threshold: float = 3.5
) -> CheckResult:
    """Flag values far from the median, measured in MAD units.

    **Not** a mean/standard-deviation z-score, and the reason is the whole point
    of the check. An outlier inflates the very standard deviation it is measured
    against -- the masking effect -- and for a population stddev the largest
    achievable z-score is bounded by `(n-1)/sqrt(n)`. With 31 observations that
    ceiling is about 5.48, so a single grossly wrong price could never reach a
    6-sigma threshold no matter how wrong it was. A detector that cannot fire on
    small samples is worse than none, because it reads as reassurance.

    Median absolute deviation has a ~50% breakdown point: the estimate barely
    moves until half the data is contaminated, so one bad print stands out
    instead of hiding behind its own influence. The 3.5 threshold is the
    Iglewicz-Hoaglin convention.

    MAD alone is not enough, because it hits zero whenever more than half the
    values are identical -- a quiet book quoting the same mid repeatedly, with
    one bad print. A naive `WHERE mad > 0` guard would then report "no
    outliers" on exactly the case the check exists for. Iglewicz and Hoaglin
    prescribe falling back to the mean absolute deviation there, scaled by
    1.253314; that fallback is what makes this fire on a near-constant series.
    Only a perfectly constant series yields no outliers, correctly: it has no
    dispersion for anything to be unusual against.
    """
    rows = con.execute(
        f"""
        WITH med AS (
            SELECT exchange, symbol, median({column}) AS m
            FROM {dataset} WHERE {column} IS NOT NULL
            GROUP BY exchange, symbol
        ),
        deviations AS (
            SELECT d.exchange, d.symbol, d.{column} AS value, m.m AS m,
                   abs(d.{column} - m.m) AS abs_dev
            FROM {dataset} d JOIN med m USING (exchange, symbol)
            WHERE d.{column} IS NOT NULL
        ),
        spread AS (
            SELECT exchange, symbol,
                   median(abs_dev) AS mad,
                   avg(abs_dev) AS mean_ad
            FROM deviations GROUP BY exchange, symbol
        ),
        scored AS (
            SELECT v.exchange, v.symbol, v.value,
                   CASE
                       WHEN s.mad > 0 THEN {_MAD_SCALE} * v.abs_dev / s.mad
                       WHEN s.mean_ad > 0 THEN v.abs_dev / ({_MEAN_AD_SCALE} * s.mean_ad)
                   END AS modified_z
            FROM deviations v JOIN spread s USING (exchange, symbol)
        )
        SELECT exchange, symbol, value, modified_z
        FROM scored
        WHERE modified_z IS NOT NULL AND modified_z > {threshold}
        ORDER BY modified_z DESC LIMIT 20
        """
    ).fetchall()
    if not rows:
        return CheckResult(
            "price_outliers", dataset, True, f"none beyond {threshold:g} modified-z"
        )
    return CheckResult(
        "price_outliers", dataset, False,
        f"{len(rows)} value(s) beyond {threshold:g} modified-z; worst {rows[0][3]:.1f}",
        rows,
    )


CADENCE_SECONDS = {
    "candles_1s": 1.0,
    "candles_10s": 10.0,
    "candles_1m": 60.0,
    "book_features": 1.0,
    "divergence": 1.0,
}

OUTLIER_COLUMN = {
    "candles_1s": "close",
    "candles_10s": "close",
    "candles_1m": "close",
    "book_features": "mid_price",
    "divergence": "divergence_bps",
}


def run_all(
    con: duckdb.DuckDBPyConnection, lake_root: pathlib.Path
) -> list[CheckResult]:
    results = []
    views = available_views(con)
    for dataset in DATASETS:
        if dataset not in views:
            results.append(
                CheckResult("presence", dataset, True, "dataset absent, checks skipped")
            )
            continue
        results.append(check_time_gaps(con, dataset, CADENCE_SECONDS[dataset]))
        results.append(check_null_rates(con, dataset))
        results.append(check_schema_drift(lake_root, dataset))
        results.append(check_price_outliers(con, dataset, OUTLIER_COLUMN[dataset]))
    return results
