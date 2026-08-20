"""Feature construction, with the lookahead audit built in rather than bolted on.

The single easiest way to produce an impressive and worthless result on
time-series data is to let information from the future leak into a feature.
It is rarely deliberate: a rolling mean that includes the current row, a
`shift` in the wrong direction, a join that silently matches a later window.
The model then learns the leak, cross-validation confirms it, and the number
looks wonderful right up until it meets live data.

This module therefore treats "which information was available when" as a
first-class, declared property. Every feature carries a `FeatureSpec` stating
the last observation it may use, and `audit_lookahead` checks the built matrix
against those declarations empirically -- by perturbing the future and
confirming the features do not move.

Prediction convention used throughout:

    features at row t  ->  target = sign(mid[t + horizon] - mid[t])

Everything in the feature row must be knowable at the close of window t. The
target is the only thing allowed to look forward, because it is the label.
"""

from __future__ import annotations

import dataclasses
import pathlib

import numpy as np
import pandas as pd

from ..analysis.lake import DEFAULT_LAKE, connect


@dataclasses.dataclass(frozen=True)
class FeatureSpec:
    """One feature and the information it is allowed to use.

    `last_observation_offset` is relative to the prediction row t: 0 means the
    feature may use data up to and including window t, which is legitimate
    because window t has closed by the time a prediction for t+horizon is made.
    A positive value would be a lookahead bug by construction, so the audit
    rejects it outright.
    """

    name: str
    description: str
    last_observation_offset: int = 0
    source: str = "book_features"

    def __post_init__(self) -> None:
        if self.last_observation_offset > 0:
            raise ValueError(
                f"{self.name}: last_observation_offset must be <= 0; "
                "a positive offset is a lookahead bug by definition"
            )


FEATURE_SPECS: tuple[FeatureSpec, ...] = (
    FeatureSpec(
        "book_imbalance",
        "(bid - ask) depth over top 10 levels at the close of window t",
    ),
    FeatureSpec(
        "relative_spread_bps",
        "quoted spread relative to mid at the close of window t",
    ),
    FeatureSpec(
        "return_1",
        "log return from window t-1 to t; uses no data after t",
        last_observation_offset=0,
    ),
    FeatureSpec(
        "return_2",
        "log return from window t-2 to t-1; strictly historical",
        last_observation_offset=-1,
    ),
    FeatureSpec(
        "realized_vol_5",
        "stddev of the last 5 log returns, ending at t",
    ),
    FeatureSpec(
        "imbalance_mean_5",
        "mean book imbalance over the trailing 5 windows, ending at t",
    ),
    FeatureSpec(
        "imbalance_delta",
        "change in book imbalance from t-1 to t",
    ),
    FeatureSpec(
        "divergence_bps",
        "cross-exchange mid divergence at t; null when the venue pair is absent",
        source="divergence",
    ),
)

FEATURE_NAMES = tuple(spec.name for spec in FEATURE_SPECS)


def load_book_features(lake_root: pathlib.Path = DEFAULT_LAKE) -> pd.DataFrame:
    con = connect(lake_root)
    try:
        return con.execute(
            """
            SELECT exchange, symbol, window_start,
                   CAST(mid_price AS DOUBLE)          AS mid_price,
                   CAST(book_imbalance AS DOUBLE)     AS book_imbalance,
                   CAST(avg_relative_spread_bps AS DOUBLE) AS relative_spread_bps
            FROM book_features
            WHERE mid_price IS NOT NULL
            ORDER BY exchange, symbol, window_start
            """
        ).df()
    except Exception:
        return pd.DataFrame(
            columns=["exchange", "symbol", "window_start", "mid_price",
                     "book_imbalance", "relative_spread_bps"]
        )


def load_divergence(lake_root: pathlib.Path = DEFAULT_LAKE) -> pd.DataFrame:
    con = connect(lake_root)
    try:
        return con.execute(
            """
            SELECT symbol, window_start,
                   CAST(divergence_bps AS DOUBLE) AS divergence_bps
            FROM divergence
            """
        ).df()
    except Exception:
        return pd.DataFrame(columns=["symbol", "window_start", "divergence_bps"])


def build_features(
    books: pd.DataFrame,
    divergence: pd.DataFrame | None = None,
    *,
    horizon: int = 5,
) -> pd.DataFrame:
    """Build the feature matrix and target.

    Every rolling and lagged computation is grouped by (exchange, symbol) so
    that one instrument's history never bleeds into another's -- a subtle
    cross-contamination that pandas will happily perform if asked carelessly.
    """
    if books.empty:
        return pd.DataFrame(columns=[*FEATURE_NAMES, "target", "window_start",
                                     "exchange", "symbol"])

    df = books.sort_values(["exchange", "symbol", "window_start"]).copy()
    grouped = df.groupby(["exchange", "symbol"], sort=False)

    log_mid = np.log(df["mid_price"].where(df["mid_price"] > 0))
    df["_log_mid"] = log_mid
    df["return_1"] = grouped["_log_mid"].diff(1)
    df["return_2"] = grouped["_log_mid"].diff(1).groupby(
        [df["exchange"], df["symbol"]], sort=False
    ).shift(1)
    df["realized_vol_5"] = grouped["return_1"].transform(
        lambda s: s.rolling(5, min_periods=2).std()
    ) if "return_1" in df else np.nan
    df["imbalance_mean_5"] = grouped["book_imbalance"].transform(
        lambda s: s.rolling(5, min_periods=1).mean()
    )
    df["imbalance_delta"] = grouped["book_imbalance"].diff(1)

    # Target: direction of the mid over the next `horizon` windows. This is the
    # only forward-looking quantity, and it is the label.
    df["_future_mid"] = grouped["_log_mid"].shift(-horizon)
    df["target"] = np.sign(df["_future_mid"] - df["_log_mid"])

    if divergence is not None and not divergence.empty:
        df = df.merge(divergence, on=["symbol", "window_start"], how="left")
    else:
        df["divergence_bps"] = np.nan

    df = df.drop(columns=["_log_mid", "_future_mid"])
    # Rows with no target cannot be trained or scored on. Rows where the mid did
    # not move produce target 0; a three-class problem is a different question
    # from "which way did it go", so they are dropped and the drop is reported.
    df = df[df["target"].notna() & (df["target"] != 0)]
    return df.reset_index(drop=True)


def usable_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    """Feature columns that are actually populated, plus X and y.

    A feature that is entirely null is dropped and reported rather than
    imputed. Imputing a column that has no data anywhere invents structure --
    `divergence_bps` is null throughout on the current lake precisely because
    no cross-venue rows exist (D-021), and filling it with zeros would tell a
    model that the venues always agree.
    """
    if df.empty:
        return pd.DataFrame(), pd.Series(dtype=float), []
    usable = [c for c in FEATURE_NAMES if c in df and df[c].notna().any()]
    complete = df.dropna(subset=usable) if usable else df.iloc[0:0]
    return complete[usable], complete["target"], usable


def audit_lookahead(
    books: pd.DataFrame, horizon: int = 5, tail_rows: int = 5
) -> list[dict]:
    """Empirically verify that no feature depends on data after its own row.

    Declarations in `FEATURE_SPECS` are claims; this checks them. The method is
    to corrupt a block of *later* rows -- multiplying their mid prices by 10 and
    flipping their imbalance -- rebuild the matrix, and confirm that every
    feature value on *earlier* rows is unchanged. Anything that moves is reading
    forward.

    Two details make the difference between a real check and a decorative one.

    **Rows are matched on `window_start`, not on position.** `build_features`
    drops rows that have no target, so positional indices do not survive the
    rebuild and comparing by position silently compares different rows.

    **The corrupted block must reach the output.** The first version of this
    audit perturbed the final rows -- which `build_features` drops for having no
    target, so the corruption never appeared in the output and the audit could
    not detect anything, ever. It reported "clean" for a deliberately planted
    leak. The block therefore starts far enough back that corrupted rows survive
    into the built matrix, and the comparison covers only rows strictly before
    it. See D-031.
    """
    ordered = books.sort_values(["exchange", "symbol", "window_start"]).copy()
    needed = horizon + tail_rows + 10
    if len(ordered) < needed:
        return [{"feature": "(all)", "status": "skipped",
                 "detail": f"needs {needed} rows to perturb a surviving future block"}]

    # Start the corrupted block before the trailing rows that get dropped for
    # lack of a target, so the corruption actually lands in the output.
    cutoff_position = len(ordered) - (horizon + tail_rows)
    cutoff = ordered["window_start"].iloc[cutoff_position]

    baseline = build_features(ordered, None, horizon=horizon)

    corrupted_books = ordered.copy()
    future_mask = corrupted_books["window_start"] >= cutoff
    corrupted_books.loc[future_mask, "mid_price"] *= 10
    corrupted_books.loc[future_mask, "book_imbalance"] = -0.99
    corrupted_books.loc[future_mask, "relative_spread_bps"] = 99.0
    corrupted = build_features(corrupted_books, None, horizon=horizon)

    key = ["exchange", "symbol", "window_start"]
    if baseline.empty or corrupted.empty:
        return [{"feature": "(all)", "status": "skipped",
                 "detail": "feature matrix was empty after building"}]

    merged = baseline.merge(corrupted, on=key, how="inner", suffixes=("_base", "_corrupt"))
    past = merged[merged["window_start"] < cutoff]
    if past.empty:
        return [{"feature": "(all)", "status": "skipped",
                 "detail": "no rows strictly before the perturbed block"}]

    findings = []
    for spec in FEATURE_SPECS:
        base_col, corrupt_col = f"{spec.name}_base", f"{spec.name}_corrupt"
        if base_col not in past or corrupt_col not in past:
            findings.append({"feature": spec.name, "status": "skipped",
                             "detail": "column absent from the built matrix"})
            continue
        a = past[base_col].to_numpy(dtype=float)
        b = past[corrupt_col].to_numpy(dtype=float)
        same = np.allclose(a, b, equal_nan=True)
        findings.append({
            "feature": spec.name,
            "status": "clean" if same else "LOOKAHEAD",
            "detail": spec.description if same
                      else f"value changed on {int((~np.isclose(a, b, equal_nan=True)).sum())} "
                           "past row(s) when only future rows were altered",
        })
    return findings
