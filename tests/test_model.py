"""Evaluation harness tests.

The real lake has one usable feature row, so it cannot exercise any of this.
These tests therefore validate the harness against **synthetic** data where the
right answer is known by construction — planted lookahead, planted signal, and
pure noise. That is the only way to know the harness would behave correctly if
real data ever arrived, and it is the difference between "the code runs" and
"the code works".
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest

from xstream.model.evaluation import (
    MIN_ROWS_FOR_METRICS,
    baseline_predictions,
    brier_score,
    calibration_bins,
    evaluate,
    walk_forward_splits,
)
from xstream.model.features import (
    FEATURE_SPECS,
    FeatureSpec,
    audit_lookahead,
    build_features,
    usable_matrix,
)


def synthetic_books(n: int = 2000, seed: int = 0, drift: float = 0.0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    steps = rng.normal(drift, 0.001, size=n)
    mid = 100 * np.exp(np.cumsum(steps))
    start = dt.datetime(2026, 8, 2, 5, tzinfo=dt.timezone.utc)
    return pd.DataFrame({
        "exchange": "kraken",
        "symbol": "BTC-USD",
        "window_start": [start + dt.timedelta(seconds=i) for i in range(n)],
        "mid_price": mid,
        "book_imbalance": rng.uniform(-1, 1, size=n),
        "relative_spread_bps": rng.uniform(0.01, 0.2, size=n),
    })


# --- walk-forward split integrity -------------------------------------------


def test_test_blocks_always_follow_their_training_data() -> None:
    """The property that makes this walk-forward rather than cheating."""
    for train, test in walk_forward_splits(2000):
        assert train.max() < test.min(), "test data must be strictly in the future"


def test_training_sets_expand_and_test_blocks_do_not_overlap() -> None:
    splits = list(walk_forward_splits(2000))
    assert len(splits) >= 2
    sizes = [len(tr) for tr, _ in splits]
    assert sizes == sorted(sizes), "training window must expand"
    seen: set[int] = set()
    for _, test in splits:
        assert not (set(test.tolist()) & seen), "test blocks must be disjoint"
        seen |= set(test.tolist())


def test_too_little_data_yields_no_splits_rather_than_a_bad_one() -> None:
    assert list(walk_forward_splits(20)) == []


# --- lookahead audit ---------------------------------------------------------


def test_audit_passes_on_correctly_built_features() -> None:
    findings = audit_lookahead(synthetic_books(500), horizon=5)
    statuses = {f["feature"]: f["status"] for f in findings}
    assert "LOOKAHEAD" not in statuses.values(), statuses


def test_audit_catches_a_planted_lookahead_bug(monkeypatch) -> None:
    """A feature that peeks one row ahead must be caught.

    This is the check that makes the audit worth having: reading the code and
    concluding "looks fine" is exactly how leaks survive.
    """
    import xstream.model.features as features_mod

    original = features_mod.build_features

    def leaky(books, divergence=None, *, horizon=5):
        df = original(books, divergence, horizon=horizon)
        if not df.empty:
            # Peek at the next row's imbalance: a classic shift-sign error.
            df["book_imbalance"] = df["book_imbalance"].shift(-1)
        return df

    monkeypatch.setattr(features_mod, "build_features", leaky)
    findings = features_mod.audit_lookahead(synthetic_books(500), horizon=5)
    leaked = [f for f in findings if f["status"] == "LOOKAHEAD"]
    assert leaked, "audit failed to detect a deliberately planted leak"


def test_a_feature_spec_cannot_declare_future_information() -> None:
    with pytest.raises(ValueError, match="lookahead bug"):
        FeatureSpec("bad", "peeks ahead", last_observation_offset=1)


def test_every_declared_feature_is_historical() -> None:
    assert all(spec.last_observation_offset <= 0 for spec in FEATURE_SPECS)


# --- baselines and metrics ---------------------------------------------------


def test_majority_baseline_follows_the_training_distribution() -> None:
    y_train = np.array([1.0] * 80 + [-1.0] * 20)
    y_test = np.array([1.0, -1.0])
    preds = baseline_predictions(y_train, y_test, None)
    assert (preds["majority_class"] == 1.0).all()


def test_persistence_baseline_extends_the_last_move() -> None:
    preds = baseline_predictions(
        np.array([1.0, -1.0]), np.array([1.0, 1.0]), np.array([0.5, -0.5])
    )
    assert list(preds["persistence"]) == [1.0, -1.0]


def test_brier_score_rewards_confident_correctness() -> None:
    y = np.array([1.0, 1.0, -1.0, -1.0])
    confident_right = brier_score(y, np.array([0.99, 0.99, 0.01, 0.01]))
    hedged = brier_score(y, np.array([0.5, 0.5, 0.5, 0.5]))
    confident_wrong = brier_score(y, np.array([0.01, 0.01, 0.99, 0.99]))
    assert confident_right < hedged < confident_wrong


def test_calibration_bins_recover_a_known_frequency() -> None:
    rng = np.random.default_rng(0)
    prob = np.full(1000, 0.7)
    y = np.where(rng.uniform(size=1000) < 0.7, 1.0, -1.0)
    rows = calibration_bins(y, prob)
    row = next(r for r in rows if r["count"] > 100)
    assert abs(row["observed_frequency"] - 0.7) < 0.05


# --- end-to-end behaviour on known data --------------------------------------


def test_insufficient_data_refuses_to_report_metrics() -> None:
    X = pd.DataFrame({"return_1": np.zeros(10)})
    report = evaluate(X, pd.Series(np.ones(10)))
    assert not report.sufficient_data
    assert report.folds == []
    assert "INSUFFICIENT DATA" in report.verdict()
    assert report.rows_required == MIN_ROWS_FOR_METRICS


def test_pure_noise_produces_no_detectable_signal() -> None:
    """The result that should be reported most often, and usually isn't."""
    frame = build_features(synthetic_books(3000, seed=1), None, horizon=5)
    X, y, _ = usable_matrix(frame)
    report = evaluate(X, y)
    assert report.sufficient_data
    verdict = report.verdict()
    # The requirement is the negative one: random-walk data must never be
    # reported as signal, whichever of the no-signal verdicts applies.
    assert "SIGNAL DETECTED" not in verdict, verdict
    assert abs(report.mean_lift) < 0.15, verdict


def test_a_planted_signal_is_detected() -> None:
    """Proves the harness is capable of finding signal, not merely of saying no.

    Without this, "no detectable signal" would be indistinguishable from a
    harness that can never detect anything.
    """
    rng = np.random.default_rng(3)
    n = 3000
    imbalance = rng.uniform(-1, 1, size=n)
    # Next move follows current imbalance, with noise.
    steps = 0.002 * imbalance + rng.normal(0, 0.0005, size=n)
    mid = 100 * np.exp(np.cumsum(np.roll(steps, 1)))
    start = dt.datetime(2026, 8, 2, 5, tzinfo=dt.timezone.utc)
    books = pd.DataFrame({
        "exchange": "kraken", "symbol": "BTC-USD",
        "window_start": [start + dt.timedelta(seconds=i) for i in range(n)],
        "mid_price": mid,
        "book_imbalance": imbalance,
        "relative_spread_bps": rng.uniform(0.01, 0.2, size=n),
    })
    frame = build_features(books, None, horizon=1)
    X, y, _ = usable_matrix(frame)
    report = evaluate(X, y)
    assert report.sufficient_data
    assert report.mean_lift > 0.05, f"planted signal missed: {report.verdict()}"


def test_all_null_features_are_dropped_not_imputed() -> None:
    frame = build_features(synthetic_books(600), None, horizon=5)
    _, _, used = usable_matrix(frame)
    assert "divergence_bps" not in used, "an all-null column must not be used"


def test_a_negative_lift_is_reported_as_no_signal() -> None:
    """A model losing to a trivial baseline is a failure, not a finding.

    Reporting by magnitude alone would call a -0.04 lift "signal" purely
    because it sits far enough from zero.
    """
    from xstream.model.evaluation import EvaluationReport, FoldResult

    report = EvaluationReport(True, 1000, 500, ["f"], [])
    report.folds = [
        FoldResult(i, 100, 100, 0.50, {"always_down": 0.56}, 0.25) for i in range(1, 6)
    ]
    verdict = report.verdict()
    assert "NO SIGNAL" in verdict
    assert "SIGNAL DETECTED" not in verdict


def test_a_clear_positive_lift_is_reported_as_signal() -> None:
    from xstream.model.evaluation import EvaluationReport, FoldResult

    report = EvaluationReport(True, 1000, 500, ["f"], [])
    report.folds = [
        FoldResult(i, 100, 100, 0.88, {"always_down": 0.52}, 0.07) for i in range(1, 6)
    ]
    assert "SIGNAL DETECTED" in report.verdict()
