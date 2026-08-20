"""Evaluation with the guardrails that stop a time-series result fooling you.

The modelling here is deliberately unremarkable -- logistic regression, then
gradient boosting. The evaluation is the point. Five specific ways to be wrong
are addressed explicitly:

1. **Random splits.** Shuffling time-series rows lets the model train on the
   future and test on the past. Only expanding-window walk-forward splits are
   provided; there is no code path that shuffles.
2. **Lookahead in features.** Audited empirically in `features.audit_lookahead`.
3. **Accuracy without a baseline.** 52% accuracy sounds like signal and is
   worthless if the majority class is 52%. Everything is reported as lift over
   the *best* naive baseline, never as raw accuracy alone.
4. **Confidence without calibration.** A model can rank well and still be badly
   calibrated, which matters more than accuracy when the output feeds a
   decision with costs. Brier score and reliability bins are reported.
5. **Too little data.** The most common failure of all. A hard minimum-row
   guard refuses to report metrics rather than producing numbers whose error
   bars swamp their values.

The intended conclusion of an honest run on real data is usually "weak or no
signal, and not exploitable after costs". That is a result, not a failure.
"""

from __future__ import annotations

import dataclasses
import math
from typing import Iterator

import numpy as np
import pandas as pd

#: Below this many usable rows, no metrics are reported at all. With a binary
#: target, the standard error on an accuracy estimate is ~0.5/sqrt(n): at 500
#: rows that is ~2.2 percentage points, which is already the same size as any
#: plausible edge. Below a few hundred rows the estimate carries no information
#: and printing it invites over-reading.
MIN_ROWS_FOR_METRICS = 500

#: Minimum test-fold size for a fold's metrics to be counted.
MIN_TEST_ROWS = 50


@dataclasses.dataclass
class FoldResult:
    fold: int
    train_rows: int
    test_rows: int
    model_accuracy: float
    baseline_accuracies: dict[str, float]
    brier: float

    @property
    def best_baseline(self) -> tuple[str, float]:
        return max(self.baseline_accuracies.items(), key=lambda kv: kv[1])

    @property
    def lift(self) -> float:
        return self.model_accuracy - self.best_baseline[1]


@dataclasses.dataclass
class EvaluationReport:
    sufficient_data: bool
    rows_available: int
    rows_required: int
    features_used: list[str]
    features_dropped: list[str]
    folds: list[FoldResult] = dataclasses.field(default_factory=list)
    calibration: list[dict] = dataclasses.field(default_factory=list)
    notes: list[str] = dataclasses.field(default_factory=list)

    @property
    def mean_lift(self) -> float | None:
        if not self.folds:
            return None
        return float(np.mean([f.lift for f in self.folds]))

    @property
    def lift_standard_error(self) -> float | None:
        if len(self.folds) < 2:
            return None
        lifts = [f.lift for f in self.folds]
        return float(np.std(lifts, ddof=1) / math.sqrt(len(lifts)))

    def verdict(self) -> str:
        """A sentence that does not overclaim."""
        if not self.sufficient_data:
            return (
                f"INSUFFICIENT DATA: {self.rows_available} usable rows against a "
                f"{self.rows_required}-row minimum. No metrics reported, because "
                "an estimate this noisy would be indistinguishable from signal."
            )
        lift = self.mean_lift
        stderr = self.lift_standard_error
        if lift is None:
            return "NO FOLDS: not enough data to form a walk-forward split."
        # A negative lift is not signal, however far from zero it sits: it
        # means the model is worse than doing something trivial. Reporting it
        # by magnitude alone would turn a failure into a finding.
        if lift <= 0:
            return (
                f"NO SIGNAL: the model underperforms the best naive baseline by "
                f"{abs(lift):.4f}. A fitted model that loses to always-down is "
                "evidence against the feature set, not for it."
            )
        if stderr and lift < 2 * stderr:
            return (
                f"NO DETECTABLE SIGNAL: mean lift {lift:+.4f} over the best naive "
                f"baseline, standard error {stderr:.4f}. The lift is inside two "
                "standard errors of zero."
            )
        return (
            f"SIGNAL DETECTED: mean lift {lift:+.4f} over the best naive baseline "
            f"(SE {stderr:.4f} across {len(self.folds)} folds). Statistical "
            "detectability is not exploitability -- a lift must still clear the "
            "cost floor in D-022, which it almost never does."
        )


def walk_forward_splits(
    n_rows: int, *, folds: int = 5, min_train: int = 100
) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """Expanding-window splits: train on the past, test on the immediate future.

    Fold k trains on everything before a cut point and tests on the block that
    follows it. Train sets grow; test blocks never overlap and never precede
    their training data. This is the only splitting strategy offered, because
    offering `KFold` alongside it would make the wrong choice available.
    """
    if n_rows < min_train + MIN_TEST_ROWS:
        return
    usable_folds = min(folds, (n_rows - min_train) // MIN_TEST_ROWS)
    if usable_folds < 1:
        return
    block = (n_rows - min_train) // usable_folds
    for k in range(usable_folds):
        train_end = min_train + k * block
        test_end = train_end + block if k < usable_folds - 1 else n_rows
        yield np.arange(0, train_end), np.arange(train_end, test_end)


def baseline_predictions(
    y_train: np.ndarray, y_test: np.ndarray, x_test_prev_return: np.ndarray | None
) -> dict[str, np.ndarray]:
    """Naive predictors any real model must beat to be interesting.

    `majority` is the one that most often embarrasses a model: on a series with
    even a mild directional drift it can be well above 50%, and a model that
    "achieves 54% accuracy" against a 56% majority class is worse than
    predicting nothing.
    """
    rng = np.random.default_rng(0)
    majority = 1.0 if (y_train > 0).mean() >= 0.5 else -1.0
    preds = {
        "always_up": np.ones_like(y_test, dtype=float),
        "always_down": -np.ones_like(y_test, dtype=float),
        "majority_class": np.full_like(y_test, majority, dtype=float),
        "random": rng.choice([-1.0, 1.0], size=len(y_test)),
    }
    if x_test_prev_return is not None:
        # Persistence: assume the last move continues. The standard strawman for
        # momentum, and often surprisingly hard to beat at short horizons.
        preds["persistence"] = np.where(x_test_prev_return >= 0, 1.0, -1.0)
    return preds


def brier_score(y_true: np.ndarray, prob_up: np.ndarray) -> float:
    """Mean squared error of the probability forecast. Lower is better.

    Reported because accuracy ignores confidence entirely: a model that is
    right 55% of the time while claiming 95% certainty is dangerous in a way
    accuracy cannot express.
    """
    outcome = (y_true > 0).astype(float)
    return float(np.mean((prob_up - outcome) ** 2))


def calibration_bins(
    y_true: np.ndarray, prob_up: np.ndarray, bins: int = 10
) -> list[dict]:
    """Predicted probability versus realized frequency, bucketed."""
    outcome = (y_true > 0).astype(float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    rows = []
    for i in range(bins):
        mask = (prob_up >= edges[i]) & (
            prob_up < edges[i + 1] if i < bins - 1 else prob_up <= edges[i + 1]
        )
        if not mask.any():
            continue
        rows.append({
            "bin": f"[{edges[i]:.1f},{edges[i+1]:.1f})",
            "count": int(mask.sum()),
            "mean_predicted": float(prob_up[mask].mean()),
            "observed_frequency": float(outcome[mask].mean()),
        })
    return rows


def evaluate(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    features_dropped: list[str] | None = None,
    model: str = "logistic",
    folds: int = 5,
) -> EvaluationReport:
    """Walk-forward evaluation against naive baselines."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    report = EvaluationReport(
        sufficient_data=len(X) >= MIN_ROWS_FOR_METRICS,
        rows_available=len(X),
        rows_required=MIN_ROWS_FOR_METRICS,
        features_used=list(X.columns),
        features_dropped=features_dropped or [],
    )
    if not report.sufficient_data:
        report.notes.append(
            "Baseline before complexity, and data before either: no model was "
            "fitted, because a metric computed here would be noise presented "
            "with a decimal point."
        )
        return report

    X_values = X.to_numpy(dtype=float)
    y_values = y.to_numpy(dtype=float)
    prev_return = X["return_1"].to_numpy(dtype=float) if "return_1" in X else None

    all_probs, all_true = [], []
    for fold, (train_idx, test_idx) in enumerate(
        walk_forward_splits(len(X_values), folds=folds), start=1
    ):
        if len(test_idx) < MIN_TEST_ROWS:
            continue
        estimator = (
            make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))
            if model == "logistic"
            else HistGradientBoostingClassifier(max_iter=200)
        )
        estimator.fit(X_values[train_idx], y_values[train_idx])
        prob_up = estimator.predict_proba(X_values[test_idx])[:, 1]
        predicted = np.where(prob_up >= 0.5, 1.0, -1.0)
        y_test = y_values[test_idx]

        baselines = baseline_predictions(
            y_values[train_idx],
            y_test,
            prev_return[test_idx] if prev_return is not None else None,
        )
        report.folds.append(
            FoldResult(
                fold=fold,
                train_rows=len(train_idx),
                test_rows=len(test_idx),
                model_accuracy=float((predicted == y_test).mean()),
                baseline_accuracies={
                    name: float((pred == y_test).mean())
                    for name, pred in baselines.items()
                },
                brier=brier_score(y_test, prob_up),
            )
        )
        all_probs.append(prob_up)
        all_true.append(y_test)

    if all_probs:
        report.calibration = calibration_bins(
            np.concatenate(all_true), np.concatenate(all_probs)
        )
    return report
