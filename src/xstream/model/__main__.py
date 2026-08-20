"""Run the prediction task and print the evaluation.

    uv run python -m xstream.model --horizon 5

Prints, in order: the lookahead audit, the data available, and the evaluation.
If there is not enough data it says so and stops, which on the current lake is
the expected and correct outcome.
"""

from __future__ import annotations

import argparse
import pathlib

from ..analysis.lake import DEFAULT_LAKE
from .evaluation import evaluate
from .features import (
    FEATURE_NAMES,
    audit_lookahead,
    build_features,
    load_book_features,
    load_divergence,
    usable_matrix,
)

RULE = "=" * 78


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Mid-price direction prediction")
    parser.add_argument("--lake", type=pathlib.Path, default=DEFAULT_LAKE)
    parser.add_argument("--horizon", type=int, default=5,
                        help="windows ahead to predict")
    parser.add_argument("--model", default="logistic",
                        choices=["logistic", "gradient_boosting"])
    args = parser.parse_args(argv)

    books = load_book_features(args.lake)
    divergence = load_divergence(args.lake)

    print(f"{RULE}\nLOOKAHEAD AUDIT\n{RULE}")
    findings = audit_lookahead(books, horizon=args.horizon)
    for finding in findings:
        marker = {"clean": "ok", "LOOKAHEAD": "FAIL", "skipped": "--"}[finding["status"]]
        print(f"  [{marker:>4}] {finding['feature']}: {finding['detail']}")
    leaks = [f for f in findings if f["status"] == "LOOKAHEAD"]
    if leaks:
        print(f"\n  {len(leaks)} feature(s) read future data. Refusing to evaluate.")
        return 1

    frame = build_features(books, divergence, horizon=args.horizon)
    X, y, used = usable_matrix(frame)
    dropped = [f for f in FEATURE_NAMES if f not in used]

    print(f"\n{RULE}\nDATA\n{RULE}")
    print(f"  book_features rows:   {len(books):,}")
    print(f"  rows with a target:   {len(frame):,}  (horizon {args.horizon})")
    print(f"  complete feature rows:{len(X):,}")
    print(f"  features used:        {', '.join(used) if used else '(none)'}")
    if dropped:
        print(f"  features dropped:     {', '.join(dropped)}")
        print("    (entirely null on this lake; dropped rather than imputed, "
              "since imputing a column with no data invents structure)")

    report = evaluate(X, y, features_dropped=dropped, model=args.model)

    print(f"\n{RULE}\nEVALUATION\n{RULE}")
    print(f"  {report.verdict()}")
    for note in report.notes:
        print(f"  note: {note}")

    if report.folds:
        print("\n  fold  train  test   model   best baseline           lift    brier")
        print("  ----  -----  ----   -----   ---------------------   -----   -----")
        for f in report.folds:
            name, acc = f.best_baseline
            print(f"  {f.fold:>4}  {f.train_rows:>5}  {f.test_rows:>4}   "
                  f"{f.model_accuracy:.3f}   {name:<15} {acc:.3f}   "
                  f"{f.lift:+.3f}   {f.brier:.3f}")

    if report.calibration:
        print("\n  calibration (predicted vs observed)")
        for row in report.calibration:
            print(f"    {row['bin']:>12}  n={row['count']:<5} "
                  f"predicted={row['mean_predicted']:.3f}  "
                  f"observed={row['observed_frequency']:.3f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
