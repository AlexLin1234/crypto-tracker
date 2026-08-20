# Evaluation

**Conclusion first: this project has produced no empirical finding about
predictability, because the available data cannot support one.** The lake holds
16 order book feature rows from a single venue, which yields exactly **one**
complete feature row after target construction. The harness refuses to report
metrics below a 500-row minimum, and refuses correctly.

That is the honest result, and this document is mostly about the machinery
built to make sure that any *future* result is honest too.

```bash
uv run python -m xstream.model --horizon 5
```

---

## What the real data supports

```
book_features rows:    16
rows with a target:     2   (horizon 5)
complete feature rows:  1
features dropped:       divergence_bps  (entirely null — see D-021)

INSUFFICIENT DATA: 1 usable rows against a 500-row minimum. No metrics
reported, because an estimate this noisy would be indistinguishable from signal.
```

The 500-row floor is not arbitrary. With a binary target the standard error on
an accuracy estimate is roughly `0.5/sqrt(n)`: at 500 rows that is ±2.2
percentage points, already the same size as any plausible microstructure edge.
Below a few hundred rows the estimate carries no information, and printing it
with a decimal point invites exactly the over-reading this milestone exists to
prevent.

`divergence_bps` is dropped rather than imputed. It is null throughout because
no cross-venue rows exist (D-021), and filling it with zeros would tell a model
the venues always agree — inventing structure out of absence.

---

## The five ways this tries not to fool itself

### 1. No random splits, ever

Shuffling time-series rows lets a model train on the future and test on the
past. `walk_forward_splits` provides expanding-window splits only, and there is
no code path that shuffles — `KFold` is not imported, deliberately, so the
wrong choice is not available to a hurried future edit.

Two properties are asserted by test: every test block lies **strictly after**
its training data, and test blocks never overlap.

### 2. The lookahead audit is empirical, not a code review

Every feature declares the last observation it may use (`FeatureSpec`), and a
positive offset raises at construction — a lookahead bug by definition.

Declarations are claims, so `audit_lookahead` checks them by experiment:
corrupt a block of later rows, rebuild the feature matrix, and confirm every
feature value on earlier rows is unchanged. Anything that moves is reading
forward.

**The first version of this audit could not detect anything.** It perturbed the
final rows — which `build_features` drops for having no target — so the
corruption never reached the output, and it reported "clean" for a deliberately
planted leak. A detector that cannot fire is worse than none, because it reads
as reassurance. It now matches rows on `window_start` rather than position, and
corrupts a block early enough to survive into the built matrix. A test plants a
`shift(-1)` leak and requires the audit to catch it (D-031).

### 3. Lift over baselines, never raw accuracy

"54% accuracy" is meaningless without knowing the majority class. Four naive
predictors are scored on every fold — always-up, always-down, majority-class,
random, and persistence (assume the last move continues) — and results are
reported as lift over the **best** of them.

A negative lift is reported as **NO SIGNAL**, not by magnitude. An earlier
version described a −0.04 lift as "weak signal" purely because it sat far
enough from zero; a fitted model that loses to always-down is evidence against
the feature set, not for it.

### 4. Calibration, not just ranking

Brier score and reliability bins are reported. A model can rank well and still
be badly calibrated, which matters more than accuracy when the output feeds a
decision with costs: being right 55% of the time while claiming 95% certainty
is dangerous in a way accuracy cannot express.

### 5. Costs, applied to any claim

Statistical detectability is not exploitability. Any lift must clear the cost
floor measured in Milestone 4: **~66 bps** for the Kraken/Binance.US pair
(D-022), against typical liquid-pair divergences of a few bps. The verdict
string says so rather than leaving the reader to remember it.

---

## Harness validation on synthetic data

The real lake cannot exercise any of the above, so the harness is validated
against data whose answer is known by construction. This is the difference
between "the code runs" and "the code works".

| scenario | rows | mean lift | SE | Brier | verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| Random walk (no signal) | 2,993 | **−0.0394** | 0.0195 | 0.257 | NO SIGNAL |
| Imbalance predicts next move | 2,997 | **+0.3800** | 0.0085 | 0.073 | SIGNAL DETECTED |

On the planted signal the model reaches 0.879 accuracy against a 0.523 best
baseline. On the random walk it reaches 0.502 against 0.543 — it **loses** to
always-down, and is reported as such.

The separation is the point: a harness that only ever says "no signal" is
indistinguishable from a broken one, so it has to be shown capable of finding
signal that is genuinely there. Brier scores move the right way too — 0.073
when the probabilities are informative, 0.257 (near the 0.25 of an
uninformative forecast) when they are not.

---

## What I do not trust, and why

- **Nothing about real predictability.** One usable row. No claim is made and
  none can be.
- **The volatility feature is a proxy, not realized volatility.** It is a
  within-window standard deviation of prices, not a returns-based estimator,
  and it is labelled that way (D-016). A model consuming it is consuming
  dispersion, not volatility.
- **Single venue.** Every feature comes from Kraken. `divergence_bps` — the
  feature this whole pipeline was built to produce — is null throughout.
- **Forty seconds of quiet market.** Even with more rows, one short window in
  one regime supports no claim about behaviour in another.
- **The synthetic validation proves the harness, not the market.** Planted
  signal is detected because it was planted. It says nothing about whether
  such structure exists in real books.

## How I would validate this properly

1. **Weeks of continuous capture across both venues**, not minutes. Enough to
   cover multiple volatility regimes, weekends, and at least one venue outage.
2. **Hold out a final period entirely** and touch it once, at the end. Every
   number above comes from walk-forward folds, which are re-run during
   development and therefore accumulate selection bias with each iteration.
3. **Purge and embargo around fold boundaries.** With a 5-window horizon, rows
   near a boundary share overlapping target windows with the training set —
   a subtle leak that walk-forward splitting alone does not remove.
4. **Multiple symbols and venues**, to distinguish a genuine microstructure
   effect from one instrument's quirk.
5. **Report a full cost-adjusted distribution**, not a mean. The relevant
   question is never "is there an edge on average" but "what fraction of
   opportunities clear the cost floor, and how large are they when they do".
6. **Pre-register the feature set and horizon** before looking at results.
   Choosing a horizon after seeing which one worked is the most common way an
   evaluation this careful still ends up overfitted.

The expected honest conclusion, with real data in hand, is *weak signal at best,
not exploitable after costs*. That would be a more credible outcome than a high
accuracy number, and any interviewer who knows markets should trust it more.
