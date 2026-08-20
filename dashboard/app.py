"""Live-ish dashboard over the Parquet lake.

    uv sync --extra dashboard
    uv run streamlit run dashboard/app.py

Deliberately not gold-plated. It reads the same DuckDB views the report uses,
so there is no second copy of the query logic to drift, and it shows the four
things a reviewer actually wants to see in sixty seconds: what is in the lake,
current book state, spread and imbalance over time, and whether any divergence
cleared the cost floor.

**It reflects whatever the pipeline has written.** Where a dataset is empty --
divergence is, on the current lake, for the reasons in D-021 -- the panel says
so rather than rendering an empty chart that looks like "no divergences
occurred" instead of "this was never populated". An empty chart and an absent
dataset mean very different things and the UI should not conflate them.
"""

from __future__ import annotations

import pathlib

import pandas as pd
import streamlit as st

from xstream.analysis.lake import DEFAULT_LAKE, connect, discover
from xstream.analysis.quality import run_all

st.set_page_config(page_title="xstream", layout="wide")

LAKE = pathlib.Path(st.sidebar.text_input("Lake path", str(DEFAULT_LAKE)))
REFRESH = st.sidebar.number_input("Auto-refresh (seconds, 0 = off)", 0, 600, 0)
if REFRESH:
    st.sidebar.caption("Re-run the page to refresh; the lake is read on each run.")


@st.cache_data(ttl=5)
def load(dataset: str, lake: str) -> pd.DataFrame:
    con = connect(pathlib.Path(lake))
    try:
        return con.execute(f"SELECT * FROM {dataset}").df()
    except Exception:
        return pd.DataFrame()


st.title("xstream — cross-exchange market data pipeline")
st.caption(
    "Streaming infrastructure and feature engineering. Not a trading bot; "
    "the prediction task exists to demonstrate evaluation rigor."
)

# --- inventory ---------------------------------------------------------------
datasets = discover(LAKE)
present = [i for i in datasets.values() if i.present]
cols = st.columns(4)
cols[0].metric("Datasets present", f"{len(present)}/{len(datasets)}")
cols[1].metric("Parquet files", sum(i.files for i in present))
cols[2].metric("Lake size", f"{sum(i.bytes for i in present):,} B")
avg = (sum(i.bytes for i in present) / sum(i.files for i in present)) if present else 0
cols[3].metric("Avg file size", f"{avg:,.0f} B",
               help="Columnar formats want hundreds of MB; small files are the "
                    "compaction target.")

missing = [name for name, info in datasets.items() if not info.present]
if missing:
    st.warning(
        f"Absent datasets: {', '.join(missing)}. Absent is a finding, not an "
        "error — see DECISIONS.md D-021."
    )

# --- book state --------------------------------------------------------------
st.header("Order book state")
books = load("book_features", str(LAKE))
if books.empty:
    st.info("No book_features yet. Run the snapshotter and the book job.")
else:
    latest = (
        books.sort_values("window_start")
        .groupby(["exchange", "symbol"], as_index=False)
        .last()
    )
    st.dataframe(
        latest[["exchange", "symbol", "window_start", "best_bid", "best_ask",
                "spread", "mid_price", "book_imbalance"]],
        use_container_width=True, hide_index=True,
    )

    pair = st.selectbox(
        "Series", sorted({f"{r.exchange}:{r.symbol}" for r in books.itertuples()})
    )
    exchange, symbol = pair.split(":")
    series = books[(books.exchange == exchange) & (books.symbol == symbol)]
    series = series.sort_values("window_start").set_index("window_start")

    left, right = st.columns(2)
    with left:
        st.subheader("Relative spread (bps)")
        st.line_chart(series[["avg_relative_spread_bps"]])
    with right:
        st.subheader("Book imbalance")
        st.line_chart(series[["book_imbalance"]])

    gaps = int(series["gap_count"].max()) if "gap_count" in series else 0
    fails = int(series["checksum_failures"].max()) if "checksum_failures" in series else 0
    st.caption(
        f"Integrity over this series — sequence gaps: {gaps}, checksum "
        f"failures: {fails}. Both should be zero; a non-zero value means a book "
        "went stale and stopped serving rather than serving corrupt state."
    )

# --- divergence --------------------------------------------------------------
st.header("Cross-exchange divergence")
divergence = load("divergence", str(LAKE))
if divergence.empty:
    st.warning(
        "No divergence rows. The job runs correctly and emits nothing because "
        "the captured data has no cross-venue overlap: only one venue produces "
        "book state, and the two venues traded different symbols. A "
        "cross-exchange detector needs two exchanges (D-021)."
    )
else:
    flagged = int(divergence["exceeds_threshold"].sum())
    tradeable = int(divergence["survives_costs"].sum())
    cols = st.columns(3)
    cols[0].metric("Joined windows", len(divergence))
    cols[1].metric("Above threshold", flagged)
    cols[2].metric(
        "Clears cost floor", tradeable,
        help="Gross divergence minus two taker fees and the half-spread crossed "
             "on each venue. Expected to be ~0 on liquid pairs.",
    )
    st.line_chart(
        divergence.sort_values("window_start").set_index("window_start")[
            ["divergence_magnitude_bps", "cost_floor_bps"]
        ]
    )
    st.caption(
        "The cost floor is the line that matters. A divergence below it is "
        "definitively not exploitable; above it is merely not yet ruled out."
    )

# --- data quality ------------------------------------------------------------
st.header("Data quality")
try:
    checks = run_all(connect(LAKE), LAKE)
    frame = pd.DataFrame(
        [{"dataset": c.dataset, "check": c.check,
          "status": "pass" if c.passed else "warn", "detail": c.detail}
         for c in checks]
    )
    warns = int((frame.status == "warn").sum())
    st.metric("Warnings", warns, help="Warnings describe the data, not a broken pipeline.")
    st.dataframe(frame, use_container_width=True, hide_index=True)
except Exception as exc:  # noqa: BLE001 - a dashboard must not die on a bad view
    st.error(f"Quality checks unavailable: {exc}")

st.divider()
st.caption(
    "Benchmarks in BENCHMARKS.md · design decisions in DECISIONS.md · "
    "evaluation in EVALUATION.md"
)
