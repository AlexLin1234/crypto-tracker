# xstream

A real-time streaming pipeline that ingests L2 order book and trade data from
two crypto exchanges simultaneously, reconstructs live order books, computes
microstructure features, and detects cross-exchange price divergences — with a
deliberately self-skeptical evaluation layer.

**This is streaming data infrastructure and feature engineering. It is not a
trading bot.** The prediction task in Milestone 7 exists as a vehicle for
demonstrating evaluation rigor, not as a claim that anything here is
profitable.

> **Project status: Milestone 5 complete.** Ingestion, order book
> reconstruction, Spark aggregation, cross-exchange divergence, and a DuckDB
> analytical layer with compaction, data-quality checks and a one-command
> report. 130 tests pass. Milestone 6 (benchmarks) is next.

## Storage and analytics

```bash
uv run python -m xstream.analysis.report        # inventory + quality + 9 queries
uv run python -m xstream.analysis.compaction    # merge small files
```

DuckDB reads the Parquet directly, so the lake *is* the database — no load
step, no second copy. Compaction merged the current lake from 9 files to 2,
**80.8% smaller**, with row counts and values verified identical either side.
It only touches partitions whose event-time hour is strictly past, since a
streaming writer may still be appending to the current one, and it writes-then-
swaps-then-deletes so an interruption never leaves a partition with neither
copy.

### A sanity check caught a real bug

`queries/06_vwap_vs_close.sql` counts rows where VWAP falls outside its own
window's high-low range — arithmetically impossible for a correct
volume-weighted average. It returned **1**.

Spark caps decimal precision at 38 digits, and when an operation needs more it
keeps precision and sacrifices *scale*, down to a floor of six decimals.
Multiplying two `DECIMAL(38,18)` values needs precision 77, so the product was
silently rescaled to `DECIMAL(38,6)` — turning a quantity of `0.00009417` into
`0.000094` before the division that produces VWAP.

Milestone 3 shipped with this. Every unit test passed, because they compared
VWAP against expected values on inputs that happened not to trigger the
rescale. Only an *invariant* check found it. Fixed by using `DECIMAL(20,8)`,
whose products land at `DECIMAL(38,13)`; full writeup in
[`DECISIONS.md`](DECISIONS.md) D-023.

Two related notes: outliers are measured in median-absolute-deviation units
because an outlier inflates the standard deviation it would be judged against
(D-026), and ingest latency is explicitly flagged invalid on replayed data,
where it measures fixture age rather than network latency (D-024).

> **Milestone 4 note: its real-data output is empty.**
> Ingestion, order book reconstruction, Spark aggregation into a Parquet lake,
> and the cross-exchange divergence join with a net-of-costs honesty layer.
> 111 tests pass. The divergence job runs clean and emits **zero rows**, because
> the captured data has no cross-venue overlap — see "Cross-exchange divergence"
> below and [`DECISIONS.md`](DECISIONS.md) D-021. **No empirical claim about
> real divergence has been made or can be made from this data.**

## Cross-exchange divergence

Two venue streams aligned onto a shared event-time grid, then equi-joined on
`(symbol, window_start)`. Aligning first is what makes differing update rates
tractable — a raw event-to-event join either explodes combinatorially or needs
an arbitrary "nearest match" tie-break that silently decides the answer.

Clock skew sets the floor on window width: if venue clocks differ by more than
the window, the same instant lands in different windows and the join compares
mismatched pairs, silently, in a way that looks like real divergence. So every
joined row carries the observed `skew_ms`, making the assumption checkable
rather than merely asserted. A venue dropping out yields no row — the join is
inner, because a "divergence" against a missing venue is an outage, and mixing
the two would make outages indistinguishable from signal.

### The honesty layer

Every row carries `net_edge_bps`: gross divergence minus two taker fees minus
the half-spread crossed on each venue. Using published retail fees and the
spreads this pipeline actually measured:

```
26 bps (Kraken) + 40 bps (Binance.US) + 0.5 × (0.02 + 0.14) = 66.08 bps
```

**A divergence must exceed ~66 bps before a naive round trip breaks even.**
Typical divergences on liquid pairs are a few bps. That gap is an order of
magnitude, so a factor-of-two error in the fee assumptions changes nothing.

And `survives_costs = true` is far weaker than it sounds — it means only that
the most *favourable* accounting hasn't ruled a divergence out. Latency, queue
position, inventory pre-positioning, displayed size and adverse selection all
subtract further and none is modelled; they are enumerated in
`xstream.analysis.economics.EXPLOITABILITY_CAVEATS` so the omissions are
explicit.

### What this project has not shown

The job produces **no rows on the captured data**, for two independent reasons:
Kraken traded only BTC-USD and Binance.US only ETH-USD (no shared symbol), and
only Kraken's books are seedable (D-012), so every snapshot row is `kraken`. A
cross-exchange detector needs two exchanges.

Join semantics are tested against synthetic rows — correct for testing pair
ordering, sign conventions and dropout handling, which are properties of the
code rather than of the market. But **no real cross-venue divergence has been
observed here**, so this project makes no empirical claim about divergence
magnitude, frequency or duration. Closing that needs a Binance REST snapshot
payload and a capture long enough to contain trades on a shared symbol.

## Stream processing

```
orderbook.raw ──► snapshotter (stateful, checksum-verified) ──► orderbook.snapshots ──┐
                                                                                       ├─► Spark ─► Parquet ─► DuckDB
trades.raw ───────────────────────────────────────────────────────────────────────────┘
```

Reconstruction sits **in front of** Spark, not inside it. The first version
derived spread and imbalance from raw deltas in Spark and was wrong: both
venues send only *changed* levels, so a delta's first bid is an arbitrary moved
level, not the best bid. The output said so loudly — Binance BTC-USD averaged a
**$216 spread** against Kraken's **$0.10**. A 1,750x gap between two liquid
venues is not a market phenomenon.

Had it been merely plausible — 3 bps against 0.5 bps — it would have shipped,
and the Milestone 4 divergence analysis would have rested on a feature that
does not mean what its name says. The full writeup is
[`DECISIONS.md`](DECISIONS.md) D-017.

After the fix, Kraken BTC-USD averages a $0.10 spread (0.02 bps) and ETH-USD
$0.024 (0.14 bps), with zero crossed books, zero gaps and zero checksum
failures across every emitted row.

Other decisions worth reading: the watermark policy and what it drops (D-015),
why realized volatility is deliberately a labelled proxy (D-016), event-time
rather than wall-clock sampling so replay is deterministic (D-018), and where
the single shuffle lives (D-019).

## Order book reconstruction

The stateful core, and the part most likely to be subtly wrong — so it is
verified against the venue's own integrity mechanism rather than against my
expectations.

**Replaying the captured Kraken session reproduces all 474 checksums with zero
mismatches** — one snapshot plus 472 incremental updates, with the CRC32
recomputed and compared after every single one. That single result verifies two
things simultaneously: that the checksum algorithm (derived empirically, since
Kraken's docs are unreachable — D-011) is correct, and that the book
reconstruction is correct. They cannot both be wrong in a way that agrees 474
consecutive times.

The two venues fail differently, so they are detected differently:

| | Kraken | Binance.US |
| --- | --- | --- |
| Mechanism | CRC32 over top-10 state | `U`/`u` update IDs |
| Says | "your book is wrong" | "you missed messages" |
| Checked | **after** applying | **before** applying |

Sequence gaps are checked before mutating, so a detected gap leaves the last
known-good book intact. Checksums describe the resulting state, so they can
only be verified after. Both are tested.

On any detected loss the book goes **STALE and stops** — it does not patch or
interpolate. A book that keeps serving after known corruption is worse than one
that halts, because every downstream consumer would get plausible numbers with
no signal they are wrong (D-013).

## Exchanges

**Kraken v2 + Binance.US.** The original plan expected Kraken + Coinbase, since
both are public and no-auth — a prior that held, as all three venues connected
cleanly. The choice turned on something reconnaissance had to discover:
Coinbase's L2 feed carries **no sequence number and no checksum**, so a dropped
book update cannot be detected at all. Kraken carries a CRC32 checksum and
Binance.US carries sequence IDs, which means order book gap detection — the
defining requirement of Milestone 2 — can be built and tested on both venues
rather than one.

The two venues also verify integrity in genuinely different ways, so the order
book layer has to abstract over checksum-based verification *and*
sequence-based gap detection. Full reasoning, including what this costs
(Binance.US is a thin venue, which will colour the divergence analysis), is in
[`DECISIONS.md`](DECISIONS.md) D-004 and D-005.

## Planned architecture

```
exchange A ─┐                     ┌─ trades.raw ─┐
            ├─ ingest (asyncio) ─►│              ├─ Spark Structured Streaming ─► Parquet lake ─► DuckDB
exchange B ─┘   normalize +       └─ orderbook.  ┘   windowed aggregates,           (date/hour/
                dual timestamps      raw              stream-stream join             exchange/symbol)
                                  (Redpanda)         divergence detection
```

Diagram gets replaced with a real one at Milestone 8, per the plan.

## Tech stack

Python 3.11+ / `uv`, `websockets` for ingestion, Redpanda as the broker,
PySpark Structured Streaming for processing, Parquet on local disk partitioned
by date/hour, DuckDB for analytics, `pytest`, Docker Compose for local infra.

## Quickstart

```bash
uv sync
docker compose up -d          # Redpanda + Console
./scripts/create_topics.sh    # trades.raw, orderbook.raw, divergence.events

# one process per venue
uv run python -m xstream.ingest --exchange kraken     --brokers localhost:19092
uv run python -m xstream.ingest --exchange binance_us --brokers localhost:19092
```

Then open the Redpanda Console at <http://localhost:8080> to inspect topics.

Redpanda is not required — the pipeline speaks the Kafka protocol, so any
Kafka-compatible broker works. Point `--brokers` at whatever is running.

Omit `--brokers` to normalize and count messages without producing — useful for
checking a feed without standing up infrastructure.

### Processing

```bash
# reconstruct books and publish top-of-book snapshots
uv run python -m xstream.orderbook.snapshotter --brokers localhost:19092

# windowed aggregates -> Parquet
uv run python -m xstream.processing.trades_job --brokers localhost:19092
uv run python -m xstream.processing.book_job   --brokers localhost:19092
```

Output lands in `data/lake/{candles_1s,candles_10s,candles_1m,book_features}`,
partitioned `date/hour/exchange/symbol` — coarsest first, so a time-bounded
query prunes whole directories before opening a file. Query it directly:

```sql
SELECT exchange, symbol, avg(spread), avg(book_imbalance)
FROM read_parquet('data/lake/book_features/**/*.parquet', hive_partitioning=true)
GROUP BY 1, 2;
```

### Replay without a live connection

The captured fixtures can be pushed through the exact same connector,
normalizer and sink that production uses:

```bash
uv run python -m xstream.ingest.replay                        # in memory
uv run python -m xstream.ingest.replay --brokers localhost:19092
```

This is how the pipeline is verified in environments that cannot reach the
exchanges, and it is what Milestone 2's deterministic order book tests and
Milestone 6's accelerated load tests build on.

### Reconnaissance

```bash
uv run python scripts/recon/kraken.py --limit 500
```

Check the exit code before committing samples — exit `3` means the venue
rejected the subscription and the frames are errors, not market data. See
[`docs/samples/README.md`](docs/samples/README.md). If the scripts report
`proxy rejected connection: HTTP 403`, you are behind a restrictive egress
policy — see [`DECISIONS.md`](DECISIONS.md) D-000.

## Repository layout

```
scripts/recon/          per-exchange connectivity probes (Milestone 0)
scripts/create_topics.sh
src/xstream/ingest/     connectors, normalization, producer, runner
  schema.py             the common internal schema every venue maps onto
  base.py               the shared connector interface
  kraken.py             Kraken v2 dialect
  binance_us.py         Binance.US dialect
  producer.py           topic routing, partition keying, Redpanda sink
  runner.py             connect/reconnect/metrics/shutdown
  replay.py             fixture replay through the real path
tests/                  pytest suite
docs/samples/           captured raw frames, used as test fixtures
docs/SCHEMAS.md         real message schemas per venue, from captured data
docker-compose.yml      Redpanda + Console
DECISIONS.md            running log of design decisions and tradeoffs
```

## The normalization layer

The two venues disagree on essentially every representational choice: envelope
shape, number encoding, timestamp format, symbol spelling, level structure, and
how book integrity is verified at all. `schema.py` is where those disagreements
are resolved exactly once.

| | Kraken v2 | Binance.US |
| --- | --- | --- |
| Subscribe | frame, acked per (channel, symbol) | **URL path**, no ack |
| Envelope | `channel` + `type` | `stream` + `data` |
| Numbers | **JSON floats** | decimal strings |
| Timestamps | ISO 8601 | epoch millis |
| Symbols | `BTC/USD` | `BTCUSD` |
| Book integrity | **CRC32 checksum** | **update IDs** |

Two consequences worth calling out, both of which would be silent bugs:

- Kraken's JSON numbers become binary floats inside `json.loads` unless it is
  told otherwise. Order books key on exact price equality, so a price one ULP
  off is a *different level* — removals miss and the book accumulates phantom
  levels. Everything parses with `parse_float=Decimal`, and `Decimal` survives
  to the broker as a string rather than a JSON number.
- Binance's `m` flag means "the buyer is the maker", so `m: true` is a **sell**
  aggression. Inverting it would flip the volume-imbalance feature's sign with
  nothing downstream to catch it.

Because neither venue has both integrity mechanisms, `BookDelta` carries both
as optional fields rather than collapsing them into one — the pipeline should
not pretend a checksum and a sequence number are the same thing.

## Documentation

- [`DECISIONS.md`](DECISIONS.md) — design decisions and tradeoffs as they are made
- `BENCHMARKS.md` — throughput and latency percentiles (Milestone 6)
- `EVALUATION.md` — prediction task, and the ways it was checked (Milestone 7)
