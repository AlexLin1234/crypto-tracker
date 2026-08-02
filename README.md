# xstream

A real-time streaming pipeline that ingests L2 order book and trade data from
two crypto exchanges simultaneously, reconstructs live order books, computes
microstructure features, and detects cross-exchange price divergences — with a
deliberately self-skeptical evaluation layer.

**This is streaming data infrastructure and feature engineering. It is not a
trading bot.** The prediction task in Milestone 7 exists as a vehicle for
demonstrating evaluation rigor, not as a claim that anything here is
profitable.

> **Project status: Milestone 1 complete.** Reconnaissance done and schemas
> documented from real captures ([`docs/SCHEMAS.md`](docs/SCHEMAS.md));
> ingestion service built with per-venue connectors behind a shared interface,
> normalization into a common schema, Redpanda producer, reconnect with
> backoff, and graceful shutdown. 65 tests pass. Milestone 2 (order book
> reconstruction) is next.

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
