# xstream

A real-time streaming pipeline that ingests L2 order book and trade data from
two crypto exchanges simultaneously, reconstructs live order books, computes
microstructure features, and detects cross-exchange price divergences — with a
deliberately self-skeptical evaluation layer.

**This is streaming data infrastructure and feature engineering. It is not a
trading bot.** The prediction task in Milestone 7 exists as a vehicle for
demonstrating evaluation rigor, not as a claim that anything here is
profitable.

> **Project status: Milestone 0 (reconnaissance), partially complete and
> currently blocked.** Repo scaffold and the per-exchange recon scripts are
> done. Live feed verification could not be performed in the cloud development
> environment because its egress policy denies all exchange hosts. See
> [`DECISIONS.md`](DECISIONS.md) entry D-000. Nothing downstream has been built
> — by design, since exchange message schemas are still unverified.

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

## Quickstart (reconnaissance only, so far)

```bash
uv sync
uv run python scripts/recon/kraken.py      # prints 10 live frames, saves samples
uv run python scripts/recon/coinbase.py
uv run python scripts/recon/binance_us.py
```

Pass `--limit N` to capture more frames. Check the exit code before committing
any samples — exit `3` means the venue rejected the subscription and the
captured frames are errors, not market data. See
[`docs/samples/README.md`](docs/samples/README.md).

Requires outbound network access to the exchanges. If the scripts report
`proxy rejected connection: HTTP 403`, you are behind a restrictive egress
policy — see [`DECISIONS.md`](DECISIONS.md) D-000.

## Repository layout

```
scripts/recon/     per-exchange connectivity probes (Milestone 0)
src/xstream/       pipeline packages (Milestones 1+)
tests/             pytest suite
docs/samples/      captured raw frames, used as test fixtures
docker/            Redpanda + Console compose stack (Milestone 1)
DECISIONS.md       running log of design decisions and tradeoffs
```

## Documentation

- [`DECISIONS.md`](DECISIONS.md) — design decisions and tradeoffs as they are made
- `BENCHMARKS.md` — throughput and latency percentiles (Milestone 6)
- `EVALUATION.md` — prediction task, and the ways it was checked (Milestone 7)
