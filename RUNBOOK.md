# What is needed to get this running for real

Everything in this repo runs today. What it cannot do today is produce
*meaningful numbers*, because two pieces of data were never obtainable from the
environment it was built in. This is the concrete list, ordered by what unblocks
the most.

Nothing here is a code defect. Each item is either data that must be captured,
or a setting on a machine.

---

## 1. Blockers — nothing downstream is real without these

### 1.1 Capture a Binance REST order book snapshot

**Why:** Binance's `@depth` websocket stream carries **diffs only**. A book
cannot be built from it alone. The documented procedure is: buffer diffs, fetch
a REST snapshot, discard buffered diffs older than the snapshot's
`lastUpdateId`, apply the rest.

That REST host was unreachable during development, so the seeding path was
deliberately **not written against a guessed schema** — the same mistake
Milestone 0 exists to prevent (D-012).

**Consequence today:** Binance books never reach READY, the snapshotter
correctly refuses to emit them, and every snapshot row is `kraken`. That alone
makes the cross-exchange divergence join produce zero rows.

**Do this:**

```bash
curl 'https://api.binance.us/api/v3/depth?symbol=BTCUSD&limit=1000' \
  > docs/samples/binance_depth_snapshot.json
curl 'https://api.binance.us/api/v3/depth?symbol=ETHUSD&limit=1000' \
  >> docs/samples/binance_depth_snapshot.json
```

Commit the file and the seeding path can be written against a known shape —
roughly 40 lines in `src/xstream/orderbook/`, plus tests, matching the existing
`OrderBook.apply(snapshot)` interface which already exists and is tested.

**Effort:** one command from you; ~1 hour of work after that.

### 1.2 Capture a long session from both venues

**Why:** the committed fixture is **40 seconds** of quiet market: 500 frames per
venue, 8 trades total, and — critically — **no symbol traded on both venues**.
Kraken printed one BTC-USD trade; Binance printed seven ETH-USD trades. A
cross-exchange detector needs an overlapping symbol.

**Consequence today:** divergence is empty (D-021), candles are sparse, the
model has one usable feature row (EVALUATION.md), and Milestone 6 could not
load-test beyond a single burst (D-014).

**Do this**, on a machine with exchange access, for at least a few hours and
ideally overnight:

```bash
uv run python scripts/recon/kraken.py     --limit 2000000
uv run python scripts/recon/binance_us.py --limit 2000000
```

Check the exit code before committing: **exit 3 means the venue rejected the
subscription** and the frames are errors, not market data.

These files will be large. `.gitignore` already excludes `recordings/`; put bulk
captures there and keep `docs/samples/*.jsonl` as the small committed fixtures.

**Effort:** one command plus wall-clock time.

---

## 2. Environment — only if running in a cloud session

Everything in this section is unnecessary on a normal laptop.

### 2.1 Allow the exchange domains

Cloud environments default to **Trusted** network access, which covers package
registries and GitHub and nothing else. The probes fail there with
`proxy rejected connection: HTTP 403`.

At [claude.ai/code](https://claude.ai/code), open the environment selector
(the cloud icon above the message box), edit the environment, set **Network
access** to **Custom**, and add:

```
*.kraken.com
*.binance.us
production.cloudfront.docker.com
```

**Check "Also include default list of common package managers"** — without it
you allow only those three lines, which breaks PyPI and GitHub.

The third domain is Docker Hub's layer CDN. Without it **no container image can
be pulled at all** (not even `alpine`): manifests fetch from `index.docker.io`
but blobs come from a `cloudfront` host that is not on the default list.

Network changes apply to **new sessions** — a running session keeps the policy
it started with.

### 2.2 Running without Docker

If containers remain unavailable, Apache Kafka runs directly from
`downloads.apache.org`, which *is* on the default allowlist, and is what this
project's own verification used:

```bash
curl -sSLO https://downloads.apache.org/kafka/4.1.2/kafka_2.13-4.1.2.tgz
tar xzf kafka_2.13-4.1.2.tgz && cd kafka_2.13-4.1.2
KRAFT_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t "$KRAFT_ID" -c config/server.properties --standalone
bin/kafka-server-start.sh config/server.properties
```

Then point every `--brokers` at `localhost:9092`. Redpanda and Kafka are
protocol-identical here, so nothing else changes.

---

## 3. Work unblocked by the data above

In dependency order. None of it is speculative — each has a defined
interface waiting.

| # | Task | Unblocked by | Effort |
| --- | --- | --- | --- |
| 3.1 | Binance book seeding from the REST snapshot | 1.1 | ~1 h |
| 3.2 | Re-run the pipeline end to end; confirm both venues reach READY | 3.1 | ~15 min |
| 3.3 | First **real** divergence numbers; fill in `queries/07` and `divergence_duration.sql` | 1.1 + 1.2 | ~30 min |
| 3.4 | Latency percentiles (p50/p95/p99) — needs **live** ingest, not replay (D-024, D-030) | 1.2 + live run | ~30 min |
| 3.5 | Re-run the model; report a real evaluation or a documented null result | 1.2 | ~30 min |
| 3.6 | 10x/100x load tests against a long recording, to find the real breaking point | 1.2 | ~1 h |
| 3.7 | Record the demo GIF for the README | live pipeline | ~30 min |

**On 3.4:** latency cannot be back-filled from recordings. On replayed data the
venue-to-ingest gap is the *age of the capture* — the pipeline reported a p50 of
about nine days. It has to be measured while ingesting live.

**On 3.5:** the honest expected outcome is "weak or no signal, not exploitable
after costs". The harness refuses to report below 500 usable rows and is
validated to detect a planted signal (+0.38 lift) while rejecting a random walk
(−0.04). A null result there is a finding, not a failure.

---

## 4. Verify a working setup

```bash
uv sync
uv run --group dev pytest -q                     # expect 159 passed
uv run python -m xstream.ingest.replay           # 1000 frames -> 975 messages, 0 errors
uv run python -m xstream.analysis.report         # inventory + quality + queries
uv run python -m xstream.bench                   # per-stage throughput
```

With a broker running, add `--brokers localhost:19092` to the replay and bench
commands. Health signals to look for:

- `errors=0` from the replay
- `checksum_failures: 0` and `gaps: 0` from the snapshotter
- `impossible_vwap_rows` **0** in `queries/06` — this is the invariant that
  caught the Spark decimal bug (D-023)
- `measurement_valid` in `queries/03` reading `NO` on replayed data; it should
  flip to `yes` only under live ingest

---

## 5. Deliberately not done

Listed so their absence is not mistaken for oversight.

- **Demo GIF** — needs a live pipeline and screen capture.
- **Coinbase connector** — the recon probe and schema documentation exist, but
  Coinbase's L2 feed has no sequence number and no checksum, so gap detection
  is impossible on it (D-004). It stays a third-choice venue.
- **Exactly-once delivery** — at-least-once was chosen deliberately (D-009).
- **Spark-side book reconstruction** — it lives in front of Spark on purpose
  (D-017).
- **Airflow / dbt / Kubernetes / Flink** — scope creep the plan explicitly ruled
  out, and nothing here needs them.
