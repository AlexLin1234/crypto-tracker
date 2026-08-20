# Design Decisions and Tradeoffs

Running log. Each entry records the decision, the alternatives considered, and
why. This is the primary source material for the README's design section.

---

## D-000: Milestone 0 blocked in the cloud dev environment — exchange egress denied

**Status:** RESOLVED 2026-08-02. The recon scripts were run on a machine with
normal egress (option 1 below) and real captures for all three venues are
committed to `docs/samples/`. All three subscribe payloads were accepted — no
error frames in 1,004 captured frames — so the unconfirmed payloads written in
D-000's shadow turned out to be correct. Schemas are documented in
`docs/SCHEMAS.md`. The cloud environment itself remains unable to reach the
exchanges; that is a network-policy setting (**Trusted** access level), not a
code problem, and is fixed by switching the environment to **Custom** access
with the exchange domains allowlisted.

**Date:** 2026-08-02

### What happened

Milestone 0 requires verifying live WebSocket access to two exchanges before
any pipeline code is written. In this environment that verification **cannot be
performed**: outbound network access is mediated by a policy-enforcing egress
proxy that denies every exchange host tested.

All outbound HTTPS goes through a local proxy (`HTTPS_PROXY=127.0.0.1:43151`)
which tunnels to an organization-policy gateway. Every candidate host was
rejected at the `CONNECT` stage:

| Host | Result |
| --- | --- |
| `api.kraken.com:443` | `403` at CONNECT |
| `ws.kraken.com:443` | `403` at CONNECT |
| `api.exchange.coinbase.com:443` | `403` at CONNECT |
| `ws-feed.exchange.coinbase.com:443` | `403` at CONNECT |
| `advanced-trade-ws.coinbase.com:443` | `403` at CONNECT |
| `api.binance.us:443` | `403` at CONNECT |
| `stream.binance.us:443` | `403` at CONNECT |
| `api.binance.com:443` | `403` at CONNECT |

This is an **allowlist**, not an exchange-specific block or a geo-block:
`example.com` is denied too, while `pypi.org` and `api.github.com` return `200`.
The allowlist appears to cover package registries and GitHub only.

The proxy's own documentation is explicit that a 403 is an organization policy
denial and must not be retried or routed around, so no workaround was attempted.

### Why this was not worked around

Three things were considered and rejected:

1. **Fabricating sample payloads.** Milestone 0's samples become the test
   fixtures for Milestone 1 normalization and Milestone 2 order book replay.
   Invented fixtures would encode invented schemas, the normalizers would be
   written to satisfy them, the tests would pass, and the whole thing would
   fail on first contact with a real socket — after four milestones of work
   had been built on top. A fixture that is wrong is strictly worse than a
   fixture that is missing.
2. **Writing the connectors from memory anyway.** The working agreement says to
   check the docs rather than guess message schemas. Documentation sites are
   behind the same 403 (`docs.kraken.com` was tried and denied), so guesses
   could not even be checked against a written source.
3. **Routing around the proxy.** Explicitly prohibited, and it is an
   organization policy boundary rather than a technical obstacle.

### What was built instead

The parts of Milestone 0 that carry no network dependency are complete and are
real deliverables, not placeholders:

- Repo scaffold: `pyproject.toml` (uv), `.gitignore`, `.env.example`, `src/`,
  `tests/`, `docker/`, `docs/`.
- Three runnable reconnaissance scripts (`scripts/recon/`), one per candidate
  exchange, which connect, subscribe, print 10 messages, save raw frames to
  `docs/samples/<exchange>.jsonl`, and exit. They are verified to execute and
  to fail with a clear diagnostic; they are **not** verified against a live
  feed. Their subscribe payloads are flagged in-file as unconfirmed.

### Options for unblocking

1. **Run the recon scripts on a machine with normal egress** (a laptop) and
   commit the resulting `docs/samples/*.jsonl`. Cheapest path; unblocks
   Milestones 1-2 immediately, since those need the fixtures more than they
   need a live socket.
2. **Have an admin allowlist the exchange hosts** for this environment, then
   re-run recon here. Needed eventually if this environment is to run the live
   pipeline at all.
3. **Proceed schema-blind** — write connectors from memory and correct them
   later. Not recommended, for the reasons above.

Options 1 and 2 are compatible; 1 unblocks development, 2 unblocks live runs.

### Consequence for exchange selection

Milestone 0 tasks 3 and 4 (pick two exchanges on evidence, document their real
message schemas) are **deliberately not answered yet**. Recording a choice here
without having seen either feed would be exactly the guessing this log exists
to prevent. The choice gets made, with reasoning, once recon output exists.

The prior expectation — to be confirmed or refuted, not assumed — is Kraken v2
plus Coinbase Exchange, on the grounds that both are public/no-auth. Binance.US
is the fallback if one of those disappoints.

---

## D-001: Python 3.11 + uv

**Date:** 2026-08-02

`uv` for dependency management, per the locked stack. Milestone-0 dependencies
are kept minimal (`websockets`, `python-dotenv`); Spark, DuckDB, scikit-learn
and Streamlit are declared as optional extras in `pyproject.toml` and promoted
into the main dependency list as each milestone begins using them. This keeps
`uv sync` fast during reconnaissance and keeps the dependency list an honest
statement of what the code actually imports today.

---

## D-002: Raw capture is never reshaped

**Date:** 2026-08-02

The recon capture helper writes each frame to disk exactly as received, one
JSON object per line, and adds no fields to the payload. Normalization is a
Milestone 1 concern and belongs in the connector, where it is unit-tested
against these fixtures. If the capture tool normalized, the fixtures could not
be used to test normalization — they would already agree with it by
construction.

---

## D-003: Kraken sends prices as JSON numbers — parse with Decimal

**Date:** 2026-08-02

Kraken v2 encodes price and quantity as JSON **numbers** (`63348.4`,
`5.1e-05`). Coinbase and Binance both encode them as strings. Python's
`json.loads` turns a JSON number into a binary float, so the precision is lost
inside the parser, before any application code can intervene.

Money in binary floats does not sum or compare exactly, and an order book is
built entirely on exact price-key matching: a level keyed by a float that is
one ULP off is a *different level*, so removals silently miss and the book
accumulates phantom levels. This would surface as unexplained checksum
failures in Milestone 2 and would be painful to diagnose.

**Decision:** the Kraken connector parses with
`json.loads(raw, parse_float=Decimal)`. Prices and quantities are `Decimal`
throughout ingestion and order book state. Conversion to float happens only at
the Parquet/Spark boundary, where the value is an analytical quantity rather
than a book key.

Cost: `Decimal` arithmetic is meaningfully slower than float. That is a
deliberate correctness-over-speed trade at the ingestion layer, and Milestone 6
should measure what it costs rather than assume.

---

## D-004: Coinbase's L2 feed has no gap-detection mechanism

**Date:** 2026-08-02

Verified against captured frames, not documentation. A Coinbase `l2update`
frame has exactly these keys:

```
["changes", "product_id", "time", "type"]
```

There is no sequence number and no checksum, on either the updates or the
snapshot. The `sequence` field exists only on the `matches`/`last_match` trade
channel, which is a separate stream and says nothing about book updates.

The consequence is concrete: **on Coinbase there is no way to know that a book
update was dropped.** Milestone 2 requires detecting gaps and triggering
resync rather than silently corrupting state. On this feed that requirement
cannot be met — the best available substitute is a periodic unconditional
resync on a timer, which is a mitigation, not a detection.

By contrast:

- **Kraken** carries a CRC32 `checksum` on every snapshot and update. Weaker
  than a sequence number (it reports that the book is wrong, not how much was
  missed) but it is a genuine verification signal.
- **Binance.US** carries `U`/`u` update IDs giving textbook gap detection
  (`U == previous_u + 1`). Checked against the capture: 493 depth updates
  across two symbols, **zero discontinuities**.

---

## D-005: Primary exchange pair is Kraken + Binance.US

**Date:** 2026-08-02
**Status:** Decided on the evidence in D-004. Reversible — see below.

The original plan expected Kraken + Coinbase, on the reasonable prior that both
are public and no-auth. That prior held: all three venues connected cleanly
with no auth and accepted the subscriptions. The choice therefore comes down to
something the plan could not have known in advance, which is exactly what
Milestone 0 exists to discover.

**Chosen: Kraken + Binance.US.**

Reasoning:

1. **Milestone 2 is the highest-value component of this project, and its
   defining requirement is gap detection with resync.** Kraken gives a checksum
   and Binance gives sequence IDs; Coinbase gives neither (D-004). Pairing
   Kraken with Coinbase would mean gap-detection tests could only be written
   against one of the two venues, which guts the milestone.
2. **The two integrity models are genuinely different**, so the order book
   interface has to abstract over checksum-based verification *and*
   sequence-based gap detection. That is a more honest distributed-systems
   design problem — and a better thing to be asked about — than two feeds that
   work the same way.
3. **The connectors differ structurally in a useful way.** Binance subscribes
   via URL path with no ack; Kraken subscribes via frame and acks each
   (channel, symbol) pair. The shared connector interface has to accommodate
   both, which is a real constraint rather than a cosmetic one.

**What this costs, stated plainly:** Binance.US is a much thinner venue than
Coinbase. Cross-exchange divergences found in Milestone 4 will partly reflect
Binance.US illiquidity rather than genuine cross-venue dislocation. The
Milestone 4 honesty layer must say so directly — that caveat is the finding,
not a footnote to it.

**Reversibility:** all three venues are captured in `docs/samples/` and
documented in `docs/SCHEMAS.md`, and normalization is per-connector behind a
shared interface. Swapping Binance.US for Coinbase later is a connector change,
not an architecture change. If Milestone 4's divergence results turn out to be
dominated by Binance.US thinness, that is the trigger to revisit.

---

## D-006: The Coinbase capture is too thin to be a fixture

**Date:** 2026-08-02

The committed Coinbase capture is only 4 frames: one subscribe ack, one
snapshot, one `last_match`, one `l2update`. Kraken and Binance.US each have
500. One book update is not enough to test order book reconstruction against.

This does not block anything right now, because D-005 makes Coinbase a
non-primary venue. It is recorded so that the thinness is not later mistaken
for "Coinbase is quiet" — the other two venues captured 500 frames over the
same kind of window, so the difference is a capture artifact, not a property of
the feed. If Coinbase is ever promoted to a primary venue, a fresh capture of
several hundred frames is a prerequisite.

Two Coinbase quirks worth carrying forward regardless, both found in those four
frames:

- The server acked channel `level2_50` when `level2_batch` was requested. A
  connector must read the ack rather than assume it got what it asked for.
- The snapshot held 6,334 bid and 16,725 ask levels in one ~570 KB frame, so
  reconnect cost on this venue is dominated by snapshot size.

---

## D-007: Redpanda over Apache Kafka for local development

**Date:** 2026-08-02

Both speak the Kafka protocol, so nothing downstream can tell them apart —
Spark's Kafka source, `confluent-kafka`, and Redpanda Console are all
protocol-level clients and none of them needed a Redpanda-specific code path.

Redpanda wins on the thing that actually matters for a project someone else has
to run: it is a single binary with no ZooKeeper and no KRaft bootstrap
ceremony, and it starts in seconds in one container. The README promises a
five-minute quickstart, and a two-service compose file keeps that promise where
a Kafka + controller setup would not.

The trade is that this is *not* what production would look like. Managed Kafka
(MSK, Confluent Cloud) brings multi-AZ replication, IAM integration and someone
else's pager. Because the protocol is identical, that migration is a broker
address change rather than a rewrite — which is the point of choosing on the
protocol rather than the implementation.

---

## D-008: Partition by canonical symbol

**Date:** 2026-08-02

Messages are keyed by canonical symbol (`BTC-USD`), not by exchange, and not by
`(exchange, symbol)`.

Kafka guarantees ordering *within a partition only*. Keying by symbol means:

- **Per-venue ordering is preserved.** Both venues' `BTC-USD` messages share a
  partition, and each venue's messages keep their relative order within it.
  Order book reconstruction (Milestone 2) needs a venue's own updates in order;
  it does not care that another venue's updates are interleaved.
- **The cross-exchange join gets locality for free.** Milestone 4 joins the two
  venues on `(symbol, event_time_window)`. Keying by symbol puts both sides of
  that join in the same partition, so the join does not require a repartition
  shuffle to co-locate them.

**The cost, stated up front:** with two symbols, exactly two partitions ever
receive data, no matter how many the topic has. Topics are created with six
partitions, so four sit idle. Partition count cannot be reduced later, and
increasing it remaps keys and breaks per-key ordering across the change, so six
is a bet on adding symbols rather than a claim that six are used.

This is deliberate, known skew. Milestone 6 is where it gets measured — an
unbalanced consumer group and a skewed Spark stage are on that milestone's list
precisely because this decision guarantees them.

The alternative, keying by `(exchange, symbol)`, doubles usable parallelism and
would be the right call at higher venue counts. It was rejected because it
splits the two sides of the Milestone 4 join across partitions, buying
parallelism the project does not yet need at the cost of the join it does.

---

## D-009: At-least-once delivery, chosen rather than defaulted into

**Date:** 2026-08-02

The producer runs with `enable.idempotence: False` and `retries: 5`. A retry
after an ambiguous failure can therefore duplicate a message.

This is a deliberate choice, not an oversight. Exactly-once across a Kafka
pipeline requires the idempotent producer plus transactional reads downstream,
which costs throughput and adds coordination that has to be maintained. Market
data does not need it, because **duplicates are detectable from the venues' own
data**: Binance messages carry `U`/`u` update IDs and Kraken trades carry
`trade_id`, so a duplicate is identifiable at the consumer without any broker
guarantee. Dropping a message would be the expensive failure; seeing one twice
is not.

What this means concretely: Milestone 2's book reconstruction must be
idempotent with respect to replayed updates, and Milestone 3's aggregations
must deduplicate on `(exchange, symbol, trade_id)` rather than assume each
message arrives once. Both are written down here so they are requirements
rather than surprises.

---

## D-010: What Milestone 1 could not be verified against

**Date:** 2026-08-02

**No container image can be pulled in this environment.** Registry manifests
fetch fine, but every registry serves layer blobs from a CDN host that is not
on the allowlist:

| Registry | Blob host | Result |
| --- | --- | --- |
| Docker Hub | `production.cloudfront.docker.com` | denied |
| ECR Public | `*.cloudfront.net` | denied |
| ghcr.io | `pkg-containers.githubusercontent.com` | denied |

The allowlist contains `production.cloudflare.docker.com` — a different CDN
from the `cloudfront` host actually used. This is not specific to Redpanda:
`alpine` fails identically from every registry above.

**The broker leg was verified anyway, without Docker.** `downloads.apache.org`
*is* on the allowlist and a JDK is pre-installed, so Apache Kafka 4.1.2 runs
directly in KRaft mode. Redpanda is Kafka-API-compatible and `confluent-kafka`
speaks the Kafka protocol to either, so the producer path, topic routing and
partition keying are exercised identically. What this does **not** verify is
the Redpanda-specific configuration in `docker-compose.yml` — the listener
split and the healthcheck are still unexercised.

Against a real broker, with the 1,000 captured frames replayed through the
production producer:

```
orderbook.raw   partition 2: 328    partition 3: 639
trades.raw      partition 2:   1    partition 3:   7
```

Every D-008 claim is now measured rather than asserted:

- **Both venues' same symbol share a partition.** Partition 2 held BTC-USD from
  binance_us (201) and kraken (127); partition 3 held ETH-USD from both. This
  is the partition locality the Milestone 4 join depends on.
- **Only 2 of 6 partitions receive data**, exactly the deliberate skew D-008
  predicted. Milestone 6 now has a measured starting point rather than a guess.

D-003 and D-004 also survive the broker boundary: a stored Kraken book message
reads `"bids": [["63348.4", "0.12158347"]]` — decimal strings, not JSON numbers
— alongside `"checksum": 2976216659` and `"first_sequence": null`.

The one leg still unverified is:

1. **Live websocket ingestion.** Exchange hosts remain denied by the
   environment's network policy.

What was verified instead, and how:

- **Normalization**, against all 1,000 captured frames from both venues: 975
  normalized messages, 0 errors, exact expected counts per message type.
- **The full ingestion path** end to end via `xstream.ingest.replay`, which
  feeds real captured frames through the same connector, normalizer, schema and
  sink that production uses. Only the transport differs.
- **Reconnect with exponential backoff**, twice over: injected into a fake
  socket in `tests/test_runner.py`, and observed live against the blocked
  network, where the denied connections produced four reconnects with jittered
  delays growing 0.57s → 1.29s → 0.74s → 3.87s.
- **Graceful shutdown on SIGTERM**, observed live: signal caught, loop exited,
  metrics reported, producer flushed.
- **Production into a real Kafka-protocol broker**, with per-partition offsets
  and stored message contents inspected, as above.

What remains genuinely unverified is narrow and specific: that the venues
accept *these connectors'* subscribe frames over a live socket. The captured
fixtures say the payloads are right, but those fixtures were produced by the
recon probes, not by the connectors. Also unexercised is the Redpanda container
configuration itself, as distinct from the Kafka protocol behaviour it serves.
Both need a machine that can reach the exchanges and a container registry.

---

## D-011: Kraken's checksum algorithm was derived from data, not documentation

**Date:** 2026-08-02

Milestone 2 needs Kraken's book checksum to detect corruption, and
`docs.kraken.com` is behind the same egress denial as the feed itself (D-000),
so the algorithm could not be read. It was derived instead, by brute-forcing
candidate formulations against the captured snapshot's real checksum and
keeping the one that reproduced it:

> Each level contributes its price, then its quantity, rendered at the
> instrument's precision with the decimal point removed and leading zeros
> stripped. Asks first, then bids, each best-first, ten levels per side. CRC32
> of the concatenation.

A single match would be weak evidence — one CRC32 collision is not proof.
What makes this trustworthy is that the implementation then reproduced
**every checksum in the capture**: one snapshot plus 472 incremental updates
per the replay, 474 book messages total, with zero mismatches. A book that had
drifted by one level, one price or one quantity would have failed on the next
message.

That result verifies two things at once, which is why it is the centrepiece
test: the checksum derivation is right, *and* the book reconstruction is right.
They cannot both be wrong in a way that agrees 474 times.

**The known soft spot** is instrument price precision, which the checksum needs
and the websocket feed never sends — it is reference data. It is currently
inferred from the widest price seen in the snapshot. JSON drops trailing zeros,
so that is a lower bound, and an instrument whose top twenty levels all happen
to end in zero would infer too narrow a precision. The failure mode is benign:
the very next checksum mismatches and the book goes stale, rather than silently
serving corrupt state. The fix, if it ever fires, is to read precision from
Kraken's instrument reference endpoint.

---

## D-012: Binance books cannot be seeded from the websocket alone

**Date:** 2026-08-02
**Status:** Open gap in Milestone 2.

Binance's `@depth` stream carries **diffs only**. There is no snapshot on the
websocket, so a book cannot be built from the stream by itself: the documented
procedure is to buffer diffs, fetch a REST snapshot from `/api/v3/depth`, then
discard buffered diffs older than the snapshot's `lastUpdateId` and apply the
rest.

That REST endpoint is on a blocked host here, so the response shape cannot be
confirmed. Writing the seeding path against a guessed schema is exactly the
mistake Milestone 0 exists to prevent (D-000), so it has **not** been written.

What this does and does not cost:

- **Gap detection is complete and tested against real data.** The `U`/`u`
  sequence rule runs through the book's own detector over the captured
  Binance updates with zero false positives, and an injected discontinuity
  makes it fire.
- **Book *state* for Binance is not reconstructible from the fixture.** The
  captured session contains no snapshot, so the books stay unseeded and every
  update is correctly rejected as `REJECTED_NOT_SEEDED`.

Kraken carries snapshots in-stream and is therefore fully reconstructed, which
is why the checksum replay above is the strong verification. Closing this gap
needs one captured REST snapshot payload — a single `curl` from a machine with
access — after which the seeding path is a small amount of code against a known
shape.

---

## D-013: A book that loses integrity stops rather than degrades

**Date:** 2026-08-02

On a detected gap or checksum mismatch the book transitions to STALE and
rejects all further updates until a snapshot re-seeds it. It does not attempt
to patch, interpolate, or carry on.

This is the entire point of having integrity checks. A book that keeps serving
after a detected loss is worse than one that stops, because everything
downstream — spread, imbalance, the Milestone 4 divergence detector, the
Milestone 7 features — would consume plausible-looking numbers with no
indication they are wrong. Silent corruption in a feature store is the failure
that is hardest to find later.

Two details follow from it:

- **Sequence gaps are checked before mutating; checksums after.** A sequence
  number describes the message, so a gap is known before anything is applied
  and the last known-good book is left intact. A checksum describes the
  resulting state, so it can only be verified once applied. Tested both ways.
- **Resync means "take the next snapshot", not anything cleverer.** A snapshot
  is the venue's own statement of truth and is the only exit from STALE.

---

## D-014: The 30-minute replay recording could not be captured

**Date:** 2026-08-02

Milestone 2 asks for 30+ minutes of recorded messages as a replay fixture. The
exchanges are unreachable from this environment (D-000), so the fixture is the
Milestone 0 capture instead: 500 frames per venue, roughly 40 seconds of
market time.

That is enough for correctness work — it exercised 474 checksum verifications
and every book code path — but it is **not** enough for the load testing in
Milestone 6, which needs a long recording to replay at 10x and 100x, nor for
observing a real gap or reconnect in the wild. Capturing it needs a machine
with feed access and `scripts/recon/*.py --limit`, which already supports
arbitrarily long captures.

---

## D-015: Watermark policy — 10 seconds, and what it costs

**Date:** 2026-08-11

Both streaming jobs use `withWatermark("event_time", "10 seconds")` on the
**exchange** timestamp, never the ingest timestamp.

A watermark is a decision about how wrong you are willing to be. Spark needs
one to know when a window can be closed and its state evicted; without it,
state for every window ever seen is retained forever and the job dies of memory
exhaustion rather than of a bad answer. Choosing 10 seconds says: *an event
more than 10 seconds late is dropped, and I accept a slightly wrong aggregate
in exchange for bounded state.*

Why 10 seconds specifically: the observed ingest latency on the captured data
is sub-second, so 10 seconds is roughly an order of magnitude of headroom for a
reconnect, a GC pause, or a burst of consumer lag. It is deliberately much
larger than the 1s finest window, so a late event usually still lands in its own
window rather than being discarded.

**Windowing on event time is what makes this honest.** Using the ingest
timestamp would guarantee no event is ever late — and would silently destroy the
lateness and clock-skew measurements this project exists to make. The two
timestamps are both carried from ingestion precisely so the difference stays
visible.

**The consequence that bit during verification:** in append mode a window is
only emitted once the watermark passes its end, so the *tail* of a bounded
replay never flushes. Replaying a finite fixture and then stopping leaves the
final windows permanently in state. This is correct behaviour, not a bug: in a
live stream event time keeps advancing and windows close continuously. It does
mean that verifying against a fixture requires either more data after the
windows of interest or a shortened watermark, and that is a property of the
test, not of the pipeline.

---

## D-016: Realized volatility is a within-window price standard deviation

**Date:** 2026-08-11

`price_stddev` is the standard deviation of trade prices inside the window. It
is **not** annualized and **not** a returns-based estimator.

A proper realized volatility is computed from log returns, usually
`sqrt(sum(r_i^2))` over the interval, and is scaled to a horizon. That needs
consecutive trades ordered within the window, which in Spark means a windowed
`lag`, which means either a second shuffle or an ordering guarantee the
aggregation does not provide.

The cruder estimator is used because it is cheap, it is well defined on the
data actually available, and — most importantly — it is *labelled* as a proxy
rather than presented as realized volatility. Milestone 7 must treat it as a
dispersion feature, not as a volatility estimate, and any claim built on it
inherits that caveat. It also returns null on a single-trade window, which is
correct: dispersion is undefined on one observation, and a zero there would be
a lie that a model would happily learn.

---

## D-017: Book features come from reconstructed state, not from raw deltas

**Date:** 2026-08-11
**This entry records a mistake and its correction.**

The first version of the Milestone 3 book job read `orderbook.raw` in Spark and
computed spread, mid-price and depth imbalance directly from the delta
messages, taking the first element of each message's `bids`/`asks` array as top
of book. The docstring justified it on the grounds that "Kraken sends the top 10
levels on every update."

**That premise is false.** Both venues send only the levels that *changed*. A
delta's first bid is whatever level happened to move, which is usually not the
best bid. Kraken updates in the capture routinely look like
`{"bids": [], "asks": [{...}]}` — one side empty entirely.

The output made the error obvious rather than subtle:

| venue / symbol | avg spread | avg relative spread |
| --- | --- | --- |
| binance_us BTC-USD | **$216.09** | **35.0 bps** |
| kraken BTC-USD | $0.10 | 0.02 bps |

A 1,750x discrepancy between two liquid venues on the same instrument is not a
market phenomenon. Had the number been merely plausible — say 3 bps against
0.5 bps — it would likely have shipped, and Milestone 4's entire divergence
analysis would have been built on a feature that does not mean what its name
says.

**The correction** was to put reconstruction in front of Spark rather than to
patch the arithmetic. `xstream.orderbook.snapshotter` consumes `orderbook.raw`,
applies deltas to real `OrderBook` instances — the same ones verified against
474 checksums in Milestone 2 — and publishes derived top-of-book state to
`orderbook.snapshots`. Spark reads that topic, where every row is already
correct, and does what it is actually good at: windowing, watermarking,
aggregation and partitioned columnar output.

Reconstruction stays outside Spark deliberately. It is inherently sequential
per (exchange, symbol), it depends on the checksum verification that only the
Python implementation has, and moving it in would mean either a stateful
`flatMapGroupsWithState` reimplementation or shipping book state through a
shuffle. The split is a decision about where state belongs, not an omission.

After the fix, over the same data: Kraken BTC-USD averages a $0.10 spread
(0.02 bps) and ETH-USD $0.024 (0.14 bps), with zero crossed books, zero gaps
and zero checksum failures across every emitted row.

**The lesson worth keeping:** the bug was caught only because the wrong number
was absurd. A quieter version of the same mistake would have survived. That is
an argument for sanity-checking derived features against known market
magnitudes as a routine step, not for trusting that plausible output is
correct.

---

## D-018: Snapshots are sampled on event time, not wall-clock time

**Date:** 2026-08-11

The snapshotter emits a book snapshot every N milliseconds of **market** time,
tracked per book from the venue timestamps, rather than every N milliseconds of
wall-clock time.

Wall-clock sampling makes a replay produce a different series than a live run.
Forty seconds of captured market data is consumed in roughly two seconds, so a
250 ms wall-clock timer fires a handful of times during ingest and then repeats
a frozen, unchanging book indefinitely. The first implementation did exactly
that, and the result was a single emitted window whose event timestamps were
all identical.

Event-time sampling yields one row per 250 ms of market time whether the source
is a live socket or a file, which is both what a feature series should mean and
what makes the replay deterministic — a prerequisite for the Milestone 6 load
tests, which replay the same data at 10x and 100x and need the output to depend
on the data rather than on how fast the machine happened to run.

---

## D-019: Where the shuffle is, and micro-batch vs continuous

**Date:** 2026-08-11

**The shuffle.** Each job has exactly one, at the `groupBy(window, exchange,
symbol)`. Everything before it — JSON parsing, decimal casts, derived columns —
is narrow and runs in the same task as the Kafka partition read. The shuffle
width is `spark.sql.shuffle.partitions`, set to 8 rather than the default 200:
with two symbols, 200 partitions means 200 mostly-empty tasks per batch, and
task scheduling overhead dominates the actual work.

That grouping key inherits the skew designed in at D-008. Messages are keyed by
symbol in Kafka, so with two symbols only two Kafka partitions carry data, and
the post-shuffle grouping likewise concentrates into few non-empty partitions.
Milestone 6 measures it.

**Micro-batch, not continuous processing.** Continuous processing offers
millisecond end-to-end latency but supports only map-like operations — no
aggregations, which is the entire job here. Micro-batch with a 10 second
processing-time trigger is the right trade for a pipeline whose output is
windowed candles: the trigger interval sets how often Parquet files land, and
files landing every 10 seconds is already fast for an analytical layer.

The trigger interval also controls the small-file problem. Ten seconds produces
six file-sets per minute per partition combination, which is why Milestone 5
needs a compaction job rather than treating it as optional polish.

---

## D-020: Align to a common event-time grid before joining, and measure the skew

**Date:** 2026-08-11

The cross-exchange join aggregates each venue into fixed event-time windows
first, then equi-joins on `(symbol, window_start)`. It does not join raw event
to raw event.

**Different update frequencies force this.** The two venues emit at completely
different rates — in the captured data Kraken produced 474 book messages and
Binance.US 493, but distributed quite differently in time. A raw event-to-event
join has no natural key: it either matches every event against every event in
the tolerance interval, which explodes, or it picks a "nearest" match, which
requires an arbitrary tie-break that silently decides the answer. Aligning to a
grid makes "the price at time T" mean the same thing on both sides, and the
join becomes an ordinary equi-join.

**Clock skew sets the floor on window size.** Venue timestamps come from the
venues' own clocks, which are not synchronized with each other. If skew exceeds
the window width, the same market instant lands in different windows and the
join compares mismatched pairs — silently, and in a way that looks like real
divergence. The window must therefore be wider than the expected skew, which is
an assumption, so every joined row carries the observed `skew_ms` between the
two venues' last event times. The assumption is checkable in the output rather
than merely asserted here.

**A venue dropping out produces no row.** The join is inner. An outer join would
emit rows with one side null, and a "divergence" measured against a missing
venue is an outage, not a divergence — putting it in the same table would make
outages indistinguishable from signal. Outage detection belongs in the
Milestone 5 data-quality checks.

The watermark is 30 seconds here rather than the 10 used by the single-venue
jobs, because a stream-stream join must retain both sides' state until it can
be certain no further match will arrive. That is a direct memory cost of the
join and is the reason the watermark is not simply set generously large.

---

## D-021: Milestone 4 produces no real rows, and why that is the honest outcome

**Date:** 2026-08-11
**Status:** Implemented and tested; real-data output is empty pending data.

The divergence job runs cleanly against the live broker, commits its batches,
and emits **zero rows**. That is the correct result for the data available, and
it is caused by two independent facts, either of which alone would be enough:

1. **No overlapping symbol in trades.** Kraken's capture contains exactly one
   trade, on BTC-USD. Binance.US's contains seven, all on ETH-USD. There is no
   symbol on which both venues traded, so a trade-based join has nothing to
   pair.
2. **Only one venue produces book state.** Binance books cannot be seeded
   without a REST snapshot from a blocked host (D-012), so they never reach
   READY and the snapshotter correctly refuses to emit them. Verified against
   the live topic: every snapshot row is `kraken`.

A cross-*exchange* divergence detector needs two exchanges. With one, an inner
join yields nothing, which is exactly what it should do.

**What was verified anyway.** Join semantics are tested against synthetic rows,
which is the right tool: pair ordering, sign conventions, dropout handling and
threshold behaviour are properties of the code, not of the market. Eighteen
tests cover them, including that non-overlapping symbols produce no rows — the
fixture's exact situation, pinned deliberately so this stays a known condition
rather than a mystery.

**What was not verified, stated plainly.** No real cross-venue divergence has
been observed, so this project has produced no empirical claim whatsoever about
divergence magnitude, frequency or duration. Nothing in the README or
`EVALUATION.md` may imply otherwise. Filling this in requires either a Binance
REST snapshot payload to enable book seeding, or a capture long enough to
contain trades on a shared symbol — most cheaply, both.

---

## D-022: The cost floor, and what "not exploitable" actually means

**Date:** 2026-08-11

Every joined row carries a `net_edge_bps` alongside its gross divergence:
gross magnitude minus two taker fees minus the half-spread crossed on each
venue. `survives_costs` is the boolean.

**The arithmetic is stark.** Using published retail taker fees (Kraken ~26 bps,
Binance.US ~40 bps) and the spreads actually measured in Milestone 3 (0.02 bps
and 0.14 bps), breakeven is:

```
26 + 40 + 0.5 * (0.02 + 0.14) = 66.08 bps
```

A cross-venue divergence on a liquid pair must exceed **66 bps** before a naive
round trip breaks even. Typical divergences on liquid instruments are a few
bps. The gap is not marginal; it is an order of magnitude, which is why a
factor-of-two error in the fee assumptions would not change the conclusion.

**`survives_costs = true` is a much weaker statement than it appears.** It means
only that the most basic and most *favourable* cost accounting has not ruled a
divergence out. It does not mean profitable. Latency, queue position, inventory
pre-positioning, displayed size and adverse selection all subtract further, and
none is modelled — they are enumerated in
`xstream.analysis.economics.EXPLOITABILITY_CAVEATS` so the omissions are
explicit rather than implied. Maker rebates, slippage past the top level,
withdrawal fees and taxes are likewise absent, and every one of them makes the
picture worse.

**Duration is computed in SQL, not in streaming state.** Assembling consecutive
flagged windows into episodes is a gaps-and-islands problem; doing it in
Structured Streaming means custom state that must stay correct across restarts
and late data, for an output nobody consumes in real time. `queries/
divergence_duration.sql` does it as a window function over the Parquet lake:
re-runnable, inspectable, and cheap. Streaming detects; batch assembles.

The framing this produces is the one worth defending in conversation: *the
detector finds excursions, and the arithmetic says essentially none of them are
tradeable.* That conclusion is more credible than a suspiciously profitable
one, and it is the actual state of the world for retail participants on liquid
venues.

---

## D-023: Spark decimal precision loss corrupted VWAP — caught by a sanity check

**Date:** 2026-08-11
**This entry records a real correctness bug found in Milestone 3's output.**

`queries/06_vwap_vs_close.sql` includes a column that should always be zero:
the count of rows where VWAP falls outside its own window's high-low range.
That is arithmetically impossible for a correct volume-weighted average — VWAP
is a weighted mean of prices inside the window, so it cannot exceed the highest
or fall below the lowest.

It came back as **1**.

**The cause.** Spark caps decimal precision at 38 digits. When an operation
needs more, it preserves precision and sacrifices *scale*, down to a floor of
six decimals. Multiplying two `DECIMAL(38,18)` values needs precision 77, so
Spark silently rescales the product to `DECIMAL(38,6)`. A crypto quantity of
`0.00009417` therefore became `0.000094` — three significant figures gone —
and both `volume` and `notional` were rounded before the division that produces
VWAP. The result was `63348.401826` on a window whose only trade printed at
`63348.4`.

Python's `Decimal` computes `(63348.4 × 0.00009417) / 0.00009417` as exactly
`63348.4`. The loss was entirely Spark's, and entirely silent.

**The fix** is `DECIMAL(20,8)` rather than `DECIMAL(38,18)`. Eight decimals is
what both venues actually send, and the product of two `DECIMAL(20,8)` values
lands at `DECIMAL(38,13)` — inside the cap, with scale to spare. After the fix
VWAP equals the trade price exactly and the impossible-row count is zero.

**The uncomfortable part.** Milestone 3 shipped with this bug. Every test
passed, because the tests compared VWAP against expected values computed on
inputs whose scale happened not to trigger the rescale. The bug only surfaced
when a query asserted an *invariant* — "VWAP lies within [low, high]" — rather
than a value. That is the transferable lesson: invariant checks catch classes
of error that example-based tests structurally cannot, and a bigger decimal
type is not automatically a safer one.

---

## D-024: Ingest latency is meaningless on replayed data, and says so

**Date:** 2026-08-11

`ingest_latency_ms` is the gap between the venue's timestamp and ours. On a
live feed that is genuine end-to-end latency. On a **replayed fixture** it is
the age of the fixture: the capture is from 2026-08-02, the replay ran on
2026-08-11, and the query duly reported a p50 latency of roughly 808,500,000 ms
— about nine days.

The number is arithmetically correct and analytically worthless, which is the
dangerous combination. `queries/03_ingest_latency_percentiles.sql` therefore
emits a `measurement_valid` column that reads
`NO - replayed fixture, not live ingest` whenever the maximum exceeds a minute,
since nothing plausibly attributable to network latency lasts that long.

The consequence for Milestone 6 is concrete: **latency percentiles cannot be
benchmarked from replayed data.** Throughput can — replay is exactly the right
tool for load testing — but the latency column of `BENCHMARKS.md` needs a live
connection, and any figure published without one would be measuring the wrong
thing.

---

## D-025: Compaction only touches closed partitions, and never deletes first

**Date:** 2026-08-11

Compaction merges a partition's small files into one and **deletes the
originals**, so its failure mode is permanent data loss rather than a bad
query. Two rules contain that risk.

**Only closed partitions.** A streaming query may still be appending to the
current hour, and rewriting a partition underneath a live writer races it.
Partitions are compacted only once their event-time hour is strictly in the
past, which is decidable from the partition path alone without coordinating
with the writer. "No file has appeared recently" is explicitly *not* accepted
as evidence a partition is finished.

**Write, then swap, then delete.** The merged output goes to a temporary file,
is moved into place, and only then are the inputs removed. An interruption at
any point leaves either the originals or both copies — never a partition with
neither.

Measured on the current lake: 9 files and 48,146 bytes became 2 files and
9,267 bytes, **80.8% smaller**, with row counts and values verified identical
before and after. The saving is mostly Parquet footer and row-group overhead,
which is exactly the cost the small-file problem imposes.

The lake still averages ~5 KB per file against a target in the hundreds of
megabytes. That gap is a property of the tiny fixture, not of the compactor,
and it will close on its own as soon as there is real volume to compact.

---

## D-026: Outliers are measured against the median, not the mean

**Date:** 2026-08-11

The outlier check uses an Iglewicz-Hoaglin modified z-score — deviation from
the median, scaled by median absolute deviation — rather than the obvious
mean-and-standard-deviation z-score.

The obvious version does not work, and a test proved it rather than an
argument. Thirty identical prices plus one grossly wrong one failed to trip a
six-sigma threshold, because **an outlier inflates the standard deviation it is
being judged against**. For a population standard deviation the largest
attainable z-score is bounded by `(n-1)/sqrt(n)`; with 31 observations that
ceiling is about 5.48, so no single value could ever reach six sigma regardless
of how wrong it was. A detector that cannot fire is worse than no detector,
because it reads as reassurance.

MAD has roughly a 50% breakdown point, so one bad print barely moves it. But
MAD alone is not sufficient either: it collapses to zero whenever more than
half the values are identical, which is exactly what a quiet book quoting the
same mid repeatedly looks like. A `WHERE mad > 0` guard would then report "no
outliers" on precisely the case of interest — the second way this check nearly
shipped broken. The prescribed fallback to mean absolute deviation, scaled by
1.253314, handles it, and only a perfectly constant series now yields no
outliers, which is correct.

---

## D-027: The bottleneck is checksum verification, and it stays

**Date:** 2026-08-11

Measured, not guessed (`BENCHMARKS.md`): order book reconstruction with CRC32
verification runs at **33,916 deltas/s**; the identical code path with
verification disabled runs at **286,232 deltas/s**. Verification is therefore
**8.4x** the cost of applying the update, and accounts for roughly 88% of the
time spent in the slowest stage of the pipeline.

Everything else has an order of magnitude or more of headroom: JSON parsing
runs at ~204k frames/s, normalization at ~237k messages/s, encoding at ~127k/s,
and the broker sustains **579,260 msg/s** — about 17x the pipeline's own
ceiling. **This pipeline is CPU-bound in its stateful stage, not I/O-bound at
the broker**, which means adding brokers or partitions buys nothing until the
book stage is sharded.

Verification stays on. An unverified book is exactly the silent-corruption
failure D-013 exists to prevent, and 34k deltas/s is still ~1,500x the
fixture's real-time rate of 23 msg/s. If it ever became a genuine constraint,
the honest lever is verifying every Nth update and accepting a bounded
detection delay — not removing verification and claiming the same guarantees.

The related promise from D-003 is also now settled: `Decimal` parsing costs
**1.62x** versus float (330,330 → 203,851 frames/s). On a stage running at
200k/s that is nowhere near the constraint, so the correctness trade was cheap.

---

## D-028: Backpressure is invisible at default settings, and brutal when it fires

**Date:** 2026-08-11

At the default 20,000-message producer queue, **backpressure never occurred** —
not once across 95,000 messages at 579k msg/s. librdkafka's background thread
drains faster than a single Python thread can enqueue, so the `BufferError`
path in `RedpandaSink.send` was effectively dead code in practice.

Forcing it by shrinking the queue to 100 messages:

| queue depth | throughput | BufferError events |
| ---: | ---: | ---: |
| 20,000 | 579,260/s | 0 |
| 100 | **926/s** | **474** |

**A 625x collapse.** Each `BufferError` blocks the producing thread in `poll()`
until the queue drains.

This is the correct behaviour — the alternative is silently discarding market
data, which is the one outcome this project treats as unacceptable — but two
things follow. First, queue depth is a tuning knob with a very sharp edge
rather than a smooth trade. Second, and worse operationally, **the symptom looks
nothing like the cause**: throughput collapses, no errors are logged, nothing
crashes, and the pipeline simply becomes slow. Anyone debugging that from the
outside would suspect the broker or the network long before the queue setting.

---

## D-029: Skew was designed in at D-008; here is what it actually costs

**Date:** 2026-08-11

Measured over a 171,000-message topic with 6 partitions:

| partition | messages | share |
| ---: | ---: | ---: |
| 2 | 46,080 | 26.9% |
| 3 | 124,920 | 73.1% |
| 0, 1, 4, 5 | 0 | 0% |

Two of six partitions carry everything, and one holds 73.1% of it. Spark
launches 6 tasks, 4 of which read nothing, and the busiest does **2.71x** the
work of the other.

None of this is a surprise — D-008 predicted it when it chose symbol keying so
that per-venue ordering is preserved and the divergence join gets partition
locality. What the measurement adds is the magnitude, and one consequence that
was not obvious when the decision was made: **the skew caps consumer group
parallelism at 2**, because a consumer group cannot usefully have more members
than partitions with data. Consumer drain measured 44,045 msg/s for a single
consumer, so the ceiling for this topic is roughly 88k msg/s no matter how many
consumers are added.

That makes partition skew a live constraint at the consumer before it becomes
one anywhere else, and it is the first thing to revisit if throughput ever
needs to scale past the book stage. The fix is more symbols — which spreads
keys naturally and costs nothing — well before it is a composite key.

---

## D-030: No latency figures are published, because none can be measured here

**Date:** 2026-08-11

`BENCHMARKS.md` deliberately contains no p50/p95/p99 latency table.

Latency is venue timestamp minus ingest timestamp. On a live feed that is real
end-to-end latency; on a **replayed fixture it is the age of the capture**. The
data was captured on 2026-08-02 and replayed on 2026-08-11, and the pipeline
duly reported a p50 of roughly 808,500,000 ms — about nine days. The number is
arithmetically correct and analytically worthless, which is the dangerous
combination: it looks like a measurement.

Rather than omit it silently,
`queries/03_ingest_latency_percentiles.sql` emits a `measurement_valid` column
reading `NO - replayed fixture, not live ingest` whenever the maximum exceeds a
minute, since nothing attributable to network latency lasts that long.

**Throughput benchmarks from replay remain valid** — replay is the correct tool
for load testing and none of those figures depends on wall-clock alignment with
market time. Only latency needs a live connection, which this environment
cannot open (D-000). Publishing a latency number from this data would be
measuring the wrong thing and presenting it as the right one.
