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
