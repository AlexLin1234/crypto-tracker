# Design Decisions and Tradeoffs

Running log. Each entry records the decision, the alternatives considered, and
why. This is the primary source material for the README's design section.

---

## D-000: Milestone 0 blocked in the cloud dev environment — exchange egress denied

**Status:** BLOCKED, awaiting a decision (see "Options" below).
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
