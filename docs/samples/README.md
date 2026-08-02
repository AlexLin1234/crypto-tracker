# Sample payloads

Real, unmodified frames captured from each exchange, one JSON object per line.
These are the test fixtures for Milestone 1 (normalization) and Milestone 2
(order book replay). Captured 2026-08-02.

| File | Frames | Contents |
| --- | --- | --- |
| `kraken.jsonl` | 500 | 472 book updates, 2 book snapshots, 20 heartbeats, 1 trade, 4 subscribe acks, 1 status |
| `binance_us.jsonl` | 500 | 493 depth updates, 7 trades |
| `coinbase.jsonl` | 4 | 1 subscribe ack, 1 snapshot, 1 `last_match`, 1 `l2update` |

No error frames in any of the 1,004 captured frames — every venue accepted its
subscription. Schemas are documented in [`../SCHEMAS.md`](../SCHEMAS.md).

## Known gap: the Coinbase capture is too thin

Four frames, of which exactly one is a book update. That is not enough to test
order book reconstruction against. It does not block current work because
Coinbase is not a primary venue (see `DECISIONS.md` D-005), but the thinness is
a capture artifact, not a property of the feed — the other two venues captured
500 frames over a comparable window. If Coinbase is ever promoted to primary, a
fresh capture of several hundred frames is a prerequisite.

## Re-capturing

On a machine with outbound access to the exchanges:

```bash
uv sync
uv run python scripts/recon/kraken.py     --limit 500
uv run python scripts/recon/coinbase.py   --limit 500
uv run python scripts/recon/binance_us.py --limit 500
```

Check the exit code before committing anything:

| Exit | Meaning |
| --- | --- |
| `0` | Clean capture. Safe to commit. |
| `1` | Could not connect. Nothing useful written. |
| `2` | Connected but no frames arrived within the timeout. |
| `3` | **The venue rejected a subscription.** Frames were captured, but they are error frames. Do not commit them. |

Note that the Coinbase snapshot frame alone is ~570 KB (6,334 bid and 16,725
ask levels), so a large Coinbase capture is bigger on disk than the frame count
suggests.

## Cloud sessions

Cloud environments default to **Trusted** network access, which does not
include the exchanges — the probes fail there with
`proxy rejected connection: HTTP 403`. To capture from a cloud session, set the
environment's network access to **Custom**, allowlist `*.kraken.com`,
`*.coinbase.com` and `*.binance.us`, and keep the "also include default list of
common package managers" option checked so PyPI and GitHub stay reachable. See
`DECISIONS.md` D-000.
