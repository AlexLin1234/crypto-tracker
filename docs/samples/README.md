# Sample payloads

**This directory is intentionally empty of samples right now.**

It is meant to hold real, unmodified frames captured from each exchange, one
JSON object per line, named `<exchange>.jsonl`. Those files are the test
fixtures for Milestone 1 (normalization) and Milestone 2 (order book replay).

They are absent because Milestone 0's live verification could not be run in the
cloud development environment — the egress policy denies all exchange hosts at
the proxy `CONNECT` stage. See `DECISIONS.md` entry **D-000** for the full
finding and the options for unblocking.

No placeholder or hand-written samples have been committed here, deliberately.
Fixtures that were invented rather than captured would silently validate
whatever the normalizers happened to do, which defeats their entire purpose.

## To populate this directory

On a machine with normal outbound network access:

```bash
uv sync
uv run python scripts/recon/kraken.py
uv run python scripts/recon/coinbase.py
uv run python scripts/recon/binance_us.py
```

Each script writes `docs/samples/<exchange>.jsonl` and prints what it captured.
Use `--limit N` to capture more than the default 10 frames; a few hundred from
each venue makes a much better Milestone 2 replay fixture and costs nothing:

```bash
uv run python scripts/recon/kraken.py --limit 500
```

## Read the exit code before committing

| Exit | Meaning |
| --- | --- |
| `0` | Clean capture. Safe to commit. |
| `1` | Could not connect. Nothing useful written. |
| `2` | Connected but no frames arrived within the timeout. |
| `3` | **The venue rejected a subscription.** Frames were captured, but they are error frames. Do not commit them. |

Exit code `3` is the one that matters. The subscribe payloads in these scripts
are written from prior knowledge and are **unconfirmed** — no live socket or
documentation page was reachable when they were written. If an exchange replies
with an error frame, that frame is the authority: fix the script's subscribe
payload to match what the venue actually wants, then re-run.

Once real samples exist, `DECISIONS.md` D-000 gets closed out and the exchange
selection (Milestone 0 task 3) gets recorded with actual reasoning.
