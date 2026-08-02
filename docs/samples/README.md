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
Commit the resulting files.

Note that the subscribe payloads in those scripts are written from prior
knowledge and are **unconfirmed**. If an exchange replies with an error frame,
that frame is the authority — fix the script, do not fix the exchange.
