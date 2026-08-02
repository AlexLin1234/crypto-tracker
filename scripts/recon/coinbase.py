#!/usr/bin/env python3
"""Milestone 0 recon: Coinbase Exchange (legacy) WebSocket feed, public/no auth.

Run:  uv run python scripts/recon/coinbase.py

This targets the LEGACY Exchange feed deliberately. The newer Advanced Trade
feed (wss://advanced-trade-ws.coinbase.com) requires CDP API keys with JWT
signing for most channels; the plan says prefer the no-auth path, so that is
what this script probes. If this feed turns out to be unavailable, the fallback
is Advanced Trade with credentials, which is a different and larger job.

VERIFICATION STATUS: the subscribe payload below is written from prior
knowledge and has NOT been confirmed against live docs or a live socket in this
environment (egress to coinbase.com is blocked here). Trust any error frame the
server returns over this file.
"""

from _common import capture, run

URL = "wss://ws-feed.exchange.coinbase.com"

# `level2_batch` is the throttled L2 channel and is the sane default for a
# pipeline; the unthrottled `level2` channel historically required auth.
# `matches` is the trade channel. `heartbeat` gives us sequence continuity
# signal even on quiet symbols, which Milestone 2 gap detection will want.
SUBSCRIPTIONS = [
    {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["level2_batch", "matches", "heartbeat"],
    },
]

if __name__ == "__main__":
    run(capture(exchange="coinbase", url=URL, subscriptions=SUBSCRIPTIONS, limit=10))
