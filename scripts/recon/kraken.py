#!/usr/bin/env python3
"""Milestone 0 recon: Kraken WebSocket v2 (public, no auth).

Run:  uv run python scripts/recon/kraken.py

VERIFICATION STATUS: the subscribe payloads below are written from prior
knowledge of the Kraken v2 API and have NOT been confirmed against live docs or
a live socket in this environment (egress to kraken.com is blocked here). The
whole point of this script is to confirm or refute them. If Kraken replies with
an error frame, trust the error frame over this file and correct it.
"""

from _common import capture, parse_args, run

URL = "wss://ws.kraken.com/v2"

# Kraken v2 uses a method/params envelope. `book` is the L2 channel; depth
# controls how many levels the snapshot carries. `trade` is the trade channel.
SUBSCRIPTIONS = [
    {
        "method": "subscribe",
        "params": {
            "channel": "book",
            "symbol": ["BTC/USD", "ETH/USD"],
            "depth": 10,
        },
    },
    {
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": ["BTC/USD", "ETH/USD"],
        },
    },
]

if __name__ == "__main__":
    args = parse_args("kraken")
    run(
        capture(
            exchange="kraken",
            url=URL,
            subscriptions=SUBSCRIPTIONS,
            limit=args.limit,
            timeout=args.timeout,
        )
    )
