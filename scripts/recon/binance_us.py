#!/usr/bin/env python3
"""Milestone 0 recon: Binance.US combined streams (public, no auth).

Run:  uv run python scripts/recon/binance_us.py

Binance.US rather than binance.com because binance.com geo-blocks US IPs. This
is the third candidate, probed only so the exchange choice in DECISIONS.md is
made on evidence rather than assumption.

Binance subscribes via the URL path for combined streams, so there is no
subscribe frame to send -- note the empty subscriptions list. That asymmetry
with Kraken/Coinbase is itself worth recording: the connector interface in
Milestone 1 has to accommodate both "subscribe by URL" and "subscribe by frame".

VERIFICATION STATUS: stream names below are written from prior knowledge and
have NOT been confirmed against live docs or a live socket in this environment
(egress to binance.us is blocked here).
"""

from _common import capture, run

# @depth is the L2 diff stream; @trade is the per-trade stream. Lowercase symbol
# names are required by Binance.
STREAMS = ["btcusd@depth", "btcusd@trade", "ethusd@depth", "ethusd@trade"]
URL = "wss://stream.binance.us:9443/stream?streams=" + "/".join(STREAMS)

if __name__ == "__main__":
    run(capture(exchange="binance_us", url=URL, subscriptions=[], limit=10))
