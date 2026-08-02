"""Shared helpers for the Milestone 0 reconnaissance scripts.

These scripts exist to answer one question before any pipeline code is written:
can we actually connect to this exchange, and what exactly does it send us?

They deliberately do no normalization. They capture raw payloads verbatim so
that the saved samples are trustworthy fixtures for the normalization tests in
Milestone 1 and the order book replay tests in Milestone 2. A fixture that has
been silently reshaped by the capture tool is worse than no fixture at all.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from typing import Any

from websockets.asyncio.client import connect

SAMPLES_DIR = pathlib.Path(__file__).resolve().parents[2] / "docs" / "samples"


def parse_args(exchange: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Capture raw frames from {exchange}")
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="number of frames to capture (default: 10)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=45.0,
        help="seconds to wait for a single frame before giving up (default: 45)",
    )
    return parser.parse_args()


def _connect_kwargs() -> dict[str, Any]:
    """Build connect() kwargs, honouring an optional HTTP CONNECT proxy.

    Most people running this need nothing here; websockets reads HTTPS_PROXY
    from the environment on its own. XSTREAM_WS_PROXY exists only to override
    that explicitly in sandboxed or corporate-network environments.
    """
    kwargs: dict[str, Any] = {
        # Exchanges are chatty; a generous queue avoids the client dropping
        # frames while we are printing to stdout.
        "max_queue": 4096,
        "open_timeout": 20,
    }
    proxy = os.environ.get("XSTREAM_WS_PROXY") or ""
    if proxy:
        kwargs["proxy"] = proxy
    return kwargs


def looks_like_error(payload: Any) -> str | None:
    """Return an error description if this frame looks like a rejection.

    The subscribe payloads in these scripts are written from prior knowledge and
    are unconfirmed. Without this check, a wrong subscribe shape would produce a
    capture full of error frames that still exited 0 -- and those frames would
    then be committed as if they were real market data fixtures.

    Each venue signals errors differently, so this is a deliberately broad
    heuristic. False positives are cheap (a warning); false negatives are not.
    """
    if not isinstance(payload, dict):
        return None

    # Coinbase Exchange: {"type": "error", "message": ..., "reason": ...}
    if payload.get("type") == "error":
        return str(payload.get("message") or payload.get("reason") or payload)

    # Kraken v2: {"method": "subscribe", "success": false, "error": ...}
    if payload.get("success") is False:
        return str(payload.get("error") or payload)

    # Binance: {"error": {"code": ..., "msg": ...}}
    err = payload.get("error")
    if err:
        return str(err)

    return None


async def capture(
    *,
    exchange: str,
    url: str,
    subscriptions: list[dict[str, Any]],
    limit: int = 10,
    timeout: float = 45.0,
) -> int:
    """Connect, send each subscription, print and save `limit` frames.

    Frames are written exactly as received, one per line, with nothing added and
    nothing removed. Returns a process exit code: 0 on a clean capture, non-zero
    if the connection failed, timed out, or the venue rejected a subscription.
    """
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SAMPLES_DIR / f"{exchange}.jsonl"

    print(f"[{exchange}] connecting to {url}")
    try:
        async with connect(url, **_connect_kwargs()) as ws:
            print(f"[{exchange}] connected")

            for sub in subscriptions:
                await ws.send(json.dumps(sub))
                print(f"[{exchange}] -> {json.dumps(sub)}")

            received = 0
            errors: list[str] = []

            with out_path.open("w", encoding="utf-8") as fh:
                while received < limit:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        print(
                            f"[{exchange}] TIMEOUT after {received} frame(s); "
                            f"nothing received within {timeout}s",
                            file=sys.stderr,
                        )
                        return 2

                    received += 1
                    text = raw if isinstance(raw, str) else raw.decode()
                    fh.write(text)
                    fh.write("\n")

                    try:
                        parsed = json.loads(text)
                    except json.JSONDecodeError:
                        print(f"[{exchange}] <- [{received}] {text[:400]}")
                        continue

                    problem = looks_like_error(parsed)
                    if problem:
                        errors.append(problem)
                        print(
                            f"[{exchange}] <- [{received}] ERROR FRAME: {problem}",
                            file=sys.stderr,
                        )
                    else:
                        print(f"[{exchange}] <- [{received}] {json.dumps(parsed)[:400]}")

            print(f"[{exchange}] captured {received} frame(s) -> {out_path}")

            if errors:
                print(
                    f"\n[{exchange}] REJECTED: {len(errors)} of {received} frames were "
                    f"errors. The subscribe payload in scripts/recon/{exchange}.py is "
                    f"probably wrong -- the venue's error frame is the authority, not "
                    f"that file. Do NOT commit {out_path.name} as a fixture.\n"
                    f"[{exchange}] first error: {errors[0]}",
                    file=sys.stderr,
                )
                return 3

            return 0

    except OSError as exc:
        # Covers DNS failure, refused connections, and proxy CONNECT rejections.
        print(f"[{exchange}] CONNECTION FAILED: {exc!r}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - recon script wants the raw reason
        print(f"[{exchange}] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


def run(coro) -> None:
    raise SystemExit(asyncio.run(coro))
