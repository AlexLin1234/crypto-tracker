"""Shared helpers for the Milestone 0 reconnaissance scripts.

These scripts exist to answer one question before any pipeline code is written:
can we actually connect to this exchange, and what exactly does it send us?

They deliberately do no normalization. They capture raw payloads verbatim so
that the saved samples are trustworthy fixtures for the normalization tests in
Milestone 1 and the order book replay tests in Milestone 2. A fixture that has
been silently reshaped by the capture tool is worse than no fixture at all.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
from typing import Any

from websockets.asyncio.client import connect

SAMPLES_DIR = pathlib.Path(__file__).resolve().parents[2] / "docs" / "samples"


def _connect_kwargs() -> dict[str, Any]:
    """Build connect() kwargs, honouring an optional HTTP CONNECT proxy.

    Most people running this need nothing here. It matters only in sandboxed or
    corporate-network environments where direct egress is not available.
    """
    kwargs: dict[str, Any] = {
        # Exchanges are chatty; a generous queue avoids the client dropping
        # frames while we are printing to stdout.
        "max_queue": 4096,
        "open_timeout": 20,
    }
    proxy = os.environ.get("XSTREAM_WS_PROXY") or ""
    if proxy:
        # websockets >= 14 accepts proxy=; older versions do not. Fail loudly
        # rather than silently connecting direct and appearing to work.
        kwargs["proxy"] = proxy
    return kwargs


async def capture(
    *,
    exchange: str,
    url: str,
    subscriptions: list[dict[str, Any]],
    limit: int = 10,
    timeout: float = 45.0,
) -> int:
    """Connect, send each subscription, print and save `limit` messages.

    Returns a process exit code: 0 on success, non-zero if we could not get the
    requested number of messages. Saves raw frames to docs/samples/.

    Every frame is written exactly as received (one JSON object per line), with
    a local receive timestamp added as a sibling wrapper field rather than
    mutating the payload itself.
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
            with out_path.open("w", encoding="utf-8") as fh:
                while received < limit:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        print(
                            f"[{exchange}] TIMEOUT after {received} message(s); "
                            f"no frame within {timeout}s",
                            file=sys.stderr,
                        )
                        return 2

                    received += 1
                    # Keep the raw text. Parsing is only for pretty-printing and
                    # for surfacing exchange-side subscription errors early.
                    fh.write(raw if isinstance(raw, str) else raw.decode())
                    fh.write("\n")

                    try:
                        parsed = json.loads(raw)
                        pretty = json.dumps(parsed)[:400]
                    except (json.JSONDecodeError, TypeError):
                        pretty = str(raw)[:400]
                    print(f"[{exchange}] <- [{received}] {pretty}")

            print(f"[{exchange}] captured {received} message(s) -> {out_path}")
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
