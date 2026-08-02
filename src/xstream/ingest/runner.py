"""Shared ingestion machinery: connect, reconnect, normalize, produce, report.

None of this differs per venue, so it lives here rather than in the connectors.
A connector answers what to connect to and what the bytes mean; this module owns
the long-running behaviour that makes the service survive an ordinary day:
reconnecting with backoff, not dying on one malformed frame, reporting rates,
and shutting down cleanly when asked.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import signal

import structlog
import websockets
from websockets.asyncio.client import connect

from .base import Connector
from .metrics import IngestMetrics
from .producer import MessageSink
from .schema import now_utc, parse_decimal_json

log = structlog.get_logger()

#: Reconnect backoff. Capped so that a long outage still retries promptly once
#: the venue returns, rather than sleeping for minutes.
BACKOFF_INITIAL = 1.0
BACKOFF_MAX = 30.0
BACKOFF_FACTOR = 2.0

STATS_INTERVAL = 10.0


async def run_connector(
    connector: Connector,
    sink: MessageSink,
    stop: asyncio.Event,
    *,
    stats_interval: float = STATS_INTERVAL,
) -> IngestMetrics:
    """Stream one venue until `stop` is set. Reconnects on its own."""
    metrics = IngestMetrics(exchange=connector.name)
    backoff = BACKOFF_INITIAL
    stats_task = asyncio.create_task(_report_stats(metrics, stop, stats_interval))

    try:
        while not stop.is_set():
            try:
                await _stream_once(connector, sink, stop, metrics)
                # A clean return means shutdown was requested.
                backoff = BACKOFF_INITIAL
            except asyncio.CancelledError:
                raise
            except (OSError, websockets.exceptions.WebSocketException) as exc:
                if stop.is_set():
                    break
                metrics.reconnect()
                # Full jitter: without it, every symbol/venue reconnects in
                # lockstep after a shared outage and hammers the venue at the
                # same instant.
                delay = random.uniform(0, backoff)
                log.warning(
                    "connection_lost",
                    exchange=connector.name,
                    error=f"{type(exc).__name__}: {exc}",
                    reconnect_in=round(delay, 2),
                    reconnects=metrics.reconnects,
                )
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                backoff = min(backoff * BACKOFF_FACTOR, BACKOFF_MAX)
    finally:
        stats_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stats_task

    log.info("connector_stopped", **metrics.snapshot())
    return metrics


async def _stream_once(
    connector: Connector,
    sink: MessageSink,
    stop: asyncio.Event,
    metrics: IngestMetrics,
) -> None:
    url = connector.stream_url()
    log.info("connecting", exchange=connector.name, url=url, symbols=connector.symbols)

    async with connect(url, max_queue=4096, open_timeout=20, ping_interval=20) as ws:
        log.info("connected", exchange=connector.name)

        for frame in connector.subscribe_frames():
            import json

            await ws.send(json.dumps(frame))

        while not stop.is_set():
            stop_task = asyncio.create_task(stop.wait())
            recv_task = asyncio.create_task(ws.recv())
            done, pending = await asyncio.wait(
                {stop_task, recv_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            if recv_task not in done:
                return  # shutdown requested

            raw = recv_task.result()
            metrics.frame()
            _handle_frame(connector, sink, metrics, raw)


def _handle_frame(
    connector: Connector, sink: MessageSink, metrics: IngestMetrics, raw
) -> None:
    """Normalize and produce one frame.

    A single bad frame must not kill a long-running ingestor, so parse and
    normalize failures are counted and logged rather than raised. They are
    counted separately from ignored frames because a rising normalize_errors
    means the venue changed its schema, which is a real alarm.
    """
    try:
        decoded = parse_decimal_json(raw)
    except ValueError as exc:
        metrics.normalize_error()
        log.warning("frame_unparseable", exchange=connector.name, error=str(exc))
        return

    ingest_timestamp = now_utc()
    try:
        messages = connector.normalize(decoded, ingest_timestamp)
    except (KeyError, ValueError, TypeError) as exc:
        metrics.normalize_error()
        log.warning(
            "normalize_failed",
            exchange=connector.name,
            error=f"{type(exc).__name__}: {exc}",
        )
        return

    if not messages:
        metrics.ignored()
        return

    for message in messages:
        sink.send(message)
    metrics.produced(len(messages))


async def _report_stats(
    metrics: IngestMetrics, stop: asyncio.Event, interval: float
) -> None:
    while not stop.is_set():
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return
        log.info("ingest_stats", **metrics.snapshot())


def install_signal_handlers(stop: asyncio.Event) -> None:
    """Set `stop` on SIGTERM/SIGINT so shutdown drains rather than aborts.

    SIGTERM matters specifically: it is what `docker stop` and orchestrators
    send, and without this the process would be killed mid-produce, losing
    whatever is sitting in the producer's queue.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_stop, stop, sig)


def _request_stop(stop: asyncio.Event, sig: signal.Signals) -> None:
    if not stop.is_set():
        log.info("shutdown_requested", signal=sig.name)
        stop.set()
