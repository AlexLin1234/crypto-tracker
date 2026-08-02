"""Ingestion service entry point.

    uv run python -m xstream.ingest --exchange kraken
    uv run python -m xstream.ingest --exchange binance_us --symbols BTC-USD

One process per venue, so a crash or a bad deploy on one connector cannot take
the other down, and so each can be restarted independently.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import structlog
from dotenv import load_dotenv

from .binance_us import BinanceUSConnector
from .kraken import KrakenConnector
from .producer import build_sink
from .runner import install_signal_handlers, run_connector
from .schema import CANONICAL_SYMBOLS

CONNECTORS = {
    KrakenConnector.name: KrakenConnector,
    BinanceUSConnector.name: BinanceUSConnector,
}


def configure_logging(json_logs: bool) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer()
            if json_logs
            else structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="xstream ingestion service")
    parser.add_argument("--exchange", required=True, choices=sorted(CONNECTORS))
    parser.add_argument(
        "--symbols",
        default=None,
        help="comma-separated canonical symbols (default: XSTREAM_SYMBOLS or BTC-USD,ETH-USD)",
    )
    parser.add_argument(
        "--brokers",
        default=None,
        help="Redpanda bootstrap servers (default: REDPANDA_BROKERS). "
        "If unset, messages are counted in memory and not produced.",
    )
    parser.add_argument("--json-logs", action="store_true")
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    symbols = (
        [s.strip() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else [s.strip() for s in os.environ.get("XSTREAM_SYMBOLS", "").split(",") if s.strip()]
        or list(CANONICAL_SYMBOLS)
    )

    connector = CONNECTORS[args.exchange](symbols)
    brokers = args.brokers or os.environ.get("REDPANDA_BROKERS")
    sink = build_sink(brokers)

    if not brokers:
        structlog.get_logger().warning(
            "no_broker_configured",
            detail="messages will be normalized and counted but not produced",
        )

    stop = asyncio.Event()
    install_signal_handlers(stop)

    try:
        await run_connector(connector, sink, stop)
    finally:
        # Drain whatever is queued before exiting; this is the point of the
        # signal handling.
        pending = sink.flush(timeout=10.0)
        if pending:
            structlog.get_logger().warning("undelivered_on_shutdown", count=pending)
        sink.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = parse_args(argv)
    configure_logging(args.json_logs)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
