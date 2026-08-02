"""Tests for the shared ingestion machinery.

The Milestone 1 done-criterion includes surviving a network interruption. That
cannot be verified against a live venue here, so the socket is faked and the
interruption is injected: the fake raises the same exception type
`websockets` raises when a connection drops, and the test asserts the runner
reconnects and keeps producing rather than exiting.
"""

from __future__ import annotations

import asyncio
import json
import pathlib

import pytest
import websockets

from xstream.ingest import InMemorySink, KrakenConnector
from xstream.ingest import runner as runner_mod

SAMPLES = pathlib.Path(__file__).resolve().parents[1] / "docs" / "samples"


def kraken_book_frames(count: int) -> list[str]:
    """Raw book frames, as text, exactly as the venue sent them.

    Filtering is done by parsing rather than substring matching: the fixtures
    store the venue's compact output (`{"channel":"book"`), so a match against
    pretty-printed spacing silently finds nothing.
    """
    lines = (SAMPLES / "kraken.jsonl").read_text().splitlines()
    books = [line for line in lines if json.loads(line).get("channel") == "book"]
    assert books, "fixture must contain book frames"
    return books[:count]


class FakeWebSocket:
    """Yields queued frames, then raises `after` to simulate a drop."""

    def __init__(self, frames: list[str], drop_after: int | None) -> None:
        self._frames = list(frames)
        self._drop_after = drop_after
        self._served = 0
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def recv(self) -> str:
        if self._drop_after is not None and self._served >= self._drop_after:
            raise websockets.exceptions.ConnectionClosedError(None, None)
        if not self._frames:
            # Nothing left: park so the test controls termination via `stop`.
            await asyncio.sleep(3600)
        self._served += 1
        return self._frames.pop(0)


class FakeConnect:
    """Stands in for `websockets.asyncio.client.connect`."""

    def __init__(self, sockets: list[FakeWebSocket]) -> None:
        self._sockets = sockets
        self.connections = 0

    def __call__(self, url, **kwargs):
        self.urls_seen = getattr(self, "urls_seen", [])
        self.urls_seen.append(url)
        return self

    async def __aenter__(self) -> FakeWebSocket:
        socket = self._sockets[min(self.connections, len(self._sockets) - 1)]
        self.connections += 1
        return socket

    async def __aexit__(self, *exc_info) -> bool:
        return False


@pytest.fixture
def no_backoff_delay(monkeypatch):
    """Keep reconnect tests fast without disabling the backoff logic itself."""
    monkeypatch.setattr(runner_mod, "BACKOFF_INITIAL", 0.001)
    monkeypatch.setattr(runner_mod, "BACKOFF_MAX", 0.001)


async def test_frames_flow_through_to_the_sink(monkeypatch) -> None:
    frames = kraken_book_frames(5)
    fake = FakeConnect([FakeWebSocket(frames, drop_after=None)])
    monkeypatch.setattr(runner_mod, "connect", fake)

    sink = InMemorySink()
    stop = asyncio.Event()
    connector = KrakenConnector(["BTC-USD", "ETH-USD"])

    task = asyncio.create_task(run_until_quiet(connector, sink, stop))
    metrics = await task

    assert metrics.frames_received == 5
    assert metrics.messages_produced == 5
    assert len(sink.messages) == 5


async def test_subscribe_frames_are_sent_on_connect(monkeypatch) -> None:
    socket = FakeWebSocket(kraken_book_frames(1), drop_after=None)
    monkeypatch.setattr(runner_mod, "connect", FakeConnect([socket]))

    stop = asyncio.Event()
    await run_until_quiet(KrakenConnector(["BTC-USD"]), InMemorySink(), stop)

    sent = [json.loads(s) for s in socket.sent]
    assert [f["params"]["channel"] for f in sent] == ["book", "trade"]


async def test_reconnects_after_a_dropped_connection(monkeypatch, no_backoff_delay) -> None:
    """The network-interruption criterion, injected rather than live."""
    first = FakeWebSocket(kraken_book_frames(3), drop_after=3)
    second = FakeWebSocket(kraken_book_frames(4), drop_after=None)
    fake = FakeConnect([first, second])
    monkeypatch.setattr(runner_mod, "connect", fake)

    sink = InMemorySink()
    stop = asyncio.Event()
    metrics = await run_until_quiet(KrakenConnector(["BTC-USD"]), sink, stop, quiet_after=0.2)

    assert metrics.reconnects == 1, "a dropped connection must be counted"
    assert fake.connections == 2, "runner must reconnect, not give up"
    # Messages from before and after the drop both arrive.
    assert metrics.messages_produced == 7


async def test_shutdown_is_graceful_and_returns_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        runner_mod, "connect", FakeConnect([FakeWebSocket(kraken_book_frames(2), None)])
    )
    sink = InMemorySink()
    stop = asyncio.Event()
    connector = KrakenConnector(["BTC-USD"])

    task = asyncio.create_task(
        runner_mod.run_connector(connector, sink, stop, stats_interval=3600)
    )
    await asyncio.sleep(0.1)
    stop.set()
    metrics = await asyncio.wait_for(task, timeout=2)

    assert metrics.messages_produced == 2


async def test_a_malformed_frame_does_not_kill_the_stream(monkeypatch) -> None:
    frames = ["{not json", *kraken_book_frames(2)]
    monkeypatch.setattr(runner_mod, "connect", FakeConnect([FakeWebSocket(frames, None)]))

    sink = InMemorySink()
    stop = asyncio.Event()
    metrics = await run_until_quiet(KrakenConnector(["BTC-USD"]), sink, stop)

    assert metrics.normalize_errors == 1
    assert metrics.messages_produced == 2, "stream continues past the bad frame"


async def test_heartbeats_count_as_ignored_not_errors(monkeypatch) -> None:
    frames = ['{"channel":"heartbeat"}', *kraken_book_frames(1)]
    monkeypatch.setattr(runner_mod, "connect", FakeConnect([FakeWebSocket(frames, None)]))

    stop = asyncio.Event()
    metrics = await run_until_quiet(KrakenConnector(["BTC-USD"]), InMemorySink(), stop)

    assert metrics.frames_ignored == 1
    assert metrics.normalize_errors == 0


async def run_until_quiet(connector, sink, stop, quiet_after: float = 0.1):
    """Run the connector, then stop it once the fake socket has drained."""
    task = asyncio.create_task(
        runner_mod.run_connector(connector, sink, stop, stats_interval=3600)
    )
    await asyncio.sleep(quiet_after)
    stop.set()
    return await asyncio.wait_for(task, timeout=3)
