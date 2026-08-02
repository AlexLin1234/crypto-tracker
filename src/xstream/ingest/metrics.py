"""Ingestion counters and the periodic stats line.

Deliberately plain: counters and a logger, no metrics backend. Milestone 6 is
where throughput and latency get measured properly; this exists so a running
ingestor is observable, and so "dropped messages" is a number someone can read
rather than a hope.
"""

from __future__ import annotations

import dataclasses
import time


@dataclasses.dataclass
class IngestMetrics:
    exchange: str
    frames_received: int = 0
    messages_produced: int = 0
    frames_ignored: int = 0
    normalize_errors: int = 0
    reconnects: int = 0
    _window_start: float = dataclasses.field(default_factory=time.monotonic)
    _window_base: int = 0

    def frame(self) -> None:
        self.frames_received += 1

    def produced(self, count: int) -> None:
        self.messages_produced += count

    def ignored(self) -> None:
        """A frame that carried no market data: heartbeat, ack, status."""
        self.frames_ignored += 1

    def normalize_error(self) -> None:
        """A frame that should have carried data but could not be parsed.

        Tracked separately from `ignored` because the two mean opposite things:
        ignored frames are normal, normalize errors are a schema drift alarm.
        """
        self.normalize_errors += 1

    def reconnect(self) -> None:
        self.reconnects += 1

    def rate_since_last_call(self) -> float:
        """Messages/sec since the previous call. Resets the window."""
        now = time.monotonic()
        elapsed = now - self._window_start
        produced = self.messages_produced - self._window_base
        self._window_start = now
        self._window_base = self.messages_produced
        return produced / elapsed if elapsed > 0 else 0.0

    def snapshot(self) -> dict[str, float | int | str]:
        return {
            "exchange": self.exchange,
            "frames_received": self.frames_received,
            "messages_produced": self.messages_produced,
            "frames_ignored": self.frames_ignored,
            "normalize_errors": self.normalize_errors,
            "reconnects": self.reconnects,
            "msgs_per_sec": round(self.rate_since_last_call(), 1),
        }
