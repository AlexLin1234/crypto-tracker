"""Routes normalized deltas to the right book and tracks resync state.

One `BookManager` owns every (exchange, symbol) book for a process. Its job
beyond dictionary lookup is to record *why* a book went stale and how often, so
that gap rates are a reported number rather than a hope. Milestone 6 reads
these counters.
"""

from __future__ import annotations

import dataclasses

from ..ingest.schema import BookDelta
from .book import ApplyOutcome, ApplyResult, BookState, OrderBook

#: Kraken maintains the subscribed depth and checksums exactly that many levels,
#: so the book must truncate to match. Binance streams unbounded diffs.
VENUE_DEPTH: dict[str, int | None] = {"kraken": 10, "binance_us": None}


@dataclasses.dataclass
class ResyncEvent:
    exchange: str
    symbol: str
    outcome: ApplyOutcome
    detail: str
    at_update: int


class BookManager:
    def __init__(self) -> None:
        self.books: dict[tuple[str, str], OrderBook] = {}
        self.resyncs: list[ResyncEvent] = []
        self.total_applied = 0
        self.total_rejected = 0

    def book_for(self, exchange: str, symbol: str) -> OrderBook:
        key = (exchange, symbol)
        if key not in self.books:
            self.books[key] = OrderBook(
                exchange, symbol, depth=VENUE_DEPTH.get(exchange)
            )
        return self.books[key]

    def apply(self, delta: BookDelta) -> ApplyResult:
        book = self.book_for(delta.exchange, delta.symbol)
        result = book.apply(delta)

        if result.ok:
            self.total_applied += 1
        else:
            self.total_rejected += 1
            # REJECTED_NOT_SEEDED is expected in bulk for a venue whose stream
            # carries no snapshot, so it is not recorded as a resync event --
            # that would drown the signal that matters.
            if result.outcome is not ApplyOutcome.REJECTED_NOT_SEEDED:
                self.resyncs.append(
                    ResyncEvent(
                        exchange=delta.exchange,
                        symbol=delta.symbol,
                        outcome=result.outcome,
                        detail=result.detail,
                        at_update=book.applied_updates,
                    )
                )
        return result

    def stale_books(self) -> list[OrderBook]:
        return [b for b in self.books.values() if b.state is BookState.STALE]

    def summary(self) -> dict:
        return {
            "books": len(self.books),
            "applied": self.total_applied,
            "rejected": self.total_rejected,
            "gaps": sum(b.gap_count for b in self.books.values()),
            "checksum_failures": sum(b.checksum_failures for b in self.books.values()),
            "stale": len(self.stale_books()),
        }
