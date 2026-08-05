"""Live L2 order book reconstruction from snapshot + incremental diffs.

This is the stateful core of the pipeline and the part most likely to be subtly
wrong, so the design is deliberately boring: one class, an explicit state
machine, and no cleverness in the hot path.

Three things drive the shape of this module.

**Prices are Decimal keys, never floats.** A book is exact price-key matching:
a level keyed by a float one ULP away from the venue's is a *different* level,
so removals silently miss and the book slowly fills with phantom levels. See
DECISIONS.md D-003.

**Integrity is verified differently per venue, and the book does not pretend
otherwise.** Kraken sends a CRC32 over the top-N state; Binance numbers
messages with `U`/`u`. One says "your book is wrong", the other says "you
missed messages". Both mean resync, but they detect different failures at
different times, so they are separate code paths behind one result type
(D-004).

**A book that has lost integrity must stop, not limp.** The whole point of gap
detection is refusing to serve state you cannot vouch for. Once a gap or
checksum mismatch is seen the book goes STALE and rejects further updates until
a snapshot re-seeds it.
"""

from __future__ import annotations

import dataclasses
import enum
import zlib
from decimal import Decimal

from sortedcontainers import SortedDict

from ..ingest.schema import BookDelta, Level


class BookState(enum.Enum):
    EMPTY = "empty"      # never seeded; updates cannot be applied
    READY = "ready"      # seeded and trusted
    STALE = "stale"      # integrity lost; awaiting a snapshot to resync


class ApplyOutcome(enum.Enum):
    SNAPSHOT_APPLIED = "snapshot_applied"
    APPLIED = "applied"
    GAP_DETECTED = "gap_detected"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    REJECTED_NOT_SEEDED = "rejected_not_seeded"   # update before any snapshot
    REJECTED_STALE = "rejected_stale"             # update while awaiting resync

    @property
    def needs_resync(self) -> bool:
        return self in {
            ApplyOutcome.GAP_DETECTED,
            ApplyOutcome.CHECKSUM_MISMATCH,
            ApplyOutcome.REJECTED_NOT_SEEDED,
            ApplyOutcome.REJECTED_STALE,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class ApplyResult:
    outcome: ApplyOutcome
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.outcome in {ApplyOutcome.APPLIED, ApplyOutcome.SNAPSHOT_APPLIED}


class OrderBook:
    """One venue's L2 book for one symbol.

    `depth` truncates each side after every update. Kraken maintains the book at
    the subscribed depth and computes its checksum over exactly that many
    levels, so the truncation is not an optimization -- the checksum will not
    match without it. Binance streams unbounded diffs and passes `depth=None`.
    """

    def __init__(
        self,
        exchange: str,
        symbol: str,
        *,
        depth: int | None = None,
        price_precision: int | None = None,
        qty_precision: int = 8,
    ) -> None:
        self.exchange = exchange
        self.symbol = symbol
        self.depth = depth
        self.qty_precision = qty_precision
        self._price_precision = price_precision

        # Both sides ascend by price. Bids read from the high end, asks from the
        # low end; one ordering keeps the code symmetric and avoids negated keys.
        self._bids: SortedDict[Decimal, Decimal] = SortedDict()
        self._asks: SortedDict[Decimal, Decimal] = SortedDict()

        self.state = BookState.EMPTY
        self.last_sequence: int | None = None
        self.last_update_time = None
        self.gap_count = 0
        self.checksum_failures = 0
        self.applied_updates = 0

    # --- application ------------------------------------------------------

    def apply(self, delta: BookDelta) -> ApplyResult:
        if delta.is_snapshot:
            return self._apply_snapshot(delta)
        return self._apply_update(delta)

    def _apply_snapshot(self, delta: BookDelta) -> ApplyResult:
        """A snapshot always wins: it is the venue's own statement of truth.

        This is the only way out of STALE, which is why resync is defined as
        'resubscribe and take the next snapshot' rather than anything cleverer.
        """
        self._bids.clear()
        self._asks.clear()
        for level in delta.bids:
            if level.qty > 0:
                self._bids[level.price] = level.qty
        for level in delta.asks:
            if level.qty > 0:
                self._asks[level.price] = level.qty

        if self._price_precision is None:
            self._price_precision = _infer_price_precision(delta)

        self._truncate()
        self.state = BookState.READY
        self.last_sequence = delta.last_sequence
        self.last_update_time = delta.exchange_timestamp

        if delta.checksum is not None:
            computed = self.checksum()
            if computed != delta.checksum:
                self.checksum_failures += 1
                self.state = BookState.STALE
                return ApplyResult(
                    ApplyOutcome.CHECKSUM_MISMATCH,
                    f"snapshot checksum {computed} != {delta.checksum}",
                )
        return ApplyResult(ApplyOutcome.SNAPSHOT_APPLIED)

    def _apply_update(self, delta: BookDelta) -> ApplyResult:
        if self.state is BookState.EMPTY:
            return ApplyResult(
                ApplyOutcome.REJECTED_NOT_SEEDED,
                "update received before any snapshot",
            )
        if self.state is BookState.STALE:
            return ApplyResult(ApplyOutcome.REJECTED_STALE, "awaiting resync")

        # Sequence-numbered venues: check *before* mutating, so a detected gap
        # leaves the last known-good book intact rather than half-updated.
        if delta.first_sequence is not None and self.last_sequence is not None:
            expected = self.last_sequence + 1
            if delta.first_sequence != expected:
                self.gap_count += 1
                self.state = BookState.STALE
                return ApplyResult(
                    ApplyOutcome.GAP_DETECTED,
                    f"expected sequence {expected}, got {delta.first_sequence}",
                )

        for level in delta.bids:
            _put(self._bids, level)
        for level in delta.asks:
            _put(self._asks, level)

        self._truncate()
        self.applied_updates += 1
        self.last_update_time = delta.exchange_timestamp
        if delta.last_sequence is not None:
            self.last_sequence = delta.last_sequence

        # Checksum venues: verify *after* mutating, since the checksum describes
        # the resulting state rather than the message.
        if delta.checksum is not None:
            computed = self.checksum()
            if computed != delta.checksum:
                self.checksum_failures += 1
                self.state = BookState.STALE
                return ApplyResult(
                    ApplyOutcome.CHECKSUM_MISMATCH,
                    f"computed {computed} != {delta.checksum}",
                )

        return ApplyResult(ApplyOutcome.APPLIED)

    def _truncate(self) -> None:
        if self.depth is None:
            return
        while len(self._bids) > self.depth:
            self._bids.popitem(0)          # drop the lowest bid
        while len(self._asks) > self.depth:
            self._asks.popitem(-1)         # drop the highest ask

    # --- integrity --------------------------------------------------------

    def checksum(self) -> int:
        """Kraken's CRC32 over the top-10 book state.

        Derived empirically from the captured frames rather than from
        documentation, which was unreachable (D-000): each level contributes
        its price then its quantity, rendered at the instrument's precision with
        the decimal point removed and leading zeros stripped; asks first, then
        bids, each best-first, ten levels each.

        The derivation is only trustworthy because it is checked against every
        real checksum in the fixture, not just one -- see
        tests/test_orderbook.py.
        """
        precision = self._price_precision if self._price_precision is not None else 1
        parts = []
        for price, qty in list(self._asks.items())[:10]:
            parts.append(_checksum_token(price, precision))
            parts.append(_checksum_token(qty, self.qty_precision))
        for price, qty in reversed(list(self._bids.items())[-10:]):
            parts.append(_checksum_token(price, precision))
            parts.append(_checksum_token(qty, self.qty_precision))
        return zlib.crc32("".join(parts).encode())

    # --- reads ------------------------------------------------------------

    @property
    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        return self._bids.peekitem(-1) if self._bids else None

    @property
    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        return self._asks.peekitem(0) if self._asks else None

    @property
    def spread(self) -> Decimal | None:
        bid, ask = self.best_bid, self.best_ask
        return ask[0] - bid[0] if bid and ask else None

    @property
    def mid_price(self) -> Decimal | None:
        bid, ask = self.best_bid, self.best_ask
        return (bid[0] + ask[0]) / 2 if bid and ask else None

    def bids(self, n: int) -> list[Level]:
        items = list(self._bids.items())[-n:]
        return [Level(price=p, qty=q) for p, q in reversed(items)]

    def asks(self, n: int) -> list[Level]:
        return [Level(price=p, qty=q) for p, q in list(self._asks.items())[:n]]

    def volume_at_depth(self, n: int) -> tuple[Decimal, Decimal]:
        """(bid volume, ask volume) over the top n levels."""
        return (
            sum((lv.qty for lv in self.bids(n)), Decimal(0)),
            sum((lv.qty for lv in self.asks(n)), Decimal(0)),
        )

    def imbalance(self, n: int = 10) -> Decimal | None:
        """(bid - ask) / (bid + ask) volume over n levels, in [-1, 1].

        The order book imbalance feature Milestones 3 and 7 consume.
        """
        bid_vol, ask_vol = self.volume_at_depth(n)
        total = bid_vol + ask_vol
        return (bid_vol - ask_vol) / total if total > 0 else None

    def is_crossed(self) -> bool:
        bid, ask = self.best_bid, self.best_ask
        return bool(bid and ask and bid[0] >= ask[0])

    def __len__(self) -> int:
        return len(self._bids) + len(self._asks)

    def __repr__(self) -> str:
        return (
            f"<OrderBook {self.exchange}:{self.symbol} {self.state.value} "
            f"bids={len(self._bids)} asks={len(self._asks)}>"
        )


def _put(side: SortedDict, level: Level) -> None:
    """Apply one level. Quantity zero removes it -- both venues signal that way."""
    if level.qty == 0:
        side.pop(level.price, None)
    else:
        side[level.price] = level.qty


def _checksum_token(value: Decimal, precision: int) -> str:
    token = f"{value:.{precision}f}".replace(".", "").lstrip("0")
    return token or "0"


def _infer_price_precision(delta: BookDelta) -> int:
    """Infer an instrument's price precision from a snapshot.

    Kraken's checksum needs the instrument's configured precision, which is
    reference data the websocket feed never sends. JSON drops trailing zeros, so
    the widest price seen is a lower bound -- across twenty levels it is
    reliable in practice, and a wrong guess surfaces immediately as a checksum
    mismatch rather than as silent corruption, which is the failure mode worth
    having.
    """
    widest = 0
    for level in (*delta.bids, *delta.asks):
        exponent = level.price.as_tuple().exponent
        if isinstance(exponent, int):
            widest = max(widest, -exponent)
    return widest
