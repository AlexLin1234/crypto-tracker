"""What a cross-venue price divergence is actually worth, net of costs.

This module exists to argue *against* the project's own most exciting output.
Detecting a price divergence between two venues is easy and looks impressive.
Almost none of it is exploitable, and a divergence detector that does not say so
is misleading rather than informative.

The framing throughout: a raw divergence is a **gross** number. Anything
presented as an opportunity has to survive fees, the spread you actually cross
on both venues, and the latency between observing and acting. Most does not.

Nothing here is trading advice or a claim of profitability. It is a cost floor:
a divergence below this floor is definitively *not* exploitable, while one above
it is merely *not yet ruled out*, which is a much weaker statement than it
sounds. See the caveats in `EXPLOITABILITY_CAVEATS`.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

#: Published retail *taker* fees in basis points, as rough public figures.
#: These are illustrative defaults, not negotiated or verified rates -- fee
#: tiers depend on 30-day volume and change without notice, and the exchange
#: fee endpoints are not reachable from this environment. Override them for any
#: analysis whose conclusion depends on the exact number. The conclusion here
#: does not: the costs dwarf the observed divergences by enough that a factor of
#: two either way changes nothing.
DEFAULT_TAKER_FEE_BPS: dict[str, Decimal] = {
    "kraken": Decimal("26"),      # ~0.26%
    "binance_us": Decimal("40"),  # ~0.40%
    "coinbase": Decimal("60"),    # ~0.60%
}

EXPLOITABILITY_CAVEATS = [
    "Latency: the divergence is measured at the venue timestamp. By the time an "
    "order could arrive, the quote may be gone -- retail round trips are tens to "
    "hundreds of milliseconds against a signal that often lasts less.",
    "Queue position: taking the far side assumes the displayed size is still "
    "there for you specifically. It frequently is not.",
    "Inventory: capturing a two-venue divergence requires capital pre-positioned "
    "on both venues. Moving assets between them is slow and itself has fees, so "
    "the capital is committed rather than free.",
    "Displayed size: top-of-book quantity bounds the trade. A wide divergence on "
    "a tiny resting size is not an opportunity, it is a rounding error.",
    "Adverse selection: a persistent divergence usually means one venue's quote "
    "is stale for a reason. Being filled against it is often the bad outcome, "
    "not the good one.",
    "This model omits maker rebates, slippage beyond the top level, withdrawal "
    "fees, funding costs, and taxes -- every one of which makes the picture "
    "worse, never better.",
]


@dataclasses.dataclass(frozen=True)
class CostModel:
    """Round-trip cost floor for acting on a two-venue divergence."""

    taker_fee_bps: dict[str, Decimal] = dataclasses.field(
        default_factory=lambda: dict(DEFAULT_TAKER_FEE_BPS)
    )
    #: Fraction of the quoted spread paid on each venue. Taking liquidity means
    #: crossing from mid to the far touch, which is half the spread per side.
    spread_crossing_fraction: Decimal = Decimal("0.5")

    def fee_bps(self, exchange: str) -> Decimal:
        if exchange not in self.taker_fee_bps:
            raise KeyError(f"no fee configured for {exchange!r}")
        return self.taker_fee_bps[exchange]

    def round_trip_cost_bps(
        self,
        exchange_a: str,
        exchange_b: str,
        spread_a_bps: Decimal,
        spread_b_bps: Decimal,
    ) -> Decimal:
        """Total cost in bps of buying on one venue and selling on the other.

        Two taker fees, plus the spread crossed on each side. Both legs are
        required: a divergence is only realizable by trading on both venues.
        """
        return (
            self.fee_bps(exchange_a)
            + self.fee_bps(exchange_b)
            + self.spread_crossing_fraction * (spread_a_bps + spread_b_bps)
        )

    def net_edge_bps(
        self,
        gross_divergence_bps: Decimal,
        exchange_a: str,
        exchange_b: str,
        spread_a_bps: Decimal,
        spread_b_bps: Decimal,
    ) -> Decimal:
        """Gross divergence minus the cost floor.

        The magnitude of the divergence is what matters, not its sign: a
        divergence in either direction is traded in the corresponding direction.
        """
        gross = abs(gross_divergence_bps)
        return gross - self.round_trip_cost_bps(
            exchange_a, exchange_b, spread_a_bps, spread_b_bps
        )

    def survives_costs(
        self,
        gross_divergence_bps: Decimal,
        exchange_a: str,
        exchange_b: str,
        spread_a_bps: Decimal,
        spread_b_bps: Decimal,
    ) -> bool:
        """Whether a divergence clears the cost floor at all.

        True does **not** mean profitable. It means only that this particular
        divergence has not been ruled out by the most basic and most favourable
        cost accounting available. Every item in `EXPLOITABILITY_CAVEATS` still
        applies, and each of them subtracts further.
        """
        return (
            self.net_edge_bps(
                gross_divergence_bps, exchange_a, exchange_b, spread_a_bps, spread_b_bps
            )
            > 0
        )

    def breakeven_divergence_bps(
        self,
        exchange_a: str,
        exchange_b: str,
        spread_a_bps: Decimal,
        spread_b_bps: Decimal,
    ) -> Decimal:
        """The divergence a venue pair must exceed before anything is left over.

        The single most useful number this module produces: it converts "we
        detected 1,204 divergences" into "how many were even arguably large
        enough to matter", which is usually a far smaller number.
        """
        return self.round_trip_cost_bps(exchange_a, exchange_b, spread_a_bps, spread_b_bps)
