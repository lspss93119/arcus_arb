from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PremiumValues:
    premium_bps: float
    sell_edge_bps: float
    buy_edge_bps: float


def calculate_premiums(
    entropy_bid: float,
    entropy_ask: float,
    hedge_bid: float,
    hedge_ask: float,
) -> PremiumValues:
    venue_a_mid = (entropy_bid + entropy_ask) / 2.0
    hedge_mid = (hedge_bid + hedge_ask) / 2.0
    return PremiumValues(
        premium_bps=(venue_a_mid / hedge_mid - 1.0) * 1e4,
        sell_edge_bps=(entropy_bid / hedge_ask - 1.0) * 1e4,
        buy_edge_bps=(hedge_bid / entropy_ask - 1.0) * 1e4,
    )
