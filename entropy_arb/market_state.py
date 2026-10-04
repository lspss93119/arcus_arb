"""Research-backed market-state policy for Arcus rolling canaries."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time
from enum import StrEnum
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")


class MarketState(StrEnum):
    WEEKEND_CLOSED = "weekend_closed"
    OVERNIGHT = "overnight"
    PREMARKET = "premarket"
    OPEN_TRANSITION = "open_transition"
    RTH_NORMAL = "rth_normal"
    AFTERHOURS = "afterhours"


@dataclass(frozen=True)
class MarketProfile:
    symbol: str
    upper_bps: float
    lower_bps: float
    max_lots: int
    live_candidate: bool = True
    enabled: bool = True
    block_open_transition_adds: bool = False


_RESEARCH_PROFILES = {
    "SPY": MarketProfile("SPY", 0.50, 0.50, 10, block_open_transition_adds=True),
    "QQQ": MarketProfile("QQQ", 0.75, 0.75, 10, block_open_transition_adds=True),
    "NVDA": MarketProfile("NVDA", 2.50, 2.50, 5),
    "SNDK": MarketProfile("SNDK", 4.00, 4.00, 0, enabled=False),
    "HYPE": MarketProfile("HYPE", 1.00, 1.00, 10, live_candidate=False),
    "XRP": MarketProfile("XRP", 0.75, 0.75, 10, live_candidate=False),
}


def research_profile(symbol: str) -> MarketProfile:
    key = str(symbol).upper()
    try:
        return _RESEARCH_PROFILES[key]
    except KeyError as exc:
        raise ValueError(f"no rolling research profile for {key}") from exc


def _as_et(now: float | datetime) -> datetime:
    if isinstance(now, datetime):
        if now.tzinfo is None:
            raise ValueError("datetime must be timezone-aware")
        return now.astimezone(NEW_YORK)
    return datetime.fromtimestamp(float(now), UTC).astimezone(NEW_YORK)


def classify_market_state(now: float | datetime) -> MarketState:
    """Classify the U.S.-equity reference-market state in New York time."""
    et = _as_et(now)
    weekday = et.weekday()  # Mon=0 ... Sun=6
    clock = et.time().replace(tzinfo=None)

    if weekday == 5:
        return MarketState.WEEKEND_CLOSED
    if weekday == 6 and clock < time(20, 0):
        return MarketState.WEEKEND_CLOSED
    if weekday == 4 and clock >= time(20, 0):
        return MarketState.WEEKEND_CLOSED

    if time(9, 30) <= clock < time(10, 0):
        return MarketState.OPEN_TRANSITION
    if time(10, 0) <= clock < time(16, 0):
        return MarketState.RTH_NORMAL
    if time(16, 0) <= clock < time(20, 0):
        return MarketState.AFTERHOURS
    if time(4, 0) <= clock < time(9, 30):
        return MarketState.PREMARKET
    return MarketState.OVERNIGHT


def allow_new_add(profile: MarketProfile, now: float | datetime) -> bool:
    """Apply only the execution filters supported by the 9/26–10/3 study."""
    if not profile.enabled or profile.max_lots <= 0:
        return False
    state = classify_market_state(now)
    if profile.block_open_transition_adds and state is MarketState.OPEN_TRANSITION:
        return False
    # Weekend is deliberately a telemetry label, not a trading ban.
    return True
