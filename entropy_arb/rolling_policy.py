"""Pure ADD/REDUCE decisions for the Arcus rolling strategy."""
from __future__ import annotations

from dataclasses import dataclass

from .market_state import MarketProfile, allow_new_add, classify_market_state
from .rolling import RollingConfig, RollingSignal, RollingWindow


@dataclass(frozen=True)
class RollingDecision:
    action: str
    direction: str
    signal: RollingSignal
    market_state: str
    reason: str


class RollingPolicy:
    """Combine rolling signals, entry persistence, inventory cap and session gate.

    Break-even selection is deliberately not done here.  REDUCE means only
    that the opposite rolling signal is present; the execution layer must use
    the lot ledger and actual executable prices to select BE-safe lots.
    """

    def __init__(self, profile: MarketProfile, config: RollingConfig) -> None:
        self.profile = profile
        self.config = config
        self.window = RollingWindow(config)
        self._armed: dict[str, float | None] = {
            "sell_arcus": None,
            "buy_arcus": None,
        }

    def reset_arming(self) -> None:
        for direction in self._armed:
            self._armed[direction] = None

    def evaluate(
        self,
        *,
        premium_bps: float,
        now: float,
        open_direction: str | None,
        open_lot_count: int,
    ) -> RollingDecision | None:
        signal = self.window.signal(
            premium_bps,
            now,
            upper_bps=self.profile.upper_bps,
            lower_bps=self.profile.lower_bps,
        )
        if signal is None:
            self.reset_arming()
            return None

        state = classify_market_state(now).value
        if open_direction is not None and signal.direction != open_direction:
            # Exit signals are not held behind the entry persistence/session
            # filter.  The lot-specific BE gate in execution remains required.
            self.reset_arming()
            return RollingDecision(
                action="reduce",
                direction=signal.direction,
                signal=signal,
                market_state=state,
                reason="opposite_signal",
            )

        if open_direction not in (None, signal.direction):
            self.reset_arming()
            return None
        if open_lot_count >= self.profile.max_lots:
            self.reset_arming()
            return None
        if not allow_new_add(self.profile, now):
            self.reset_arming()
            return None

        for direction in self._armed:
            if direction != signal.direction:
                self._armed[direction] = None
        armed = self._armed[signal.direction]
        if armed is None:
            self._armed[signal.direction] = now
            return None
        if now - armed < self.config.persistence_sec:
            return None
        return RollingDecision(
            action="add",
            direction=signal.direction,
            signal=signal,
            market_state=state,
            reason="persistent_signal",
        )
