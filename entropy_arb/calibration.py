"""Pure Phase B0 calibration rules.

The runtime wiring lives in :mod:`entropy_arb.arcus_execution`; this module
keeps quote math, fill accumulation, lifecycle transitions, and hard stops
small enough to test without a network or an exchange account.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

ARCUS_CALIBRATION_QTY = Decimal("0.01")
RH_HEDGE_MIN_QTY = Decimal("0.01")
PLACE_EDGE_BPS = Decimal("4.0")
CANCEL_EDGE_BPS = Decimal("1.5")
RH_HEDGE_SLIPPAGE_ALLOWANCE_BPS = Decimal("2.0")
RH_HEDGE_SLIPPAGE_HARD_CAP_BPS = Decimal("20.0")
MAX_ARCUS_FILL_EVENTS = 20
MAX_ARCUS_FILLED_NOTIONAL_USD = Decimal("500")
MAX_SESSION_LOSS_USD = Decimal("5")
MAX_RUNTIME_SECONDS = 60 * 60


def _decimal(value: Any, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be a finite decimal")
    return result


def _rate(bps: Decimal) -> Decimal:
    return _decimal(bps, "fee") / Decimal("10000")


def expected_edge_bps(
    *,
    side: str,
    arcus_price: Decimal,
    hedge_price: Decimal,
    arcus_fee_bps: Decimal,
    rh_fee_bps: Decimal,
    rh_slippage_allowance_bps: Decimal,
) -> float:
    """Return all-in expected edge in bps for one maker fill and hedge.

    The denominator is Arcus quote notional.  A SELL maker fill receives cash
    on Arcus and buys the RH offer; a BUY maker fill pays Arcus and sells the
    RH bid.  The RH allowance is modelled as an adverse price adjustment, not
    as an additional fee.
    """
    side = side.upper()
    arcus_price = _decimal(arcus_price, "arcus_price")
    hedge_price = _decimal(hedge_price, "hedge_price")
    if arcus_price <= 0 or hedge_price <= 0:
        raise ValueError("prices must be > 0")
    arcus_rate = _rate(_decimal(arcus_fee_bps, "arcus_fee_bps"))
    rh_rate = _rate(_decimal(rh_fee_bps, "rh_fee_bps"))
    allowance = _rate(_decimal(rh_slippage_allowance_bps, "rh_slippage_allowance_bps"))
    if side == "SELL":
        proceeds = arcus_price * (Decimal("1") - arcus_rate)
        hedge_cost = hedge_price * (Decimal("1") + rh_rate + allowance)
        edge = proceeds - hedge_cost
    elif side == "BUY":
        hedge_proceeds = hedge_price * (Decimal("1") - rh_rate - allowance)
        arcus_cost = arcus_price * (Decimal("1") + arcus_rate)
        edge = hedge_proceeds - arcus_cost
    else:
        raise ValueError("side must be BUY or SELL")
    return float(edge / arcus_price * Decimal("10000"))


@dataclass(frozen=True)
class QuoteCandidate:
    side: str
    price: Decimal
    quantity: Decimal
    hedge_side: str
    hedge_price: Decimal
    fair_price: Decimal
    expected_edge_bps: Decimal
    expected_usd: Decimal


def _round_to_tick(value: Decimal, tick: Decimal, *, up: bool) -> Decimal:
    if tick <= 0:
        raise ValueError("tick size must be > 0")
    units = value / tick
    rounding = ROUND_CEILING if up else ROUND_FLOOR
    return units.to_integral_value(rounding=rounding) * tick


def _field(inputs: Any, name: str) -> Decimal:
    return _decimal(getattr(inputs, name), name)


def build_quote_candidates(inputs: Any) -> list[QuoteCandidate]:
    """Build at most one candidate per side from current public BBOs.

    ``inputs`` is intentionally duck-typed so the engine can pass a small
    immutable view while tests and future tooling can use a simple namespace.
    The rolling center shifts the fair Arcus reference; actual edge is still
    checked against the executable RH bid/ask.
    """
    arcus_bid = _field(inputs, "arcus_bid")
    arcus_ask = _field(inputs, "arcus_ask")
    rh_bid = _field(inputs, "rh_bid")
    rh_ask = _field(inputs, "rh_ask")
    if not (
        arcus_bid > 0 and arcus_ask >= arcus_bid and rh_bid > 0 and rh_ask >= rh_bid
    ):
        return []

    center_bps = _field(inputs, "center_bps")
    tick = _field(inputs, "arcus_tick_size")
    step = _field(inputs, "arcus_step_size")
    arcus_fee = _field(inputs, "arcus_maker_fee_bps")
    rh_fee = _field(inputs, "rh_taker_fee_bps")
    allowance = _field(inputs, "rh_slippage_allowance_bps")
    quantity = _decimal(getattr(inputs, "quantity", ARCUS_CALIBRATION_QTY), "quantity")
    if quantity <= 0 or quantity % step != 0:
        raise ValueError("calibration quantity must be an exact Arcus step multiple")

    fair = ((rh_bid + rh_ask) / Decimal("2")) * (
        Decimal("1") + center_bps / Decimal("10000")
    )
    modeled_cost_bps = arcus_fee + rh_fee + allowance
    # Place a deliberately simple quote around fair, with the minimum edge
    # buffer baked into the target.  The current Arcus top level is used as a
    # floor/ceiling so the quote remains a resting post-only order.
    sell_target = fair * (
        Decimal("1") + (modeled_cost_bps + PLACE_EDGE_BPS) / Decimal("10000")
    )
    buy_target = fair * (
        Decimal("1") - (modeled_cost_bps + PLACE_EDGE_BPS) / Decimal("10000")
    )
    sell_price = _round_to_tick(max(arcus_ask, sell_target), tick, up=True)
    buy_price = _round_to_tick(min(arcus_bid, buy_target), tick, up=False)

    candidates = []
    for side, price, hedge_side, hedge_price in (
        ("SELL", sell_price, "BUY", rh_ask),
        ("BUY", buy_price, "SELL", rh_bid),
    ):
        if price <= 0:
            continue
        # ALO safety is checked again by the network client immediately before
        # submission against the latest Arcus BBO.
        edge = Decimal(
            str(
                expected_edge_bps(
                    side=side,
                    arcus_price=price,
                    hedge_price=hedge_price,
                    arcus_fee_bps=arcus_fee,
                    rh_fee_bps=rh_fee,
                    rh_slippage_allowance_bps=allowance,
                )
            )
        )
        candidates.append(
            QuoteCandidate(
                side=side,
                price=price,
                quantity=quantity,
                hedge_side=hedge_side,
                hedge_price=hedge_price,
                fair_price=fair,
                expected_edge_bps=edge,
                expected_usd=(edge / Decimal("10000")) * price * quantity,
            )
        )
    return candidates


def choose_quote(candidates: list[QuoteCandidate]) -> QuoteCandidate | None:
    """Select one qualifying side; never return simultaneous bid and ask."""
    qualifying = [c for c in candidates if c.expected_edge_bps >= PLACE_EDGE_BPS]
    if not qualifying:
        return None
    # Deterministic tie-breaker keeps a session one-sided without introducing
    # a preference that could masquerade as a tuned market-making policy.
    return max(
        qualifying,
        key=lambda candidate: (candidate.expected_edge_bps, candidate.side == "SELL"),
    )


@dataclass
class FillInstruction:
    quantity: Decimal
    hedge_side: str


@dataclass
class FillAccumulator:
    rh_min_qty: Decimal = RH_HEDGE_MIN_QTY
    rh_step: Decimal = RH_HEDGE_MIN_QTY
    unhedged_qty: Decimal = Decimal("0")
    side: str | None = None

    @property
    def residual_exposure(self) -> Decimal:
        return self.unhedged_qty

    def add_fill(self, *, side: str, quantity: Decimal) -> FillInstruction | None:
        side = side.upper()
        quantity = _decimal(quantity, "quantity")
        if side not in ("BUY", "SELL") or quantity <= 0:
            raise ValueError("fill side must be BUY/SELL and quantity > 0")
        if self.side is None:
            self.side = side
        elif self.side != side:
            raise RuntimeError("one calibration order cannot mix Arcus fill sides")
        self.unhedged_qty += quantity
        if self.unhedged_qty < self.rh_min_qty:
            return None
        units = (self.unhedged_qty / self.rh_step).to_integral_value(
            rounding=ROUND_FLOOR
        )
        hedge_qty = units * self.rh_step
        if hedge_qty < self.rh_min_qty or hedge_qty > self.unhedged_qty:
            return None
        self.unhedged_qty -= hedge_qty
        return FillInstruction(
            quantity=hedge_qty,
            hedge_side="BUY" if side == "SELL" else "SELL",
        )

    def reset_if_flat(self) -> None:
        """Allow a later one-sided order to use the opposite side.

        ``side`` describes the direction of the currently unhedged residual,
        not a session-wide inventory restriction.  Once the residual is flat,
        a subsequent calibration order may validly quote the other side.
        """
        if self.unhedged_qty == 0:
            self.side = None


@dataclass
class CalibrationLifecycle:
    state: str = "CANCELED"
    expected_edge_at_creation: Decimal | None = None
    original_qty: Decimal = ARCUS_CALIBRATION_QTY
    filled_qty: Decimal = Decimal("0")
    remaining_qty: Decimal = ARCUS_CALIBRATION_QTY
    client_id: str | None = None
    order_id: str | None = None
    quote_created_ts_ms: int | None = None

    TERMINAL = frozenset(("FILLED", "CANCELED", "REJECTED"))

    def mark_placed(
        self,
        *,
        expected_edge_bps: Decimal,
        client_id: str | None = None,
        order_id: str | None = None,
        original_qty: Decimal = ARCUS_CALIBRATION_QTY,
    ) -> None:
        if self.state not in self.TERMINAL and self.state != "CANCELED":
            raise RuntimeError("maximum one Arcus order may be live")
        self.state = "OPEN"
        self.expected_edge_at_creation = _decimal(
            expected_edge_bps, "expected_edge_bps"
        )
        self.original_qty = _decimal(original_qty, "original_qty")
        self.filled_qty = Decimal("0")
        self.remaining_qty = self.original_qty
        self.client_id = client_id
        self.order_id = order_id
        self.quote_created_ts_ms = None

    def request_cancel(self) -> None:
        if self.state in ("OPEN", "PARTIAL"):
            self.state = "CANCEL_SENT"

    def should_cancel(self, current_edge_bps: Decimal) -> bool:
        return (
            self.state in ("OPEN", "PARTIAL")
            and _decimal(current_edge_bps, "current_edge_bps") < CANCEL_EDGE_BPS
        )

    def record_fill(self, quantity: Decimal) -> None:
        if self.state == "REJECTED":
            raise RuntimeError("fill received after terminal order state")
        quantity = _decimal(quantity, "quantity")
        if quantity <= 0:
            raise ValueError("fill quantity must be > 0")
        self.filled_qty += quantity
        self.remaining_qty = max(self.original_qty - self.filled_qty, Decimal("0"))
        # A fill arriving after CANCEL_SENT remains actionable.  If the
        # terminal order update won the race locally, preserve that terminal
        # state while still increasing filled_qty; the authoritative fill is
        # accounted for separately by the runtime controller.
        if self.state not in self.TERMINAL:
            self.state = "PARTIAL"

    def record_order_status(
        self, status: str, remaining_qty: Decimal | None = None
    ) -> None:
        normalized = status.upper()
        # A delayed non-terminal update must not reopen an order after a
        # terminal update has already won the local race.  Fills are handled
        # separately and remain authoritative even after CANCELED.
        if self.state in self.TERMINAL and normalized in ("OPEN", "PARTIALLY_FILLED"):
            return
        if remaining_qty is not None:
            self.remaining_qty = max(
                _decimal(remaining_qty, "remaining_qty"), Decimal("0")
            )
        if normalized in ("OPEN", "PARTIALLY_FILLED"):
            self.state = (
                "PARTIAL"
                if self.filled_qty > 0 or self.remaining_qty < self.original_qty
                else "OPEN"
            )
        elif normalized in ("FILLED",):
            self.state = "FILLED"
        elif normalized in ("CANCELED", "MARGIN_CANCELED"):
            self.state = "CANCELED"
        elif normalized == "REJECTED":
            self.state = "REJECTED"
        else:
            raise ValueError(f"unsupported Arcus order status {status!r}")

    async def graceful_shutdown(self, gateway: Any) -> None:
        if self.state not in self.TERMINAL:
            self.request_cancel()
            await gateway.cancel_calibration_order(
                order_id=self.order_id, client_id=self.client_id
            )
        await gateway.reconcile()


@dataclass(frozen=True)
class SessionLimits:
    max_fill_events: int = MAX_ARCUS_FILL_EVENTS
    max_filled_notional_usd: Decimal = MAX_ARCUS_FILLED_NOTIONAL_USD
    max_loss_usd: Decimal = MAX_SESSION_LOSS_USD
    max_runtime_seconds: int = MAX_RUNTIME_SECONDS
    max_order_qty: Decimal = ARCUS_CALIBRATION_QTY


@dataclass
class SessionRisk:
    limits: SessionLimits
    started_monotonic: float = field(default_factory=time.monotonic)
    fill_events: int = 0
    filled_notional_usd: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    halted: bool = False
    halt_reason: str | None = None

    def halt(self, reason: str) -> None:
        if not self.halted:
            self.halted = True
            self.halt_reason = reason

    def on_arcus_fill(self, price: Decimal, quantity: Decimal) -> None:
        self.fill_events += 1
        self.filled_notional_usd += abs(
            _decimal(price, "price") * _decimal(quantity, "quantity")
        )
        if self.fill_events >= self.limits.max_fill_events:
            self.halt("max Arcus fill events reached")
        elif self.filled_notional_usd >= self.limits.max_filled_notional_usd:
            self.halt("max Arcus filled notional reached")

    def on_realized_pnl(self, value: Decimal) -> None:
        self.realized_pnl += _decimal(value, "realized_pnl")
        if self.realized_pnl <= -abs(self.limits.max_loss_usd):
            self.halt("max session loss reached")

    def check_runtime(self, *, now_monotonic: float | None = None) -> None:
        now = time.monotonic() if now_monotonic is None else now_monotonic
        if now - self.started_monotonic >= self.limits.max_runtime_seconds:
            self.halt("max runtime reached")

    def on_hedge_failure(self, reason: str) -> None:
        self.halt(f"RH hedge failure: {reason}")

    def on_telemetry_failure(self, reason: str) -> None:
        self.halt(f"telemetry failure: {reason}")

    def on_arcus_disconnect(self, lifecycle: CalibrationLifecycle) -> None:
        lifecycle.request_cancel()
        self.halt("Arcus account websocket disconnected")

    def check_residual(self, residual: Decimal, *, quantization_unit: Decimal) -> None:
        if _decimal(residual, "residual") > self.limits.max_order_qty + _decimal(
            quantization_unit, "quantization_unit"
        ):
            self.halt("Arcus residual exposure exceeded calibration envelope")


@dataclass(frozen=True)
class MarketHealth:
    arcus_l2_healthy: bool
    rh_healthy: bool
    bbo_fresh: bool
    account_ws_healthy: bool
    arcus_sequence_state: str
    outside_rth: bool | None
    arcus_status: str = "ONLINE"
    active_resync: bool = False

    @property
    def can_quote(self) -> bool:
        return (
            self.arcus_l2_healthy
            and self.rh_healthy
            and self.bbo_fresh
            and self.account_ws_healthy
            and self.arcus_sequence_state.upper() == "OK"
            and not self.active_resync
            and self.arcus_status.upper() == "ONLINE"
        )


@dataclass
class CalibrationPnL:
    actual_usd: Decimal = Decimal("0")

    def add_arcus_fill(
        self, *, side: str, price: Decimal, quantity: Decimal, fee: Decimal
    ) -> None:
        notional = _decimal(price, "price") * _decimal(quantity, "quantity")
        fee = _decimal(fee, "fee")
        if side.upper() == "SELL":
            self.actual_usd += notional - fee
        elif side.upper() == "BUY":
            self.actual_usd -= notional + fee
        else:
            raise ValueError("side must be BUY or SELL")

    def add_rh_hedge(
        self, *, side: str, price: Decimal, quantity: Decimal, fee: Decimal
    ) -> None:
        notional = _decimal(price, "price") * _decimal(quantity, "quantity")
        fee = _decimal(fee, "fee")
        if side.upper() == "SELL":
            self.actual_usd += notional - fee
        elif side.upper() == "BUY":
            self.actual_usd -= notional + fee
        else:
            raise ValueError("side must be BUY or SELL")
