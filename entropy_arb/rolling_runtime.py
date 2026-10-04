"""Long-running rolling ADD/REDUCE controller for Arcus maker × RH taker.

The controller deliberately composes the already-tested CalibrationController
instead of duplicating its order, fill, hedge, and reconciliation machinery.
It owns only strategy state, inventory lots, maker repricing, and BE-safe
reduction selection.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_FLOOR
from typing import Any

from .calibration import QuoteCandidate, expected_edge_bps
from .calibration_runtime import CalibrationController, HedgeExecutionResult
from .lot_ledger import LotLedger, LotLedgerError
from .market_state import MarketProfile
from .rolling import RollingConfig
from .rolling_policy import RollingDecision, RollingPolicy

log = logging.getLogger("arcus-rolling")


def _decimal(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{label} must be decimal-compatible") from exc
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    return result


@dataclass
class RollingOrderIntent:
    execution_id: str
    action: str
    direction: str
    allocations: list[dict[str, float]] = field(default_factory=list)


@dataclass
class _PendingFill:
    intent: RollingOrderIntent
    fill_key: str
    side: str
    price: Decimal
    quantity: Decimal


class RollingArcusController:
    """Drive rolling signals through one safe Arcus maker order at a time."""

    def __init__(
        self,
        *,
        executor: CalibrationController,
        profile: MarketProfile,
        ledger: LotLedger,
        clip_usd: Decimal,
        quantity_step: Decimal,
        min_quantity: Decimal,
        min_notional: Decimal,
        max_quantity: Decimal | None = None,
        rolling_config: RollingConfig | None = None,
        reprice_sec: float = 30.0,
        tolerance: Decimal = Decimal("0.000000001"),
    ) -> None:
        if not profile.enabled:
            raise ValueError(f"{profile.symbol} rolling profile is disabled")
        self.executor = executor
        self.profile = profile
        self.ledger = ledger
        self.clip_usd = _decimal(clip_usd, "clip_usd")
        self.quantity_step = _decimal(quantity_step, "quantity_step")
        self.min_quantity = _decimal(min_quantity, "min_quantity")
        self.min_notional = _decimal(min_notional, "min_notional")
        self.max_quantity = (
            _decimal(max_quantity, "max_quantity")
            if max_quantity is not None
            else None
        )
        self.reprice_sec = float(reprice_sec)
        self.tolerance = _decimal(tolerance, "tolerance")
        if self.clip_usd <= 0:
            raise ValueError("clip_usd must be > 0")
        if self.quantity_step <= 0 or self.min_quantity <= 0:
            raise ValueError("quantity step/minimum must be > 0")
        if self.min_notional < 0:
            raise ValueError("min_notional must be >= 0")
        if self.max_quantity is not None and self.max_quantity < self.min_quantity:
            raise ValueError("max_quantity must be >= min_quantity")
        if not math.isfinite(self.reprice_sec) or self.reprice_sec <= 0:
            raise ValueError("reprice_sec must be finite and > 0")

        self.config = rolling_config or RollingConfig()
        self.policy = RollingPolicy(profile, self.config)
        self._intents: dict[str, RollingOrderIntent] = {}
        self._pending_fills: list[_PendingFill] = []
        self._order_placed_mono: float | None = None
        self._lot_sequence = 0
        self.add_fills = 0
        self.reduce_fills = 0
        self.realized_capture_usd = 0.0
        self.last_realized_capture_bps: float | None = None
        self.executor.on_processed_fill = self._on_processed_fill

    @property
    def open_reference_notional(self) -> Decimal:
        return Decimal(str(self.ledger.reference_notional))

    @property
    def max_reference_notional(self) -> Decimal:
        return self.clip_usd * self.profile.max_lots

    @property
    def open_clip_equivalents(self) -> int:
        if self.open_reference_notional <= 0:
            return 0
        return int(
            math.ceil(float(self.open_reference_notional / self.clip_usd) - 1e-12)
        )

    def ingest_minute(self, row: Any) -> bool:
        """Feed one completed Arcus minute row into the causal rolling window."""
        if isinstance(row, dict):
            payload = row
        else:
            payload = {
                "minute_ts": getattr(row, "minute_ts"),
                "samples": getattr(row, "samples"),
                "premium_close_bps": getattr(row, "premium_close_bps"),
            }
        return self.policy.window.ingest_row(payload)

    def seed_minutes(self, rows: list[dict[str, object]]) -> int:
        return sum(int(self.ingest_minute(row)) for row in rows)

    def validate_authoritative_positions(
        self, *, arcus_position: Decimal, rh_position: Decimal
    ) -> None:
        self.ledger.validate_positions(
            {
                "arcus": float(arcus_position),
                "hedge": float(rh_position),
            }
        )

    def _books(self) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        arcus_bid = self.executor.arcus.book.best_bid()
        arcus_ask = self.executor.arcus.book.best_ask()
        rh_bid = self.executor.hedge.book.best_bid()
        rh_ask = self.executor.hedge.book.best_ask()
        if None in (arcus_bid, arcus_ask, rh_bid, rh_ask):
            raise RuntimeError("rolling strategy requires complete Arcus/RH BBO")
        return (
            _decimal(arcus_bid, "arcus_bid"),
            _decimal(arcus_ask, "arcus_ask"),
            _decimal(rh_bid, "rh_bid"),
            _decimal(rh_ask, "rh_ask"),
        )

    def _premium_bps(self) -> float:
        arcus_bid, arcus_ask, rh_bid, rh_ask = self._books()
        arcus_mid = (arcus_bid + arcus_ask) / Decimal("2")
        rh_mid = (rh_bid + rh_ask) / Decimal("2")
        return float((arcus_mid / rh_mid - Decimal("1")) * Decimal("10000"))

    def _round_quantity(self, quantity: Decimal) -> Decimal:
        units = (quantity / self.quantity_step).to_integral_value(
            rounding=ROUND_FLOOR
        )
        result = units * self.quantity_step
        if self.max_quantity is not None:
            max_units = (self.max_quantity / self.quantity_step).to_integral_value(
                rounding=ROUND_FLOOR
            )
            result = min(result, max_units * self.quantity_step)
        return result

    def _quantity_for_usd(self, side: str, usd: Decimal) -> Decimal | None:
        arcus_bid, arcus_ask, _, _ = self._books()
        price = arcus_bid if side == "BUY" else arcus_ask
        quantity = self._round_quantity(usd / price)
        if quantity < self.min_quantity:
            return None
        if quantity * price < self.min_notional:
            return None
        return quantity

    def _candidate(self, *, side: str, quantity: Decimal) -> QuoteCandidate:
        arcus_bid, arcus_ask, rh_bid, rh_ask = self._books()
        side = side.upper()
        if side == "BUY":
            price = arcus_bid
            hedge_side = "SELL"
            hedge_price = rh_bid
        elif side == "SELL":
            price = arcus_ask
            hedge_side = "BUY"
            hedge_price = rh_ask
        else:
            raise ValueError("side must be BUY or SELL")
        edge = Decimal(
            str(
                expected_edge_bps(
                    side=side,
                    arcus_price=price,
                    hedge_price=hedge_price,
                    arcus_fee_bps=self.executor.arcus_maker_fee_bps,
                    rh_fee_bps=self.executor.rh_taker_fee_bps,
                    rh_slippage_allowance_bps=Decimal("0"),
                )
            )
        )
        return QuoteCandidate(
            side=side,
            price=price,
            quantity=quantity,
            hedge_side=hedge_side,
            hedge_price=hedge_price,
            fair_price=price,
            expected_edge_bps=edge,
            expected_usd=edge / Decimal("10000") * price * quantity,
        )

    def _eligible_reduce_allocations(
        self, direction: str, quantity_cap: Decimal
    ) -> list[dict[str, float]]:
        arcus_bid, arcus_ask, rh_bid, rh_ask = self._books()
        floor = self.config.min_exit_capture_bps
        scored: list[tuple[float, Any]] = []
        for lot in self.ledger.lots:
            if lot.direction == "sell_arcus" and direction == "buy_arcus":
                capture = lot.expected_exit_capture_bps(
                    float(arcus_bid),
                    float(rh_bid),
                    float(self.executor.arcus_maker_fee_bps),
                    float(self.executor.rh_taker_fee_bps),
                )
            elif lot.direction == "buy_arcus" and direction == "sell_arcus":
                capture = lot.expected_exit_capture_bps(
                    float(rh_ask),
                    float(arcus_ask),
                    float(self.executor.rh_taker_fee_bps),
                    float(self.executor.arcus_maker_fee_bps),
                )
            else:
                continue
            if capture + 1e-12 >= floor:
                scored.append((capture, lot))
        scored.sort(key=lambda item: item[0], reverse=True)

        remaining = quantity_cap
        allocations: list[dict[str, float]] = []
        for _capture, lot in scored:
            if remaining <= self.tolerance:
                break
            qty = min(Decimal(str(lot.open_qty)), remaining)
            qty = self._round_quantity(qty)
            if qty <= self.tolerance:
                continue
            allocations.append({"lot_id": lot.lot_id, "qty": float(qty)})
            remaining -= qty
        return allocations

    def _decision_order(
        self, decision: RollingDecision
    ) -> tuple[QuoteCandidate, list[dict[str, float]]] | None:
        if decision.action == "add":
            remaining_usd = self.max_reference_notional - self.open_reference_notional
            if remaining_usd <= 0:
                return None
            order_usd = min(self.clip_usd, remaining_usd)
            side = "SELL" if decision.direction == "sell_arcus" else "BUY"
            quantity = self._quantity_for_usd(side, order_usd)
            if quantity is None:
                return None
            return self._candidate(side=side, quantity=quantity), []

        if decision.action != "reduce":
            return None
        side = "SELL" if decision.direction == "sell_arcus" else "BUY"
        clip_qty = self._quantity_for_usd(side, self.clip_usd)
        if clip_qty is None:
            return None
        allocations = self._eligible_reduce_allocations(
            decision.direction, clip_qty
        )
        if not allocations:
            return None
        total = sum(Decimal(str(item["qty"])) for item in allocations)
        total = self._round_quantity(total)
        if total <= self.tolerance:
            return None
        return self._candidate(side=side, quantity=total), allocations

    async def _place_decision(self, decision: RollingDecision) -> bool:
        prepared = self._decision_order(decision)
        if prepared is None:
            return False
        candidate, allocations = prepared
        submitted = await self.executor.place_quote(
            candidate, reduce_only=decision.action == "reduce"
        )
        if not submitted:
            return False
        execution_id = self.executor.current_execution_id
        if not execution_id:
            raise RuntimeError("rolling maker placement has no execution id")
        self._intents[execution_id] = RollingOrderIntent(
            execution_id=execution_id,
            action=decision.action,
            direction=decision.direction,
            allocations=[dict(item) for item in allocations],
        )
        self._order_placed_mono = time.monotonic()
        log.info(
            "[rolling] %s %s state=%s px=%s qty=%s center=%s threshold=(%s,%s) "
            "open_notional=%s",
            decision.action.upper(),
            decision.direction,
            decision.market_state,
            candidate.price,
            candidate.quantity,
            decision.signal.center_bps,
            self.profile.upper_bps,
            self.profile.lower_bps,
            self.open_reference_notional,
        )
        return True

    async def step(self, *, now: float | None = None) -> RollingDecision | None:
        now = time.time() if now is None else float(now)
        self.executor.risk.check_runtime()
        health = self.executor.market_health()
        if self.executor.risk.halted or not health.can_quote:
            if self.executor.has_live_order:
                await self.executor.cancel_outstanding()
            return None

        if self.executor.has_live_order:
            if (
                self._order_placed_mono is not None
                and time.monotonic() - self._order_placed_mono >= self.reprice_sec
            ):
                await self.executor.cancel_outstanding()
            return None

        if bool(getattr(self.executor, "_terminal_reconcile_pending", False)):
            await self.executor.reconcile()
            return None

        decision = self.policy.evaluate(
            premium_bps=self._premium_bps(),
            now=now,
            open_direction=self.ledger.direction,
            open_lot_count=self.open_clip_equivalents,
        )
        if decision is None:
            return None
        await self._place_decision(decision)
        return decision

    @staticmethod
    def _fill_key(fill: Any) -> str:
        trade_id = getattr(fill, "trade_id", None)
        if trade_id:
            return f"trade:{trade_id}"
        return (
            f"fill:{getattr(fill, 'client_id', '')}:"
            f"{getattr(fill, 'created_at_us', '')}:{getattr(fill, 'quantity', '')}"
        )

    def _intent_for_fill(self, fill: Any) -> RollingOrderIntent:
        lookup = getattr(self.executor, "_context_for_fill", None)
        context = lookup(fill) if callable(lookup) else None
        execution_id = getattr(context, "execution_id", None)
        if execution_id is None:
            execution_id = self.executor.current_execution_id
        intent = self._intents.get(str(execution_id))
        if intent is None:
            raise LotLedgerError("processed Arcus fill has no rolling order intent")
        return intent

    def _add_matched_lot(
        self,
        pending: _PendingFill,
        quantity: Decimal,
        hedge: HedgeExecutionResult,
    ) -> None:
        self._lot_sequence += 1
        direction = pending.intent.direction
        if direction == "sell_arcus":
            buy_venue, sell_venue = "RH", "ARCUS"
            buy_px, sell_px = hedge.avg_px, pending.price
            buy_fee = self.executor.rh_taker_fee_bps
            sell_fee = self.executor.arcus_maker_fee_bps
        elif direction == "buy_arcus":
            buy_venue, sell_venue = "ARCUS", "RH"
            buy_px, sell_px = pending.price, hedge.avg_px
            buy_fee = self.executor.arcus_maker_fee_bps
            sell_fee = self.executor.rh_taker_fee_bps
        else:
            raise LotLedgerError(f"unknown add direction {direction}")
        self.ledger.add_lot(
            lot_id=f"{pending.intent.execution_id}:{self._lot_sequence}",
            source_event_id=pending.fill_key,
            direction=direction,
            open_qty=float(quantity),
            entry_ts=time.time(),
            buy_venue=buy_venue,
            sell_venue=sell_venue,
            buy_avg_px=float(buy_px),
            sell_avg_px=float(sell_px),
            buy_fee_bps=float(buy_fee),
            sell_fee_bps=float(sell_fee),
        )
        self.add_fills += 1

    def _consume_reduce_allocations(
        self,
        intent: RollingOrderIntent,
        quantity: Decimal,
        *,
        arcus_price: Decimal,
        hedge: HedgeExecutionResult,
    ) -> None:
        remaining = quantity
        closes: list[dict[str, float]] = []
        for item in intent.allocations:
            if remaining <= self.tolerance:
                break
            available = Decimal(str(item["qty"]))
            if available <= self.tolerance:
                continue
            qty = min(available, remaining)
            lot = next(
                (candidate for candidate in self.ledger.lots
                 if candidate.lot_id == item["lot_id"]),
                None,
            )
            if lot is None:
                raise LotLedgerError(
                    f"reduce allocation references missing lot {item['lot_id']}"
                )
            if lot.direction == "sell_arcus":
                buy_px, sell_px = arcus_price, hedge.avg_px
                buy_fee = self.executor.arcus_maker_fee_bps
                sell_fee = self.executor.rh_taker_fee_bps
            else:
                buy_px, sell_px = hedge.avg_px, arcus_price
                buy_fee = self.executor.rh_taker_fee_bps
                sell_fee = self.executor.arcus_maker_fee_bps
            capture_usd, capture_bps = self.ledger.realized_capture(
                lot.lot_id,
                float(qty),
                buy_px=float(buy_px),
                sell_px=float(sell_px),
                buy_fee_bps=float(buy_fee),
                sell_fee_bps=float(sell_fee),
            )
            self.realized_capture_usd += capture_usd
            self.last_realized_capture_bps = capture_bps
            closes.append({"lot_id": lot.lot_id, "qty": float(qty)})
            item["qty"] = float(available - qty)
            remaining -= qty
        if remaining > self.tolerance:
            raise LotLedgerError(
                f"reduce fill exceeds reserved BE allocations by {remaining}"
            )
        self.ledger.close_allocations(closes)
        self.reduce_fills += 1
        log.info(
            "[rolling] REDUCE settled qty=%s capture_bps=%s open_notional=%s",
            quantity,
            self.last_realized_capture_bps,
            self.open_reference_notional,
        )

    async def _on_processed_fill(
        self,
        fill: Any,
        hedge: HedgeExecutionResult | None,
        _fee_used: Decimal,
        was_actionable: bool,
    ) -> None:
        if not was_actionable:
            return
        try:
            intent = self._intent_for_fill(fill)
            pending = _PendingFill(
                intent=intent,
                fill_key=self._fill_key(fill),
                side=str(fill.side).upper(),
                price=_decimal(fill.price, "Arcus fill price"),
                quantity=_decimal(fill.quantity, "Arcus fill quantity"),
            )
            self._pending_fills.append(pending)
            if hedge is None:
                return

            remaining = _decimal(hedge.filled_qty, "RH hedge filled quantity")
            while remaining > self.tolerance and self._pending_fills:
                current = self._pending_fills[0]
                matched = min(current.quantity, remaining)
                if current.intent.action == "add":
                    self._add_matched_lot(current, matched, hedge)
                elif current.intent.action == "reduce":
                    self._consume_reduce_allocations(
                        current.intent,
                        matched,
                        arcus_price=current.price,
                        hedge=hedge,
                    )
                else:
                    raise LotLedgerError(
                        f"unknown rolling intent action {current.intent.action}"
                    )
                current.quantity -= matched
                remaining -= matched
                if current.quantity <= self.tolerance:
                    self._pending_fills.pop(0)
            if remaining > self.tolerance:
                raise LotLedgerError(
                    f"RH hedge quantity exceeds pending Arcus fills by {remaining}"
                )
        except Exception as exc:
            self.executor.risk.halt(f"rolling ledger failure: {exc}")
            with contextlib.suppress(Exception):
                await self.executor.cancel_outstanding()
            raise

    async def on_fill(self, fill: Any) -> None:
        await self.executor.on_fill(fill)

    async def on_order(self, update: Any) -> None:
        await self.executor.on_order(update)
        if not self.executor.has_live_order:
            self._order_placed_mono = None

    async def on_disconnect(self) -> None:
        await self.executor.on_disconnect()

    async def on_connect(self) -> None:
        await self.executor.on_connect()

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                await self.step()
                if self.executor.risk.halted:
                    stop.set()
                    break
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.2)
                except TimeoutError:
                    pass
        finally:
            if self.executor.has_live_order:
                with contextlib.suppress(Exception):
                    await self.executor.cancel_outstanding()
