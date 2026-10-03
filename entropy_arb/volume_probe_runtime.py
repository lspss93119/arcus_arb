"""One-shot BUILD/HEDGE/UNWIND controller for the V1 volume probe.

This module composes the existing :class:`CalibrationController`.  It owns
only the second phase and round lifecycle; Arcus identity, fill correlation,
RH IOC hedging, and reconciliation remain in the B0 controller.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from .arcus_execution import ArcusRateLimited
from .calibration import QuoteCandidate
from .calibration_runtime import (
    RECONCILIATION_BACKOFF_CAP_SEC,
    RECONCILIATION_BACKOFF_INITIAL_SEC,
    CalibrationController,
    HedgeExecutionResult,
)
from .volume_probe import (
    ProbeCandidate,
    ProbeConfig,
    ProbeRoundMetrics,
    ProbeState,
    ProbeStateMachine,
    ProbeStatus,
    VolumeProbeRoundWriter,
    build_probe_candidate,
    unwind_side,
)


def _now_text() -> str:
    return datetime.now(UTC).isoformat()


log = logging.getLogger("volume-probe")
RECONCILIATION_DEADLINE_SEC = 15.0
RECONCILIATION_POLL_SEC = 1.0


def _decimal(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal")
    return result


@dataclass
class _PhaseMetrics:
    base_qty: Decimal = Decimal("0")
    hedged_qty: Decimal = Decimal("0")
    arcus_notional: Decimal = Decimal("0")
    rh_notional: Decimal = Decimal("0")
    arcus_fees: Decimal = Decimal("0")
    rh_fees: Decimal = Decimal("0")
    fill_events: int = 0
    reprices: int = 0
    first_quote_at: str | None = None
    first_fill_at: str | None = None
    completed_at: str | None = None
    fill_to_send_ms: list[int] = field(default_factory=list)
    fill_to_fill_ms: list[int] = field(default_factory=list)
    max_slippage_bps: Decimal = Decimal("0")

    @property
    def arcus_avg_px(self) -> Decimal | None:
        if self.base_qty == 0:
            return None
        return self.arcus_notional / self.base_qty

    @property
    def rh_avg_px(self) -> Decimal | None:
        if self.hedged_qty == 0:
            return None
        return self.rh_notional / self.hedged_qty

    @property
    def avg_fill_to_send_ms(self) -> float | None:
        if not self.fill_to_send_ms:
            return None
        return sum(self.fill_to_send_ms) / len(self.fill_to_send_ms)

    @property
    def avg_fill_to_fill_ms(self) -> float | None:
        if not self.fill_to_fill_ms:
            return None
        return sum(self.fill_to_fill_ms) / len(self.fill_to_fill_ms)


class VolumeProbeController:
    """Coordinate exactly one maker build and one maker unwind.

    The controller is deliberately usable with a small fake executor in unit
    tests.  In production ``executor`` is a configured
    :class:`CalibrationController` whose callbacks are attached to the Arcus
    account feed.
    """

    def __init__(
        self,
        *,
        executor: CalibrationController,
        config: ProbeConfig,
        symbol: str,
        session_id: str | None = None,
        writer: VolumeProbeRoundWriter | None = None,
        tolerance: Decimal = Decimal("0"),
    ) -> None:
        self.executor = executor
        self.config = config
        self.symbol = symbol
        executor_session_id = getattr(executor, "session_id", None)
        if session_id is not None and executor_session_id is not None:
            if session_id != executor_session_id:
                raise ValueError("volume probe and calibration session IDs must match")
        elif session_id is None and executor_session_id is not None:
            session_id = str(executor_session_id)
        self.session_id = session_id or f"vp-{uuid.uuid4().hex[:12]}"
        self.writer = writer
        self._metrics_written = False
        self._processed_fill_keys: set[str] = set()
        if hasattr(executor, "on_processed_fill"):
            executor.on_processed_fill = self._on_processed_fill
        self.tolerance = _decimal(tolerance, "quantity tolerance")
        if self.tolerance < 0:
            raise ValueError("quantity tolerance must be >= 0")
        self.machine = ProbeStateMachine()
        self.phase: str | None = None
        self.phase_target_qty: Decimal | None = None
        self.build_base_qty = Decimal("0")
        self.unwind_target_qty: Decimal | None = None
        self.unwind_base_qty = Decimal("0")
        self._order_placed_mono: float | None = None
        self._terminal_cancel_requested = False
        self._round_started_mono = time.monotonic()
        self._phase_started_mono: dict[str, float] = {}
        self._phase_completed_mono: dict[str, float] = {}
        self._failure_reason: str | None = None
        self._status: ProbeStatus | None = None
        self._terminal_log_keys: set[str] = set()
        self._phase_metrics = {
            "build": _PhaseMetrics(),
            "unwind": _PhaseMetrics(),
        }
        self.metrics = ProbeRoundMetrics(
            session_id=self.session_id,
            symbol=symbol,
            probe_side=config.probe_side,
            clip_usd=config.clip_usd,
            started_at=_now_text(),
        )

    @property
    def state(self) -> ProbeState:
        return self.machine.state

    @property
    def failure_reason(self) -> str | None:
        return self._failure_reason

    @property
    def status(self) -> ProbeStatus | None:
        return self._status

    @property
    def metrics_written(self) -> bool:
        return self._metrics_written

    def _expected_arcus_side(self) -> str | None:
        if self.phase == "build":
            return self.config.probe_side.upper()
        if self.phase == "unwind":
            return unwind_side(self.config.probe_side).upper()
        return None

    @property
    def reprice_allowed(self) -> bool:
        """Only permit a new quote after the prior order is terminal."""

        return not bool(getattr(self.executor, "has_live_order", False)) and not bool(
            getattr(self.executor, "_terminal_reconcile_pending", False)
        )

    def _phase(self) -> _PhaseMetrics:
        if self.phase not in self._phase_metrics:
            raise RuntimeError("volume-probe phase is not active")
        return self._phase_metrics[self.phase]

    def _log_terminal(self, reason: str) -> None:
        lower = reason.lower()
        if "placeorder rejected" in lower:
            event = "place_rejected"
        elif "reconciliation" in lower or "deadline" in lower:
            event = "RECONCILIATION_REQUIRED"
        elif "hedge" in lower or "unresolved" in lower:
            event = "hedge failure/unresolved"
        elif "timeout" in lower or "runtime" in lower:
            event = "TIMEOUT"
        else:
            event = "HALTED"
        key = f"{event}:{reason}"
        if key in self._terminal_log_keys:
            return
        self._terminal_log_keys.add(key)
        log.info("[volume-probe] %s reason=%s", event, reason)

    def _halt(self, reason: str) -> None:
        self._failure_reason = reason
        self._log_terminal(reason)
        if self.state in (ProbeState.DONE, ProbeState.RECONCILIATION_REQUIRED):
            self.machine.state = ProbeState.RECONCILIATION_REQUIRED
            self._status = ProbeStatus.RECONCILIATION_REQUIRED
            return
        if self.state not in (
            ProbeState.HALTED,
            ProbeState.RECONCILIATION_REQUIRED,
        ):
            self.machine.transition(ProbeState.HALTED)
        self._status = (
            ProbeStatus.TIMEOUT
            if "timeout" in reason.lower() or "runtime" in reason.lower()
            else ProbeStatus.HALTED
        )

    def _require_residual_reconciliation(self) -> bool:
        residual = _decimal(
            getattr(self.executor.accumulator, "residual_exposure", 0),
            "FillAccumulator residual",
        )
        if residual <= self.tolerance:
            return False
        self._failure_reason = self._failure_reason or (
            f"unresolved FillAccumulator residual exposure: {residual}"
        )
        self.machine.state = ProbeState.RECONCILIATION_REQUIRED
        self._status = ProbeStatus.RECONCILIATION_REQUIRED
        return True

    def begin_build(self, quantity: Decimal) -> None:
        quantity = _decimal(quantity, "build quantity")
        if quantity <= 0:
            raise ValueError("build quantity must be > 0")
        self.machine.transition(ProbeState.BUILD)
        self.phase = "build"
        self.phase_target_qty = quantity
        self._phase_started_mono["build"] = time.monotonic()
        self.metrics.build_first_quote_at = None

    def mark_preorder_only(
        self,
        *,
        arcus_position: Decimal,
        rh_position: Decimal,
    ) -> ProbeStatus:
        """Finalize a no-approval preview without entering a failure state."""

        if self.state is not ProbeState.BUILD or self._status is not None:
            raise RuntimeError("volume probe is not ready for PREORDER_ONLY")
        if not self.reprice_allowed:
            raise RuntimeError("cannot mark PREORDER_ONLY with a pending Arcus order")
        self.metrics.final_arcus_position = _decimal(
            arcus_position, "final Arcus position"
        )
        self.metrics.final_rh_position = _decimal(rh_position, "final RH position")
        self.metrics.finished_at = _now_text()
        self.metrics.status = ProbeStatus.PREORDER_ONLY
        self._status = ProbeStatus.PREORDER_ONLY
        return self._status

    def begin_unwind(self) -> None:
        if self.state is not ProbeState.HEDGED:
            raise RuntimeError("cannot begin unwind before BUILD is fully hedged")
        if self.build_base_qty <= self.tolerance:
            raise RuntimeError("cannot unwind a zero build quantity")
        accumulator = getattr(self.executor, "accumulator", None)
        residual = _decimal(
            getattr(accumulator, "residual_exposure", 0),
            "FillAccumulator residual",
        )
        if residual != 0:
            reason = "cannot begin unwind with nonzero FillAccumulator residual"
            self._halt(reason)
            raise RuntimeError(f"volume probe halted: {reason}")
        reset_if_flat = getattr(accumulator, "reset_if_flat", None)
        if callable(reset_if_flat):
            reset_if_flat()
        if hasattr(accumulator, "side") and getattr(accumulator, "side") is not None:
            reason = "FillAccumulator side was not cleared before unwind"
            self._halt(reason)
            raise RuntimeError(f"volume probe halted: {reason}")
        self.machine.transition(ProbeState.UNWIND)
        self.phase = "unwind"
        self.phase_target_qty = self.build_base_qty
        self.unwind_target_qty = self.build_base_qty
        self._phase_started_mono["unwind"] = time.monotonic()
        self.metrics.unwind_started_at = _now_text()

    def finish_unwind(self) -> None:
        if self.state is not ProbeState.UNWIND:
            raise RuntimeError("cannot finish unwind outside UNWIND")
        if self.unwind_base_qty + self.tolerance < (self.unwind_target_qty or 0):
            raise RuntimeError("unwind quantity is below the actual build target")
        self._phase().completed_at = _now_text()
        self.metrics.unwind_complete_at = self._phase().completed_at
        started = self._phase_started_mono.get("unwind")
        if started is not None:
            self.metrics.unwind_seconds = time.monotonic() - started
        self._phase_completed_mono["unwind"] = time.monotonic()
        self.machine.transition(ProbeState.FLAT)
        log.info("[volume-probe] UNWIND complete")
        self.phase = None
        self.phase_target_qty = None

    def record_reprice(self) -> None:
        if not self.reprice_allowed:
            raise RuntimeError(
                "cannot reprice before Arcus order reaches terminal state"
            )
        if self.phase in self._phase_metrics:
            self._phase_metrics[self.phase].reprices += 1

    def _record_fill_metrics(
        self,
        *,
        arcus_side: str,
        arcus_price: Decimal,
        arcus_quantity: Decimal,
        arcus_fee: Decimal | None,
        hedge: HedgeExecutionResult | None,
    ) -> None:
        phase = self._phase()
        phase_name = (self.phase or "unknown").upper()
        quantity = _decimal(arcus_quantity, "Arcus fill quantity")
        price = _decimal(arcus_price, "Arcus fill price")
        phase.base_qty += quantity
        phase.arcus_notional += price * quantity
        phase.arcus_fees += _decimal(arcus_fee or 0, "Arcus fee")
        phase.fill_events += 1
        now = _now_text()
        if phase.first_fill_at is None:
            phase.first_fill_at = now
        if hedge is not None:
            expected_side = "BUY" if arcus_side.upper() == "SELL" else "SELL"
            if hedge.hedge_side != expected_side:
                self._halt("RH hedge side did not oppose Arcus fill")
                raise RuntimeError(self._failure_reason or "RH hedge side mismatch")
            if hedge.filled_qty + self.tolerance < quantity:
                self._halt("RH hedge partial fill")
                raise RuntimeError(self._failure_reason or "RH hedge partial fill")
            phase.hedged_qty += hedge.filled_qty
            phase.rh_notional += hedge.avg_px * hedge.filled_qty
            phase.rh_fees += hedge.fee
            if hedge.fill_to_hedge_send_ms is not None:
                phase.fill_to_send_ms.append(hedge.fill_to_hedge_send_ms)
            phase.fill_to_fill_ms.append(hedge.fill_to_rh_fill_ms)
            phase.max_slippage_bps = max(
                phase.max_slippage_bps, hedge.realized_slippage_bps
            )
        log.info(
            "[volume-probe] %s fill qty=%s avg_px=%s",
            phase_name,
            quantity,
            price,
        )
        if hedge is not None:
            log.info(
                "[volume-probe] %s hedge side=%s qty=%s avg_px=%s latency_ms=%s",
                phase_name,
                hedge.hedge_side,
                hedge.filled_qty,
                hedge.avg_px,
                hedge.fill_to_rh_fill_ms,
            )
        if self.phase == "build":
            self.build_base_qty = phase.base_qty
            target = self.phase_target_qty
            if (
                target is not None
                and phase.base_qty + self.tolerance >= target
                and phase.hedged_qty + self.tolerance >= phase.base_qty
            ):
                phase.completed_at = now
                self.metrics.build_complete_at = now
                self._phase_completed_mono["build"] = time.monotonic()
                self.machine.transition(ProbeState.HEDGED)
                log.info("[volume-probe] BUILD complete")
        elif self.phase == "unwind":
            self.unwind_base_qty = phase.base_qty

    def record_hedged_fill(
        self,
        *,
        arcus_side: str,
        arcus_price: Decimal,
        arcus_quantity: Decimal,
        arcus_fee: Decimal | None,
        hedge: HedgeExecutionResult | None,
        allow_unhedged: bool = False,
    ) -> None:
        if self.state not in (ProbeState.BUILD, ProbeState.UNWIND):
            raise RuntimeError("fill received outside BUILD or UNWIND")
        expected_side = self._expected_arcus_side()
        if expected_side is not None and arcus_side.upper() != expected_side:
            reason = (
                f"Arcus fill side {arcus_side!r} does not match "
                f"{self.phase} side {expected_side}"
            )
            self._halt(reason)
            raise RuntimeError(f"volume probe halted: {reason}")
        if hedge is None and not allow_unhedged:
            reason = str(
                getattr(getattr(self.executor, "risk", None), "halt_reason", None)
                or "RH hedge unresolved"
            )
            self._halt(reason)
            raise RuntimeError(f"volume probe halted: {reason}")
        self._record_fill_metrics(
            arcus_side=arcus_side,
            arcus_price=arcus_price,
            arcus_quantity=arcus_quantity,
            arcus_fee=arcus_fee,
            hedge=hedge,
        )

    def candidate(self, *, best_bid: Decimal, best_ask: Decimal) -> ProbeCandidate:
        if self.phase not in ("build", "unwind"):
            raise RuntimeError("no active volume-probe quote phase")
        target = self.phase_target_qty
        if target is None:
            raise RuntimeError("volume-probe phase has no target quantity")
        remaining = target - self._phase().base_qty
        if remaining <= self.tolerance:
            raise RuntimeError("volume-probe phase has no remaining quantity")
        side = (
            self.config.probe_side
            if self.phase == "build"
            else unwind_side(self.config.probe_side)
        )
        return build_probe_candidate(
            probe_side=side,
            quantity=remaining,
            best_bid=best_bid,
            best_ask=best_ask,
        )

    def finalize_positions(
        self,
        *,
        arcus_position: Decimal,
        rh_position: Decimal,
        arcus_open_orders: Iterable[Any],
        rh_open_orders: Iterable[Mapping[str, Any]],
    ) -> ProbeStatus:
        arcus = _decimal(arcus_position, "final Arcus position")
        rh = _decimal(rh_position, "final RH position")
        arcus_orders = tuple(arcus_open_orders)
        rh_orders = tuple(rh_open_orders)
        self.metrics.final_arcus_position = arcus
        self.metrics.final_rh_position = rh
        residual = _decimal(
            getattr(self.executor.accumulator, "residual_exposure", 0),
            "FillAccumulator residual",
        )
        if (
            abs(arcus) > self.tolerance
            or abs(rh) > self.tolerance
            or abs(residual) > self.tolerance
            or arcus_orders
            or rh_orders
            or self.state is ProbeState.RECONCILIATION_REQUIRED
            or (
                self.state is not ProbeState.FLAT
                and self.state is not ProbeState.HALTED
            )
        ):
            self._failure_reason = self._failure_reason or (
                "final positions/orders/residual are not flat"
            )
            if self.state is not ProbeState.RECONCILIATION_REQUIRED:
                self.machine.transition(ProbeState.RECONCILIATION_REQUIRED)
            self._status = ProbeStatus.RECONCILIATION_REQUIRED
            return self._status
        if self.state is ProbeState.HALTED:
            return self._status or ProbeStatus.HALTED
        self.machine.transition(ProbeState.DONE)
        self.metrics.finished_at = _now_text()
        self._status = ProbeStatus.COMPLETED
        pnl = getattr(getattr(self.executor, "pnl", None), "actual_usd", None)
        log.info(
            "[volume-probe] FINAL arcus_position=%s rh_position=%s status=%s "
            "pnl_usd=%s",
            arcus,
            rh,
            self._status.value,
            pnl,
        )
        return self._status

    def _runtime_candidate(self) -> QuoteCandidate:
        book = self.executor.arcus.book
        bid = _decimal(book.best_bid(), "Arcus best bid")
        ask = _decimal(book.best_ask(), "Arcus best ask")
        probe = self.candidate(best_bid=bid, best_ask=ask)
        hedge_book = self.executor.hedge.book
        hedge_bid = _decimal(hedge_book.best_bid(), "RH best bid")
        hedge_ask = _decimal(hedge_book.best_ask(), "RH best ask")
        hedge_price = hedge_ask if probe.hedge_side == "BUY" else hedge_bid
        return QuoteCandidate(
            side=probe.arcus_side,
            price=probe.price,
            quantity=probe.quantity,
            hedge_side=probe.hedge_side,
            hedge_price=hedge_price,
            fair_price=probe.price,
            expected_edge_bps=Decimal("0"),
            expected_usd=Decimal("0"),
        )

    async def place_next_quote(self) -> None:
        if not self.reprice_allowed:
            raise RuntimeError("cannot place a second volume-probe order")
        candidate = self._runtime_candidate()
        phase = self._phase()
        if phase.first_quote_at is None:
            phase.first_quote_at = _now_text()
        await self.executor.place_quote(candidate)
        self._terminal_cancel_requested = False
        self._order_placed_mono = time.monotonic()
        log.info(
            "[volume-probe] %s quote placed side=%s price=%s qty=%s",
            (self.phase or "unknown").upper(),
            candidate.side,
            candidate.price,
            candidate.quantity,
        )

    @staticmethod
    def _fill_key(fill: Any) -> str:
        trade_id = getattr(fill, "trade_id", None)
        if trade_id:
            return f"trade:{trade_id}"
        return f"object:{id(fill)}"

    async def _on_processed_fill(
        self,
        fill: Any,
        hedge: HedgeExecutionResult | None,
        fee_used: Decimal,
        was_actionable: bool,
    ) -> None:
        if not was_actionable:
            return
        key = self._fill_key(fill)
        if key in self._processed_fill_keys:
            return
        self._processed_fill_keys.add(key)
        await self._account_processed_fill(
            fill=fill,
            hedge=hedge,
            arcus_fee=fee_used,
        )

    async def _account_processed_fill(
        self,
        *,
        fill: Any,
        hedge: HedgeExecutionResult | None,
        arcus_fee: Decimal | None,
    ) -> None:
        if hedge is None:
            residual = _decimal(
                getattr(self.executor.accumulator, "residual_exposure", 0),
                "FillAccumulator residual",
            )
            if getattr(self.executor.risk, "halted", False) or residual <= 0:
                reason = str(
                    getattr(self.executor.risk, "halt_reason", None)
                    or "RH hedge unresolved"
                )
                self._halt(reason)
                cancel = getattr(self.executor, "cancel_outstanding", None)
                if callable(cancel):
                    with contextlib.suppress(Exception):
                        await cancel()
                return
            # The inherited FillAccumulator intentionally holds a sub-minimum
            # Arcus fill until a later same-side fill makes one executable RH
            # IOC.  Record that pending exposure, but never treat it as
            # hedged or advance to UNWIND.
            self.record_hedged_fill(
                arcus_side=fill.side,
                arcus_price=fill.price,
                arcus_quantity=fill.quantity,
                arcus_fee=arcus_fee,
                hedge=None,
                allow_unhedged=True,
            )
            return
        try:
            self.record_hedged_fill(
                arcus_side=fill.side,
                arcus_price=fill.price,
                arcus_quantity=fill.quantity,
                arcus_fee=arcus_fee,
                hedge=hedge,
            )
        except RuntimeError as exc:
            reason = str(exc) or "Arcus fill could not be assigned to a probe phase"
            halt = getattr(self.executor.risk, "halt", None)
            if callable(halt):
                halt(reason)
            self._halt(reason)
            cancel = getattr(self.executor, "cancel_outstanding", None)
            if callable(cancel):
                with contextlib.suppress(Exception):
                    await cancel()
            return
        if getattr(self.executor.risk, "halted", False):
            self._halt(
                str(
                    getattr(self.executor.risk, "halt_reason", None)
                    or "executor halted"
                )
            )

    async def on_fill(self, fill: Any) -> None:
        context_lookup = getattr(self.executor, "_context_for_fill", None)
        if callable(context_lookup) and context_lookup(fill) is None:
            # The account stream is account-scoped.  A new fill that cannot be
            # tied to this probe is an identity ambiguity, not a harmless
            # unrelated event; stop before the inherited controller can
            # ignore it.
            account_state = getattr(self.executor, "account_state", None)
            is_pre_start_fill = getattr(account_state, "is_pre_start_fill", None)
            if callable(is_pre_start_fill) and is_pre_start_fill(fill):
                return
            if not bool(getattr(fill, "is_snapshot", False)):
                reason = "unknown Arcus fill identity during volume probe"
                halt = getattr(self.executor.risk, "halt", None)
                if callable(halt):
                    halt(reason)
                self._halt(reason)
                cancel = getattr(self.executor, "cancel_outstanding", None)
                if callable(cancel):
                    with contextlib.suppress(Exception):
                        await cancel()
                return
        key = self._fill_key(fill)
        before = int(getattr(self.executor.risk, "fill_events", 0))
        await self.executor.on_fill(fill)
        if key in self._processed_fill_keys:
            self._processed_fill_keys.discard(key)
            if getattr(self.executor.risk, "halted", False):
                self._halt(
                    str(
                        getattr(self.executor.risk, "halt_reason", None)
                        or "executor halted"
                    )
                )
            return
        after = int(getattr(self.executor.risk, "fill_events", 0))
        if after == before:
            if getattr(self.executor.risk, "halted", False):
                self._halt(
                    str(
                        getattr(self.executor.risk, "halt_reason", None)
                        or "executor halted"
                    )
                )
            return
        fee_record = getattr(self.executor, "_arcus_fee_records", {}).get(
            getattr(fill, "trade_id", None)
        )
        fee = getattr(fee_record, "fee_used", None)
        hedge = getattr(self.executor, "last_hedge_result", None)
        arcus_fee = fee if fee is not None else getattr(fill, "fee", None)
        await self._account_processed_fill(
            fill=fill,
            hedge=hedge,
            arcus_fee=arcus_fee,
        )

    async def on_order(self, update: Any) -> None:
        await self.executor.on_order(update)

    async def on_disconnect(self) -> None:
        if self.state is ProbeState.DONE or self.status in (
            ProbeStatus.COMPLETED,
            ProbeStatus.PREORDER_ONLY,
        ):
            return
        await self.executor.on_disconnect()
        self._halt(
            str(getattr(self.executor.risk, "halt_reason", None) or "Arcus disconnect")
        )

    async def on_connect(self) -> None:
        await self.executor.on_connect()

    def _require_reconciliation(self, reason: str) -> None:
        if self.status is ProbeStatus.RECONCILIATION_REQUIRED:
            return
        self._failure_reason = self._failure_reason or reason
        marker = getattr(self.executor, "mark_reconciliation_required", None)
        if callable(marker):
            marker(reason)
        self.machine.state = ProbeState.RECONCILIATION_REQUIRED
        self._status = ProbeStatus.RECONCILIATION_REQUIRED
        self._log_terminal(reason)
        log.warning("[ARCUS] reconciliation required: %s", reason)

    @staticmethod
    def _bounded_retry_delay(retry_after: Any, backoff: float) -> float:
        try:
            requested = float(retry_after)
        except (TypeError, ValueError):
            requested = math.nan
        if not math.isfinite(requested):
            return min(
                RECONCILIATION_BACKOFF_CAP_SEC,
                max(RECONCILIATION_BACKOFF_INITIAL_SEC, backoff),
            )
        return max(RECONCILIATION_BACKOFF_INITIAL_SEC, backoff, requested)

    async def _reconcile_terminal(self, deadline: float) -> bool:
        backoff = RECONCILIATION_BACKOFF_INITIAL_SEC
        while not self.reprice_allowed:
            if time.monotonic() >= deadline:
                self._require_reconciliation(
                    "Arcus order terminal reconciliation deadline expired"
                )
                return False
            status: str | None = None
            retry_after: Any = None
            error: Exception | None = None
            try:
                result = await self.executor.reconcile()
            except ArcusRateLimited as exc:
                status = "rate_limited"
                retry_after = exc.retry_after
                error = exc
            except Exception as exc:
                self._require_reconciliation(
                    f"Arcus terminal reconciliation failed ({type(exc).__name__})"
                )
                return False
            else:
                status = getattr(result, "status", None)
                retry_after = getattr(result, "retry_after", None)
                error = getattr(result, "error", None)

            if self.reprice_allowed:
                return True
            if status == "failed":
                reason = (
                    f"Arcus terminal reconciliation failed ({type(error).__name__})"
                    if error is not None
                    else "Arcus terminal reconciliation failed"
                )
                self._require_reconciliation(reason)
                return False
            now = time.monotonic()
            if now >= deadline:
                self._require_reconciliation(
                    "Arcus order terminal reconciliation deadline expired"
                )
                return False
            if status == "rate_limited":
                delay = self._bounded_retry_delay(retry_after, backoff)
                backoff = min(RECONCILIATION_BACKOFF_CAP_SEC, backoff * 2.0)
            else:
                delay = RECONCILIATION_POLL_SEC
                backoff = RECONCILIATION_BACKOFF_INITIAL_SEC
            remaining = deadline - now
            await asyncio.sleep(min(delay, remaining))
        return True

    async def _cancel_terminal(self) -> bool:
        if self.reprice_allowed:
            return True
        if (
            getattr(self.executor, "has_live_order", False)
            and not self._terminal_cancel_requested
        ):
            try:
                await self.executor.cancel_outstanding()
            except Exception as exc:
                self._require_reconciliation(
                    f"Arcus cancel failed ({type(exc).__name__})"
                )
                return False
            self._terminal_cancel_requested = True
        deadline = time.monotonic() + RECONCILIATION_DEADLINE_SEC
        return await self._reconcile_terminal(deadline)

    async def _read_final_state(
        self,
        reader: Callable[
            [], Awaitable[tuple[Decimal, Decimal, list[Any], list[Mapping[str, Any]]]]
        ],
    ) -> tuple[Decimal, Decimal, list[Any], list[Mapping[str, Any]]] | None:
        deadline = time.monotonic() + RECONCILIATION_DEADLINE_SEC
        backoff = RECONCILIATION_BACKOFF_INITIAL_SEC
        while True:
            if time.monotonic() >= deadline:
                self._require_reconciliation(
                    "final reconciliation deadline expired; state is unknown"
                )
                return None
            try:
                return await reader()
            except ArcusRateLimited as exc:
                delay = self._bounded_retry_delay(exc.retry_after, backoff)
                backoff = min(RECONCILIATION_BACKOFF_CAP_SEC, backoff * 2.0)
                log.warning(
                    "[ARCUS] reconciliation rate limited; retry in %.1fs", delay
                )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._require_reconciliation(
                        "final reconciliation deadline expired; state is unknown"
                    )
                    return None
                await asyncio.sleep(min(delay, remaining))
            except Exception as exc:
                log.exception("final reconciliation failed")
                self._require_reconciliation(
                    f"final reconciliation failed ({type(exc).__name__})"
                )
                return None

    def _sync_metrics(self) -> None:
        build = self._phase_metrics["build"]
        unwind = self._phase_metrics["unwind"]
        self.metrics.build_base_qty = build.base_qty or None
        self.metrics.build_first_quote_at = build.first_quote_at
        self.metrics.build_first_fill_at = build.first_fill_at
        self.metrics.build_complete_at = build.completed_at
        self.metrics.build_arcus_avg_px = build.arcus_avg_px
        self.metrics.build_rh_avg_px = build.rh_avg_px
        self.metrics.build_reprices = build.reprices
        self.metrics.build_fill_events = build.fill_events
        self.metrics.unwind_base_qty = unwind.base_qty or None
        self.metrics.unwind_complete_at = unwind.completed_at
        self.metrics.unwind_arcus_avg_px = unwind.arcus_avg_px
        self.metrics.unwind_rh_avg_px = unwind.rh_avg_px
        self.metrics.unwind_reprices = unwind.reprices
        self.metrics.unwind_fill_events = unwind.fill_events
        self.metrics.arcus_fees = build.arcus_fees + unwind.arcus_fees
        self.metrics.rh_fees = build.rh_fees + unwind.rh_fees
        self.metrics.max_rh_slippage_bps = max(
            build.max_slippage_bps, unwind.max_slippage_bps
        )
        lat_send = build.fill_to_send_ms + unwind.fill_to_send_ms
        lat_fill = build.fill_to_fill_ms + unwind.fill_to_fill_ms
        self.metrics.avg_fill_to_hedge_send_ms = (
            sum(lat_send) / len(lat_send) if lat_send else None
        )
        self.metrics.avg_fill_to_rh_fill_ms = (
            sum(lat_fill) / len(lat_fill) if lat_fill else None
        )
        pnl = getattr(self.executor, "pnl", None)
        self.metrics.realized_round_pnl_usd = getattr(pnl, "actual_usd", None)
        build_started = self._phase_started_mono.get("build")
        build_completed = self._phase_completed_mono.get("build")
        if build_started is not None and build_completed is not None:
            self.metrics.build_seconds = max(0.0, build_completed - build_started)
        unwind_started = self._phase_started_mono.get("unwind")
        unwind_completed = self._phase_completed_mono.get("unwind")
        if unwind_started is not None and unwind_completed is not None:
            self.metrics.unwind_seconds = max(0.0, unwind_completed - unwind_started)

    async def run(
        self,
        stop: asyncio.Event,
        *,
        quantity: Decimal,
        final_state_reader: Callable[
            [], Awaitable[tuple[Decimal, Decimal, list[Any], list[Mapping[str, Any]]]]
        ],
    ) -> ProbeStatus:
        started = time.monotonic()
        try:
            if self.state is ProbeState.FLAT:
                self.begin_build(quantity)
            elif self.state is not ProbeState.BUILD:
                raise RuntimeError("volume probe is not ready to start BUILD")
            await self.place_next_quote()
            while not stop.is_set():
                if getattr(self.executor, "_terminal_reconcile_pending", False):
                    if not await self._reconcile_terminal(
                        time.monotonic() + RECONCILIATION_DEADLINE_SEC
                    ):
                        break
                self.executor.risk.check_runtime()
                if getattr(self.executor.risk, "halted", False):
                    reason = str(
                        getattr(self.executor.risk, "halt_reason", None)
                        or "executor halted"
                    )
                    self._halt(reason)
                    if await self._cancel_terminal():
                        final = await self._read_final_state(final_state_reader)
                        if final is not None:
                            self.finalize_positions(
                                arcus_position=final[0],
                                rh_position=final[1],
                                arcus_open_orders=final[2],
                                rh_open_orders=final[3],
                            )
                    break
                health = self.executor.market_health()
                if not health.can_quote:
                    self.executor.risk.halt("volume-probe market health gate failed")
                    self._halt("volume-probe market health gate failed")
                    await self._cancel_terminal()
                    break
                if self.state is ProbeState.HEDGED and self.reprice_allowed:
                    self.begin_unwind()
                    await self.place_next_quote()
                elif (
                    self.state is ProbeState.UNWIND and not self.executor.has_live_order
                ):
                    if self._phase().base_qty + self.tolerance >= (
                        self.phase_target_qty or 0
                    ):
                        self.finish_unwind()
                        final = await self._read_final_state(final_state_reader)
                        if final is not None:
                            self.finalize_positions(
                                arcus_position=final[0],
                                rh_position=final[1],
                                arcus_open_orders=final[2],
                                rh_open_orders=final[3],
                            )
                        break
                    if self.reprice_allowed:
                        await self.place_next_quote()
                elif (
                    self.state is ProbeState.BUILD and not self.executor.has_live_order
                ):
                    if self._require_residual_reconciliation():
                        break
                    if self.reprice_allowed:
                        await self.place_next_quote()
                elif self.executor.has_live_order:
                    if (
                        self._order_placed_mono is not None
                        and time.monotonic() - self._order_placed_mono
                        >= self.config.reprice_sec
                    ):
                        if not await self._cancel_terminal():
                            break
                        if self.state in (ProbeState.BUILD, ProbeState.UNWIND):
                            self.record_reprice()
                            await self.place_next_quote()
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.05)
                except TimeoutError:
                    pass
        except TimeoutError:
            self._halt("volume-probe runtime timeout")
            await self._cancel_terminal()
        except Exception as exc:
            self._halt(str(exc))
            if not self.reprice_allowed:
                await self._cancel_terminal()
        finally:
            if self._require_residual_reconciliation():
                pass
            elif self._status is None:
                if self.state is ProbeState.HALTED:
                    self._status = (
                        ProbeStatus.TIMEOUT
                        if "runtime" in (self._failure_reason or "")
                        else ProbeStatus.HALTED
                    )
                elif self.state is ProbeState.DONE:
                    self._status = ProbeStatus.COMPLETED
                else:
                    self._status = ProbeStatus.RECONCILIATION_REQUIRED
            status = self._status
            if status is None:
                status = ProbeStatus.RECONCILIATION_REQUIRED
                self._status = status
            self.metrics.status = status
            self.metrics.failure_reason = self._failure_reason
            if self.metrics.finished_at is None:
                self.metrics.finished_at = _now_text()
            self.metrics.round_seconds = time.monotonic() - started
            self._sync_metrics()
            try:
                if self.writer is not None:
                    self.writer.append(self.metrics)
                self._metrics_written = True
            except Exception as exc:
                self._failure_reason = (
                    self._failure_reason or f"round log append failed: {exc}"
                )
                self._status = ProbeStatus.RECONCILIATION_REQUIRED
            finally:
                stop.set()
        final_status = self._status
        if final_status is None:
            final_status = ProbeStatus.RECONCILIATION_REQUIRED
        return final_status
