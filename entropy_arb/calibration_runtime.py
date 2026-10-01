"""Runtime controller for the deliberately tiny Phase B0 calibration.

The controller is intentionally separate from the mature two-taker engine.
Its only Arcus mutation is a signed LIMIT+ALO maker order and its only
follow-up mutation is an explicit cancel for that same calibration order.
Lighter execution is delegated to the existing :class:`LighterVenue`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from .arcus_execution import (
    ArcusAccountRest,
    ArcusAccountState,
    ArcusAloWouldCross,
    ArcusFeeTier,
    ArcusMakerClient,
    ArcusOrderUpdate,
    ArcusUserFill,
)
from .calibration import (
    ARCUS_CALIBRATION_QTY,
    RH_HEDGE_MIN_QTY,
    RH_HEDGE_SLIPPAGE_ALLOWANCE_BPS,
    RH_HEDGE_SLIPPAGE_HARD_CAP_BPS,
    CalibrationLifecycle,
    CalibrationPnL,
    FillAccumulator,
    MarketHealth,
    QuoteCandidate,
    SessionLimits,
    SessionRisk,
    build_quote_candidates,
    choose_quote,
    expected_edge_bps,
)
from .storage import ArcusCalibrationEventRow, MarketHistoryStore
from .venue_lighter import LighterAccountLimits

log = logging.getLogger("arcus-calibration")

RH_API_URL = "https://api.rh.lighter.xyz"
RH_PROFILE_NAME = "robinhood"


def _decimal(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal")
    return result


def resolve_verified_rh_fee_bps(
    hedge: Any,
    account_limits: LighterAccountLimits | None = None,
) -> Decimal:
    """Apply the authenticated account-specific RH fee to the B0 venue.

    The configured ``hedge.taker_fee_bps`` and public ``orderBooks.taker_fee``
    are intentionally ignored here.  A zero fee is valid, but only when the
    current authenticated ``accountLimits`` response proves it.
    """
    if not isinstance(account_limits, LighterAccountLimits):
        raise RuntimeError(
            "B0 requires authenticated Lighter accountLimits fee state; "
            "public orderBooks.taker_fee is not account-specific"
        )
    if account_limits.source != "accountLimits":
        raise RuntimeError(
            "B0 requires RH fee source=accountLimits; refusing unverified fee"
        )
    value = _decimal(account_limits.taker_fee_bps, "verified RH fee")
    if value < 0:
        raise RuntimeError("verified RH taker fee must be non-negative")

    # These are runtime observations.  The configured fee remains unchanged;
    # all B0 edge/PnL calculations consume the returned account value.
    hedge.fee_bps = value
    hedge.fee_bps_verified = True
    hedge.fee_source = account_limits.source
    hedge.account_limits = account_limits
    return value


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


@dataclass(frozen=True)
class PreOrderState:
    """All non-secret state shown immediately before a possible first order."""

    fee_tier: str
    maker_fee_bps: Decimal
    taker_fee_bps: Decimal
    rh_account_tier: str | None
    rh_account_tier_name: str | None
    rh_current_maker_fee_tick: int | None
    rh_current_taker_fee_tick: int | None
    rh_verified_maker_fee_bps: Decimal | None
    rh_verified_taker_fee_bps: Decimal | None
    rh_fee_source: str | None
    arcus_position: Decimal
    rh_position: Decimal
    arcus_open_orders: tuple[str, ...]
    rh_open_orders: tuple[str, ...]
    arcus_bid: Decimal | None
    arcus_ask: Decimal | None
    rh_bid: Decimal | None
    rh_ask: Decimal | None
    premium_bps: Decimal | None
    center_bps: Decimal
    center_source: str
    proposed_quote: QuoteCandidate | None
    outside_rth: bool | None
    limits: SessionLimits


@dataclass(frozen=True)
class _OrderContext:
    """Immutable correlation data retained through a cancel/fill race."""

    execution_id: str
    candidate: QuoteCandidate
    lifecycle: CalibrationLifecycle


@dataclass
class _ArcusFeeRecord:
    """Fee used for accounting until REST can confirm the fill fee."""

    context: _OrderContext
    fee_used: Decimal
    estimated: bool
    matched_in_risk: bool = False


@dataclass(frozen=True)
class HedgeExecutionResult:
    """Authoritative RH fill data exposed to the volume-probe wrapper."""

    hedge_side: str
    filled_qty: Decimal
    avg_px: Decimal
    fee: Decimal
    realized_slippage_bps: Decimal
    fill_to_hedge_send_ms: int | None
    fill_to_rh_fill_ms: int


class CalibrationTelemetry:
    """Append-only B0 lifecycle writer backed by the existing WAL store."""

    _FIELDS = (
        "client_id",
        "order_id",
        "arcus_side",
        "arcus_quote_price",
        "quote_qty",
        "quote_created_ts_ms",
        "expected_edge_bps",
        "expected_edge_at_fill_bps",
        "expected_usd",
        "fill_trade_id",
        "fill_ts_us",
        "arcus_fill_price",
        "arcus_fill_qty",
        "arcus_fee",
        "arcus_fee_is_estimated",
        "fill_to_hedge_send_ms",
        "fill_to_rh_fill_ms",
        "rh_signal_bid",
        "rh_signal_ask",
        "rh_hedge_side",
        "rh_hedge_qty",
        "rh_hedge_avg_fill",
        "rh_fee",
        "matched_edge_usd",
        "actual_usd",
        "remaining_arcus_qty",
        "unhedged_residual_qty",
        "halt_reason",
        "account_sequence_id",
        "rh_order_send_ts_ms",
        "rh_ack_ts_ms",
        "rh_fill_receive_ts_ms",
        "rh_realized_slippage_bps",
        "is_outside_rth",
        "center_bps",
        "center_source",
        "arcus_maker_fee_bps",
        "rh_taker_fee_bps",
    )

    def __init__(
        self,
        store: MarketHistoryStore,
        *,
        session_id: str,
        risk: SessionRisk,
        lifecycle: CalibrationLifecycle,
        pnl: CalibrationPnL,
        accumulator: FillAccumulator,
        session_limits: SessionLimits,
        context_provider: Any | None = None,
    ) -> None:
        self.store = store
        self.session_id = session_id
        self.risk = risk
        self.lifecycle = lifecycle
        self.pnl = pnl
        self.accumulator = accumulator
        self.session_limits = session_limits
        self.context_provider = context_provider
        self.events_written = 0

    def record(self, event_type: str, **values: Any) -> None:
        row_values: dict[str, Any] = {field: None for field in self._FIELDS}
        try:
            if self.context_provider is not None:
                row_values.update(self.context_provider())
            row_values.update(values)
            lifecycle_state = values.get("lifecycle_state", self.lifecycle.state)
            row_values.update(
                {
                    "session_id": self.session_id,
                    "execution_id": values.get("execution_id") or self.session_id,
                    "event_type": event_type,
                    "event_ts_ms": int(values.get("event_ts_ms", time.time() * 1000)),
                    "event_local_receive_monotonic_ns": values.get(
                        "event_local_receive_monotonic_ns", time.monotonic_ns()
                    ),
                    "lifecycle_state": lifecycle_state,
                }
            )
            if row_values["remaining_arcus_qty"] is None:
                row_values["remaining_arcus_qty"] = _text(self.lifecycle.remaining_qty)
            if row_values["unhedged_residual_qty"] is None:
                row_values["unhedged_residual_qty"] = _text(
                    self.accumulator.residual_exposure
                )
            if row_values["actual_usd"] is None:
                row_values["actual_usd"] = _text(self.pnl.actual_usd)
            if row_values["halt_reason"] is None:
                row_values["halt_reason"] = self.risk.halt_reason
            if row_values["arcus_fee_is_estimated"] is None:
                row_values["arcus_fee_is_estimated"] = False
            self.store.append_arcus_calibration_event(
                ArcusCalibrationEventRow(**row_values)
            )
            self.events_written += 1
        except Exception as exc:
            # A telemetry failure is itself a hard stop.  The loss cap must
            # never be bypassed because the audit stream is unavailable.
            self.risk.on_telemetry_failure(str(exc))
            log.exception("B0 telemetry append failed")

    async def flush(self) -> None:
        try:
            report = await asyncio.to_thread(self.store.flush)
        except Exception as exc:
            self.risk.on_telemetry_failure(str(exc))
            log.exception("B0 telemetry flush failed")
            return
        if not report.ok:
            self.risk.on_telemetry_failure("SQLite flush returned not-ok")
            return
        dropped = self.store.dropped_rows.get("arcus_calibration_events", 0)
        if dropped:
            self.risk.on_telemetry_failure(
                f"SQLite calibration buffer dropped {dropped} row(s)"
            )


class CalibrationController:
    """One-sided Arcus maker / RH hedge state machine for B0."""

    def __init__(
        self,
        *,
        arcus: Any,
        hedge: Any,
        maker: ArcusMakerClient,
        account_feed: Any,
        account_rest: ArcusAccountRest,
        account_state: ArcusAccountState,
        metadata: Any,
        fee_tier: ArcusFeeTier,
        strategy: Any,
        store: MarketHistoryStore,
        allow_first_order: bool,
        staleness_sec: float,
        rh_fee_bps: Decimal | None = None,
        session_id: str | None = None,
        session_limits: SessionLimits | None = None,
        client_prefix: str = "b0-",
    ) -> None:
        if not client_prefix:
            raise ValueError("Arcus client prefix must not be empty")
        self.arcus = arcus
        self.hedge = hedge
        self.maker = maker
        self.account_feed = account_feed
        self.account_rest = account_rest
        self.account_state = account_state
        self.metadata = metadata
        self.fee_tier = fee_tier
        self.strategy = strategy
        self.allow_first_order = allow_first_order
        self.staleness_sec = staleness_sec
        self._rh_fee_bps = (
            _decimal(rh_fee_bps, "verified RH fee")
            if rh_fee_bps is not None
            else _decimal(getattr(hedge, "fee_bps", 0), "RH fee")
        )
        self.rh_account_limits = getattr(hedge, "account_limits", None)
        self.order_prefix = client_prefix
        self.session_id = session_id or f"{client_prefix}{uuid.uuid4().hex[:12]}"
        self.limits = session_limits or SessionLimits()
        self.risk = SessionRisk(self.limits)
        self.pnl = CalibrationPnL()

        rh_step = Decimal(1).scaleb(-int(getattr(hedge, "size_decimals", 2)))
        rh_min = max(
            RH_HEDGE_MIN_QTY,
            rh_step,
            _decimal(getattr(hedge, "min_base", 0), "RH min base"),
        )
        if rh_min > self.limits.max_order_qty:
            if client_prefix == "b0-":
                raise RuntimeError(
                    f"RH minimum executable quantity {rh_min} exceeds fixed "
                    f"Arcus calibration quantity {ARCUS_CALIBRATION_QTY}"
                )
            raise RuntimeError(
                f"RH minimum executable quantity {rh_min} exceeds probe "
                f"quantity {self.limits.max_order_qty}"
            )
        self.accumulator = FillAccumulator(rh_min_qty=rh_min, rh_step=rh_step)
        self.lifecycle = CalibrationLifecycle()
        self.telemetry = CalibrationTelemetry(
            store,
            session_id=self.session_id,
            risk=self.risk,
            lifecycle=self.lifecycle,
            pnl=self.pnl,
            accumulator=self.accumulator,
            session_limits=self.limits,
            context_provider=self._telemetry_context,
        )
        self.calibration_prefix = f"{client_prefix}{self.session_id[-8:]}-"
        self.current_execution_id: str | None = None
        self.current_candidate: QuoteCandidate | None = None
        self._execution_number = 0
        self._order_contexts: dict[str, _OrderContext] = {}
        self._arcus_fee_records: dict[str, _ArcusFeeRecord] = {}
        self._current_context: _OrderContext | None = None
        self._terminal_reconcile_pending = False
        self._cancel_sent = False
        self._cancel_pending = False
        self._last_recorded_halt_reason: str | None = None
        # Arcus cash-flow is recorded when each fill arrives, but the session
        # loss cap is realized only as the corresponding RH hedge completes.
        # Keep a checkpoint so a hedge that unlocks accumulated sub-minimum
        # fills includes all of their Arcus proceeds/costs exactly once.
        self._matched_pnl_checkpoint = Decimal("0")
        self._last_hedge_matched = False
        self.last_hedge_result: HedgeExecutionResult | None = None

    def center_source(self) -> str:
        """Return the center provenance used by the current B0 calculation."""
        mode = getattr(self.strategy, "center_mode", None)
        if mode == "rolling":
            return (
                "rolling"
                if bool(getattr(self.strategy, "_rolling_ready", False))
                else "fallback"
            )
        if getattr(self.strategy, "requires_observations", False):
            state = self.strategy.state()
            return (
                "rolling"
                if getattr(state, "ready", False)
                and getattr(state, "center_bps", None) is not None
                else "fallback"
            )
        return "fallback"

    def _telemetry_context(self) -> dict[str, Any]:
        attributes = getattr(self.arcus, "latest_attributes", None)
        return {
            "is_outside_rth": getattr(attributes, "is_outside_rth", None),
            "center_bps": _text(self.center_bps()),
            "center_source": self.center_source(),
            "arcus_maker_fee_bps": _text(self.arcus_maker_fee_bps),
            "rh_taker_fee_bps": _text(self.rh_taker_fee_bps),
        }

    @property
    def has_live_order(self) -> bool:
        return self.lifecycle.state not in CalibrationLifecycle.TERMINAL

    @staticmethod
    def _context_key(kind: str, value: str | None) -> str | None:
        return f"{kind}:{value}" if value else None

    def _register_order_context(self, context: _OrderContext) -> None:
        for kind, value in (
            ("client", context.lifecycle.client_id),
            ("order", context.lifecycle.order_id),
        ):
            key = self._context_key(kind, value)
            if key is not None:
                self._order_contexts[key] = context

    def _forget_order_context(self, context: _OrderContext) -> None:
        for kind, value in (
            ("client", context.lifecycle.client_id),
            ("order", context.lifecycle.order_id),
        ):
            key = self._context_key(kind, value)
            if key is not None and self._order_contexts.get(key) is context:
                self._order_contexts.pop(key, None)

    def _context_for_ids(
        self, *, client_id: str | None = None, order_id: str | None = None
    ) -> _OrderContext | None:
        for kind, value in (("client", client_id), ("order", order_id)):
            key = self._context_key(kind, value)
            if key is not None and key in self._order_contexts:
                return self._order_contexts[key]
        return None

    def _context_for_order(self, order: ArcusOrderUpdate) -> _OrderContext | None:
        if order.market_id is not None and order.market_id != self.metadata.market_id:
            return None
        return self._context_for_ids(client_id=order.client_id, order_id=order.order_id)

    def _context_for_fill(self, fill: ArcusUserFill) -> _OrderContext | None:
        if fill.market_id is not None and fill.market_id != self.metadata.market_id:
            return None
        return self._context_for_ids(client_id=fill.client_id, order_id=fill.order_id)

    @property
    def arcus_maker_fee_bps(self) -> Decimal:
        return self.fee_tier.maker_fee_bps

    @property
    def rh_taker_fee_bps(self) -> Decimal:
        return self._rh_fee_bps

    def _book_bbo(self, venue: Any) -> tuple[Decimal | None, Decimal | None]:
        bid = venue.book.best_bid()
        ask = venue.book.best_ask()
        return (
            _decimal(bid, "bid") if bid is not None else None,
            _decimal(ask, "ask") if ask is not None else None,
        )

    def center_bps(self) -> Decimal:
        state = self.strategy.state()
        center = getattr(state, "center_bps", None)
        if center is None:
            center = getattr(self.strategy, "fixed_center_bps", 0.0)
        return _decimal(center, "center_bps")

    def current_premium_bps(self) -> Decimal | None:
        ab, aa = self._book_bbo(self.arcus)
        rb, ra = self._book_bbo(self.hedge)
        if None in (ab, aa, rb, ra):
            return None
        assert ab is not None and aa is not None and rb is not None and ra is not None
        return ((ab + aa) / 2 / ((rb + ra) / 2) - 1) * Decimal("10000")

    def market_health(self) -> MarketHealth:
        attributes = getattr(self.arcus, "latest_attributes", None)
        # RTH state is recorded as regime telemetry.  It is intentionally not
        # a quote gate in B0; market status, feed freshness, and sequence
        # health remain strict gates below.
        outside_rth = (
            getattr(attributes, "is_outside_rth", None)
            if attributes is not None
            else None
        )
        rh_ready = True
        ready_to_trade = getattr(self.hedge, "ready_to_trade", None)
        if callable(ready_to_trade):
            try:
                rh_ready = bool(ready_to_trade())
            except Exception:
                rh_ready = False
        account_channels_healthy = getattr(
            self.account_feed, "required_channels_healthy", True
        )
        if callable(account_channels_healthy):
            try:
                account_channels_healthy = bool(account_channels_healthy())
            except Exception:
                account_channels_healthy = False
        return MarketHealth(
            arcus_l2_healthy=(
                bool(getattr(self.arcus.book, "ready", False))
                and getattr(self.arcus.book, "sequence_health", "STALE") == "OK"
            ),
            rh_healthy=(bool(getattr(self.hedge.book, "ready", False)) and rh_ready),
            bbo_fresh=(
                self.arcus.book.is_fresh(self.staleness_sec)
                and self.hedge.book.is_fresh(self.staleness_sec)
            ),
            account_ws_healthy=bool(
                getattr(self.account_feed, "healthy", False)
                and getattr(self.account_feed, "ready", asyncio.Event()).is_set()
                and account_channels_healthy
            ),
            arcus_sequence_state=str(
                getattr(self.arcus.book, "sequence_health", "STALE")
            ),
            outside_rth=outside_rth,
            arcus_status=str(getattr(self.metadata, "status", "UNKNOWN")),
            active_resync=(
                str(getattr(self.arcus.book, "sequence_health", "STALE")) == "RESYNC"
            ),
        )

    def _candidate_inputs(self) -> SimpleNamespace | None:
        ab, aa = self._book_bbo(self.arcus)
        rb, ra = self._book_bbo(self.hedge)
        if None in (ab, aa, rb, ra):
            return None
        assert ab is not None and aa is not None and rb is not None and ra is not None
        return SimpleNamespace(
            arcus_bid=ab,
            arcus_ask=aa,
            rh_bid=rb,
            rh_ask=ra,
            center_bps=self.center_bps(),
            arcus_tick_size=_decimal(self.metadata.tick_size, "Arcus tick size"),
            arcus_step_size=_decimal(self.metadata.step_size, "Arcus step size"),
            arcus_maker_fee_bps=self.arcus_maker_fee_bps,
            rh_taker_fee_bps=self.rh_taker_fee_bps,
            rh_slippage_allowance_bps=RH_HEDGE_SLIPPAGE_ALLOWANCE_BPS,
            quantity=self.limits.max_order_qty,
        )

    def proposed_quote(self) -> QuoteCandidate | None:
        if not self.market_health().can_quote:
            return None
        inputs = self._candidate_inputs()
        if inputs is None:
            return None
        candidates = build_quote_candidates(inputs)
        min_notional = getattr(self.metadata, "min_order_notional", None)
        if min_notional is not None:
            minimum = _decimal(min_notional, "Arcus min order notional")
            candidates = [
                candidate
                for candidate in candidates
                if candidate.price * candidate.quantity >= minimum
            ]
        return choose_quote(candidates)

    def pre_order_state(
        self,
        *,
        arcus_position: Decimal,
        rh_position: Decimal,
        arcus_open_orders: list[ArcusOrderUpdate],
        rh_open_orders: list[Mapping[str, Any]],
    ) -> PreOrderState:
        ab, aa = self._book_bbo(self.arcus)
        rb, ra = self._book_bbo(self.hedge)
        rh_limits = self.rh_account_limits
        return PreOrderState(
            fee_tier=self.fee_tier.name,
            maker_fee_bps=self.arcus_maker_fee_bps,
            taker_fee_bps=self.fee_tier.taker_fee_bps,
            rh_account_tier=getattr(rh_limits, "user_tier", None),
            rh_account_tier_name=getattr(rh_limits, "user_tier_name", None),
            rh_current_maker_fee_tick=getattr(
                rh_limits, "current_maker_fee_tick", None
            ),
            rh_current_taker_fee_tick=getattr(
                rh_limits, "current_taker_fee_tick", None
            ),
            rh_verified_maker_fee_bps=getattr(rh_limits, "maker_fee_bps", None),
            rh_verified_taker_fee_bps=getattr(rh_limits, "taker_fee_bps", None),
            rh_fee_source=getattr(rh_limits, "source", None),
            arcus_position=arcus_position,
            rh_position=rh_position,
            arcus_open_orders=tuple(
                order.order_id or order.client_id or "<unknown>"
                for order in arcus_open_orders
            ),
            rh_open_orders=tuple(
                str(order.get("order_id", order.get("id", "<unknown>")))
                for order in rh_open_orders
            ),
            arcus_bid=ab,
            arcus_ask=aa,
            rh_bid=rb,
            rh_ask=ra,
            premium_bps=self.current_premium_bps(),
            center_bps=self.center_bps(),
            center_source=self.center_source(),
            proposed_quote=self.proposed_quote(),
            outside_rth=(
                getattr(
                    getattr(self.arcus, "latest_attributes", None),
                    "is_outside_rth",
                    None,
                )
            ),
            limits=self.limits,
        )

    def _record_halt(self) -> None:
        if (
            self.risk.halted
            and self.risk.halt_reason != self._last_recorded_halt_reason
        ):
            self._last_recorded_halt_reason = self.risk.halt_reason
            self.telemetry.record("halt", halt_reason=self.risk.halt_reason)

    @staticmethod
    def _fill_event_ts_ms(fill: ArcusUserFill) -> int:
        """Use exchange time when supplied, otherwise the local receive time."""
        if fill.created_at_us is not None and fill.created_at_us > 0:
            return fill.created_at_us // 1000
        if fill.local_receive_ts_ms is not None:
            return int(fill.local_receive_ts_ms)
        return int(time.time() * 1000)

    def _estimated_arcus_fee(self, fill: ArcusUserFill) -> Decimal:
        """Return the resolved-tier estimate used only until REST reconciliation."""
        return fill.price * fill.quantity * self.arcus_maker_fee_bps / Decimal("10000")

    async def _refresh_actual_arcus_fee(
        self, fill: ArcusUserFill, context: _OrderContext
    ) -> bool:
        """Backfill a store-only fee without ever replaying the fill hedge."""
        try:
            rows = await self.account_rest.fills(
                self.maker.credentials.account_address,
                self.metadata.symbol,
                self.maker.credentials.account_index,
                from_us=fill.created_at_us,
            )
        except Exception as exc:
            self.risk.on_telemetry_failure(
                f"Arcus actual fill fee reconciliation failed: {exc}"
            )
            return False
        for row in rows:
            if row.trade_id == fill.trade_id and row.fee is not None:
                self._apply_actual_arcus_fee(row)
                return True
        return False

    def _apply_actual_arcus_fee(self, fill: ArcusUserFill) -> None:
        if not fill.trade_id or fill.fee is None:
            return
        record = self._arcus_fee_records.get(fill.trade_id)
        if record is None or not record.estimated:
            return
        actual_fee = _decimal(fill.fee, "Arcus actual fee")
        fee_delta = actual_fee - record.fee_used
        if fee_delta != 0:
            # The provisional PnL already charged record.fee_used.  Fees are
            # always cash costs, irrespective of BUY/SELL direction.
            self.pnl.actual_usd -= fee_delta
            if record.matched_in_risk:
                self._matched_pnl_checkpoint -= fee_delta
                self.risk.on_realized_pnl(-fee_delta)
        record.fee_used = actual_fee
        record.estimated = False
        self.telemetry.record(
            "fill_fee_reconciled",
            execution_id=record.context.execution_id,
            client_id=record.context.lifecycle.client_id,
            order_id=record.context.lifecycle.order_id,
            arcus_side=fill.side,
            arcus_quote_price=_text(record.context.candidate.price),
            quote_qty=_text(record.context.candidate.quantity),
            quote_created_ts_ms=record.context.lifecycle.quote_created_ts_ms,
            expected_edge_bps=_text(record.context.lifecycle.expected_edge_at_creation),
            fill_trade_id=fill.trade_id,
            fill_ts_us=fill.created_at_us,
            arcus_fill_price=_text(fill.price),
            arcus_fill_qty=_text(fill.quantity),
            arcus_fee=_text(actual_fee),
            arcus_fee_is_estimated=False,
            actual_usd=_text(self.pnl.actual_usd),
            event_ts_ms=self._fill_event_ts_ms(fill),
            event_local_receive_monotonic_ns=fill.local_receive_monotonic_ns,
            lifecycle_state=record.context.lifecycle.state,
            account_sequence_id=self.account_state.account_sequence_id,
        )

    async def on_disconnect(self) -> None:
        self.risk.on_arcus_disconnect(self.lifecycle)
        self._cancel_pending = True
        self._cancel_sent = False
        self.telemetry.record("account_disconnect")

    async def on_connect(self) -> None:
        if self._cancel_pending and self.account_feed.healthy:
            await self.cancel_outstanding()
        # Subscription snapshots are useful but asynchronous.  A bounded
        # public REST backfill closes any reconnect gap for known calibration
        # orders and fills without trusting socket closure as cancellation.
        await self.reconcile()

    async def on_order(self, update: ArcusOrderUpdate) -> None:
        context = self._context_for_order(update)
        if context is None:
            return
        if update.last_sequence_id is not None:
            self.account_state.account_sequence_id = update.last_sequence_id
        elif update.sequence_number is not None:
            self.account_state.account_sequence_id = update.sequence_number
        try:
            context.lifecycle.record_order_status(update.status, update.remaining_size)
        except ValueError:
            self.risk.on_telemetry_failure(
                f"unsupported Arcus order status {update.status}"
            )
            return
        self.telemetry.record(
            "order_update",
            client_id=update.client_id,
            order_id=update.order_id,
            arcus_side=update.side,
            arcus_quote_price=_text(update.price),
            quote_qty=_text(update.original_size),
            remaining_arcus_qty=_text(update.remaining_size),
            execution_id=context.execution_id,
            quote_created_ts_ms=context.lifecycle.quote_created_ts_ms,
            expected_edge_bps=_text(context.lifecycle.expected_edge_at_creation),
            lifecycle_state=context.lifecycle.state,
            account_sequence_id=self.account_state.account_sequence_id,
        )
        if update.status in ("REJECTED",):
            self.risk.halt("Arcus calibration order rejected")
            self._record_halt()
        if update.status in ("FILLED", "CANCELED", "MARGIN_CANCELED"):
            if context is self._current_context:
                # A terminal order update is not enough by itself to permit a
                # replacement: a concurrently arriving userFill must first be
                # reconciled from the account stream/REST backfill.
                self._terminal_reconcile_pending = True
            self.risk.check_residual(
                self.accumulator.residual_exposure,
                quantization_unit=self.accumulator.rh_step,
            )
            if self.risk.halted:
                await self.cancel_outstanding()
                self._record_halt()

    async def on_fill(self, fill: ArcusUserFill) -> None:
        self.last_hedge_result = None
        context = self._context_for_fill(fill)
        if context is None and (
            not fill.is_snapshot
            and fill.client_id
            and fill.client_id.startswith(self.order_prefix)
        ):
            # A stale calibration order must never be silently ignored if it
            # fills during startup cancellation or reconnect recovery.
            self.risk.halt(f"fill received for an unknown {self.order_prefix} clientId")
            self.telemetry.record(
                "unexpected_fill",
                client_id=fill.client_id,
                order_id=fill.order_id,
                fill_trade_id=fill.trade_id or None,
                halt_reason=self.risk.halt_reason,
            )
            await self.cancel_outstanding()
            self._record_halt()
            return
        if context is None:
            return
        if not self.account_state.should_hedge_fill(fill):
            # A userFills frame may omit the store-only fee.  If the same
            # trade was already hedged using the verified tier estimate,
            # reconcile its actual REST fee without issuing a second hedge.
            if fill.trade_id and fill.fee is not None:
                self._apply_actual_arcus_fee(fill)
            return
        if not fill.trade_id:
            self.risk.on_telemetry_failure(
                "Arcus userFills omitted tradeId; refusing non-idempotent hedge"
            )
            self.telemetry.record(
                "fill",
                execution_id=context.execution_id,
                client_id=fill.client_id,
                order_id=fill.order_id,
                fill_ts_us=fill.created_at_us or None,
                arcus_side=fill.side,
                arcus_fill_price=_text(fill.price),
                arcus_fill_qty=_text(fill.quantity),
                event_ts_ms=self._fill_event_ts_ms(fill),
                event_local_receive_monotonic_ns=fill.local_receive_monotonic_ns,
                lifecycle_state=context.lifecycle.state,
                account_sequence_id=self.account_state.account_sequence_id,
            )
            await self.cancel_outstanding()
            self._record_halt()
            return
        if fill.created_at_us is not None and fill.created_at_us <= 0:
            self.risk.on_telemetry_failure(
                "Arcus userFills supplied an invalid exchange timestamp"
            )
            await self.cancel_outstanding()
            self._record_halt()
            return
        fee_is_estimated = fill.fee is None
        fee_for_accounting = (
            self._estimated_arcus_fee(fill) if fee_is_estimated else fill.fee
        )
        assert fee_for_accounting is not None

        try:
            context.lifecycle.record_fill(fill.quantity)
            self.risk.on_arcus_fill(fill.price, fill.quantity)
            self.pnl.add_arcus_fill(
                side=fill.side,
                price=fill.price,
                quantity=fill.quantity,
                fee=fee_for_accounting,
            )
        except Exception as exc:
            self.risk.on_telemetry_failure(str(exc))
            await self.cancel_outstanding()
            self._record_halt()
            return

        expected_at_fill = self._expected_edge_at_fill(fill)
        instruction = self.accumulator.add_fill(side=fill.side, quantity=fill.quantity)
        self.telemetry.record(
            "fill",
            execution_id=context.execution_id,
            client_id=fill.client_id,
            order_id=fill.order_id,
            arcus_side=fill.side,
            arcus_quote_price=_text(context.candidate.price),
            quote_qty=_text(context.candidate.quantity),
            quote_created_ts_ms=context.lifecycle.quote_created_ts_ms,
            expected_edge_bps=_text(context.lifecycle.expected_edge_at_creation),
            expected_edge_at_fill_bps=_text(expected_at_fill),
            expected_usd=_text(context.candidate.expected_usd),
            fill_trade_id=fill.trade_id or None,
            fill_ts_us=fill.created_at_us or None,
            arcus_fill_price=_text(fill.price),
            arcus_fill_qty=_text(fill.quantity),
            arcus_fee=_text(fee_for_accounting),
            arcus_fee_is_estimated=fee_is_estimated,
            remaining_arcus_qty=_text(context.lifecycle.remaining_qty),
            event_ts_ms=self._fill_event_ts_ms(fill),
            event_local_receive_monotonic_ns=fill.local_receive_monotonic_ns,
            lifecycle_state=context.lifecycle.state,
            account_sequence_id=self.account_state.account_sequence_id,
        )

        self._arcus_fee_records[fill.trade_id] = _ArcusFeeRecord(
            context=context,
            fee_used=fee_for_accounting,
            estimated=fee_is_estimated,
        )

        if self.risk.halted and (self.risk.halt_reason or "").startswith(
            "telemetry failure"
        ):
            # Do not issue a hedge when the audit/loss-control stream has
            # already failed.  The fill remains persisted when possible and
            # is reconciled on shutdown; no new Arcus quote is allowed.
            await self.cancel_outstanding()
            self._record_halt()
            return
        if instruction is not None:
            self._last_hedge_matched = False
            await self._hedge_instruction(fill, instruction, expected_at_fill, context)
            fee_record = self._arcus_fee_records.get(fill.trade_id)
            if fee_record is not None:
                fee_record.matched_in_risk = self._last_hedge_matched
        if fee_is_estimated and not await self._refresh_actual_arcus_fee(fill, context):
            # The immediate hedge above is still required to remove exposure,
            # but continuing without an actual fee would make the all-in loss
            # calculation unreliable.  Stop and reconcile rather than
            # disabling the loss cap or opening another quote.
            self.risk.on_telemetry_failure(
                "Arcus actual fill fee unavailable after immediate hedge"
            )
        self.risk.check_residual(
            self.accumulator.residual_exposure,
            quantization_unit=self.accumulator.rh_step,
        )
        if self.risk.halted:
            await self.cancel_outstanding()
            self._record_halt()

    def _expected_edge_at_fill(self, fill: ArcusUserFill) -> Decimal | None:
        bid, ask = self._book_bbo(self.hedge)
        hedge_price = ask if fill.side == "SELL" else bid
        if hedge_price is None:
            return None
        return _decimal(
            expected_edge_bps(
                side=fill.side,
                arcus_price=fill.price,
                hedge_price=hedge_price,
                arcus_fee_bps=self.arcus_maker_fee_bps,
                rh_fee_bps=self.rh_taker_fee_bps,
                rh_slippage_allowance_bps=RH_HEDGE_SLIPPAGE_ALLOWANCE_BPS,
            ),
            "expected_edge_at_fill_bps",
        )

    async def _hedge_instruction(
        self,
        fill: ArcusUserFill,
        instruction: Any,
        expected_at_fill: Decimal | None,
        context: _OrderContext,
    ) -> None:
        self.last_hedge_result = None
        if not self.hedge.book.is_fresh(self.staleness_sec):
            await self._hedge_failure("RH market feed is stale")
            return
        bid, ask = self._book_bbo(self.hedge)
        if bid is None or ask is None:
            await self._hedge_failure("RH BBO unavailable")
            return
        signal_mono = time.monotonic_ns()
        # Existing Lighter send_taker provides IOC/avg-price protection.  The
        # limit is deliberately bounded at 20 bps from the observed BBO.
        if instruction.hedge_side == "BUY":
            limit_px = ask * (
                Decimal("1") + RH_HEDGE_SLIPPAGE_HARD_CAP_BPS / Decimal("10000")
            )
            is_buy = True
        else:
            limit_px = bid * (
                Decimal("1") - RH_HEDGE_SLIPPAGE_HARD_CAP_BPS / Decimal("10000")
            )
            is_buy = False
        try:
            result = await self.hedge.send_taker(
                is_buy=is_buy,
                qty=float(instruction.quantity),
                limit_px=float(limit_px),
                reduce_only=False,
            )
        except Exception as exc:
            await self._hedge_failure(f"exception: {exc}")
            return
        send_mono = signal_mono
        try:
            filled_qty = _decimal(result.get("filled_base", 0), "RH filled_base")
            avg_px = result.get("avg_px")
            status = str(result.get("status", "")).lower()
        except Exception as exc:
            await self._hedge_failure(f"invalid RH hedge result: {exc}")
            return
        if result.get("unresolved") or status in {
            "timeout",
            "send-failed",
            "sent-unconfirmed",
            "unknown",
        }:
            await self._hedge_failure(f"RH hedge {status or 'unresolved'}")
            return
        if filled_qty <= 0 or avg_px is None:
            await self._hedge_failure("RH hedge returned no authoritative fill")
            return
        avg_px_decimal = _decimal(avg_px, "RH avg_px")
        if filled_qty > instruction.quantity:
            await self._hedge_failure("RH hedge filled more than Arcus fill")
            return
        rh_fee = result.get("fee")
        if rh_fee is None:
            rh_fee_decimal = (
                avg_px_decimal * filled_qty * self.rh_taker_fee_bps / Decimal("10000")
            )
        else:
            rh_fee_decimal = _decimal(rh_fee, "RH fee")
        if instruction.hedge_side == "BUY":
            realized_slippage = ((avg_px_decimal / ask) - Decimal("1")) * Decimal(
                "10000"
            )
        else:
            realized_slippage = (Decimal("1") - (avg_px_decimal / bid)) * Decimal(
                "10000"
            )
        if realized_slippage > RH_HEDGE_SLIPPAGE_HARD_CAP_BPS:
            # The bounded order has already executed; do not pretend it can be
            # undone.  Record the breach and stop all new Arcus quoting.
            self.risk.halt("RH hedge realized slippage exceeded 20 bps emergency cap")
        try:
            self.pnl.add_rh_hedge(
                side=instruction.hedge_side,
                price=avg_px_decimal,
                quantity=filled_qty,
                fee=rh_fee_decimal,
            )
            matched_edge = self.pnl.actual_usd - self._matched_pnl_checkpoint
            self._matched_pnl_checkpoint = self.pnl.actual_usd
            self.risk.on_realized_pnl(matched_edge)
            self._last_hedge_matched = True
        except Exception as exc:
            self.risk.on_telemetry_failure(str(exc))
            await self._hedge_failure("unable to calculate actual PnL")
            return
        fill_receive_ns = fill.local_receive_monotonic_ns
        fill_to_send_ms = (
            max(0, send_mono - fill_receive_ns) // 1_000_000
            if fill_receive_ns is not None
            else None
        )
        fill_to_fill_ms = max(0, time.monotonic_ns() - send_mono) // 1_000_000
        self.last_hedge_result = HedgeExecutionResult(
            hedge_side=instruction.hedge_side,
            filled_qty=filled_qty,
            avg_px=avg_px_decimal,
            fee=rh_fee_decimal,
            realized_slippage_bps=realized_slippage,
            fill_to_hedge_send_ms=fill_to_send_ms,
            fill_to_rh_fill_ms=fill_to_fill_ms,
        )
        if filled_qty < instruction.quantity:
            self.accumulator.unhedged_qty += instruction.quantity - filled_qty
            self.risk.halt("RH hedge partially filled")
        fee_record = self._arcus_fee_records.get(fill.trade_id)
        accounted_arcus_fee = (
            fee_record.fee_used if fee_record is not None else fill.fee
        )
        fee_is_estimated = bool(fee_record and fee_record.estimated)
        self.telemetry.record(
            "hedge",
            execution_id=context.execution_id,
            client_id=context.lifecycle.client_id,
            order_id=context.lifecycle.order_id,
            arcus_side=fill.side,
            arcus_quote_price=_text(context.candidate.price),
            quote_qty=_text(context.candidate.quantity),
            quote_created_ts_ms=context.lifecycle.quote_created_ts_ms,
            expected_edge_bps=_text(context.lifecycle.expected_edge_at_creation),
            expected_edge_at_fill_bps=_text(expected_at_fill),
            expected_usd=_text(context.candidate.expected_usd),
            fill_trade_id=fill.trade_id or None,
            fill_ts_us=fill.created_at_us or None,
            arcus_fill_price=_text(fill.price),
            arcus_fill_qty=_text(fill.quantity),
            arcus_fee=_text(accounted_arcus_fee),
            arcus_fee_is_estimated=fee_is_estimated,
            fill_to_hedge_send_ms=fill_to_send_ms,
            fill_to_rh_fill_ms=fill_to_fill_ms,
            rh_signal_bid=_text(bid),
            rh_signal_ask=_text(ask),
            rh_hedge_side=instruction.hedge_side,
            rh_hedge_qty=_text(filled_qty),
            rh_hedge_avg_fill=_text(avg_px_decimal),
            rh_fee=_text(rh_fee_decimal),
            remaining_arcus_qty=_text(context.lifecycle.remaining_qty),
            rh_order_send_ts_ms=result.get("order_send_ts_ms"),
            rh_ack_ts_ms=result.get("ack_ts_ms"),
            rh_fill_receive_ts_ms=result.get("fill_receive_ts_ms"),
            rh_realized_slippage_bps=_text(realized_slippage),
            matched_edge_usd=_text(matched_edge),
            actual_usd=_text(self.pnl.actual_usd),
            unhedged_residual_qty=_text(self.accumulator.residual_exposure),
            event_ts_ms=self._fill_event_ts_ms(fill),
            event_local_receive_monotonic_ns=fill.local_receive_monotonic_ns,
            lifecycle_state=context.lifecycle.state,
            account_sequence_id=self.account_state.account_sequence_id,
        )
        if self.risk.halted:
            await self.cancel_outstanding()
            self._record_halt()

    async def _hedge_failure(self, reason: str) -> None:
        self.risk.on_hedge_failure(reason)
        self.telemetry.record("hedge_failure", halt_reason=self.risk.halt_reason)
        await self.cancel_outstanding()
        self._record_halt()

    async def cancel_outstanding(self) -> None:
        if not self.has_live_order:
            return
        self.lifecycle.request_cancel()
        if not getattr(self.account_feed, "healthy", False):
            self._cancel_pending = True
            return
        if self._cancel_sent:
            return
        identifier_order = self.lifecycle.order_id
        identifier_client = None if identifier_order else self.lifecycle.client_id
        if identifier_order is None and identifier_client is None:
            self._cancel_pending = True
            return
        self._cancel_sent = True
        try:
            response = await self.maker.cancel_calibration_order(
                market_id=self.metadata.market_id,
                order_id=identifier_order,
                client_id=identifier_client,
            )
            status = int(response.get("status", 0))
            if status not in (200, 202):
                raise RuntimeError(f"Arcus cancel rejected with status={status}")
            self._cancel_pending = False
            self.telemetry.record(
                "cancel_requested",
                client_id=self.lifecycle.client_id,
                order_id=self.lifecycle.order_id,
                account_sequence_id=self.account_state.account_sequence_id,
            )
        except Exception as exc:
            self._cancel_sent = False
            self._cancel_pending = True
            self.risk.halt(f"Arcus cancel unresolved: {exc}")
            self.telemetry.record("cancel_failure", halt_reason=self.risk.halt_reason)

    async def _place_quote(self, candidate: QuoteCandidate) -> None:
        self._execution_number += 1
        self.current_execution_id = f"{self.session_id}-e{self._execution_number}"
        client_id = f"{self.calibration_prefix}{self._execution_number}"
        self._cancel_sent = False
        self._cancel_pending = False
        # A retired order may still deliver a fill after cancellation.  Give
        # the new order its own lifecycle object so the old context remains
        # immutable in identity and can still be reconciled independently.
        self.lifecycle = CalibrationLifecycle()
        self.telemetry.lifecycle = self.lifecycle
        self.account_state.calibration_client_ids.add(client_id)
        self.lifecycle.mark_placed(
            expected_edge_bps=candidate.expected_edge_bps,
            client_id=client_id,
            original_qty=candidate.quantity,
        )
        self.lifecycle.quote_created_ts_ms = int(time.time() * 1000)
        self.current_candidate = candidate
        context = _OrderContext(
            execution_id=self.current_execution_id,
            candidate=candidate,
            lifecycle=self.lifecycle,
        )
        self._current_context = context
        self._register_order_context(context)
        try:
            ack = await self.maker.place_alo(
                market_id=self.metadata.market_id,
                side=candidate.side,
                price=candidate.price,
                quantity=candidate.quantity,
                tick_size=_decimal(self.metadata.tick_size, "Arcus tick size"),
                step_size=_decimal(self.metadata.step_size, "Arcus step size"),
                best_bid=self._book_bbo(self.arcus)[0],
                best_ask=self._book_bbo(self.arcus)[1],
                client_id=client_id,
            )
        except ArcusAloWouldCross:
            self._forget_order_context(context)
            self.account_state.calibration_client_ids.discard(client_id)
            self.lifecycle.state = "CANCELED"
            self.current_candidate = None
            self.current_execution_id = None
            self._current_context = None
            return
        except Exception as exc:
            # A timeout is not proof of rejection; fail closed and reconcile
            # the clientId through the account stream rather than retrying.
            self.risk.halt(f"Arcus ALO placement unresolved: {exc}")
            self.telemetry.record(
                "place_failure", client_id=client_id, halt_reason=self.risk.halt_reason
            )
            await self.cancel_outstanding()
            return
        self.lifecycle.order_id = ack.order_id
        if ack.client_id and ack.client_id != client_id:
            self.account_state.calibration_client_ids.add(ack.client_id)
            self._order_contexts[f"client:{ack.client_id}"] = context
        if ack.order_id:
            self.account_state.calibration_client_ids.add(ack.order_id)
        self._register_order_context(context)
        self.telemetry.record(
            "quote_created",
            execution_id=self.current_execution_id,
            client_id=client_id,
            order_id=ack.order_id,
            arcus_side=candidate.side,
            arcus_quote_price=_text(candidate.price),
            quote_qty=_text(candidate.quantity),
            quote_created_ts_ms=self.lifecycle.quote_created_ts_ms,
            expected_edge_bps=_text(candidate.expected_edge_bps),
            expected_usd=_text(candidate.expected_usd),
            account_sequence_id=self.account_state.account_sequence_id,
        )

    async def place_quote(self, candidate: QuoteCandidate) -> None:
        """Place one already-validated LIMIT+ALO candidate.

        B0 continues to use :meth:`step`; the volume probe uses this narrow
        boundary so it can choose a fresh best-side candidate without
        inheriting B0's edge-selection policy.
        """

        await self._place_quote(candidate)

    def _current_order_edge(self) -> Decimal | None:
        if not self.current_candidate or not self.lifecycle.client_id:
            return None
        bid, ask = self._book_bbo(self.hedge)
        if bid is None or ask is None:
            return None
        hedge_price = ask if self.current_candidate.side == "SELL" else bid
        return _decimal(
            expected_edge_bps(
                side=self.current_candidate.side,
                arcus_price=self.current_candidate.price,
                hedge_price=hedge_price,
                arcus_fee_bps=self.arcus_maker_fee_bps,
                rh_fee_bps=self.rh_taker_fee_bps,
                rh_slippage_allowance_bps=RH_HEDGE_SLIPPAGE_ALLOWANCE_BPS,
            ),
            "current expected edge",
        )

    async def step(self) -> QuoteCandidate | None:
        self.risk.check_runtime()
        health = self.market_health()
        if not health.can_quote and self.has_live_order:
            self.risk.halt("market health gate failed")
            self.lifecycle.request_cancel()
        if self._cancel_pending and self.account_feed.healthy:
            await self.cancel_outstanding()
        if self.risk.halted:
            await self.cancel_outstanding()
            if self._terminal_reconcile_pending:
                await self.reconcile()
            self._record_halt()
            return None
        if self._terminal_reconcile_pending:
            # A terminal orders update can race with a userFills update.  Do
            # not replace the order until the account stream and read-only
            # fills endpoint have had a chance to close that race.
            await self.reconcile()
            if self._terminal_reconcile_pending:
                return None
        if self.has_live_order:
            edge = self._current_order_edge()
            if edge is not None and edge < Decimal("1.5"):
                await self.cancel_outstanding()
            return None
        self.accumulator.reset_if_flat()
        if self.accumulator.residual_exposure != 0:
            # A sub-minimum residual is visible and intentionally not rounded
            # into a new hedge or adopted by a replacement quote.
            return None
        candidate = self.proposed_quote()
        if candidate is not None and self.allow_first_order:
            await self._place_quote(candidate)
        return candidate

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                await self.step()
                if self.risk.halted and self._halt_reconciled():
                    stop.set()
                    break
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.2)
                except TimeoutError:
                    pass
        finally:
            await self.shutdown()

    def _halt_reconciliation_failure(self, operation: str, exc: Exception) -> None:
        detail = str(exc).strip() or repr(exc)
        reason = (
            f"Arcus reconciliation {operation} failed ({type(exc).__name__}): {detail}"
        )
        log.exception("%s", reason)
        self.risk.on_telemetry_failure(reason)
        self._record_halt()

    async def reconcile(self) -> None:
        """Refresh known calibration orders after a cancel/fill race."""
        if not self._order_contexts:
            return
        try:
            orders = await self.account_rest.open_orders(
                self.maker.credentials.account_address,
                self.metadata.symbol,
                self.maker.credentials.account_index,
            )
        except Exception as exc:
            self._halt_reconciliation_failure("open_orders", exc)
            return
        try:
            fills = await self.account_rest.fills(
                self.maker.credentials.account_address,
                self.metadata.symbol,
                self.maker.credentials.account_index,
            )
        except Exception as exc:
            self._halt_reconciliation_failure("fills", exc)
            return

        # The REST fills endpoint is newest-first.  Dispatch oldest-first so
        # multiple fills recovered from a cancel/disconnect race preserve the
        # same causal order as the account stream.  should_hedge_fill() makes
        # this idempotent when the websocket delivered one already.
        for fill in sorted(
            fills,
            key=lambda item: (
                item.created_at_us if item.created_at_us is not None else 0,
                item.trade_id,
            ),
        ):
            await self.on_fill(fill)
        for order in orders:
            if self._context_for_order(order) is not None:
                await self.on_order(order)

        # An open-orders REST read cannot by itself prove that a canceled
        # order reached a terminal state.  The account feed caches the latest
        # order update, including recent-closed updates from its snapshot.
        latest_orders = getattr(self.account_feed, "latest_orders", {})
        if isinstance(latest_orders, Mapping):
            seen_contexts: set[int] = set()
            for update in latest_orders.values():
                if not isinstance(update, ArcusOrderUpdate):
                    continue
                context = self._context_for_order(update)
                if context is None or id(context) in seen_contexts:
                    continue
                seen_contexts.add(id(context))
                await self.on_order(update)

        if (
            self._current_context is not None
            and self._current_context.lifecycle.state in CalibrationLifecycle.TERMINAL
        ):
            self._terminal_reconcile_pending = False

    async def shutdown(self) -> None:
        if self.has_live_order:
            await self.cancel_outstanding()
        deadline = time.monotonic() + 15.0
        while (
            self.has_live_order or self._terminal_reconcile_pending
        ) and time.monotonic() < deadline:
            await self.reconcile()
            if self.has_live_order or self._terminal_reconcile_pending:
                await asyncio.sleep(1.0)
        if self.has_live_order or self._terminal_reconcile_pending:
            self.risk.halt("Arcus shutdown reconciliation unresolved")
            self._record_halt()
        await self.telemetry.flush()

    def _halt_reconciled(self) -> bool:
        """Return true when a halted session may safely end its run loop."""
        return bool(
            getattr(self.account_feed, "healthy", False)
            and not self.has_live_order
            and not self._terminal_reconcile_pending
        )


async def fetch_lighter_open_orders(hedge: Any) -> list[Mapping[str, Any]]:
    """Read RH active orders through the authenticated official SDK API.

    The Lighter REST endpoint is account-scoped.  Keep this preflight on the
    Robinhood deployment and use the same signer/token identity as the
    preceding authenticated ``accountLimits`` request.  This helper remains
    read-only and never uses the Lighter order mutation methods.
    """
    creds = getattr(getattr(hedge, "conf", None), "lighter_creds", None)
    account_index = getattr(creds, "account_index", None)
    api_key_index = getattr(creds, "api_key_index", None)
    if account_index is None or api_key_index is None:
        raise RuntimeError("RH account index is required to verify open orders")
    if isinstance(account_index, bool) or not isinstance(account_index, int):
        raise RuntimeError("RH account index is invalid")
    if isinstance(api_key_index, bool) or not isinstance(api_key_index, int):
        raise RuntimeError("RH API key index is invalid")

    profile = getattr(hedge, "profile", None)
    if (
        getattr(profile, "name", None) != RH_PROFILE_NAME
        or getattr(profile, "api_url", None) != RH_API_URL
    ):
        raise RuntimeError(
            "RH open-order verification requires the Robinhood deployment host"
        )

    signer = getattr(hedge, "signer", None)
    if signer is None:
        raise RuntimeError("RH open-order verification requires signer")
    if getattr(signer, "account_index", None) != account_index:
        raise RuntimeError("RH account index mismatch between signer and request")
    signer_host = getattr(
        getattr(getattr(signer, "api_client", None), "configuration", None),
        "host",
        None,
    )
    if signer_host != RH_API_URL:
        raise RuntimeError("RH signer deployment host mismatch")

    account_limits = getattr(hedge, "account_limits", None)
    limits_account_index = getattr(account_limits, "account_index", account_index)
    if limits_account_index != account_index:
        raise RuntimeError("RH account index mismatch with accountLimits")

    market_id = getattr(hedge, "market_id", None)
    if isinstance(market_id, bool) or not isinstance(market_id, int) or market_id < 0:
        raise RuntimeError("RH SNDK market id is invalid")

    order_api = getattr(signer, "order_api", None)
    account_active_orders = getattr(order_api, "account_active_orders", None)
    if not callable(account_active_orders):
        raise RuntimeError("official Lighter OrderApi is unavailable")
    try:
        auth, auth_error = signer.create_auth_token_with_expiry(
            api_key_index=api_key_index
        )
    except Exception:
        raise RuntimeError("RH accountActiveOrders authentication failed") from None
    if auth_error is not None or not auth:
        raise RuntimeError("RH accountActiveOrders authentication failed")

    try:
        response = await account_active_orders(
            authorization=auth,
            account_index=account_index,
            market_id=market_id,
            market_type=None,
        )
    except Exception as exc:
        raise RuntimeError(
            "RH accountActiveOrders request failed " + _lighter_api_error_detail(exc)
        ) from None

    rows = _lighter_active_order_rows(response)
    result: list[Mapping[str, Any]] = []
    for row in rows:
        if _lighter_order_market_id(row) == market_id:
            result.append(row)
    return result


def _lighter_active_order_rows(response: Any) -> list[Mapping[str, Any]]:
    """Normalize an official ``Orders`` model or a test transport mapping."""
    if isinstance(response, Mapping):
        code = response.get("code")
        rows = response.get("orders", response.get("active_orders"))
    else:
        code = getattr(response, "code", None)
        rows = getattr(response, "orders", None)
        if rows is None and hasattr(response, "to_dict"):
            converted = response.to_dict()
            if isinstance(converted, Mapping):
                code = converted.get("code", code)
                rows = converted.get("orders", converted.get("active_orders"))
    if code is not None and code != 200:
        raise RuntimeError(f"RH active-order response returned code={code}")
    if rows is None:
        raise RuntimeError("RH active-order response omitted its order array")
    if not isinstance(rows, list):
        raise RuntimeError("RH active-order response has an invalid order array")

    normalized: list[Mapping[str, Any]] = []
    for row in rows:
        if isinstance(row, Mapping):
            normalized.append(row)
            continue
        converted = None
        if hasattr(row, "to_dict"):
            converted = row.to_dict()
        elif hasattr(row, "model_dump"):
            converted = row.model_dump(by_alias=True)
        if not isinstance(converted, Mapping):
            raise RuntimeError("RH active-order response has an invalid order")
        normalized.append(converted)
    return normalized


def _lighter_order_market_id(row: Mapping[str, Any]) -> int:
    for key in ("market_id", "marketId", "market_index", "marketIndex"):
        if key not in row:
            continue
        value = row[key]
        if isinstance(value, bool):
            break
        try:
            return int(value)
        except (TypeError, ValueError):
            break
    raise RuntimeError("RH active-order row omitted a valid market id")


def _lighter_api_error_detail(exc: Exception) -> str:
    """Keep status/ResultCode detail without echoing exception/token text."""
    parts: list[str] = []
    status = getattr(exc, "status", None)
    if status is not None:
        parts.append(f"status={status}")

    body = getattr(exc, "body", None)
    if body is not None and hasattr(body, "to_dict"):
        body = body.to_dict()
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8", errors="replace")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except (TypeError, ValueError):
            body = None
    if isinstance(body, Mapping):
        for key in ("code", "result_code", "resultCode", "message", "error", "msg"):
            if key not in body:
                continue
            value = body[key]
            if isinstance(value, (str, int, float, bool)) or value is None:
                parts.append(f"{key}={value}")
    return " ".join(parts) if parts else "status=unknown"
