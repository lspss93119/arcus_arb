"""Pure rules and append-only round telemetry for the V1 volume probe.

The live controller is deliberately kept separate from these rules so sizing,
post-only side selection, state transitions, and CSV output can be tested
without credentials or an exchange connection.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from enum import Enum
from pathlib import Path
from typing import Any


class ProbeStatus(str, Enum):
    COMPLETED = "COMPLETED"
    TIMEOUT = "TIMEOUT"
    HALTED = "HALTED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    PREORDER_ONLY = "PREORDER_ONLY"


class ProbeState(str, Enum):
    FLAT = "FLAT"
    BUILD = "BUILD"
    HEDGED = "HEDGED"
    UNWIND = "UNWIND"
    DONE = "DONE"
    HALTED = "HALTED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


@dataclass
class ProbeStateMachine:
    state: ProbeState = ProbeState.FLAT

    _ALLOWED = {
        ProbeState.FLAT: frozenset((ProbeState.BUILD, ProbeState.DONE)),
        ProbeState.BUILD: frozenset(
            (ProbeState.HEDGED, ProbeState.HALTED, ProbeState.RECONCILIATION_REQUIRED)
        ),
        ProbeState.HEDGED: frozenset((ProbeState.UNWIND, ProbeState.HALTED)),
        ProbeState.UNWIND: frozenset(
            (ProbeState.FLAT, ProbeState.HALTED, ProbeState.RECONCILIATION_REQUIRED)
        ),
        ProbeState.DONE: frozenset(),
        ProbeState.HALTED: frozenset(
            (ProbeState.RECONCILIATION_REQUIRED,)
        ),
        ProbeState.RECONCILIATION_REQUIRED: frozenset(),
    }

    def transition(self, target: ProbeState | str) -> ProbeState:
        target_state = target if isinstance(target, ProbeState) else ProbeState(target)
        if target_state not in self._ALLOWED[self.state]:
            raise RuntimeError(
                f"invalid volume-probe transition "
                f"{self.state.value}->{target_state.value}"
            )
        self.state = target_state
        return self.state


@dataclass(frozen=True)
class ProbeConfig:
    clip_usd: Decimal
    probe_side: str
    reprice_sec: float = 30.0
    max_runtime_sec: int = 1800
    max_loss_usd: Decimal = Decimal("10")

    def __post_init__(self) -> None:
        clip = _decimal(self.clip_usd, "probe_clip_usd")
        loss = _decimal(self.max_loss_usd, "probe_max_loss_usd")
        side = _side(self.probe_side)
        if clip <= 0:
            raise ValueError("probe_clip_usd must be > 0")
        if self.reprice_sec <= 0:
            raise ValueError("probe_reprice_sec must be > 0")
        if self.max_runtime_sec <= 0:
            raise ValueError("probe_max_runtime_sec must be > 0")
        if loss <= 0:
            raise ValueError("probe_max_loss_usd must be > 0")
        object.__setattr__(self, "clip_usd", clip)
        object.__setattr__(self, "max_loss_usd", loss)
        object.__setattr__(self, "probe_side", side.lower())


@dataclass(frozen=True)
class ProbeCandidate:
    arcus_side: str
    hedge_side: str
    price: Decimal
    quantity: Decimal
    best_bid: Decimal
    best_ask: Decimal

    @property
    def notional_usd(self) -> Decimal:
        return self.price * self.quantity


def _decimal(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal")
    return result


def _side(value: str) -> str:
    normalized = str(value).upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError("probe_side must be buy or sell")
    return normalized


def compute_probe_quantity(
    *,
    clip_usd: Decimal,
    arcus_bid: Decimal,
    arcus_ask: Decimal,
    probe_side: str,
    arcus_step: Decimal,
    arcus_min_size: Decimal | None,
    arcus_max_size: Decimal | None,
    arcus_min_notional: Decimal | None,
    rh_step: Decimal,
    rh_min_size: Decimal | None,
) -> Decimal:
    """Compute one executable quantity from a fresh two-sided BBO.

    The requested notional is converted using the Arcus mid and rounded down
    on the Arcus grid.  RH's grid/minimum are validation gates rather than a
    second rounding boundary, so an incompatible quantity fails closed.
    """

    clip = _decimal(clip_usd, "probe_clip_usd")
    bid = _decimal(arcus_bid, "arcus_bid")
    ask = _decimal(arcus_ask, "arcus_ask")
    step = _decimal(arcus_step, "arcus_step")
    rh_grid = _decimal(rh_step, "rh_step")
    side = _side(probe_side)
    if clip <= 0:
        raise ValueError("probe_clip_usd must be > 0")
    if bid <= 0 or ask <= 0 or ask < bid:
        raise ValueError("Arcus BBO must be positive and ordered")
    if step <= 0 or rh_grid <= 0:
        raise ValueError("venue step sizes must be > 0")

    mid = (bid + ask) / Decimal("2")
    raw_qty = clip / mid
    quantity = (raw_qty / step).to_integral_value(rounding=ROUND_FLOOR) * step
    if quantity <= 0:
        raise ValueError("probe quantity rounds to zero")
    minimum = (
        _decimal(arcus_min_size, "Arcus min order size")
        if arcus_min_size is not None
        else None
    )
    maximum = (
        _decimal(arcus_max_size, "Arcus max order size")
        if arcus_max_size is not None
        else None
    )
    min_notional = (
        _decimal(arcus_min_notional, "Arcus min order notional")
        if arcus_min_notional is not None
        else None
    )
    rh_minimum = (
        _decimal(rh_min_size, "RH minimum quantity")
        if rh_min_size is not None
        else None
    )
    if minimum is not None and quantity < minimum:
        raise ValueError("probe quantity is below Arcus min order size")
    if maximum is not None and quantity > maximum:
        raise ValueError("probe quantity exceeds Arcus max order size")
    if min_notional is not None:
        order_price = bid if side == "BUY" else ask
        if quantity * order_price < min_notional:
            raise ValueError("probe order is below Arcus min order notional")
    if rh_minimum is not None and quantity < rh_minimum:
        raise ValueError("probe quantity is below RH minimum quantity")
    if quantity % rh_grid != 0:
        raise ValueError("probe quantity is not aligned to RH step")
    return quantity


def alo_would_cross(
    *, side: str, price: Decimal, best_bid: Decimal, best_ask: Decimal
) -> bool:
    normalized = _side(side)
    px = _decimal(price, "price")
    bid = _decimal(best_bid, "best_bid")
    ask = _decimal(best_ask, "best_ask")
    if normalized == "BUY":
        return px >= ask
    return px <= bid


def build_probe_candidate(
    *,
    probe_side: str,
    quantity: Decimal,
    best_bid: Decimal,
    best_ask: Decimal,
) -> ProbeCandidate:
    """Build the resting top-of-book quote for either probe phase."""

    side = _side(probe_side)
    qty = _decimal(quantity, "quantity")
    bid = _decimal(best_bid, "best_bid")
    ask = _decimal(best_ask, "best_ask")
    if qty <= 0:
        raise ValueError("quantity must be > 0")
    if bid <= 0 or ask <= 0 or ask < bid:
        raise ValueError("Arcus BBO must be positive and ordered")
    price = bid if side == "BUY" else ask
    if alo_would_cross(side=side, price=price, best_bid=bid, best_ask=ask):
        raise ValueError("Arcus ALO candidate would cross current BBO")
    return ProbeCandidate(
        arcus_side=side,
        hedge_side="SELL" if side == "BUY" else "BUY",
        price=price,
        quantity=qty,
        best_bid=bid,
        best_ask=ask,
    )


def unwind_side(side: str) -> str:
    """Return the opposite CLI side while preserving caller case style."""

    result = "sell" if _side(side) == "BUY" else "buy"
    return result.upper() if str(side).isupper() else result


ROUND_FIELDS = (
    "session_id",
    "symbol",
    "probe_side",
    "clip_usd",
    "started_at",
    "build_first_quote_at",
    "build_first_fill_at",
    "build_complete_at",
    "unwind_started_at",
    "unwind_complete_at",
    "finished_at",
    "build_base_qty",
    "build_arcus_avg_px",
    "build_rh_avg_px",
    "build_reprices",
    "build_fill_events",
    "unwind_base_qty",
    "unwind_arcus_avg_px",
    "unwind_rh_avg_px",
    "unwind_reprices",
    "unwind_fill_events",
    "build_seconds",
    "unwind_seconds",
    "round_seconds",
    "arcus_fees",
    "rh_fees",
    "realized_round_pnl_usd",
    "max_rh_slippage_bps",
    "avg_fill_to_hedge_send_ms",
    "avg_fill_to_rh_fill_ms",
    "final_arcus_position",
    "final_rh_position",
    "status",
    "failure_reason",
)


@dataclass
class ProbeRoundMetrics:
    session_id: str
    symbol: str
    probe_side: str
    clip_usd: Decimal
    started_at: str | None = None
    build_first_quote_at: str | None = None
    build_first_fill_at: str | None = None
    build_complete_at: str | None = None
    unwind_started_at: str | None = None
    unwind_complete_at: str | None = None
    finished_at: str | None = None
    build_base_qty: Decimal | None = None
    build_arcus_avg_px: Decimal | None = None
    build_rh_avg_px: Decimal | None = None
    build_reprices: int = 0
    build_fill_events: int = 0
    unwind_base_qty: Decimal | None = None
    unwind_arcus_avg_px: Decimal | None = None
    unwind_rh_avg_px: Decimal | None = None
    unwind_reprices: int = 0
    unwind_fill_events: int = 0
    build_seconds: float | None = None
    unwind_seconds: float | None = None
    round_seconds: float | None = None
    arcus_fees: Decimal | None = None
    rh_fees: Decimal | None = None
    realized_round_pnl_usd: Decimal | None = None
    max_rh_slippage_bps: Decimal | None = None
    avg_fill_to_hedge_send_ms: float | None = None
    avg_fill_to_rh_fill_ms: float | None = None
    final_arcus_position: Decimal | None = None
    final_rh_position: Decimal | None = None
    status: ProbeStatus | str = ProbeStatus.PREORDER_ONLY
    failure_reason: str | None = None

    def to_row(self) -> dict[str, str]:
        return {field: _csv_text(getattr(self, field)) for field in ROUND_FIELDS}


def _csv_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


class VolumeProbeRoundWriter:
    """Append exactly one CSV row for each probe session/round."""

    def __init__(self, path: str | Path = "logs/volume_probe_rounds.csv") -> None:
        self.path = Path(path)

    def append(self, metrics: ProbeRoundMetrics) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self.path.exists() or self.path.stat().st_size == 0
        with self.path.open("a", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=ROUND_FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow(metrics.to_row())
