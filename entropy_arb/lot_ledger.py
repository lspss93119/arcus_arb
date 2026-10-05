"""Persistent Arcus rolling lots and actual-fill break-even accounting."""

from __future__ import annotations

import json
import math
import os
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, replace

SCHEMA_VERSION = 1
_EPS = 1e-12
_VALID_DIRECTIONS = frozenset(("sell_arcus", "buy_arcus"))


class LotLedgerError(RuntimeError):
    """Raised when rolling inventory state cannot be trusted."""


def _finite_positive(value: object, label: str) -> float:
    try:
        result = float(str(value))
    except (TypeError, ValueError) as exc:
        raise LotLedgerError(f"{label} must be numeric") from exc
    if not math.isfinite(result) or result <= 0:
        raise LotLedgerError(f"{label} must be finite and > 0")
    return result


def _finite_nonnegative(value: object, label: str) -> float:
    try:
        result = float(str(value))
    except (TypeError, ValueError) as exc:
        raise LotLedgerError(f"{label} must be numeric") from exc
    if not math.isfinite(result) or result < 0:
        raise LotLedgerError(f"{label} must be finite and >= 0")
    return result


@dataclass(frozen=True)
class Lot:
    lot_id: str
    source_event_id: str
    direction: str
    open_qty: float
    entry_ts: float
    buy_venue: str
    sell_venue: str
    buy_avg_px: float
    sell_avg_px: float
    buy_fee_bps: float
    sell_fee_bps: float
    entry_cash_per_base: float
    reference_entry_notional_per_base: float

    def required_exit_cash_per_base(self, min_exit_capture_bps: float) -> float:
        floor = _finite_nonnegative(min_exit_capture_bps, "min_exit_capture_bps")
        return -self.entry_cash_per_base + (
            floor / 1e4 * self.reference_entry_notional_per_base
        )

    def exit_cash_per_base(
        self,
        buy_px: float,
        sell_px: float,
        buy_fee_bps: float,
        sell_fee_bps: float,
    ) -> float:
        buy_price = _finite_positive(buy_px, "exit buy_px")
        sell_price = _finite_positive(sell_px, "exit sell_px")
        buy_fee = _finite_nonnegative(buy_fee_bps, "exit buy_fee_bps") / 1e4
        sell_fee = _finite_nonnegative(sell_fee_bps, "exit sell_fee_bps") / 1e4
        return sell_price * (1.0 - sell_fee) - buy_price * (1.0 + buy_fee)

    def expected_exit_capture_bps(
        self,
        buy_px: float,
        sell_px: float,
        buy_fee_bps: float,
        sell_fee_bps: float,
    ) -> float:
        exit_cash = self.exit_cash_per_base(buy_px, sell_px, buy_fee_bps, sell_fee_bps)
        return (
            (self.entry_cash_per_base + exit_cash)
            / self.reference_entry_notional_per_base
            * 1e4
        )

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> Lot:
        required = (
            "lot_id",
            "source_event_id",
            "direction",
            "open_qty",
            "entry_ts",
            "buy_venue",
            "sell_venue",
            "buy_avg_px",
            "sell_avg_px",
            "buy_fee_bps",
            "sell_fee_bps",
            "entry_cash_per_base",
            "reference_entry_notional_per_base",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise LotLedgerError(
                "lot is missing required fields: " + ", ".join(missing)
            )
        try:
            lot = cls(
                lot_id=str(raw["lot_id"]),
                source_event_id=str(raw["source_event_id"]),
                direction=str(raw["direction"]),
                open_qty=_finite_positive(raw["open_qty"], "lot.open_qty"),
                entry_ts=_finite_positive(raw["entry_ts"], "lot.entry_ts"),
                buy_venue=str(raw["buy_venue"]),
                sell_venue=str(raw["sell_venue"]),
                buy_avg_px=_finite_positive(raw["buy_avg_px"], "lot.buy_avg_px"),
                sell_avg_px=_finite_positive(raw["sell_avg_px"], "lot.sell_avg_px"),
                buy_fee_bps=_finite_nonnegative(raw["buy_fee_bps"], "lot.buy_fee_bps"),
                sell_fee_bps=_finite_nonnegative(
                    raw["sell_fee_bps"], "lot.sell_fee_bps"
                ),
                entry_cash_per_base=float(str(raw["entry_cash_per_base"])),
                reference_entry_notional_per_base=_finite_positive(
                    raw["reference_entry_notional_per_base"],
                    "lot.reference_entry_notional_per_base",
                ),
            )
        except (TypeError, ValueError) as exc:
            raise LotLedgerError("lot contains invalid numeric fields") from exc
        if lot.direction not in _VALID_DIRECTIONS:
            raise LotLedgerError(f"invalid lot direction: {lot.direction}")
        if not lot.lot_id or not lot.source_event_id:
            raise LotLedgerError("lot identity fields must be non-empty")
        if not lot.buy_venue or not lot.sell_venue:
            raise LotLedgerError("lot venue fields must be non-empty")
        if not math.isfinite(lot.entry_cash_per_base):
            raise LotLedgerError("lot contains non-finite economics")
        return lot


class LotLedger:
    """A versioned, pair-specific collection of open Arcus rolling lots."""

    def __init__(
        self,
        path: str | None = None,
        *,
        symbol: str = "",
        hedge: str = "",
        tolerance: float = 1e-9,
    ) -> None:
        self.path = path
        self.symbol = symbol
        self.hedge = hedge
        self.tolerance = max(float(tolerance), _EPS)
        self._lots: list[Lot] = []

    @property
    def lots(self) -> tuple[Lot, ...]:
        return tuple(self._lots)

    @property
    def total_qty(self) -> float:
        return sum(lot.open_qty for lot in self._lots)

    @property
    def reference_notional(self) -> float:
        return sum(
            lot.open_qty * lot.reference_entry_notional_per_base for lot in self._lots
        )

    @property
    def direction(self) -> str | None:
        directions = {lot.direction for lot in self._lots}
        if len(directions) > 1:
            raise LotLedgerError("ledger contains mixed rolling directions")
        return next(iter(directions), None)

    @property
    def first_entry_ts(self) -> float | None:
        return min((lot.entry_ts for lot in self._lots), default=None)

    def _payload(self, lots: Iterable[Lot]) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "symbol": self.symbol,
            "hedge": self.hedge,
            "lots": [asdict(lot) for lot in lots],
        }

    def _persist(self, lots: Iterable[Lot]) -> None:
        if not self.path:
            return
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(
            prefix=".lots-", suffix=".tmp", dir=directory, text=True
        )
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(
                    self._payload(lots), fh, sort_keys=True, separators=(",", ":")
                )
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(temp_path, self.path)
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception as exc:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass
            raise LotLedgerError(f"cannot persist ledger {self.path}: {exc}") from exc

    def save(self) -> None:
        self._persist(self._lots)

    def load(self) -> int:
        if not self.path or not os.path.exists(self.path):
            self._lots = []
            return 0
        try:
            with open(self.path) as fh:
                payload = json.load(fh)
            if not isinstance(payload, dict):
                raise LotLedgerError("root must be an object")
            if payload.get("schema_version") != SCHEMA_VERSION:
                raise LotLedgerError(
                    f"unsupported schema_version {payload.get('schema_version')!r}"
                )
            if self.symbol and payload.get("symbol") != self.symbol:
                raise LotLedgerError("ledger symbol does not match runtime pair")
            if self.hedge and payload.get("hedge") != self.hedge:
                raise LotLedgerError("ledger hedge does not match runtime pair")
            raw_lots = payload.get("lots")
            if not isinstance(raw_lots, list):
                raise LotLedgerError("lots must be a list")
            lots = [Lot.from_dict(raw) for raw in raw_lots]
            ids = [lot.lot_id for lot in lots]
            if len(ids) != len(set(ids)):
                raise LotLedgerError("ledger contains duplicate lot ids")
            self._lots = lots
            self.direction
            return len(lots)
        except LotLedgerError as exc:
            raise LotLedgerError(f"cannot load ledger {self.path}: {exc}") from exc
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise LotLedgerError(f"cannot load ledger {self.path}: {exc}") from exc

    def add_lot(
        self,
        *,
        lot_id: str,
        source_event_id: str,
        direction: str,
        open_qty: float,
        entry_ts: float,
        buy_venue: str,
        sell_venue: str,
        buy_avg_px: float,
        sell_avg_px: float,
        buy_fee_bps: float,
        sell_fee_bps: float,
    ) -> Lot:
        if direction not in _VALID_DIRECTIONS:
            raise LotLedgerError(f"invalid lot direction: {direction}")
        if not str(lot_id) or not str(source_event_id):
            raise LotLedgerError("lot identity fields must be non-empty")
        if not str(buy_venue) or not str(sell_venue):
            raise LotLedgerError("lot venue fields must be non-empty")
        if self.direction not in (None, direction):
            raise LotLedgerError("cannot add opposite-direction rolling lot")
        if any(lot.lot_id == lot_id for lot in self._lots):
            raise LotLedgerError(f"duplicate lot id: {lot_id}")
        qty = _finite_positive(open_qty, "open_qty")
        entry_time = _finite_positive(entry_ts, "entry_ts")
        buy_px = _finite_positive(buy_avg_px, "buy_avg_px")
        sell_px = _finite_positive(sell_avg_px, "sell_avg_px")
        buy_fee = _finite_nonnegative(buy_fee_bps, "buy_fee_bps")
        sell_fee = _finite_nonnegative(sell_fee_bps, "sell_fee_bps")
        entry_cash = sell_px * (1.0 - sell_fee / 1e4) - buy_px * (1.0 + buy_fee / 1e4)
        reference = (buy_px + sell_px) / 2.0
        lot = Lot(
            lot_id=str(lot_id),
            source_event_id=str(source_event_id),
            direction=direction,
            open_qty=qty,
            entry_ts=entry_time,
            buy_venue=str(buy_venue),
            sell_venue=str(sell_venue),
            buy_avg_px=buy_px,
            sell_avg_px=sell_px,
            buy_fee_bps=buy_fee,
            sell_fee_bps=sell_fee,
            entry_cash_per_base=entry_cash,
            reference_entry_notional_per_base=reference,
        )
        candidate = [*self._lots, lot]
        self._persist(candidate)
        self._lots = candidate
        return lot

    def exit_candidates(self, min_exit_capture_bps: float) -> tuple[dict, ...]:
        floor = _finite_nonnegative(min_exit_capture_bps, "min_exit_capture_bps")
        ordered = sorted(
            self._lots, key=lambda lot: lot.required_exit_cash_per_base(floor)
        )
        return tuple(
            {
                "lot_id": lot.lot_id,
                "direction": lot.direction,
                "open_qty": lot.open_qty,
                "entry_ts": lot.entry_ts,
                "entry_cash_per_base": lot.entry_cash_per_base,
                "reference_entry_notional_per_base": lot.reference_entry_notional_per_base,
                "required_exit_cash_per_base": lot.required_exit_cash_per_base(floor),
            }
            for lot in ordered
        )

    def close_allocations(self, allocations: Iterable[Mapping[str, object]]) -> float:
        requested = list(allocations)
        by_id = {lot.lot_id: lot for lot in self._lots}
        decrements: dict[str, float] = {}
        for item in requested:
            lot_id = str(item.get("lot_id", ""))
            if lot_id not in by_id:
                raise LotLedgerError(f"unknown lot id in close: {lot_id}")
            qty = _finite_positive(item.get("qty"), "close qty")
            decrements[lot_id] = decrements.get(lot_id, 0.0) + qty
        for lot_id, qty in decrements.items():
            if qty > by_id[lot_id].open_qty + self.tolerance:
                raise LotLedgerError(f"close exceeds open lot quantity: {lot_id}")
        candidate = []
        for lot in self._lots:
            remaining = lot.open_qty - decrements.get(lot.lot_id, 0.0)
            if remaining > self.tolerance:
                candidate.append(replace(lot, open_qty=remaining))
        self._persist(candidate)
        self._lots = candidate
        return sum(decrements.values())

    def validate_positions(self, positions: Mapping[str, object]) -> None:
        """Validate authoritative Arcus/RH positions against the lot ledger."""
        try:
            arcus = float(str(positions.get("arcus", 0.0)))
            hedge = float(str(positions.get("hedge", 0.0)))
        except (TypeError, ValueError) as exc:
            raise LotLedgerError("positions contain invalid values") from exc
        direction = self.direction
        if direction is None:
            if abs(arcus) > self.tolerance or abs(hedge) > self.tolerance:
                raise LotLedgerError("flat ledger has non-flat authoritative position")
            return
        qty = self.total_qty
        expected = (-qty, qty) if direction == "sell_arcus" else (qty, -qty)
        if (
            abs(arcus - expected[0]) > self.tolerance
            or abs(hedge - expected[1]) > self.tolerance
        ):
            raise LotLedgerError(
                "position mismatch: "
                f"direction={direction} ledger_qty={qty:.12g} "
                f"authoritative=({arcus:.12g},{hedge:.12g})"
            )

    def realized_capture(
        self,
        lot_id: str,
        qty: float,
        *,
        buy_px: float,
        sell_px: float,
        buy_fee_bps: float,
        sell_fee_bps: float,
    ) -> tuple[float, float]:
        lot = next(
            (candidate for candidate in self._lots if candidate.lot_id == lot_id),
            None,
        )
        if lot is None:
            raise LotLedgerError(f"unknown lot id: {lot_id}")
        quantity = _finite_positive(qty, "closed qty")
        exit_cash = lot.exit_cash_per_base(buy_px, sell_px, buy_fee_bps, sell_fee_bps)
        cash_per_base = lot.entry_cash_per_base + exit_cash
        return (
            quantity * cash_per_base,
            cash_per_base / lot.reference_entry_notional_per_base * 1e4,
        )
