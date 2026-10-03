"""Fail-closed supervisor for repeated one-shot Arcus volume probes.

The supervisor in this module observes child-process CSV telemetry only.  It
does not import an exchange client or implement any order, hedge, or
reconciliation behavior.
"""

from __future__ import annotations

import csv
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any


class BuilderStatus(StrEnum):
    PREORDER_ONLY = "PREORDER_ONLY"
    COMPLETED = "COMPLETED"
    STOPPED_MAX_ROUNDS = "STOPPED_MAX_ROUNDS"
    STOPPED_MAX_LOSS = "STOPPED_MAX_LOSS"
    CHILD_FAILED = "CHILD_FAILED"
    ROUND_NOT_COMPLETED = "ROUND_NOT_COMPLETED"
    INTERRUPTED = "INTERRUPTED"


BUILDER_SESSION_FIELDS = (
    "builder_session_id",
    "symbol",
    "probe_side",
    "clip_usd",
    "target_arcus_volume_usd",
    "max_rounds",
    "started_at",
    "finished_at",
    "rounds_started",
    "rounds_completed",
    "cumulative_arcus_volume_usd",
    "cumulative_rh_volume_usd",
    "cumulative_realized_pnl_usd",
    "max_round_slippage_bps",
    "status",
    "failure_reason",
)


def _decimal(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal")
    return result


def _positive_decimal(value: Any, field: str) -> Decimal:
    result = _decimal(value, field)
    if result <= 0:
        raise ValueError(f"{field} must be > 0")
    return result


def _csv_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, StrEnum):
        return str(value.value)
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


@dataclass(frozen=True)
class BuilderConfig:
    config_path: Path
    symbol: str
    hedge: str
    probe_side: str
    clip_usd: Decimal
    target_volume_usd: Decimal
    max_rounds: int
    max_loss_usd: Decimal
    inter_round_delay_sec: Decimal
    round_log_path: Path
    builder_log_path: Path
    repo_root: Path
    confirm_mainnet: bool = False
    approve_live_builder: bool = False

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if self.hedge != "lighter-rh":
            raise ValueError("hedge must be lighter-rh")
        if self.probe_side not in {"buy", "sell"}:
            raise ValueError("probe_side must be buy or sell")
        if self.max_rounds <= 0:
            raise ValueError("max_rounds must be > 0")
        object.__setattr__(self, "clip_usd", _positive_decimal(self.clip_usd, "clip_usd"))
        object.__setattr__(
            self,
            "target_volume_usd",
            _positive_decimal(self.target_volume_usd, "target_volume_usd"),
        )
        object.__setattr__(
            self, "max_loss_usd", _positive_decimal(self.max_loss_usd, "max_loss_usd")
        )
        delay = _decimal(self.inter_round_delay_sec, "inter_round_delay_sec")
        if delay < 0:
            raise ValueError("inter_round_delay_sec must be >= 0")
        object.__setattr__(self, "inter_round_delay_sec", delay)


@dataclass(frozen=True)
class BuilderSessionRow:
    builder_session_id: str
    symbol: str
    probe_side: str
    clip_usd: Decimal
    target_arcus_volume_usd: Decimal
    max_rounds: int
    started_at: str
    finished_at: str | None
    rounds_started: int
    rounds_completed: int
    cumulative_arcus_volume_usd: Decimal
    cumulative_rh_volume_usd: Decimal
    cumulative_realized_pnl_usd: Decimal
    max_round_slippage_bps: Decimal
    status: BuilderStatus | str
    failure_reason: str | None

    def to_row(self) -> dict[str, str]:
        return {
            field: _csv_text(getattr(self, field)) for field in BUILDER_SESSION_FIELDS
        }


class VolumeBuilderSessionWriter:
    """Append exactly one row per builder invocation."""

    def __init__(self, path: str | Path = "logs/volume_builder_sessions.csv") -> None:
        self.path = Path(path)

    def append(self, row: BuilderSessionRow) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self.path.exists() or self.path.stat().st_size == 0
        with self.path.open("a", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=BUILDER_SESSION_FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow(row.to_row())


def read_round_rows(path: str | Path) -> list[dict[str, str]]:
    csv_path = Path(path)
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return []
    with csv_path.open("r", encoding="utf-8", newline="") as stream:
        return [dict(row) for row in csv.DictReader(stream)]


def _required_row_decimal(row: Mapping[str, str], field: str) -> Decimal:
    value = row.get(field)
    if value is None or not value.strip():
        raise ValueError(f"round row missing {field}")
    return _decimal(value, field)


def realized_round_volumes(
    row: Mapping[str, str],
) -> tuple[Decimal, Decimal]:
    """Return authoritative Arcus and RH turnover for one completed round."""

    build_qty = _required_row_decimal(row, "build_base_qty")
    build_arcus_px = _required_row_decimal(row, "build_arcus_avg_px")
    unwind_qty = _required_row_decimal(row, "unwind_base_qty")
    unwind_arcus_px = _required_row_decimal(row, "unwind_arcus_avg_px")
    build_rh_px = _required_row_decimal(row, "build_rh_avg_px")
    unwind_rh_px = _required_row_decimal(row, "unwind_rh_avg_px")
    arcus_volume = abs(build_qty * build_arcus_px) + abs(unwind_qty * unwind_arcus_px)
    rh_volume = abs(build_qty * build_rh_px) + abs(unwind_qty * unwind_rh_px)
    return arcus_volume, rh_volume


def build_child_argv(config: BuilderConfig) -> list[str]:
    """Build the exact approved one-shot child argv without shell parsing."""

    return [
        sys.executable,
        "main.py",
        "--config",
        str(config.config_path),
        "--symbol",
        config.symbol,
        "--hedge",
        config.hedge,
        "--volume-probe",
        "--confirm-mainnet",
        "--approve-first-order",
        "--probe-side",
        config.probe_side,
        "--probe-clip-usd",
        _csv_text(config.clip_usd),
        "--no-dashboard",
    ]
