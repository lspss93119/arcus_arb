"""Fail-closed supervisor for repeated one-shot Arcus volume probes.

The supervisor in this module observes child-process CSV telemetry only.  It
does not import an exchange client or implement any order, hedge, or
reconciliation behavior.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import os
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4


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
        object.__setattr__(
            self, "clip_usd", _positive_decimal(self.clip_usd, "clip_usd")
        )
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
        if not write_header:
            with self.path.open("r", encoding="utf-8", newline="") as stream:
                header = tuple(next(csv.reader(stream), []))
            if header != BUILDER_SESSION_FIELDS:
                raise ValueError("volume builder session CSV header mismatch")
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


@dataclass(frozen=True)
class _CompletedRound:
    session_id: str
    arcus_volume_usd: Decimal
    rh_volume_usd: Decimal
    realized_pnl_usd: Decimal
    max_slippage_bps: Decimal


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


class VolumeBuilder:
    """Supervise fresh one-shot volume-probe child processes."""

    def __init__(
        self,
        config: BuilderConfig,
        *,
        popen_factory: Callable[..., Any] = subprocess.Popen,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self._popen_factory = popen_factory
        self._sleep_fn = sleep_fn
        self._active_process: Any | None = None
        self._interrupt_requested = False

    def request_interrupt(self) -> None:
        """Request a clean stop and forward SIGINT to an active child."""

        self._interrupt_requested = True
        process = self._active_process
        if process is not None:
            with contextlib.suppress(OSError, ProcessLookupError):
                process.send_signal(signal.SIGINT)

    def _handle_sigint(self, _signum: int, _frame: Any) -> None:
        self.request_interrupt()

    def _validate_new_round(
        self,
        before_rows: list[dict[str, str]],
        after_rows: list[dict[str, str]],
    ) -> _CompletedRound:
        if len(after_rows) != len(before_rows) + 1:
            raise ValueError("round CSV did not append exactly one new row")
        row = after_rows[-1]
        session_id = (row.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("round row has no session_id")
        if session_id in {item.get("session_id", "") for item in before_rows}:
            raise ValueError("round row reused an existing session_id")
        status = (row.get("status") or "").strip()
        if status != "COMPLETED":
            raise ValueError(f"round status={status or '<empty>'}")
        for field in ("final_arcus_position", "final_rh_position"):
            position = _required_row_decimal(row, field)
            if position != 0:
                raise ValueError(f"round {field}={position} is not flat")
        if row.get("failure_reason") is None or row["failure_reason"].strip():
            raise ValueError("completed round has failure_reason")
        arcus_volume, rh_volume = realized_round_volumes(row)
        realized_pnl = _required_row_decimal(row, "realized_round_pnl_usd")
        max_slippage = _required_row_decimal(row, "max_rh_slippage_bps")
        return _CompletedRound(
            session_id=session_id,
            arcus_volume_usd=arcus_volume,
            rh_volume_usd=rh_volume,
            realized_pnl_usd=realized_pnl,
            max_slippage_bps=max_slippage,
        )

    def _launch_and_wait(self, argv: list[str]) -> int:
        process = self._popen_factory(
            argv,
            shell=False,
            env=os.environ.copy(),
            cwd=str(self.config.repo_root),
        )
        self._active_process = process
        try:
            if self._interrupt_requested:
                process.send_signal(signal.SIGINT)
            return int(process.wait())
        finally:
            self._active_process = None

    def _preorder_only(self, argv: list[str]) -> None:
        print("[builder] PREORDER_ONLY no child launched")
        print(f"[builder] child command: {shlex.join(argv)}")

    def _interrupted_round_reason(self, before_rows: list[dict[str, str]]) -> str:
        try:
            rows = read_round_rows(self.config.round_log_path)
        except Exception as exc:
            return (
                f"builder interrupted; latest round unavailable ({type(exc).__name__})"
            )
        if len(rows) > len(before_rows):
            latest = rows[-1]
            session_id = latest.get("session_id") or "<empty>"
            round_status = latest.get("status") or "<empty>"
            return (
                f"builder interrupted; latest round session_id={session_id} "
                f"status={round_status}"
            )
        return "builder interrupted; no new round row observed"

    def run(self) -> BuilderSessionRow:
        started_at = _utc_now()
        builder_session_id = f"vb-{uuid4().hex[:12]}"
        rounds_started = 0
        rounds_completed = 0
        cumulative_arcus = Decimal("0")
        cumulative_rh = Decimal("0")
        cumulative_pnl = Decimal("0")
        max_slippage = Decimal("0")
        status: BuilderStatus | str = BuilderStatus.CHILD_FAILED
        failure_reason: str | None = None
        argv = build_child_argv(self.config)
        previous_sigint = signal.signal(signal.SIGINT, self._handle_sigint)

        try:
            if self._interrupt_requested:
                status = BuilderStatus.INTERRUPTED
                failure_reason = "builder interrupted"
            elif not (self.config.confirm_mainnet and self.config.approve_live_builder):
                status = BuilderStatus.PREORDER_ONLY
                failure_reason = (
                    "missing --confirm-mainnet and/or --approve-live-builder"
                )
                self._preorder_only(argv)
            else:
                before_rows = read_round_rows(self.config.round_log_path)
                while rounds_completed < self.config.max_rounds:
                    if self._interrupt_requested:
                        status = BuilderStatus.INTERRUPTED
                        failure_reason = "builder interrupted"
                        break
                    rounds_started += 1
                    try:
                        returncode = self._launch_and_wait(argv)
                    except Exception as exc:
                        if self._interrupt_requested:
                            status = BuilderStatus.INTERRUPTED
                            failure_reason = self._interrupted_round_reason(before_rows)
                            break
                        status = BuilderStatus.CHILD_FAILED
                        failure_reason = (
                            f"child launch/wait failed ({type(exc).__name__})"
                        )
                        break
                    if returncode != 0:
                        if self._interrupt_requested:
                            status = BuilderStatus.INTERRUPTED
                            failure_reason = self._interrupted_round_reason(before_rows)
                            break
                        status = BuilderStatus.CHILD_FAILED
                        failure_reason = f"child exit code={returncode}"
                        break
                    if self._interrupt_requested:
                        status = BuilderStatus.INTERRUPTED
                        failure_reason = self._interrupted_round_reason(before_rows)
                        break
                    after_rows = read_round_rows(self.config.round_log_path)
                    try:
                        completed = self._validate_new_round(before_rows, after_rows)
                    except ValueError as exc:
                        status = BuilderStatus.ROUND_NOT_COMPLETED
                        failure_reason = str(exc)
                        break
                    before_rows = after_rows
                    rounds_completed += 1
                    cumulative_arcus += completed.arcus_volume_usd
                    cumulative_rh += completed.rh_volume_usd
                    cumulative_pnl += completed.realized_pnl_usd
                    max_slippage = max(max_slippage, completed.max_slippage_bps)
                    print(
                        f"[builder] round={rounds_completed}/{self.config.max_rounds}"
                    )
                    print("status=COMPLETED")
                    print(
                        f"arcus_volume=${completed.arcus_volume_usd} "
                        f"rh_volume=${completed.rh_volume_usd}"
                    )
                    print(
                        f"cumulative_arcus=${cumulative_arcus}/"
                        f"{self.config.target_volume_usd} "
                        f"cumulative_pnl=${cumulative_pnl}"
                    )
                    if cumulative_pnl <= -self.config.max_loss_usd:
                        status = BuilderStatus.STOPPED_MAX_LOSS
                        break
                    if cumulative_arcus >= self.config.target_volume_usd:
                        status = BuilderStatus.COMPLETED
                        break
                    if rounds_completed >= self.config.max_rounds:
                        status = BuilderStatus.STOPPED_MAX_ROUNDS
                        break
                    if self._interrupt_requested:
                        status = BuilderStatus.INTERRUPTED
                        failure_reason = "builder interrupted"
                        break
                    self._sleep_fn(float(self.config.inter_round_delay_sec))
                    if self._interrupt_requested:
                        status = BuilderStatus.INTERRUPTED
                        failure_reason = "builder interrupted"
                        break
                else:
                    status = BuilderStatus.STOPPED_MAX_ROUNDS
        finally:
            signal.signal(signal.SIGINT, previous_sigint)
            row = BuilderSessionRow(
                builder_session_id=builder_session_id,
                symbol=self.config.symbol,
                probe_side=self.config.probe_side,
                clip_usd=self.config.clip_usd,
                target_arcus_volume_usd=self.config.target_volume_usd,
                max_rounds=self.config.max_rounds,
                started_at=started_at,
                finished_at=_utc_now(),
                rounds_started=rounds_started,
                rounds_completed=rounds_completed,
                cumulative_arcus_volume_usd=cumulative_arcus,
                cumulative_rh_volume_usd=cumulative_rh,
                cumulative_realized_pnl_usd=cumulative_pnl,
                max_round_slippage_bps=max_slippage,
                status=status,
                failure_reason=failure_reason,
            )
            VolumeBuilderSessionWriter(self.config.builder_log_path).append(row)
        return row


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Supervise fresh one-shot Arcus volume-probe rounds"
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--hedge", required=True, choices=("lighter-rh",))
    parser.add_argument("--probe-side", required=True, choices=("buy", "sell"))
    parser.add_argument("--clip-usd", type=Decimal, required=True)
    parser.add_argument("--target-volume-usd", type=Decimal, required=True)
    parser.add_argument("--max-rounds", type=int, required=True)
    parser.add_argument("--max-loss-usd", type=Decimal, required=True)
    parser.add_argument("--inter-round-delay-sec", type=Decimal, default=Decimal("2"))
    parser.add_argument("--confirm-mainnet", action="store_true")
    parser.add_argument("--approve-live-builder", action="store_true")
    parser.add_argument(
        "--round-log",
        default="logs/volume_probe_rounds.csv",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--builder-log",
        default="logs/volume_builder_sessions.csv",
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = BuilderConfig(
            config_path=Path(args.config),
            symbol=args.symbol,
            hedge=args.hedge,
            probe_side=args.probe_side,
            clip_usd=args.clip_usd,
            target_volume_usd=args.target_volume_usd,
            max_rounds=args.max_rounds,
            max_loss_usd=args.max_loss_usd,
            inter_round_delay_sec=args.inter_round_delay_sec,
            round_log_path=Path(args.round_log),
            builder_log_path=Path(args.builder_log),
            repo_root=Path(__file__).resolve().parents[1],
            confirm_mainnet=args.confirm_mainnet,
            approve_live_builder=args.approve_live_builder,
        )
    except ValueError as exc:
        parser.error(str(exc))
    row = VolumeBuilder(config).run()
    print(f"[builder] finished status={row.status}")
    if row.failure_reason:
        print(f"[builder] failure_reason={row.failure_reason}")
    if row.status in {
        BuilderStatus.CHILD_FAILED,
        BuilderStatus.ROUND_NOT_COMPLETED,
        BuilderStatus.INTERRUPTED,
    }:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
