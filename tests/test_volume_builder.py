from __future__ import annotations

import os
import sys
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from entropy_arb.volume_probe import ProbeRoundMetrics, VolumeProbeRoundWriter
from tools.volume_builder import (
    BUILDER_SESSION_FIELDS,
    BuilderConfig,
    BuilderSessionRow,
    BuilderStatus,
    VolumeBuilder,
    VolumeBuilderSessionWriter,
    build_child_argv,
    read_round_rows,
    realized_round_volumes,
)


def _config(tmp_path: Path) -> BuilderConfig:
    return BuilderConfig(
        config_path=Path("config.yaml"),
        symbol="HYPE",
        hedge="lighter-rh",
        probe_side="sell",
        clip_usd=Decimal("20"),
        target_volume_usd=Decimal("400"),
        max_rounds=10,
        max_loss_usd=Decimal("5"),
        inter_round_delay_sec=Decimal("2"),
        round_log_path=tmp_path / "volume_probe_rounds.csv",
        builder_log_path=tmp_path / "volume_builder_sessions.csv",
        repo_root=tmp_path,
    )


def _approved_config(tmp_path: Path, **changes) -> BuilderConfig:
    return replace(
        _config(tmp_path),
        confirm_mainnet=True,
        approve_live_builder=True,
        **changes,
    )


def _append_completed_round(
    path: Path,
    *,
    session_id: str = "vp-1",
    status: str = "COMPLETED",
    final_arcus_position: Decimal | str = Decimal("0"),
    final_rh_position: Decimal | str = Decimal("0"),
    failure_reason: str | None = None,
) -> None:
    VolumeProbeRoundWriter(path).append(
        ProbeRoundMetrics(
            session_id=session_id,
            symbol="HYPE",
            probe_side="sell",
            clip_usd=Decimal("20"),
            build_base_qty=Decimal("0.10"),
            build_arcus_avg_px=Decimal("100"),
            build_rh_avg_px=Decimal("100.1"),
            unwind_base_qty=Decimal("0.10"),
            unwind_arcus_avg_px=Decimal("101"),
            unwind_rh_avg_px=Decimal("100.9"),
            realized_round_pnl_usd=Decimal("-0.04"),
            max_rh_slippage_bps=Decimal("2.1"),
            final_arcus_position=Decimal(str(final_arcus_position)),
            final_rh_position=Decimal(str(final_rh_position)),
            status=status,
            failure_reason=failure_reason,
        )
    )


class _FakeProcess:
    def __init__(self, returncode: int = 0) -> None:
        self.returncode = returncode
        self.wait_calls = 0
        self.signals: list[int] = []

    def wait(self) -> int:
        self.wait_calls += 1
        return self.returncode

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)


class _FakePopen:
    def __init__(self, callback=None, *, returncode: int = 0) -> None:
        self.callback = callback
        self.returncode = returncode
        self.calls: list[tuple[list[str], dict[str, object]]] = []
        self.processes: list[_FakeProcess] = []

    def __call__(self, argv, **kwargs):
        self.calls.append((list(argv), dict(kwargs)))
        if self.callback is not None:
            self.callback()
        process = _FakeProcess(self.returncode)
        self.processes.append(process)
        return process


def test_build_child_argv_matches_one_shot_volume_probe_command(tmp_path) -> None:
    argv = build_child_argv(_config(tmp_path))

    assert argv == [
        sys.executable,
        "main.py",
        "--config",
        "config.yaml",
        "--symbol",
        "HYPE",
        "--hedge",
        "lighter-rh",
        "--volume-probe",
        "--confirm-mainnet",
        "--approve-first-order",
        "--probe-side",
        "sell",
        "--probe-clip-usd",
        "20",
        "--no-dashboard",
    ]


def test_realized_round_volumes_use_authoritative_arcus_and_rh_fields() -> None:
    row = {
        "build_base_qty": "0.10",
        "build_arcus_avg_px": "100",
        "unwind_base_qty": "0.10",
        "unwind_arcus_avg_px": "101",
        "build_rh_avg_px": "100.1",
        "unwind_rh_avg_px": "100.9",
    }

    arcus_volume, rh_volume = realized_round_volumes(row)

    assert arcus_volume == Decimal("20.10")
    assert rh_volume == Decimal("20.10")


def test_realized_round_volumes_reject_missing_authoritative_fields() -> None:
    with pytest.raises(ValueError, match="build_arcus_avg_px"):
        realized_round_volumes(
            {
                "build_base_qty": "0.10",
                "unwind_base_qty": "0.10",
                "unwind_arcus_avg_px": "101",
                "build_rh_avg_px": "100.1",
                "unwind_rh_avg_px": "100.9",
            }
        )


def test_round_csv_reader_preserves_decimal_text_and_order(tmp_path) -> None:
    path = tmp_path / "volume_probe_rounds.csv"
    path.write_text(
        "session_id,status,final_arcus_position,final_rh_position\n"
        "vp-1,COMPLETED,0,0\n",
        encoding="utf-8",
    )

    assert read_round_rows(path) == [
        {
            "session_id": "vp-1",
            "status": "COMPLETED",
            "final_arcus_position": "0",
            "final_rh_position": "0",
        }
    ]


def test_builder_session_writer_has_stable_append_only_schema(tmp_path) -> None:
    path = tmp_path / "logs" / "volume_builder_sessions.csv"
    writer = VolumeBuilderSessionWriter(path)
    row = BuilderSessionRow(
        builder_session_id="vb-1",
        symbol="HYPE",
        probe_side="sell",
        clip_usd=Decimal("20"),
        target_arcus_volume_usd=Decimal("400"),
        max_rounds=10,
        started_at="2026-10-03T00:00:00+00:00",
        finished_at="2026-10-03T00:01:00+00:00",
        rounds_started=1,
        rounds_completed=1,
        cumulative_arcus_volume_usd=Decimal("20.10"),
        cumulative_rh_volume_usd=Decimal("20.10"),
        cumulative_realized_pnl_usd=Decimal("-0.04"),
        max_round_slippage_bps=Decimal("2.1"),
        status=BuilderStatus.COMPLETED,
        failure_reason=None,
    )

    writer.append(row)
    writer.append(row)

    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0].split(",") == list(BUILDER_SESSION_FIELDS)
    assert len(lines) == 3
    assert lines[1].split(",")[0] == "vb-1"
    assert lines[1].split(",")[-2] == "COMPLETED"
    assert lines[1].split(",")[-1] == ""


def test_missing_live_approval_does_not_launch_child_and_logs_preorder_only(
    tmp_path, capsys
) -> None:
    config = _config(tmp_path)
    popen = _FakePopen()

    result = VolumeBuilder(config, popen_factory=popen).run()

    assert result.status is BuilderStatus.PREORDER_ONLY
    assert popen.calls == []
    assert "PREORDER_ONLY" in capsys.readouterr().out
    assert read_round_rows(config.builder_log_path)[0]["status"] == "PREORDER_ONLY"


def test_successful_completed_round_uses_new_flat_session_and_no_shell(
    tmp_path,
) -> None:
    config = _approved_config(tmp_path, target_volume_usd=Decimal("20.10"))
    popen = _FakePopen(
        callback=lambda: _append_completed_round(
            config.round_log_path, session_id="vp-new"
        )
    )

    result = VolumeBuilder(config, popen_factory=popen).run()

    assert result.status is BuilderStatus.COMPLETED
    assert result.rounds_started == 1
    assert result.rounds_completed == 1
    assert result.cumulative_arcus_volume_usd == Decimal("20.10")
    assert result.cumulative_rh_volume_usd == Decimal("20.10")
    assert popen.calls[0][1]["shell"] is False
    assert popen.calls[0][1]["env"] == os.environ.copy()


def test_round_reusing_existing_session_id_stops_without_counting_it(tmp_path) -> None:
    config = _approved_config(tmp_path)
    _append_completed_round(config.round_log_path, session_id="vp-old")
    popen = _FakePopen(
        callback=lambda: _append_completed_round(
            config.round_log_path, session_id="vp-old"
        )
    )

    result = VolumeBuilder(config, popen_factory=popen).run()

    assert result.status is BuilderStatus.ROUND_NOT_COMPLETED
    assert result.rounds_completed == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("final_arcus_position", "0.01"),
        ("final_rh_position", "0.01"),
    ],
)
def test_nonflat_completed_round_stops_builder(
    tmp_path, field: str, value: str
) -> None:
    config = _approved_config(tmp_path)
    kwargs = {field: value}
    popen = _FakePopen(
        callback=lambda: _append_completed_round(
            config.round_log_path, session_id="vp-nonflat", **kwargs
        )
    )

    result = VolumeBuilder(config, popen_factory=popen).run()

    assert result.status is BuilderStatus.ROUND_NOT_COMPLETED
    assert result.rounds_completed == 0


@pytest.mark.parametrize("status", ["HALTED", "RECONCILIATION_REQUIRED"])
def test_noncompleted_round_status_stops_builder(tmp_path, status: str) -> None:
    config = _approved_config(tmp_path)
    popen = _FakePopen(
        callback=lambda: _append_completed_round(
            config.round_log_path, session_id="vp-failed", status=status
        )
    )

    result = VolumeBuilder(config, popen_factory=popen).run()

    assert result.status is BuilderStatus.ROUND_NOT_COMPLETED
    assert result.rounds_completed == 0


def test_nonzero_child_exit_stops_builder(tmp_path) -> None:
    config = _approved_config(tmp_path)
    popen = _FakePopen(returncode=7)

    result = VolumeBuilder(config, popen_factory=popen).run()

    assert result.status is BuilderStatus.CHILD_FAILED
    assert result.rounds_started == 1
    assert result.rounds_completed == 0
