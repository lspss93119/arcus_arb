from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

from tools.volume_builder import (
    BUILDER_SESSION_FIELDS,
    BuilderConfig,
    BuilderSessionRow,
    BuilderStatus,
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
