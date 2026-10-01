from __future__ import annotations

from decimal import Decimal

import pytest

from entropy_arb.volume_probe import (
    ProbeRoundMetrics,
    ProbeState,
    ProbeStateMachine,
    ProbeStatus,
    VolumeProbeRoundWriter,
    build_probe_candidate,
    compute_probe_quantity,
    unwind_side,
)


def _quantity(**overrides) -> Decimal:
    values = dict(
        clip_usd=Decimal("9.99"),
        arcus_bid=Decimal("99.90"),
        arcus_ask=Decimal("100.10"),
        probe_side="sell",
        arcus_step=Decimal("0.001"),
        arcus_min_size=Decimal("0.001"),
        arcus_max_size=Decimal("1"),
        arcus_min_notional=Decimal("1"),
        rh_step=Decimal("0.001"),
        rh_min_size=Decimal("0.001"),
    )
    values.update(overrides)
    return compute_probe_quantity(**values)


def test_clip_usd_rounds_down_from_arcus_mid_by_arcus_step() -> None:
    assert _quantity() == Decimal("0.099")


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"clip_usd": Decimal("0.01")}, "zero"),
        ({"arcus_min_size": Decimal("0.1")}, "min"),
        ({"arcus_max_size": Decimal("0.05")}, "max"),
        ({"arcus_min_notional": Decimal("20")}, "notional"),
        ({"rh_min_size": Decimal("0.2")}, "RH minimum"),
        ({"rh_step": Decimal("0.01")}, "RH step"),
    ],
)
def test_quantity_validation_rejects_unhedgeable_or_out_of_bounds_clip(
    overrides: dict[str, Decimal], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _quantity(**overrides)


def test_buy_build_candidate_is_arcus_best_bid_and_rh_sell() -> None:
    candidate = build_probe_candidate(
        probe_side="buy",
        quantity=Decimal("0.1"),
        best_bid=Decimal("99.90"),
        best_ask=Decimal("100.10"),
    )
    assert candidate.arcus_side == "BUY"
    assert candidate.hedge_side == "SELL"
    assert candidate.price == Decimal("99.90")


def test_sell_build_candidate_is_arcus_best_ask_and_rh_buy() -> None:
    candidate = build_probe_candidate(
        probe_side="sell",
        quantity=Decimal("0.1"),
        best_bid=Decimal("99.90"),
        best_ask=Decimal("100.10"),
    )
    assert candidate.arcus_side == "SELL"
    assert candidate.hedge_side == "BUY"
    assert candidate.price == Decimal("100.10")


@pytest.mark.parametrize("side", ["buy", "sell"])
def test_probe_candidate_never_crosses_arcus_bbo(side: str) -> None:
    with pytest.raises(ValueError, match="cross"):
        build_probe_candidate(
            probe_side=side,
            quantity=Decimal("0.1"),
            best_bid=Decimal("100.10"),
            best_ask=Decimal("100.10"),
        )


def test_unwind_side_reverses_arcus_and_hedge_direction() -> None:
    assert unwind_side("buy") == "sell"
    assert unwind_side("SELL") == "BUY"


def test_round_state_machine_allows_only_one_probe_round() -> None:
    machine = ProbeStateMachine()
    machine.transition(ProbeState.BUILD)
    machine.transition(ProbeState.HEDGED)
    machine.transition(ProbeState.UNWIND)
    machine.transition(ProbeState.FLAT)
    machine.transition(ProbeState.DONE)
    assert machine.state is ProbeState.DONE
    with pytest.raises(RuntimeError, match="invalid"):
        machine.transition(ProbeState.BUILD)


def test_round_state_machine_allows_fail_closed_terminal_states() -> None:
    machine = ProbeStateMachine()
    machine.transition(ProbeState.BUILD)
    machine.transition(ProbeState.HALTED)
    assert machine.state is ProbeState.HALTED


def test_round_writer_appends_exact_header_and_status(tmp_path) -> None:
    path = tmp_path / "logs" / "volume_probe_rounds.csv"
    writer = VolumeProbeRoundWriter(path)
    writer.append(
        ProbeRoundMetrics(
            session_id="vp-session",
            symbol="HYPE-USD",
            probe_side="sell",
            clip_usd=Decimal("10"),
            status=ProbeStatus.PREORDER_ONLY,
        )
    )
    writer.append(
        ProbeRoundMetrics(
            session_id="vp-session-2",
            symbol="HYPE-USD",
            probe_side="buy",
            clip_usd=Decimal("11"),
            status=ProbeStatus.COMPLETED,
        )
    )
    lines = path.read_text().splitlines()
    assert lines[0].startswith("session_id,symbol,probe_side,clip_usd,started_at,")
    assert len(lines) == 3
    assert "PREORDER_ONLY" in lines[1]
    assert "COMPLETED" in lines[2]
