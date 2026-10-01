from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from entropy_arb.calibration_runtime import HedgeExecutionResult
from entropy_arb.volume_probe import (
    ProbeConfig,
    ProbeRoundMetrics,
    ProbeState,
    ProbeStateMachine,
    ProbeStatus,
    VolumeProbeRoundWriter,
    build_probe_candidate,
    compute_probe_quantity,
    unwind_side,
)
from entropy_arb.volume_probe_runtime import VolumeProbeController
from main import validate_runtime_gates


def _quantity(**overrides) -> Decimal:
    values: dict[str, Any] = dict(
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


def _probe_controller(*, side: str = "sell") -> VolumeProbeController:
    class FakeRisk:
        halted = False
        halt_reason = None
        fill_events = 0

        def check_runtime(self) -> None:
            return None

    class FakeAccumulator:
        residual_exposure = Decimal("0")
        rh_step = Decimal("0.001")

        def reset_if_flat(self) -> None:
            return None

    executor = type(
        "FakeExecutor",
        (),
        {
            "risk": FakeRisk(),
            "accumulator": FakeAccumulator(),
            "has_live_order": False,
            "_terminal_reconcile_pending": False,
        },
    )()
    return VolumeProbeController(
        executor=executor,
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side=side),
        symbol="HYPE-USD",
        session_id="vp-test",
    )


def _hedge(*, side: str, qty: str) -> HedgeExecutionResult:
    return HedgeExecutionResult(
        hedge_side=side,
        filled_qty=Decimal(qty),
        avg_px=Decimal("99.90"),
        fee=Decimal("0.01"),
        realized_slippage_bps=Decimal("1.0"),
        fill_to_hedge_send_ms=2,
        fill_to_rh_fill_ms=8,
    )


def test_partial_build_fill_is_hedged_then_unwind_reverses_actual_qty() -> None:
    controller = _probe_controller(side="sell")
    controller.begin_build(Decimal("0.1"))
    controller.record_hedged_fill(
        arcus_side="SELL",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.04"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="BUY", qty="0.04"),
    )
    assert controller.state is ProbeState.BUILD
    controller.record_hedged_fill(
        arcus_side="SELL",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.06"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="BUY", qty="0.06"),
    )
    assert controller.state is ProbeState.HEDGED
    controller.begin_unwind()
    assert controller.unwind_target_qty == Decimal("0.1")
    candidate = controller.candidate(
        best_bid=Decimal("99.90"), best_ask=Decimal("100.10")
    )
    assert candidate.arcus_side == "BUY"
    assert candidate.hedge_side == "SELL"
    assert candidate.quantity == Decimal("0.1")


def test_unresolved_hedge_halts_without_advancing_to_unwind() -> None:
    controller = _probe_controller()
    controller.begin_build(Decimal("0.1"))
    controller.executor.risk.halted = True
    controller.executor.risk.halt_reason = "RH hedge failure: timeout"
    with pytest.raises(RuntimeError, match="halt"):
        controller.record_hedged_fill(
            arcus_side="SELL",
            arcus_price=Decimal("100"),
            arcus_quantity=Decimal("0.1"),
            arcus_fee=Decimal("0.01"),
            hedge=None,
        )
    assert controller.state is ProbeState.HALTED


def test_active_phase_rejects_fill_from_the_other_direction() -> None:
    controller = _probe_controller(side="sell")
    controller.begin_build(Decimal("0.1"))
    with pytest.raises(RuntimeError, match="does not match"):
        controller.record_hedged_fill(
            arcus_side="BUY",
            arcus_price=Decimal("100"),
            arcus_quantity=Decimal("0.1"),
            arcus_fee=Decimal("0.01"),
            hedge=_hedge(side="SELL", qty="0.1"),
        )
    assert controller.state is ProbeState.HALTED


def test_subminimum_arcus_fill_stays_pending_without_advancing_to_unwind() -> None:
    class FakeRisk:
        fill_events = 0
        halted = False
        halt_reason = None

    class FakeExecutor:
        def __init__(self) -> None:
            self.risk = FakeRisk()
            self.accumulator = SimpleNamespace(residual_exposure=Decimal("0"))
            self.last_hedge_result = None
            self._arcus_fee_records: dict[str, Any] = {}

        def _context_for_fill(self, fill: Any) -> object:
            return object()

        async def on_fill(self, fill: Any) -> None:
            self.risk.fill_events += 1
            self.accumulator.residual_exposure = fill.quantity

    executor = FakeExecutor()
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
    )
    controller.begin_build(Decimal("0.1"))
    asyncio.run(
        controller.on_fill(
            SimpleNamespace(
                trade_id="trade-1",
                is_snapshot=False,
                side="SELL",
                price=Decimal("100"),
                quantity=Decimal("0.005"),
                fee=Decimal("0.01"),
            )
        )
    )
    assert controller.state is ProbeState.BUILD
    assert controller.build_base_qty == Decimal("0.005")
    assert controller.status is None


def test_reprice_requires_cancel_terminal_barrier() -> None:
    controller = _probe_controller()
    controller.begin_build(Decimal("0.1"))
    cast(Any, controller.executor).has_live_order = True
    assert controller.reprice_allowed is False
    cast(Any, controller.executor).has_live_order = False
    cast(Any, controller.executor)._terminal_reconcile_pending = True
    assert controller.reprice_allowed is False
    cast(Any, controller.executor)._terminal_reconcile_pending = False
    assert controller.reprice_allowed is True


def test_flat_final_state_is_completed_and_nonflat_requires_reconciliation() -> None:
    completed = _probe_controller()
    completed.begin_build(Decimal("0.1"))
    completed.record_hedged_fill(
        arcus_side="SELL",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.1"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="BUY", qty="0.1"),
    )
    completed.begin_unwind()
    completed.record_hedged_fill(
        arcus_side="BUY",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.1"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="SELL", qty="0.1"),
    )
    completed.finish_unwind()
    assert (
        completed.finalize_positions(
            arcus_position=Decimal("0"),
            rh_position=Decimal("0"),
            arcus_open_orders=(),
            rh_open_orders=(),
        )
        is ProbeStatus.COMPLETED
    )

    nonflat = _probe_controller()
    nonflat.begin_build(Decimal("0.1"))
    assert (
        nonflat.finalize_positions(
            arcus_position=Decimal("0.1"),
            rh_position=Decimal("-0.1"),
            arcus_open_orders=(),
            rh_open_orders=(),
        )
        is ProbeStatus.RECONCILIATION_REQUIRED
    )


def test_one_shot_run_unwinds_and_never_overlaps_orders() -> None:
    class FakeBook:
        def best_bid(self) -> Decimal:
            return Decimal("99.90")

        def best_ask(self) -> Decimal:
            return Decimal("100.10")

    class FakeExecutor:
        def __init__(self) -> None:
            self.risk = SimpleNamespace(
                halted=False,
                halt_reason=None,
                fill_events=0,
                check_runtime=lambda: None,
            )
            self.accumulator = SimpleNamespace(residual_exposure=Decimal("0"))
            self.arcus = SimpleNamespace(book=FakeBook())
            self.hedge = SimpleNamespace(book=FakeBook())
            self.pnl = SimpleNamespace(actual_usd=Decimal("0"))
            self.has_live_order = False
            self._terminal_reconcile_pending = False
            self.orders: list[Any] = []
            self.controller: VolumeProbeController | None = None

        def market_health(self):
            return SimpleNamespace(can_quote=True)

        async def place_quote(self, candidate) -> None:
            assert not self.has_live_order
            self.has_live_order = True
            self.orders.append(candidate)
            assert self.controller is not None
            self.controller.record_hedged_fill(
                arcus_side=candidate.side,
                arcus_price=candidate.price,
                arcus_quantity=candidate.quantity,
                arcus_fee=Decimal("0.01"),
                hedge=_hedge(
                    side=candidate.hedge_side,
                    qty=str(candidate.quantity),
                ),
            )
            self.has_live_order = False

        async def cancel_outstanding(self) -> None:
            self.has_live_order = False

        async def reconcile(self) -> None:
            self._terminal_reconcile_pending = False

    executor = FakeExecutor()
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
        session_id="vp-run",
    )
    executor.controller = controller

    async def final_state_reader():
        return Decimal("0"), Decimal("0"), [], []

    status = asyncio.run(
        controller.run(
            asyncio.Event(),
            quantity=Decimal("0.1"),
            final_state_reader=final_state_reader,
        )
    )
    assert status is ProbeStatus.COMPLETED
    assert [order.side for order in executor.orders] == ["SELL", "BUY"]
    assert [order.quantity for order in executor.orders] == [
        Decimal("0.1"),
        Decimal("0.1"),
    ]


def test_reprice_cancels_and_reconciles_before_reposting_remaining() -> None:
    class FakeBook:
        def best_bid(self) -> Decimal:
            return Decimal("99.90")

        def best_ask(self) -> Decimal:
            return Decimal("100.10")

    class FakeRisk:
        halted = False
        halt_reason = None
        fill_events = 0

        def check_runtime(self) -> None:
            return None

    class FakeExecutor:
        def __init__(self) -> None:
            self.risk = FakeRisk()
            self.accumulator = SimpleNamespace(residual_exposure=Decimal("0"))
            self.arcus = SimpleNamespace(book=FakeBook())
            self.hedge = SimpleNamespace(book=FakeBook())
            self.pnl = SimpleNamespace(actual_usd=Decimal("0"))
            self.has_live_order = False
            self._terminal_reconcile_pending = False
            self.orders: list[Any] = []
            self.cancel_calls = 0
            self.controller: VolumeProbeController | None = None

        def market_health(self):
            return SimpleNamespace(can_quote=True)

        async def place_quote(self, candidate) -> None:
            assert not self.has_live_order
            self.has_live_order = True
            self.orders.append(candidate)
            if len(self.orders) >= 2:
                assert self.controller is not None
                self.controller.record_hedged_fill(
                    arcus_side=candidate.side,
                    arcus_price=candidate.price,
                    arcus_quantity=candidate.quantity,
                    arcus_fee=Decimal("0.01"),
                    hedge=_hedge(
                        side=candidate.hedge_side,
                        qty=str(candidate.quantity),
                    ),
                )
                self.has_live_order = False

        async def cancel_outstanding(self) -> None:
            self.cancel_calls += 1
            self.has_live_order = False
            self._terminal_reconcile_pending = True

        async def reconcile(self) -> None:
            self._terminal_reconcile_pending = False

    executor = FakeExecutor()
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(
            clip_usd=Decimal("10"),
            probe_side="sell",
            reprice_sec=0.001,
        ),
        symbol="HYPE-USD",
        session_id="vp-reprice",
    )
    executor.controller = controller

    async def final_state_reader():
        return Decimal("0"), Decimal("0"), [], []

    status = asyncio.run(
        controller.run(
            asyncio.Event(),
            quantity=Decimal("0.1"),
            final_state_reader=final_state_reader,
        )
    )
    assert status is ProbeStatus.COMPLETED
    assert executor.cancel_calls == 1
    assert controller.metrics.build_reprices == 1
    assert [order.side for order in executor.orders] == ["SELL", "SELL", "BUY"]


def test_volume_probe_runtime_gates_are_independent_from_b0() -> None:
    assert (
        validate_runtime_gates(False, False, True, volume_probe=True) == "volume-probe"
    )
    with pytest.raises(ValueError, match="--confirm-mainnet"):
        validate_runtime_gates(False, False, False, volume_probe=True)
    with pytest.raises(ValueError, match="mutually exclusive"):
        validate_runtime_gates(True, False, True, volume_probe=True)
    with pytest.raises(ValueError, match="mutually exclusive"):
        validate_runtime_gates(False, True, True, volume_probe=True)


def test_readme_documents_preflight_and_approved_volume_probe_commands() -> None:
    readme = Path("README.md").read_text()
    assert "--volume-probe" in readme
    assert "--confirm-mainnet" in readme
    assert "--probe-side sell" in readme
    assert "--probe-clip-usd 10" in readme
    assert "--approve-first-order" in readme
    for deferred in (
        "multiple clip target builder",
        "funding direction",
        "hold",
        "repeated rounds",
        "automatic market selection",
    ):
        assert deferred in readme
