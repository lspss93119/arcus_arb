from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from entropy_arb.arcus_execution import (
    ArcusAccountRest,
    ArcusAccountState,
    ArcusFeeTier,
    ArcusMakerClient,
    ArcusOrderUpdate,
    ArcusRateLimited,
    ArcusUserFill,
)
from entropy_arb.calibration import FillAccumulator, QuoteCandidate, SessionLimits
from entropy_arb.calibration_runtime import (
    CalibrationController,
    HedgeExecutionResult,
    ReconciliationResult,
)
from entropy_arb.storage import MarketHistoryStore
from entropy_arb.volume_probe import (
    ProbeConfig,
    ProbeRoundMetrics,
    ProbeState,
    ProbeStateMachine,
    ProbeStatus,
    VolumeProbeRoundWriter,
    build_probe_candidate,
    common_executable_step,
    compute_probe_quantity,
    probe_tolerance,
    unwind_side,
)
from entropy_arb.volume_probe_runtime import VolumeProbeController
from main import validate_runtime_gates


def test_arcus_account_rest_exposes_429_retry_after_without_request_details() -> None:
    class Response:
        status = 429
        headers = {"Retry-After": "2.5"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def json(self):
            raise AssertionError("429 response body must not be required")

        def raise_for_status(self):
            raise AssertionError("429 must be handled before raise_for_status")

    class Session:
        def get(self, *_args, **_kwargs):
            return Response()

    async def exercise() -> None:
        rest = ArcusAccountRest(cast(Any, Session()), rest_url="https://secret.invalid")
        with pytest.raises(ArcusRateLimited) as caught:
            await rest.get("/v1/fills", {"address": "secret-address"})
        assert caught.value.retry_after == 2.5
        assert "secret" not in str(caught.value)

    asyncio.run(exercise())


def test_arcus_account_rest_429_without_retry_after_is_explicit() -> None:
    class Response:
        status = 429
        headers: dict[str, str] = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def json(self):
            return {}

        def raise_for_status(self):
            raise AssertionError("429 must be handled before raise_for_status")

    class Session:
        def get(self, *_args, **_kwargs):
            return Response()

    async def exercise() -> None:
        rest = ArcusAccountRest(cast(Any, Session()))
        with pytest.raises(ArcusRateLimited) as caught:
            await rest.get("/v1/openOrders", {})
        assert caught.value.retry_after is None

    asyncio.run(exercise())


def test_arcus_account_rest_non_429_keeps_http_failure_behavior() -> None:
    class Response:
        status = 503
        headers: dict[str, str] = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def json(self):
            return {}

        def raise_for_status(self):
            raise RuntimeError("http 503")

    class Session:
        def get(self, *_args, **_kwargs):
            return Response()

    async def exercise() -> None:
        rest = ArcusAccountRest(cast(Any, Session()))
        with pytest.raises(RuntimeError, match="http 503"):
            await rest.get("/v1/fills", {})

    asyncio.run(exercise())


def test_reconcile_rate_limit_result_keeps_pending_and_uses_bounded_backoff(
    tmp_path, monkeypatch
) -> None:
    stack = _reconciliation_probe_stack(tmp_path, with_probe=False)
    executor = stack.executor
    executor._terminal_reconcile_pending = True
    executor._order_contexts = {"client:pending": object()}
    clock = [100.0]
    monkeypatch.setattr(
        "entropy_arb.calibration_runtime.time.monotonic", lambda: clock[0]
    )

    class RateLimitedRest:
        def __init__(self) -> None:
            self.open_orders_calls = 0
            self.fills_calls = 0

        async def open_orders(self, *args: Any, **kwargs: Any):
            self.open_orders_calls += 1
            if self.open_orders_calls < 3:
                raise ArcusRateLimited()
            return []

        async def fills(self, *args: Any, **kwargs: Any):
            self.fills_calls += 1
            return []

    rest = RateLimitedRest()
    executor.account_rest = cast(ArcusAccountRest, rest)

    async def exercise() -> None:
        first = await executor.reconcile()
        assert isinstance(first, ReconciliationResult)
        assert first.status == "rate_limited"
        assert first.retry_after == 1.0
        assert executor._terminal_reconcile_pending is True
        assert rest.open_orders_calls == 1
        assert rest.fills_calls == 0
        assert executor.risk.halted is False
        assert executor.step is not None
        assert await executor.step() is None
        assert rest.open_orders_calls == 1
        clock[0] += 1.0

        second = await executor.reconcile()
        assert second.status == "rate_limited"
        assert second.retry_after == 2.0
        assert rest.open_orders_calls == 2
        clock[0] += 2.0

        third = await executor.reconcile()
        assert third.status == "success"
        assert rest.open_orders_calls == 3
        assert rest.fills_calls == 1
        assert executor._terminal_reconcile_pending is True

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


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
        ({"rh_step": Decimal("0")}, "step sizes"),
    ],
)
def test_quantity_validation_rejects_unhedgeable_or_out_of_bounds_clip(
    overrides: dict[str, Decimal], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _quantity(**overrides)


@pytest.mark.parametrize(
    "arcus_step, rh_step, expected",
    [
        (Decimal("0.000001"), Decimal("0.001"), Decimal("0.001")),
        (Decimal("0.01"), Decimal("0.001"), Decimal("0.01")),
        (Decimal("0.002"), Decimal("0.003"), Decimal("0.006")),
    ],
)
def test_common_executable_step_is_the_smallest_shared_decimal_grid(
    arcus_step: Decimal, rh_step: Decimal, expected: Decimal
) -> None:
    assert common_executable_step(arcus_step=arcus_step, rh_step=rh_step) == expected


def test_quantity_rounds_down_to_common_grid_and_stays_below_raw_quantity() -> None:
    quantity = _quantity(
        clip_usd=Decimal("18.7643"),
        arcus_bid=Decimal("99.90"),
        arcus_ask=Decimal("100.10"),
        arcus_step=Decimal("0.000001"),
        arcus_min_size=Decimal("0.001"),
        rh_step=Decimal("0.001"),
        rh_min_size=Decimal("0.1"),
    )
    raw_quantity = Decimal("18.7643") / Decimal("100")
    assert quantity == Decimal("0.187")
    assert quantity <= raw_quantity
    assert quantity % Decimal("0.000001") == 0
    assert quantity % Decimal("0.001") == 0


def test_rh_buy_min_quote_uses_conservative_best_ask() -> None:
    with pytest.raises(
        ValueError,
        match="probe clip rounds below RH minimum quote notional; increase --probe-clip-usd",
    ):
        _quantity(
            clip_usd=Decimal("9.99"),
            rh_bid=Decimal("99"),
            rh_ask=Decimal("100"),
            rh_min_quote=Decimal("10"),
        )


def test_rh_sell_min_quote_uses_conservative_best_bid() -> None:
    with pytest.raises(
        ValueError,
        match="probe clip rounds below RH minimum quote notional; increase --probe-clip-usd",
    ):
        _quantity(
            clip_usd=Decimal("9.99"),
            probe_side="buy",
            rh_bid=Decimal("100"),
            rh_ask=Decimal("1000"),
            rh_min_quote=Decimal("10"),
        )


def test_rh_min_quote_passes_when_hedge_reference_notional_is_large_enough() -> None:
    quantity = _quantity(
        clip_usd=Decimal("10.10"),
        rh_bid=Decimal("100"),
        rh_ask=Decimal("100"),
        rh_min_quote=Decimal("10"),
    )
    assert quantity == Decimal("0.101")


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


def _real_accumulator_controller(
    *, side: str = "sell", tolerance: Decimal = Decimal("0")
) -> VolumeProbeController:
    class FakeRisk:
        halted = False
        halt_reason = None
        fill_events = 0

    executor = SimpleNamespace(
        risk=FakeRisk(),
        accumulator=FillAccumulator(
            rh_min_qty=Decimal("0.01"), rh_step=Decimal("0.01")
        ),
        has_live_order=False,
        _terminal_reconcile_pending=False,
    )
    return VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side=side),
        symbol="HYPE-USD",
        tolerance=tolerance,
    )


def test_volume_probe_session_id_matches_executor() -> None:
    seed = _real_accumulator_controller()
    executor = cast(Any, seed.executor)
    executor.session_id = "vp-shared"

    matched = VolumeProbeController(
        executor=executor,
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
        session_id="vp-shared",
    )
    assert matched.session_id == executor.session_id == matched.metrics.session_id

    with pytest.raises(ValueError, match="session IDs must match"):
        VolumeProbeController(
            executor=executor,
            config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
            symbol="HYPE-USD",
            session_id="vp-other",
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


def _reconciliation_probe_stack(tmp_path, *, with_probe: bool = True):
    class FakeBook:
        ready = True
        sequence_health = "OK"

        def best_bid(self) -> Decimal:
            return Decimal("99.90")

        def best_ask(self) -> Decimal:
            return Decimal("100.10")

        def is_fresh(self, max_age_sec: float) -> bool:
            return True

    class FakeFeed:
        healthy = True
        required_channels_healthy = True

        def __init__(self) -> None:
            self.latest_orders: dict[str, Any] = {}

    class FakeRest:
        def __init__(self) -> None:
            self.fills_data: list[ArcusUserFill] = []

        async def open_orders(self, *args: Any, **kwargs: Any):
            return []

        async def fills(self, *args: Any, **kwargs: Any):
            return list(self.fills_data)

    class FakeMaker:
        def __init__(self) -> None:
            self.credentials = SimpleNamespace(
                account_address="0x" + "11" * 20,
                account_index=0,
            )
            self.quantities: list[Decimal] = []

        async def place_alo(self, **kwargs: Any):
            self.quantities.append(Decimal(str(kwargs["quantity"])))
            return SimpleNamespace(
                order_id=f"order-{len(self.quantities)}",
                client_id=kwargs["client_id"],
            )

        async def cancel_calibration_order(self, **kwargs: Any):
            return {"status": 202}

    class FakeHedge:
        def __init__(self) -> None:
            self.book = FakeBook()
            self.size_decimals = 2
            self.min_base = 0.01
            self.fee_bps = 1.0
            self.hedges: list[dict[str, Any]] = []

        async def send_taker(self, **kwargs: Any):
            self.hedges.append(kwargs)
            return {
                "status": "filled",
                "filled_base": kwargs["qty"],
                "avg_px": 100.00,
                "fee": 0.01,
                "order_send_ts_ms": 2,
                "ack_ts_ms": 3,
                "fill_receive_ts_ms": 4,
            }

    feed = FakeFeed()
    rest = FakeRest()
    maker = FakeMaker()
    hedge = FakeHedge()
    executor = CalibrationController(
        arcus=SimpleNamespace(
            book=FakeBook(),
            latest_attributes=SimpleNamespace(is_outside_rth=False),
        ),
        hedge=hedge,
        maker=cast(ArcusMakerClient, maker),
        account_feed=feed,
        account_rest=cast(ArcusAccountRest, rest),
        account_state=ArcusAccountState(startup_watermark_us=0),
        metadata=SimpleNamespace(
            market_id=33,
            symbol="HYPE-USD",
            tick_size="0.01",
            step_size="0.01",
            status="ONLINE",
        ),
        fee_tier=ArcusFeeTier(1, "base", 20, 100),
        strategy=SimpleNamespace(
            fixed_center_bps=0.0,
            state=lambda: SimpleNamespace(center_bps=0.0),
        ),
        store=MarketHistoryStore(tmp_path / "reconciliation.sqlite"),
        allow_first_order=True,
        staleness_sec=10.0,
        rh_fee_bps=Decimal("1.0"),
        session_id="vp-reconciliation-test",
        session_limits=SessionLimits(max_order_qty=Decimal("0.10")),
    )
    probe = None
    if with_probe:
        probe = VolumeProbeController(
            executor=executor,
            config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
            symbol="HYPE-USD",
            session_id="vp-reconciliation-test",
        )
    return SimpleNamespace(
        executor=executor,
        feed=feed,
        rest=rest,
        maker=maker,
        hedge=hedge,
        probe=probe,
    )


def test_reconcile_routes_recovered_build_and_unwind_fills_once(tmp_path) -> None:
    stack = _reconciliation_probe_stack(tmp_path)
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    def current_fill(*, trade_id: str, side: str, quantity: str) -> ArcusUserFill:
        return ArcusUserFill(
            trade_id=trade_id,
            order_id=executor.lifecycle.order_id or "",
            client_id=executor.lifecycle.client_id,
            market_id=33,
            market_display_name="HYPE-USD",
            side=side,
            price=Decimal("100.10") if side == "SELL" else Decimal("99.90"),
            quantity=Decimal(quantity),
            fee=Decimal("0.01"),
            created_at_us=1_000 + len(stack.hedge.hedges) + 1,
            sequence_number=len(stack.hedge.hedges) + 1,
            is_snapshot=False,
            local_receive_ts_ms=10,
            local_receive_monotonic_ns=10,
        )

    async def mark_terminal() -> None:
        candidate = executor.current_candidate
        assert candidate is not None
        await executor.on_order(
            ArcusOrderUpdate(
                order_id=executor.lifecycle.order_id or "",
                client_id=executor.lifecycle.client_id,
                market_id=33,
                market_display_name="HYPE-USD",
                side=candidate.side,
                status="CANCELED",
                state="CANCELED",
                price=candidate.price,
                original_size=candidate.quantity,
                remaining_size=candidate.quantity,
                avg_fill_price=None,
                created_at_us=1_000,
                updated_at_us=1_001,
                sequence_number=len(stack.hedge.hedges) + 1,
                is_snapshot=False,
            )
        )

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        await probe.place_next_quote()
        assert stack.maker.quantities == [Decimal("0.10")]

        first = current_fill(trade_id="build-1", side="SELL", quantity="0.04")
        await mark_terminal()
        stack.rest.fills_data = [first]
        await executor.reconcile()
        assert len(stack.hedge.hedges) == 1
        assert Decimal(str(stack.hedge.hedges[0]["qty"])) == Decimal("0.04")
        assert probe.build_base_qty == Decimal("0.04")
        assert probe.state is ProbeState.BUILD

        await probe.on_fill(first)
        assert len(stack.hedge.hedges) == 1
        assert probe.build_base_qty == Decimal("0.04")

        await probe.place_next_quote()
        assert stack.maker.quantities == [Decimal("0.10"), Decimal("0.06")]

        second = current_fill(trade_id="build-2", side="SELL", quantity="0.06")
        await mark_terminal()
        stack.rest.fills_data = [first, second]
        await executor.reconcile()
        assert len(stack.hedge.hedges) == 2
        assert probe.build_base_qty == Decimal("0.10")
        assert probe.state is ProbeState.HEDGED

        probe.begin_unwind()
        await probe.place_next_quote()
        assert stack.maker.quantities == [
            Decimal("0.10"),
            Decimal("0.06"),
            Decimal("0.10"),
        ]

        unwind = current_fill(trade_id="unwind-1", side="BUY", quantity="0.10")
        await mark_terminal()
        stack.rest.fills_data = [first, second, unwind]
        await executor.reconcile()
        assert len(stack.hedge.hedges) == 3
        assert probe.unwind_base_qty == Decimal("0.10")
        assert probe.state is ProbeState.UNWIND

        await probe.on_fill(unwind)
        assert len(stack.hedge.hedges) == 3
        assert probe.unwind_base_qty == Decimal("0.10")

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def test_reconcile_fills_rate_limit_waits_before_retrying_fills(
    tmp_path, monkeypatch
) -> None:
    stack = _reconciliation_probe_stack(tmp_path, with_probe=False)
    executor = stack.executor
    executor._terminal_reconcile_pending = True
    executor._order_contexts = {"client:pending": object()}
    clock = [100.0]
    monkeypatch.setattr(
        "entropy_arb.calibration_runtime.time.monotonic", lambda: clock[0]
    )

    class RateLimitedFillsRest:
        def __init__(self) -> None:
            self.open_orders_calls = 0
            self.fills_calls = 0

        async def open_orders(self, *args: Any, **kwargs: Any):
            self.open_orders_calls += 1
            return []

        async def fills(self, *args: Any, **kwargs: Any):
            self.fills_calls += 1
            if self.fills_calls == 1:
                raise ArcusRateLimited()
            return []

    rest = RateLimitedFillsRest()
    executor.account_rest = cast(ArcusAccountRest, rest)

    async def exercise() -> None:
        first = await executor.reconcile()
        assert first.status == "rate_limited"
        assert rest.open_orders_calls == 1
        assert rest.fills_calls == 1

        gated = await executor.reconcile()
        assert gated.status == "rate_limited"
        assert rest.open_orders_calls == 1
        assert rest.fills_calls == 1

        clock[0] += 1.0
        success = await executor.reconcile()
        assert success.status == "success"
        assert rest.open_orders_calls == 2
        assert rest.fills_calls == 2

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def test_b0_reconcile_without_probe_observer_keeps_normal_fill_path(tmp_path) -> None:
    stack = _reconciliation_probe_stack(tmp_path, with_probe=False)
    executor = stack.executor
    candidate = QuoteCandidate(
        side="SELL",
        price=Decimal("100.10"),
        quantity=Decimal("0.01"),
        hedge_side="BUY",
        hedge_price=Decimal("100.10"),
        fair_price=Decimal("100.10"),
        expected_edge_bps=Decimal("4.2"),
        expected_usd=Decimal("0.0042"),
    )

    async def exercise() -> None:
        await executor.place_quote(candidate)
        fill = ArcusUserFill(
            trade_id="b0-reconcile-1",
            order_id=executor.lifecycle.order_id or "",
            client_id=executor.lifecycle.client_id,
            market_id=33,
            market_display_name="HYPE-USD",
            side="SELL",
            price=Decimal("100.10"),
            quantity=Decimal("0.01"),
            fee=Decimal("0.01"),
            created_at_us=1_001,
            sequence_number=1,
            is_snapshot=False,
        )
        stack.rest.fills_data = [fill]
        await executor.reconcile()
        assert executor.risk.fill_events == 1
        assert len(stack.hedge.hedges) == 1
        assert executor.accumulator.residual_exposure == Decimal("0")

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


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


def test_real_fill_accumulator_resets_before_opposite_unwind_fill() -> None:
    controller = _real_accumulator_controller()
    controller.begin_build(Decimal("0.1"))
    build_instruction = controller.executor.accumulator.add_fill(
        side="SELL", quantity=Decimal("0.1")
    )
    assert build_instruction is not None
    controller.record_hedged_fill(
        arcus_side="SELL",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.1"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="BUY", qty="0.1"),
    )
    assert controller.executor.accumulator.side == "SELL"
    assert controller.executor.accumulator.residual_exposure == Decimal("0")

    controller.begin_unwind()

    assert controller.executor.accumulator.side is None
    unwind_instruction = controller.executor.accumulator.add_fill(
        side="BUY", quantity=Decimal("0.1")
    )
    assert unwind_instruction is not None
    controller.record_hedged_fill(
        arcus_side="BUY",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.1"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="SELL", qty="0.1"),
    )
    assert controller.unwind_base_qty == Decimal("0.1")


def test_nonzero_accumulator_residual_blocks_unwind() -> None:
    controller = _real_accumulator_controller()
    controller.begin_build(Decimal("0.1"))
    controller.record_hedged_fill(
        arcus_side="SELL",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.1"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="BUY", qty="0.1"),
    )
    controller.executor.accumulator.unhedged_qty = Decimal("0.001")

    with pytest.raises(RuntimeError, match="residual"):
        controller.begin_unwind()

    assert controller.state is ProbeState.HALTED


def test_one_full_step_short_does_not_complete_build() -> None:
    controller = _real_accumulator_controller(
        tolerance=probe_tolerance(arcus_step=Decimal("0.01"), rh_step=Decimal("0.01"))
    )
    controller.begin_build(Decimal("0.1"))
    controller.record_hedged_fill(
        arcus_side="SELL",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.09"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="BUY", qty="0.09"),
    )
    assert controller.state is ProbeState.BUILD


def test_one_full_step_final_residual_is_not_completed() -> None:
    controller = _real_accumulator_controller(
        tolerance=probe_tolerance(arcus_step=Decimal("0.01"), rh_step=Decimal("0.01"))
    )
    controller.begin_build(Decimal("0.1"))
    controller.record_hedged_fill(
        arcus_side="SELL",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.1"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="BUY", qty="0.1"),
    )
    controller.begin_unwind()
    controller.record_hedged_fill(
        arcus_side="BUY",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.1"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="SELL", qty="0.1"),
    )
    controller.finish_unwind()

    assert (
        controller.finalize_positions(
            arcus_position=Decimal("0.01"),
            rh_position=Decimal("-0.01"),
            arcus_open_orders=(),
            rh_open_orders=(),
        )
        is ProbeStatus.RECONCILIATION_REQUIRED
    )


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


def test_persistent_terminal_429_has_one_cancel_and_finite_reconciliation(
    monkeypatch,
) -> None:
    class FakeRisk:
        halted = False
        halt_reason = None

        def halt(self, reason: str) -> None:
            self.halted = True
            self.halt_reason = reason

    class FakeExecutor:
        def __init__(self) -> None:
            self.risk = FakeRisk()
            self.has_live_order = True
            self._terminal_reconcile_pending = True
            self.cancel_calls = 0
            self.reconcile_calls = 0
            self.orders: list[Any] = []
            self.hedges: list[Any] = []
            self.reconciliation_required_calls = 0

        async def cancel_outstanding(self) -> None:
            self.cancel_calls += 1
            self.has_live_order = False
            self._terminal_reconcile_pending = True

        async def reconcile(self) -> ReconciliationResult:
            self.reconcile_calls += 1
            return ReconciliationResult(status="rate_limited", retry_after=1.0)

        def mark_reconciliation_required(self, reason: str) -> None:
            self.reconciliation_required_calls += 1
            self.risk.halt(reason)

    clock = [0.0]
    delays: list[float] = []
    monkeypatch.setattr(
        "entropy_arb.volume_probe_runtime.time.monotonic", lambda: clock[0]
    )

    async def advance_sleep(delay: float) -> None:
        delays.append(delay)
        clock[0] += delay

    monkeypatch.setattr("entropy_arb.volume_probe_runtime.asyncio.sleep", advance_sleep)
    executor = FakeExecutor()
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
    )

    result = asyncio.run(controller._cancel_terminal())

    assert result is False
    assert controller.status is ProbeStatus.RECONCILIATION_REQUIRED
    assert controller.state is ProbeState.RECONCILIATION_REQUIRED
    assert executor.cancel_calls == 1
    assert executor.reconcile_calls <= 5
    assert delays[:4] == [1.0, 2.0, 4.0, 5.0]
    assert max(delays) <= 5.0
    assert executor.reconciliation_required_calls == 1
    assert executor.orders == []
    assert executor.hedges == []


def test_final_state_rate_limit_retries_then_allows_flat_completion(
    monkeypatch,
) -> None:
    controller = _probe_controller()
    clock = [0.0]
    monkeypatch.setattr(
        "entropy_arb.volume_probe_runtime.time.monotonic", lambda: clock[0]
    )

    async def advance_sleep(delay: float) -> None:
        clock[0] += delay

    monkeypatch.setattr("entropy_arb.volume_probe_runtime.asyncio.sleep", advance_sleep)
    calls = 0

    async def final_state_reader():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ArcusRateLimited(2.0)
        return Decimal("0"), Decimal("0"), [], []

    final = asyncio.run(controller._read_final_state(final_state_reader))

    assert final is not None
    assert calls == 2
    assert (
        controller.finalize_positions(
            arcus_position=final[0],
            rh_position=final[1],
            arcus_open_orders=final[2],
            rh_open_orders=final[3],
        )
        is ProbeStatus.COMPLETED
    )


def test_persistent_final_state_429_never_completes(monkeypatch) -> None:
    controller = _probe_controller()
    clock = [0.0]
    monkeypatch.setattr(
        "entropy_arb.volume_probe_runtime.time.monotonic", lambda: clock[0]
    )

    async def advance_sleep(delay: float) -> None:
        clock[0] += delay

    monkeypatch.setattr("entropy_arb.volume_probe_runtime.asyncio.sleep", advance_sleep)
    calls = 0

    async def final_state_reader():
        nonlocal calls
        calls += 1
        raise ArcusRateLimited()

    final = asyncio.run(controller._read_final_state(final_state_reader))

    assert final is None
    assert calls <= 5
    assert controller.status is ProbeStatus.RECONCILIATION_REQUIRED
    assert controller.state is ProbeState.RECONCILIATION_REQUIRED


def test_shutdown_skips_second_reconciliation_after_probe_failure(tmp_path) -> None:
    stack = _reconciliation_probe_stack(tmp_path, with_probe=False)
    executor = stack.executor
    executor._terminal_reconcile_pending = True
    executor._order_contexts = {"client:pending": object()}

    class NoRetryRest:
        calls = 0

        async def open_orders(self, *args: Any, **kwargs: Any):
            self.calls += 1
            raise AssertionError("shutdown started a duplicate reconciliation")

        async def fills(self, *args: Any, **kwargs: Any):
            self.calls += 1
            raise AssertionError("shutdown started a duplicate reconciliation")

    rest = NoRetryRest()
    executor.account_rest = cast(ArcusAccountRest, rest)
    executor.mark_reconciliation_required("probe terminal reconciliation deadline")

    try:
        asyncio.run(executor.shutdown())
        assert rest.calls == 0
    finally:
        executor.telemetry.store.close()


def test_shutdown_persistent_429_uses_bounded_backoff(tmp_path, monkeypatch) -> None:
    stack = _reconciliation_probe_stack(tmp_path, with_probe=False)
    executor = stack.executor
    executor._terminal_reconcile_pending = True
    executor._order_contexts = {"client:pending": object()}
    clock = [0.0]
    delays: list[float] = []
    monkeypatch.setattr(
        "entropy_arb.calibration_runtime.time.monotonic", lambda: clock[0]
    )

    async def advance_sleep(delay: float) -> None:
        delays.append(delay)
        clock[0] += delay

    monkeypatch.setattr("entropy_arb.calibration_runtime.asyncio.sleep", advance_sleep)

    class Persistent429Rest:
        calls = 0

        async def open_orders(self, *args: Any, **kwargs: Any):
            self.calls += 1
            raise ArcusRateLimited()

        async def fills(self, *args: Any, **kwargs: Any):
            self.calls += 1
            raise ArcusRateLimited()

    rest = Persistent429Rest()
    executor.account_rest = cast(ArcusAccountRest, rest)

    try:
        asyncio.run(executor.shutdown())
        assert rest.calls <= 5
        assert delays[:4] == [1.0, 2.0, 4.0, 5.0]
        assert max(delays) <= 5.0
        assert executor.reconciliation_required is True
    finally:
        executor.telemetry.store.close()


def test_terminal_reconcile_barrier_precedes_unwind() -> None:
    class FakeBook:
        def best_bid(self) -> Decimal:
            return Decimal("99.90")

        def best_ask(self) -> Decimal:
            return Decimal("100.10")

    class FakeRisk:
        def __init__(self) -> None:
            self.halted = False
            self.halt_reason: str | None = None
            self.fill_events = 0
            self.checks = 0

        def check_runtime(self) -> None:
            self.checks += 1
            if self.checks >= 4:
                self.halted = True
                self.halt_reason = "test runtime timeout"

        def halt(self, reason: str) -> None:
            self.halted = True
            self.halt_reason = reason

    class FakeExecutor:
        def __init__(self) -> None:
            self.risk = FakeRisk()
            self.accumulator = FillAccumulator(
                rh_min_qty=Decimal("0.01"), rh_step=Decimal("0.01")
            )
            self.arcus = SimpleNamespace(book=FakeBook())
            self.hedge = SimpleNamespace(book=FakeBook())
            self.pnl = SimpleNamespace(actual_usd=Decimal("0"))
            self.has_live_order = False
            self._terminal_reconcile_pending = False
            self.orders: list[Any] = []
            self.reconcile_calls = 0
            self.controller: VolumeProbeController | None = None

        def market_health(self):
            return SimpleNamespace(can_quote=True)

        async def place_quote(self, candidate) -> None:
            assert not self.has_live_order
            self.has_live_order = True
            self.orders.append(candidate)
            assert self.controller is not None
            instruction = self.accumulator.add_fill(
                side=candidate.side, quantity=candidate.quantity
            )
            assert instruction is not None
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
            self._terminal_reconcile_pending = True

        async def cancel_outstanding(self) -> None:
            self.has_live_order = False

        async def reconcile(self) -> None:
            self.reconcile_calls += 1
            self._terminal_reconcile_pending = False

    executor = FakeExecutor()
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
        tolerance=probe_tolerance(arcus_step=Decimal("0.01"), rh_step=Decimal("0.01")),
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
    assert executor.reconcile_calls >= 2
    assert [order.side for order in executor.orders] == ["SELL", "BUY"]


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
