from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from entropy_arb.arcus_execution import (
    ArcusAccountRest,
    ArcusAccountState,
    ArcusAloWouldCross,
    ArcusFeeTier,
    ArcusMakerClient,
    ArcusOrderRejected,
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
from entropy_arb.volume_probe_runtime import (
    VolumeProbeController,
    format_volume_probe_health,
)
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


def test_arcus_account_rest_fills_passes_microsecond_from_filter() -> None:
    class Response:
        status = 200
        headers: dict[str, str] = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def json(self):
            return {"fills": []}

        def raise_for_status(self):
            return None

    class Session:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def get(self, url, *, params, timeout):
            del timeout
            self.calls.append((url, params))
            return Response()

    async def exercise() -> None:
        session = Session()
        rest = ArcusAccountRest(cast(Any, session), rest_url="https://arcus.test")
        assert (
            await rest.fills(
                "0x" + "11" * 20,
                "HYPE-USD",
                0,
                from_us=1_791_005_438_292_751,
            )
            == []
        )
        assert session.calls == [
            (
                "https://arcus.test/v1/fills",
                {
                    "address": "0x" + "11" * 20,
                    "market": "HYPE-USD",
                    "accountIndex": 0,
                    "limit": 1000,
                    "from": 1_791_005_438_292_751,
                },
            )
        ]

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


def _reconciliation_probe_stack(
    tmp_path,
    *,
    with_probe: bool = True,
    place_exception: Exception | None = None,
    hedge_result: dict[str, Any] | None = None,
    hedge_exception: Exception | None = None,
    rh_bid: Decimal = Decimal("99.90"),
    rh_ask: Decimal = Decimal("100.10"),
    rh_size_decimals: int = 2,
    rh_min_base: Decimal = Decimal("0.01"),
    rh_min_quote: Decimal | None = None,
    max_order_qty: Decimal = Decimal("0.10"),
    startup_watermark_us: int = 0,
    client_prefix: str = "b0-",
):
    class FakeBook:
        ready = True
        sequence_health = "OK"

        def __init__(self, bid: Decimal, ask: Decimal) -> None:
            self.bid = bid
            self.ask = ask

        def best_bid(self) -> Decimal:
            return self.bid

        def best_ask(self) -> Decimal:
            return self.ask

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
            self.open_orders_calls = 0
            self.fills_calls = 0
            self.fills_from_us: list[int | None] = []

        async def open_orders(self, *args: Any, **kwargs: Any):
            self.open_orders_calls += 1
            return []

        async def fills(self, *args: Any, **kwargs: Any):
            self.fills_calls += 1
            self.fills_from_us.append(kwargs.get("from_us"))
            return list(self.fills_data)

    class FakeMaker:
        def __init__(self) -> None:
            self.credentials = SimpleNamespace(
                account_address="0x" + "11" * 20,
                account_index=0,
            )
            self.quantities: list[Decimal] = []
            self.place_calls = 0
            self.cancel_calls = 0

        async def place_alo(self, **kwargs: Any):
            self.place_calls += 1
            self.quantities.append(Decimal(str(kwargs["quantity"])))
            if place_exception is not None:
                raise place_exception
            return SimpleNamespace(
                order_id=f"order-{len(self.quantities)}",
                client_id=kwargs["client_id"],
            )

        async def cancel_calibration_order(self, **kwargs: Any):
            self.cancel_calls += 1
            return {"status": 202}

    class FakeHedge:
        def __init__(self) -> None:
            self.book = FakeBook(rh_bid, rh_ask)
            self.size_decimals = rh_size_decimals
            self.min_base = rh_min_base
            self.fee_bps = 1.0
            self.hedges: list[dict[str, Any]] = []
            if rh_min_quote is not None:
                self.min_quote = rh_min_quote

        async def send_taker(self, **kwargs: Any):
            self.hedges.append(kwargs)
            if hedge_exception is not None:
                raise hedge_exception
            if hedge_result is not None:
                return dict(hedge_result)
            avg_px = self.book.best_ask() if kwargs["is_buy"] else self.book.best_bid()
            return {
                "status": "filled",
                "filled_base": kwargs["qty"],
                "avg_px": avg_px,
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
            book=FakeBook(Decimal("99.90"), Decimal("100.10")),
            latest_attributes=SimpleNamespace(is_outside_rth=False),
        ),
        hedge=hedge,
        maker=cast(ArcusMakerClient, maker),
        account_feed=feed,
        account_rest=cast(ArcusAccountRest, rest),
        account_state=ArcusAccountState(startup_watermark_us=startup_watermark_us),
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
        session_limits=SessionLimits(max_order_qty=max_order_qty),
        client_prefix=client_prefix,
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


def _stack_candidate(quantity: Decimal = Decimal("0.200")) -> QuoteCandidate:
    return QuoteCandidate(
        side="SELL",
        price=Decimal("100.10"),
        quantity=quantity,
        hedge_side="BUY",
        hedge_price=Decimal("100.10"),
        fair_price=Decimal("100.10"),
        expected_edge_bps=Decimal("4.2"),
        expected_usd=Decimal("0.01"),
    )


def _stack_fill(
    executor: CalibrationController,
    *,
    trade_id: str = "hedge-fill",
    quantity: Decimal = Decimal("0.200"),
) -> ArcusUserFill:
    return ArcusUserFill(
        trade_id=trade_id,
        order_id=executor.lifecycle.order_id or "",
        client_id=executor.lifecycle.client_id,
        market_id=33,
        market_display_name="HYPE-USD",
        side="SELL",
        price=Decimal("100.10"),
        quantity=quantity,
        fee=Decimal("0.01"),
        created_at_us=1_001,
        sequence_number=1,
        is_snapshot=False,
    )


@pytest.mark.parametrize("status", [401, 403])
def test_explicit_place_order_rejected_halts_volume_probe_without_cancel_or_reconcile(
    tmp_path, caplog, status: int
) -> None:
    caplog.set_level(logging.INFO, logger="volume-probe")
    rejection = ArcusOrderRejected(
        status=status,
        code="AUTH_FAILED",
        message="permission denied",
    )
    stack = _reconciliation_probe_stack(
        tmp_path,
        place_exception=rejection,
    )
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def final_state_reader():
        return Decimal("0"), Decimal("0"), [], []

    async def exercise() -> None:
        status_result = await probe.run(
            asyncio.Event(),
            quantity=Decimal("0.10"),
            final_state_reader=final_state_reader,
        )
        assert status_result is ProbeStatus.HALTED
        assert probe.state is ProbeState.HALTED
        assert probe.status is ProbeStatus.HALTED
        assert executor.lifecycle.state == "REJECTED"
        assert executor.current_candidate is None
        assert executor.current_execution_id is None
        assert executor._current_context is None
        assert executor.has_live_order is False
        assert executor._terminal_reconcile_pending is False
        assert executor.reconciliation_required is False
        assert stack.maker.place_calls == 1
        assert stack.maker.cancel_calls == 0
        assert stack.rest.open_orders_calls == 0
        assert stack.rest.fills_calls == 0
        assert stack.hedge.hedges == []
        assert executor.account_state.calibration_client_ids == set()
        assert executor.telemetry.store.flush().ok
        rows = executor.telemetry.store._conn.execute(
            "SELECT event_type, lifecycle_state, halt_reason "
            "FROM arcus_calibration_events ORDER BY id"
        ).fetchall()
        assert any(
            event_type == "place_rejected"
            and lifecycle_state == "REJECTED"
            and f"status={status}" in (halt_reason or "")
            for event_type, lifecycle_state, halt_reason in rows
        )
        assert not any(event_type == "place_failure" for event_type, *_ in rows)
        assert (
            f"[volume-probe] place_rejected reason=Arcus placeOrder rejected: "
            f"status={status} code=AUTH_FAILED message=permission denied"
        ) in caplog.text
        assert executor.lifecycle.client_id is not None
        await executor.on_fill(
            ArcusUserFill(
                trade_id="late-rejected-fill",
                order_id="rejected-order",
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
        )
        assert stack.hedge.hedges == []

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def test_async_zero_fill_post_only_rejection_sets_reconcile_barrier_without_halt(
    tmp_path,
) -> None:
    stack = _reconciliation_probe_stack(tmp_path)
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        await probe.place_next_quote()
        candidate = executor.current_candidate
        assert candidate is not None
        await executor.on_order(
            ArcusOrderUpdate(
                order_id=executor.lifecycle.order_id or "order-post-only",
                client_id=executor.lifecycle.client_id,
                market_id=33,
                market_display_name="HYPE-USD",
                side=candidate.side,
                status="REJECTED",
                state="REJECTED",
                price=candidate.price,
                original_size=candidate.quantity,
                remaining_size=candidate.quantity,
                avg_fill_price=None,
                created_at_us=1_000,
                updated_at_us=1_001,
                sequence_number=1,
                is_snapshot=False,
                filled_size=Decimal("0"),
                rejection_reason="POST_ONLY_WOULD_CROSS",
            )
        )

        assert executor.lifecycle.state == "REJECTED"
        assert executor.risk.halted is False
        assert executor._terminal_reconcile_pending is True
        assert stack.hedge.hedges == []
        executor.telemetry.store.flush()
        rows = executor.telemetry.store._conn.execute(
            "SELECT event_type, lifecycle_state, halt_reason "
            "FROM arcus_calibration_events ORDER BY id"
        ).fetchall()
        assert any(
            event_type == "post_only_reject"
            and lifecycle_state == "REJECTED"
            and halt_reason == "POST_ONLY_WOULD_CROSS"
            for event_type, lifecycle_state, halt_reason in rows
        )

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


@pytest.mark.parametrize(
    "update_fields",
    [
        {"filled_size": Decimal("0.001")},
        {"filled_size": None},
        {"remaining_size": Decimal("0.099")},
        {"rejection_reason": "OTHER_REASON"},
    ],
)
def test_async_non_proven_post_only_rejection_still_halts(
    tmp_path, update_fields: dict[str, Any]
) -> None:
    stack = _reconciliation_probe_stack(tmp_path)
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        await probe.place_next_quote()
        candidate = executor.current_candidate
        assert candidate is not None
        await executor.on_order(
            ArcusOrderUpdate(
                order_id=executor.lifecycle.order_id or "order-post-only",
                client_id=executor.lifecycle.client_id,
                market_id=33,
                market_display_name="HYPE-USD",
                side=candidate.side,
                status="REJECTED",
                state="REJECTED",
                price=candidate.price,
                original_size=candidate.quantity,
                remaining_size=update_fields.pop("remaining_size", candidate.quantity),
                avg_fill_price=None,
                created_at_us=1_000,
                updated_at_us=1_001,
                sequence_number=1,
                is_snapshot=False,
                filled_size=update_fields.pop("filled_size", Decimal("0")),
                rejection_reason=update_fields.pop(
                    "rejection_reason", "POST_ONLY_WOULD_CROSS"
                ),
            )
        )

        assert executor.risk.halted is True
        assert executor._terminal_reconcile_pending is False

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def test_local_would_cross_returns_no_submission_and_cleans_context(tmp_path) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        place_exception=ArcusAloWouldCross("would cross"),
    )
    executor = stack.executor

    async def exercise() -> None:
        submitted = await executor.place_quote(_stack_candidate())

        assert submitted is False
        assert executor.risk.halted is False
        assert executor.current_candidate is None
        assert executor.current_execution_id is None
        assert executor._current_context is None
        assert executor.has_live_order is False
        assert executor.account_state.calibration_client_ids == set()

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def _post_only_reject_update(
    executor: CalibrationController,
    *,
    filled_size: Decimal = Decimal("0"),
    rejection_reason: str = "POST_ONLY_WOULD_CROSS",
) -> ArcusOrderUpdate:
    candidate = executor.current_candidate
    assert candidate is not None
    return ArcusOrderUpdate(
        order_id=executor.lifecycle.order_id or "",
        client_id=executor.lifecycle.client_id,
        market_id=33,
        market_display_name="HYPE-USD",
        side=candidate.side,
        status="REJECTED",
        state="REJECTED",
        price=candidate.price,
        original_size=candidate.quantity,
        remaining_size=candidate.quantity,
        avg_fill_price=None,
        created_at_us=1_000,
        updated_at_us=1_001,
        sequence_number=1,
        is_snapshot=False,
        filled_size=filled_size,
        rejection_reason=rejection_reason,
    )


def test_post_only_retry_waits_for_reconcile_delay_and_fresh_bbo(tmp_path) -> None:
    stack = _reconciliation_probe_stack(tmp_path)
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        assert await probe.place_next_quote() is True
        first_execution_id = executor.current_execution_id
        first_update = _post_only_reject_update(executor)
        await probe.on_order(first_update)
        await probe.on_order(first_update)
        assert probe._consecutive_post_only_rejects == 1
        assert executor._terminal_reconcile_pending is True

        await executor.reconcile()
        assert executor._terminal_reconcile_pending is False
        assert await probe.place_next_quote() is False
        assert stack.maker.place_calls == 1

        executor.arcus.book.ask = Decimal("100.20")
        probe._post_only_retry_not_before_mono = 0.0
        assert await probe.place_next_quote() is True
        assert stack.maker.place_calls == 2
        assert executor.current_execution_id != first_execution_id
        assert executor.current_candidate is not None
        assert executor.current_candidate.price == Decimal("100.20")

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


@pytest.mark.parametrize("normal_status", ["OPEN", "PARTIALLY_FILLED"])
def test_post_only_retry_counter_resets_on_normal_update(
    tmp_path, normal_status: str
) -> None:
    stack = _reconciliation_probe_stack(tmp_path)
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        await probe.place_next_quote()
        await probe.on_order(_post_only_reject_update(executor))
        assert probe._consecutive_post_only_rejects == 1
        await executor.reconcile()
        probe._post_only_retry_not_before_mono = 0.0
        assert await probe.place_next_quote() is True

        candidate = executor.current_candidate
        assert candidate is not None
        await probe.on_order(
            ArcusOrderUpdate(
                order_id=executor.lifecycle.order_id or "",
                client_id=executor.lifecycle.client_id,
                market_id=33,
                market_display_name="HYPE-USD",
                side=candidate.side,
                status=normal_status,
                state=normal_status,
                price=candidate.price,
                original_size=candidate.quantity,
                remaining_size=candidate.quantity,
                avg_fill_price=None,
                created_at_us=1_000,
                updated_at_us=1_001,
                sequence_number=1,
                is_snapshot=False,
            )
        )
        assert probe._consecutive_post_only_rejects == 0

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def test_post_only_retry_policy_applies_during_unwind(tmp_path) -> None:
    stack = _reconciliation_probe_stack(tmp_path)
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        probe.build_base_qty = Decimal("0.10")
        probe.machine.transition(ProbeState.HEDGED)
        probe.begin_unwind()
        await probe.place_next_quote()
        assert executor.current_candidate is not None
        assert executor.current_candidate.side == "BUY"

        await probe.on_order(_post_only_reject_update(executor))

        assert probe._consecutive_post_only_rejects == 1
        assert probe.status is None
        assert executor.risk.halted is False
        assert executor._terminal_reconcile_pending is True

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def test_five_consecutive_post_only_rejections_halt_without_sixth_quote(
    tmp_path,
) -> None:
    stack = _reconciliation_probe_stack(tmp_path)
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def exercise() -> None:
        probe.begin_build(Decimal("0.60"))
        await probe.place_next_quote()
        for attempt in range(5):
            await probe.on_order(_post_only_reject_update(executor))
            if attempt == 4:
                break
            await executor.reconcile()
            probe._post_only_retry_not_before_mono = 0.0
            assert await probe.place_next_quote() is True

        assert probe._consecutive_post_only_rejects == 5
        assert probe.status is ProbeStatus.HALTED
        assert executor.risk.halted is True
        assert stack.maker.place_calls == 5

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def test_local_would_cross_uses_same_cooldown_and_fresh_retry(tmp_path, caplog) -> None:
    stack = _reconciliation_probe_stack(tmp_path)
    executor = stack.executor
    probe = stack.probe
    assert probe is not None
    calls = 0

    async def place_alo(**kwargs: Any):
        nonlocal calls
        calls += 1
        stack.maker.place_calls += 1
        stack.maker.quantities.append(Decimal(str(kwargs["quantity"])))
        if calls == 1:
            raise ArcusAloWouldCross("would cross")
        return SimpleNamespace(
            order_id=f"order-local-{calls}",
            client_id=kwargs["client_id"],
        )

    stack.maker.place_alo = place_alo
    caplog.set_level(logging.INFO, logger="volume-probe")

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        first_client_id = None
        assert await probe.place_next_quote() is False
        first_client_id = executor.lifecycle.client_id
        assert executor.risk.halted is False
        assert stack.maker.place_calls == 1
        assert "quote placed" not in caplog.text

        assert await probe.place_next_quote() is False
        assert stack.maker.place_calls == 1
        executor.arcus.book.ask = Decimal("100.30")
        probe._post_only_retry_not_before_mono = 0.0
        assert await probe.place_next_quote() is True
        assert stack.maker.place_calls == 2
        assert executor.lifecycle.client_id != first_client_id
        assert executor.current_candidate is not None
        assert executor.current_candidate.price == Decimal("100.30")

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


def test_volume_probe_logs_build_unwind_and_flat_completion(caplog) -> None:
    class FakeBook:
        def __init__(self, bid: str, ask: str) -> None:
            self.bid = Decimal(bid)
            self.ask = Decimal(ask)

        def best_bid(self) -> Decimal:
            return self.bid

        def best_ask(self) -> Decimal:
            return self.ask

    class FakeAccumulator:
        residual_exposure = Decimal("0")

        def reset_if_flat(self) -> None:
            return None

    class FakeRisk:
        halted = False
        halt_reason = None

    class FakeExecutor:
        def __init__(self) -> None:
            self.risk = FakeRisk()
            self.accumulator = FakeAccumulator()
            self.arcus = SimpleNamespace(book=FakeBook("100", "100.1"))
            self.hedge = SimpleNamespace(book=FakeBook("99.9", "100.1"))
            self.pnl = SimpleNamespace(actual_usd=Decimal("0.12"))
            self.has_live_order = False
            self._terminal_reconcile_pending = False
            self.orders: list[QuoteCandidate] = []

        async def place_quote(self, candidate: QuoteCandidate) -> None:
            self.orders.append(candidate)

    executor = FakeExecutor()
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
        session_id="vp-log-test",
    )

    caplog.set_level(logging.INFO, logger="volume-probe")
    controller.begin_build(Decimal("0.10"))
    asyncio.run(controller.place_next_quote())
    controller.record_hedged_fill(
        arcus_side="SELL",
        arcus_price=Decimal("100"),
        arcus_quantity=Decimal("0.10"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="BUY", qty="0.10"),
    )
    controller.begin_unwind()
    asyncio.run(controller.place_next_quote())
    controller.record_hedged_fill(
        arcus_side="BUY",
        arcus_price=Decimal("100.1"),
        arcus_quantity=Decimal("0.10"),
        arcus_fee=Decimal("0.01"),
        hedge=_hedge(side="SELL", qty="0.10"),
    )
    controller.finish_unwind()
    assert (
        controller.finalize_positions(
            arcus_position=Decimal("0"),
            rh_position=Decimal("0"),
            arcus_open_orders=[],
            rh_open_orders=[],
        )
        is ProbeStatus.COMPLETED
    )

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "[volume-probe] BUILD quote placed side=SELL" in messages
    assert "[volume-probe] BUILD fill qty=0.10 avg_px=100" in messages
    assert "[volume-probe] BUILD hedge side=BUY qty=0.10" in messages
    assert "[volume-probe] BUILD complete" in messages
    assert "[volume-probe] UNWIND quote placed side=BUY" in messages
    assert "[volume-probe] UNWIND fill qty=0.10 avg_px=100.1" in messages
    assert "[volume-probe] UNWIND hedge side=SELL qty=0.10" in messages
    assert "[volume-probe] UNWIND complete" in messages
    assert (
        "[volume-probe] FINAL arcus_position=0 rh_position=0 "
        "status=COMPLETED pnl_usd=0.12"
    ) in messages


@pytest.mark.parametrize(
    ("reason", "event"),
    [
        ("Arcus placeOrder rejected: status=401 permission denied", "place_rejected"),
        ("RH hedge unresolved", "hedge failure/unresolved"),
        ("volume-probe runtime timeout", "TIMEOUT"),
        ("market health gate failed", "HALTED"),
    ],
)
def test_volume_probe_logs_terminal_reasons(caplog, reason: str, event: str) -> None:
    caplog.set_level(logging.INFO, logger="volume-probe")

    controller = _probe_controller()
    controller._halt(reason)

    assert f"[volume-probe] {event} reason={reason}" in caplog.text


def test_volume_probe_logs_reconciliation_required_reason(caplog) -> None:
    caplog.set_level(logging.INFO, logger="volume-probe")

    controller = _probe_controller()
    controller._require_reconciliation("Arcus terminal reconciliation deadline expired")

    assert (
        "[volume-probe] RECONCILIATION_REQUIRED reason="
        "Arcus terminal reconciliation deadline expired"
    ) in caplog.text


def test_volume_probe_health_failure_detail_includes_each_health_field() -> None:
    ready = asyncio.Event()
    ready.set()

    class Book:
        ready = False
        sequence_health = "BOUNDARY"
        first_delta_after_snapshot = False

        def best_bid(self) -> Decimal:
            return Decimal("100")

        def best_ask(self) -> Decimal:
            return Decimal("101")

        def is_fresh(self, _max_age_sec: float) -> bool:
            return True

    class HedgeBook(Book):
        ready = True

    detail = format_volume_probe_health(
        arcus=SimpleNamespace(book=Book()),
        hedge=SimpleNamespace(
            book=HedgeBook(),
            ready_to_trade=lambda: True,
        ),
        account_feed=SimpleNamespace(
            healthy=True,
            ready=ready,
            required_channels_healthy=True,
        ),
        staleness_sec=10.0,
    )

    assert detail == (
        "arcus_ready=False arcus_sequence=BOUNDARY "
        "arcus_first_delta_after_snapshot=False arcus_fresh=True "
        "rh_ready=True rh_trade_ready=True rh_fresh=True "
        "account_ws_healthy=True"
    )


def test_volume_probe_runtime_health_failure_logs_detailed_reason(caplog) -> None:
    ready = asyncio.Event()
    ready.set()

    class Book:
        ready = False
        sequence_health = "BOUNDARY"
        first_delta_after_snapshot = False

        def best_bid(self) -> Decimal:
            return Decimal("100")

        def best_ask(self) -> Decimal:
            return Decimal("101")

        def is_fresh(self, _max_age_sec: float) -> bool:
            return True

    class HedgeBook(Book):
        ready = True

    class Risk:
        halted = False
        halt_reason: str | None = None

        def check_runtime(self) -> None:
            return None

        def halt(self, reason: str) -> None:
            self.halted = True
            self.halt_reason = reason

    executor = SimpleNamespace(
        risk=Risk(),
        accumulator=SimpleNamespace(residual_exposure=Decimal("0")),
        has_live_order=False,
        _terminal_reconcile_pending=False,
        arcus=SimpleNamespace(book=Book()),
        hedge=SimpleNamespace(book=HedgeBook(), ready_to_trade=lambda: True),
        account_feed=SimpleNamespace(
            healthy=True,
            ready=ready,
            required_channels_healthy=True,
        ),
        staleness_sec=10.0,
        market_health=lambda: SimpleNamespace(can_quote=False),
    )
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
    )

    async def no_initial_quote() -> bool:
        return False

    setattr(controller, "place_next_quote", no_initial_quote)
    caplog.set_level(logging.INFO, logger="volume-probe")

    async def final_state_reader():
        return Decimal("0"), Decimal("0"), [], []

    status = asyncio.run(
        controller.run(
            asyncio.Event(),
            quantity=Decimal("0.10"),
            final_state_reader=final_state_reader,
        )
    )

    assert status is ProbeStatus.HALTED
    assert (
        "volume-probe market health gate failed: arcus_ready=False "
        "arcus_sequence=BOUNDARY arcus_first_delta_after_snapshot=False "
        "arcus_fresh=True rh_ready=True rh_trade_ready=True rh_fresh=True "
        "account_ws_healthy=True"
    ) in caplog.text


def test_completed_volume_probe_ignores_expected_shutdown_disconnect(caplog) -> None:
    caplog.set_level(logging.INFO, logger="volume-probe")
    controller = _probe_controller()
    controller.machine.state = ProbeState.DONE
    controller._status = ProbeStatus.COMPLETED

    asyncio.run(controller.on_disconnect())

    assert controller.state is ProbeState.DONE
    assert controller.status is ProbeStatus.COMPLETED
    assert "disconnect" not in caplog.text.lower()


def _disconnect_tracking_probe() -> tuple[VolumeProbeController, Any]:
    class FakeRisk:
        halted = False
        halt_reason = None

        def __init__(self) -> None:
            self.halt_calls = 0

        def halt(self, reason: str) -> None:
            self.halt_calls += 1
            self.halted = True
            self.halt_reason = reason

    class FakeExecutor:
        def __init__(self) -> None:
            self.risk = FakeRisk()
            self.has_live_order = False
            self._terminal_reconcile_pending = False
            self.disconnect_calls = 0

        async def on_disconnect(self) -> None:
            self.disconnect_calls += 1
            self.risk.halt("Arcus account websocket disconnected")

    executor = FakeExecutor()
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
        session_id="vp-disconnect-test",
    )
    return controller, executor


def test_preorder_only_disconnect_is_intentional_noop(caplog) -> None:
    caplog.set_level(logging.INFO, logger="volume-probe")
    controller, executor = _disconnect_tracking_probe()
    controller.begin_build(Decimal("0.10"))

    controller.mark_preorder_only(
        arcus_position=Decimal("0"),
        rh_position=Decimal("0"),
    )
    asyncio.run(controller.on_disconnect())

    assert controller.state is ProbeState.BUILD
    assert controller.status is ProbeStatus.PREORDER_ONLY
    assert controller.metrics.status is ProbeStatus.PREORDER_ONLY
    assert controller.metrics.final_arcus_position == Decimal("0")
    assert controller.metrics.final_rh_position == Decimal("0")
    assert controller.metrics.finished_at is not None
    assert controller.failure_reason is None
    assert executor.disconnect_calls == 0
    assert executor.risk.halt_calls == 0
    assert "HALTED" not in caplog.text


def test_active_probe_disconnect_still_halts(caplog) -> None:
    caplog.set_level(logging.INFO, logger="volume-probe")
    controller, executor = _disconnect_tracking_probe()
    controller.begin_build(Decimal("0.10"))

    asyncio.run(controller.on_disconnect())

    assert controller.state is ProbeState.HALTED
    assert controller.status is ProbeStatus.HALTED
    assert controller.failure_reason == "Arcus account websocket disconnected"
    assert executor.disconnect_calls == 1
    assert executor.risk.halt_calls == 1
    assert "[volume-probe] HALTED reason=Arcus account websocket disconnected" in (
        caplog.text
    )


@pytest.mark.parametrize(
    "place_exception",
    [
        TimeoutError("placement websocket timeout"),
        ConnectionError("connection lost after send"),
    ],
)
def test_placement_timeout_or_connection_loss_keeps_cancel_reconciliation_safety_path(
    tmp_path, place_exception: Exception
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        place_exception=place_exception,
    )
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        await probe.place_next_quote()
        assert stack.maker.place_calls == 1
        assert executor.risk.halted is True
        assert executor.lifecycle.state == "CANCEL_SENT"
        assert stack.maker.cancel_calls == 1
        assert executor.reconciliation_required is False
        with pytest.raises(RuntimeError, match="cannot place a second"):
            await probe.place_next_quote()
        assert stack.maker.place_calls == 1

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


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


@pytest.mark.parametrize("created_at_us", [1_999, 2_000])
def test_reconcile_ignores_unknown_prestart_vp_fill_and_uses_watermark(
    tmp_path, created_at_us: int
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        startup_watermark_us=2_000,
        client_prefix="vp-",
    )
    executor = stack.executor

    async def exercise() -> None:
        await executor._place_quote(_stack_candidate())
        stack.rest.fills_data = [
            ArcusUserFill(
                trade_id="previous-session-fill",
                order_id="vp-previous-order",
                client_id="vp-previous-session-1",
                market_id=33,
                market_display_name="HYPE-USD",
                side="SELL",
                price=Decimal("100.10"),
                quantity=Decimal("0.04"),
                fee=Decimal("0.01"),
                created_at_us=created_at_us,
                sequence_number=1,
                is_snapshot=False,
            )
        ]
        await executor.reconcile()

    try:
        asyncio.run(exercise())
        assert stack.rest.fills_from_us == [2_000]
        assert stack.hedge.hedges == []
        assert executor.risk.halted is False
        rows = executor.telemetry.store._conn.execute(
            "SELECT event_type FROM arcus_calibration_events"
        ).fetchall()
        assert not any(event_type == "unexpected_fill" for (event_type,) in rows)
    finally:
        executor.telemetry.store.close()


@pytest.mark.parametrize("created_at_us", [2_001, None])
def test_reconcile_unknown_non_snapshot_vp_fill_after_or_without_timestamp_halts(
    tmp_path, created_at_us: int | None
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        startup_watermark_us=2_000,
        client_prefix="vp-",
    )
    executor = stack.executor

    async def exercise() -> None:
        await executor._place_quote(_stack_candidate())
        stack.rest.fills_data = [
            ArcusUserFill(
                trade_id="unsafe-unknown-fill",
                order_id="vp-unsafe-order",
                client_id="vp-unsafe-session-1",
                market_id=33,
                market_display_name="HYPE-USD",
                side="SELL",
                price=Decimal("100.10"),
                quantity=Decimal("0.04"),
                fee=Decimal("0.01"),
                created_at_us=created_at_us,
                sequence_number=1,
                is_snapshot=False,
            )
        ]
        await executor.reconcile()

    try:
        asyncio.run(exercise())
        assert executor.risk.halted is True
        assert stack.hedge.hedges == []
        executor.telemetry.store.flush()
        rows = executor.telemetry.store._conn.execute(
            "SELECT event_type FROM arcus_calibration_events"
        ).fetchall()
        assert any(event_type == "unexpected_fill" for (event_type,) in rows)
    finally:
        executor.telemetry.store.close()


def test_reconcile_mixed_historical_and_current_fill_is_owned_once(tmp_path) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        startup_watermark_us=2_000,
    )
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    async def exercise() -> None:
        probe.begin_build(Decimal("0.10"))
        await probe.place_next_quote()
        current = ArcusUserFill(
            trade_id="current-session-fill",
            order_id=executor.lifecycle.order_id or "",
            client_id=executor.lifecycle.client_id,
            market_id=33,
            market_display_name="HYPE-USD",
            side="SELL",
            price=Decimal("100.10"),
            quantity=Decimal("0.04"),
            fee=Decimal("0.01"),
            created_at_us=2_001,
            sequence_number=2,
            is_snapshot=False,
        )
        historical = ArcusUserFill(
            trade_id="old-session-fill",
            order_id="vp-old-order",
            client_id="vp-old-session-1",
            market_id=33,
            market_display_name="HYPE-USD",
            side="SELL",
            price=Decimal("100.10"),
            quantity=Decimal("0.04"),
            fee=Decimal("0.01"),
            created_at_us=2_000,
            sequence_number=1,
            is_snapshot=False,
        )
        stack.rest.fills_data = [current, historical]
        await executor.reconcile()
        await probe.on_fill(current)
        stack.rest.fills_data = [current, historical]
        await executor.reconcile()

    try:
        asyncio.run(exercise())
        assert len(stack.hedge.hedges) == 1
        assert Decimal(str(stack.hedge.hedges[0]["qty"])) == Decimal("0.04")
        assert probe.build_base_qty == Decimal("0.04")
        assert executor.risk.halted is False
        rows = executor.telemetry.store._conn.execute(
            "SELECT event_type FROM arcus_calibration_events"
        ).fetchall()
        assert not any(event_type == "unexpected_fill" for (event_type,) in rows)
    finally:
        executor.telemetry.store.close()


def test_volume_probe_unknown_historical_fill_uses_prestart_ownership_rule(
    tmp_path,
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        startup_watermark_us=2_000,
    )
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    historical = ArcusUserFill(
        trade_id="old-volume-probe-fill",
        order_id="vp-old-order",
        client_id="vp-old-session-1",
        market_id=33,
        market_display_name="HYPE-USD",
        side="SELL",
        price=Decimal("100.10"),
        quantity=Decimal("0.04"),
        fee=Decimal("0.01"),
        created_at_us=1_999,
        sequence_number=1,
        is_snapshot=False,
    )

    try:
        probe.begin_build(Decimal("0.10"))
        asyncio.run(probe.on_fill(historical))
        assert probe.state is ProbeState.BUILD
        assert probe.status is None
        assert executor.risk.halted is False
        assert stack.hedge.hedges == []
    finally:
        executor.telemetry.store.close()


def test_volume_probe_unknown_poststart_fill_still_halts(tmp_path) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        startup_watermark_us=2_000,
    )
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    poststart = ArcusUserFill(
        trade_id="unsafe-volume-probe-fill",
        order_id="vp-unsafe-order",
        client_id="vp-unsafe-session-1",
        market_id=33,
        market_display_name="HYPE-USD",
        side="SELL",
        price=Decimal("100.10"),
        quantity=Decimal("0.04"),
        fee=Decimal("0.01"),
        created_at_us=2_001,
        sequence_number=1,
        is_snapshot=False,
    )

    try:
        probe.begin_build(Decimal("0.10"))
        asyncio.run(probe.on_fill(poststart))
        assert probe.state is ProbeState.HALTED
        assert probe.status is ProbeStatus.HALTED
        assert executor.risk.halted is True
        assert stack.hedge.hedges == []
    finally:
        executor.telemetry.store.close()


def test_reconcile_aggregates_min_quote_partial_fills_once_for_build_and_unwind(
    tmp_path,
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        rh_bid=Decimal("50"),
        rh_ask=Decimal("50"),
        rh_size_decimals=3,
        rh_min_base=Decimal("0.100"),
        rh_min_quote=Decimal("10"),
        max_order_qty=Decimal("0.200"),
    )
    executor = stack.executor
    probe = stack.probe
    assert probe is not None

    def current_fill(
        *, trade_id: str, side: str, quantity: str, sequence: int
    ) -> ArcusUserFill:
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
            created_at_us=1_000 + sequence,
            sequence_number=sequence,
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
                sequence_number=1,
                is_snapshot=False,
            )
        )

    async def exercise() -> None:
        probe.begin_build(Decimal("0.200"))
        await probe.place_next_quote()
        first = current_fill(
            trade_id="build-min-1", side="SELL", quantity="0.100", sequence=1
        )
        second = current_fill(
            trade_id="build-min-2", side="SELL", quantity="0.100", sequence=2
        )
        await mark_terminal()
        stack.rest.fills_data = [first]
        await executor.reconcile()
        assert stack.hedge.hedges == []
        assert executor.accumulator.residual_exposure == Decimal("0.100")
        assert probe.build_base_qty == Decimal("0.100")
        assert probe.state is ProbeState.BUILD

        stack.rest.fills_data = [first, second]
        await executor.reconcile()
        assert len(stack.hedge.hedges) == 1
        assert Decimal(str(stack.hedge.hedges[0]["qty"])) == Decimal("0.200")
        assert stack.hedge.hedges[0]["is_buy"] is True
        assert executor.accumulator.residual_exposure == Decimal("0")
        assert probe.build_base_qty == Decimal("0.200")
        assert probe.state is ProbeState.HEDGED

        await probe.on_fill(first)
        await probe.on_fill(second)
        assert len(stack.hedge.hedges) == 1
        assert probe.build_base_qty == Decimal("0.200")

        probe.begin_unwind()
        await probe.place_next_quote()
        unwind_first = current_fill(
            trade_id="unwind-min-1", side="BUY", quantity="0.100", sequence=3
        )
        unwind_second = current_fill(
            trade_id="unwind-min-2", side="BUY", quantity="0.100", sequence=4
        )
        await mark_terminal()
        stack.rest.fills_data = [first, second, unwind_first]
        await executor.reconcile()
        assert len(stack.hedge.hedges) == 1
        assert executor.accumulator.residual_exposure == Decimal("0.100")
        assert probe.unwind_base_qty == Decimal("0.100")
        assert probe.state is ProbeState.UNWIND

        stack.rest.fills_data = [first, second, unwind_first, unwind_second]
        await executor.reconcile()
        assert len(stack.hedge.hedges) == 2
        assert Decimal(str(stack.hedge.hedges[1]["qty"])) == Decimal("0.200")
        assert stack.hedge.hedges[1]["is_buy"] is False
        assert executor.accumulator.residual_exposure == Decimal("0")
        assert probe.unwind_base_qty == Decimal("0.200")

    try:
        asyncio.run(exercise())
    finally:
        executor.telemetry.store.close()


@pytest.mark.parametrize(
    "arcus_side, rh_bid, rh_ask, quantity, expected_hedges",
    [
        ("SELL", Decimal("50"), Decimal("60"), Decimal("0.170"), 1),
        ("BUY", Decimal("50"), Decimal("60"), Decimal("0.190"), 0),
    ],
)
def test_runtime_uses_correct_fresh_hedge_side_for_min_quote(
    tmp_path,
    arcus_side: str,
    rh_bid: Decimal,
    rh_ask: Decimal,
    quantity: Decimal,
    expected_hedges: int,
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        rh_bid=rh_bid,
        rh_ask=rh_ask,
        rh_size_decimals=3,
        rh_min_base=Decimal("0.100"),
        rh_min_quote=Decimal("10"),
        max_order_qty=Decimal("0.200"),
    )
    executor = stack.executor
    candidate = QuoteCandidate(
        side=arcus_side,
        price=Decimal("100"),
        quantity=quantity,
        hedge_side="BUY" if arcus_side == "SELL" else "SELL",
        hedge_price=rh_ask if arcus_side == "SELL" else rh_bid,
        fair_price=Decimal("100"),
        expected_edge_bps=Decimal("4"),
        expected_usd=Decimal("0"),
    )

    async def exercise() -> None:
        await executor._place_quote(candidate)
        fill = ArcusUserFill(
            trade_id=f"fresh-{arcus_side}",
            order_id=executor.lifecycle.order_id or "",
            client_id=executor.lifecycle.client_id,
            market_id=33,
            market_display_name="HYPE-USD",
            side=arcus_side,
            price=Decimal("100"),
            quantity=quantity,
            fee=Decimal("0.01"),
            created_at_us=1_001,
            sequence_number=1,
            is_snapshot=False,
        )
        await executor.on_fill(fill)

    try:
        asyncio.run(exercise())
        assert len(stack.hedge.hedges) == expected_hedges
        if expected_hedges == 0:
            assert executor.accumulator.residual_exposure == quantity
        else:
            assert executor.accumulator.residual_exposure == Decimal("0")
    finally:
        executor.telemetry.store.close()


def test_min_quote_is_rechecked_before_send_and_retained_on_price_move(
    tmp_path,
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        rh_bid=Decimal("49"),
        rh_ask=Decimal("50"),
        rh_size_decimals=3,
        rh_min_base=Decimal("0.100"),
        rh_min_quote=Decimal("10"),
        max_order_qty=Decimal("0.200"),
    )
    executor = stack.executor

    class FallingBook:
        ready = True
        sequence_health = "OK"

        def __init__(self) -> None:
            self.reads = 0

        def best_bid(self) -> Decimal:
            self.reads += 1
            return Decimal("49") if self.reads <= 4 else Decimal("39")

        def best_ask(self) -> Decimal:
            self.reads += 1
            return Decimal("50") if self.reads <= 4 else Decimal("40")

        def is_fresh(self, _max_age_sec: float) -> bool:
            return True

    executor.hedge.book = FallingBook()
    candidate = QuoteCandidate(
        side="SELL",
        price=Decimal("100"),
        quantity=Decimal("0.200"),
        hedge_side="BUY",
        hedge_price=Decimal("50"),
        fair_price=Decimal("100"),
        expected_edge_bps=Decimal("4"),
        expected_usd=Decimal("0"),
    )

    async def exercise() -> None:
        await executor._place_quote(candidate)
        fill = ArcusUserFill(
            trade_id="price-move",
            order_id=executor.lifecycle.order_id or "",
            client_id=executor.lifecycle.client_id,
            market_id=33,
            market_display_name="HYPE-USD",
            side="SELL",
            price=Decimal("100"),
            quantity=Decimal("0.200"),
            fee=Decimal("0.01"),
            created_at_us=1_001,
            sequence_number=1,
            is_snapshot=False,
        )
        await executor.on_fill(fill)

    try:
        asyncio.run(exercise())
        assert stack.hedge.hedges == []
        assert executor.accumulator.unhedged_qty == Decimal("0.200")
        assert executor.accumulator.pending_hedge_qty == Decimal("0")
        assert executor.accumulator.residual_exposure == Decimal("0.200")
        assert executor.risk.halted is False
        rows = executor.telemetry.store._conn.execute(
            "SELECT event_type FROM arcus_calibration_events"
        ).fetchall()
        assert not any(row[0] == "hedge_failure" for row in rows)
    finally:
        executor.telemetry.store.close()


def test_calibration_full_authoritative_hedge_clears_reservation(tmp_path) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        max_order_qty=Decimal("0.200"),
        hedge_result={
            "status": "filled",
            "filled_base": 0.200,
            "avg_px": 100.10,
            "fee": 0.01,
            "unresolved": False,
        },
    )
    executor = stack.executor

    async def exercise() -> None:
        await executor._place_quote(_stack_candidate())
        await executor.on_fill(_stack_fill(executor))

    try:
        asyncio.run(exercise())
        assert len(stack.hedge.hedges) == 1
        assert executor.accumulator.unhedged_qty == Decimal("0")
        assert executor.accumulator.pending_hedge_qty == Decimal("0")
        assert executor.accumulator.residual_exposure == Decimal("0")
        assert executor.risk.halted is False
    finally:
        executor.telemetry.store.close()


def test_calibration_definitive_zero_fill_restores_reservation_without_retry(
    tmp_path,
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        max_order_qty=Decimal("0.200"),
        hedge_result={
            "status": "rejected",
            "filled_base": 0.0,
            "avg_px": None,
            "err": "minimum quote rejection",
            "unresolved": False,
        },
    )
    executor = stack.executor

    async def exercise() -> None:
        await executor._place_quote(_stack_candidate())
        fill = _stack_fill(executor)
        await executor.on_fill(fill)
        await executor.on_fill(fill)

    try:
        asyncio.run(exercise())
        assert len(stack.hedge.hedges) == 1
        assert executor.accumulator.unhedged_qty == Decimal("0.200")
        assert executor.accumulator.pending_hedge_qty == Decimal("0")
        assert executor.accumulator.residual_exposure == Decimal("0.200")
        assert executor.risk.halted is True
    finally:
        executor.telemetry.store.close()


def test_calibration_partial_authoritative_hedge_restores_only_remainder(
    tmp_path,
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        max_order_qty=Decimal("0.200"),
        hedge_result={
            "status": "filled",
            "filled_base": 0.120,
            "avg_px": 100.10,
            "fee": 0.01,
            "unresolved": False,
        },
    )
    executor = stack.executor

    async def exercise() -> None:
        await executor._place_quote(_stack_candidate())
        await executor.on_fill(_stack_fill(executor))

    try:
        asyncio.run(exercise())
        assert len(stack.hedge.hedges) == 1
        assert executor.accumulator.unhedged_qty == Decimal("0.080")
        assert executor.accumulator.pending_hedge_qty == Decimal("0")
        assert executor.accumulator.residual_exposure == Decimal("0.080")
        assert executor.risk.halted is True
    finally:
        executor.telemetry.store.close()


@pytest.mark.parametrize(
    "hedge_result, hedge_exception",
    [
        (
            {
                "status": "timeout",
                "filled_base": 0.0,
                "avg_px": None,
                "unresolved": True,
            },
            None,
        ),
        (None, TimeoutError("transport outcome unknown")),
    ],
    ids=["timeout-result", "transport-exception"],
)
def test_calibration_ambiguous_hedge_keeps_pending_without_retry(
    tmp_path,
    hedge_result: dict[str, Any] | None,
    hedge_exception: Exception | None,
) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        max_order_qty=Decimal("0.200"),
        hedge_result=hedge_result,
        hedge_exception=hedge_exception,
    )
    executor = stack.executor

    async def exercise() -> None:
        await executor._place_quote(_stack_candidate())
        fill = _stack_fill(executor)
        await executor.on_fill(fill)
        await executor.on_fill(fill)

    try:
        asyncio.run(exercise())
        assert len(stack.hedge.hedges) == 1
        assert executor.accumulator.unhedged_qty == Decimal("0")
        assert executor.accumulator.pending_hedge_qty == Decimal("0.200")
        assert executor.accumulator.residual_exposure == Decimal("0.200")
        assert executor.risk.halted is True
    finally:
        executor.telemetry.store.close()


def test_reconcile_recovered_ambiguous_hedge_is_not_retried(tmp_path) -> None:
    stack = _reconciliation_probe_stack(
        tmp_path,
        with_probe=False,
        max_order_qty=Decimal("0.200"),
        hedge_result={
            "status": "sent-unconfirmed",
            "filled_base": 0.0,
            "avg_px": None,
            "unresolved": True,
        },
    )
    executor = stack.executor

    async def exercise() -> None:
        await executor._place_quote(_stack_candidate())
        fill = _stack_fill(executor, trade_id="recovered-ambiguous")
        stack.rest.fills_data = [fill]
        await executor.reconcile()
        await executor.reconcile()

    try:
        asyncio.run(exercise())
        assert len(stack.hedge.hedges) == 1
        assert executor.accumulator.pending_hedge_qty == Decimal("0.200")
        assert executor.accumulator.unhedged_qty == Decimal("0")
    finally:
        executor.telemetry.store.close()


def test_volume_probe_pending_reservation_cannot_finalize_completed() -> None:
    controller = _real_accumulator_controller()
    controller.begin_build(Decimal("0.2"))
    instruction = controller.executor.accumulator.add_fill(
        side="SELL", quantity=Decimal("0.2")
    )
    assert instruction is not None
    controller.machine.state = ProbeState.FLAT
    controller.phase = None
    controller.phase_target_qty = None

    status = controller.finalize_positions(
        arcus_position=Decimal("0"),
        rh_position=Decimal("0"),
        arcus_open_orders=(),
        rh_open_orders=(),
    )

    assert status is ProbeStatus.RECONCILIATION_REQUIRED
    assert controller.state is ProbeState.RECONCILIATION_REQUIRED
    assert controller.executor.accumulator.pending_hedge_qty == Decimal("0.2")


def test_probe_timeout_with_min_quote_residual_requires_reconciliation() -> None:
    class FakeBook:
        def best_bid(self) -> Decimal:
            return Decimal("49")

        def best_ask(self) -> Decimal:
            return Decimal("50")

    class FakeRisk:
        halted = False
        halt_reason = None

        def check_runtime(self) -> None:
            raise TimeoutError("test timeout")

    class FakeExecutor:
        def __init__(self) -> None:
            self.risk = FakeRisk()
            self.accumulator = FillAccumulator(
                rh_min_qty=Decimal("0.100"),
                rh_step=Decimal("0.001"),
                rh_min_quote=Decimal("10"),
                unhedged_qty=Decimal("0.100"),
                side="SELL",
            )
            self.arcus = SimpleNamespace(book=FakeBook())
            self.hedge = SimpleNamespace(book=FakeBook())
            self.has_live_order = False
            self._terminal_reconcile_pending = False

        async def place_quote(self, _candidate: Any) -> None:
            return None

    executor = FakeExecutor()
    controller = VolumeProbeController(
        executor=cast(Any, executor),
        config=ProbeConfig(clip_usd=Decimal("10"), probe_side="sell"),
        symbol="HYPE-USD",
    )

    async def final_state_reader():
        return Decimal("0"), Decimal("0"), [], []

    status = asyncio.run(
        controller.run(
            asyncio.Event(),
            quantity=Decimal("0.100"),
            final_state_reader=final_state_reader,
        )
    )

    assert status is ProbeStatus.RECONCILIATION_REQUIRED
    assert controller.state is ProbeState.RECONCILIATION_REQUIRED
    assert controller.status is ProbeStatus.RECONCILIATION_REQUIRED


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
    controller.executor.accumulator.settle_hedge(
        instruction_quantity=build_instruction.quantity,
        filled_quantity=build_instruction.quantity,
    )
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
    controller.executor.accumulator.settle_hedge(
        instruction_quantity=unwind_instruction.quantity,
        filled_quantity=unwind_instruction.quantity,
    )
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
            self.accumulator.settle_hedge(
                instruction_quantity=instruction.quantity,
                filled_quantity=instruction.quantity,
            )
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
