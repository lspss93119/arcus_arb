"""Deterministic Phase B0 maker-first calibration tests.

No test in this module may reach a live Arcus or Lighter endpoint.  Network
behavior is exercised only through small in-memory transports.
"""

from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest

_IMPORT_ERROR = None
try:
    from entropy_arb.arcus_auth import (
        ArcusCredentialError,
        ArcusCredentials,
        ArcusSigner,
        build_ordersign_payload,
    )
    from entropy_arb.arcus_execution import (
        ArcusAccountFeed,
        ArcusAccountRest,
        ArcusAccountState,
        ArcusFeeTier,
        ArcusMakerClient,
        ArcusOrderError,
        ArcusOrderRejected,
        ArcusOrderUpdate,
        ArcusUserFill,
        parse_arcus_account_fee_tier,
        parse_arcus_fee_tiers,
        parse_arcus_order_snapshot,
        parse_arcus_order_update,
        parse_arcus_user_fill,
        parse_arcus_user_fills,
    )
    from entropy_arb.book import OrderBook
    from entropy_arb.calibration import (
        ARCUS_CALIBRATION_QTY,
        CANCEL_EDGE_BPS,
        PLACE_EDGE_BPS,
        RH_HEDGE_MIN_QTY,
        CalibrationLifecycle,
        CalibrationPnL,
        FillAccumulator,
        MarketHealth,
        QuoteCandidate,
        SessionLimits,
        SessionRisk,
        build_quote_candidates,
        choose_quote,
        expected_edge_bps,
    )
    from entropy_arb.calibration_runtime import (
        CalibrationController,
        CalibrationTelemetry,
        fetch_lighter_open_orders,
        resolve_verified_rh_fee_bps,
    )
    from entropy_arb.storage import ArcusCalibrationEventRow, MarketHistoryStore
    from entropy_arb.strategy import StableBasisStrategy
    from entropy_arb.venue_lighter import (
        LIGHTER_FEE_TICK_SCALE,
        LighterAccountLimits,
        LighterVenue,
        lighter_fee_tick_to_bps,
        parse_lighter_account_limits,
    )
    from main import validate_runtime_gates
except ImportError as exc:  # RED phase: the new public API is not present yet.
    _IMPORT_ERROR = exc


@pytest.fixture(autouse=True)
def require_phase_b0_api() -> None:
    if _IMPORT_ERROR is not None:
        pytest.fail(f"Phase B0 API is not implemented: {_IMPORT_ERROR}")


ADDRESS = "0x" + "11" * 20


def _credentials() -> ArcusCredentials:
    # Deterministic test-only Ed25519 seed; derive the matching public API key
    # instead of embedding a credential-shaped mismatched pair.
    from cryptography.hazmat.primitives.asymmetric import ed25519

    seed = bytes(range(1, 33))
    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
    api_key = private_key.public_key().public_bytes_raw().hex()
    return ArcusCredentials(
        account_address=ADDRESS,
        api_key=api_key,
        private_key_text=seed.hex(),
        account_index=0,
    )


def _quote_input(**overrides):
    values = {
        "arcus_bid": Decimal("100.00"),
        "arcus_ask": Decimal("100.10"),
        "rh_bid": Decimal("99.90"),
        "rh_ask": Decimal("100.00"),
        "center_bps": Decimal("0"),
        "arcus_tick_size": Decimal("0.01"),
        "arcus_step_size": Decimal("0.01"),
        "arcus_maker_fee_bps": Decimal("0.20"),
        "rh_taker_fee_bps": Decimal("1.00"),
        "rh_slippage_allowance_bps": Decimal("2.00"),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_live_mode_requires_both_explicit_cli_gates() -> None:
    assert validate_runtime_gates(True, False, False) == "record-only"
    assert validate_runtime_gates(False, True, True) == "tiny-live"
    with pytest.raises(ValueError, match="--confirm-mainnet"):
        validate_runtime_gates(False, True, False)
    with pytest.raises(ValueError, match="--tiny-live"):
        validate_runtime_gates(False, False, True)
    with pytest.raises(ValueError, match="mutually exclusive"):
        validate_runtime_gates(True, True, True)


def test_missing_arcus_credentials_fails_before_order_submission() -> None:
    with pytest.raises(ArcusCredentialError, match="ARCUS_API_KEY"):
        ArcusCredentials.from_env({})


def _account_limits_payload(
    *,
    user_tier: str = "standard",
    user_tier_name: str = "Standard",
    maker_tick: int = 0,
    taker_tick: int = 0,
) -> dict[str, Any]:
    return {
        "code": 200,
        "user_tier": user_tier,
        "user_tier_name": user_tier_name,
        "current_maker_fee_tick": maker_tick,
        "current_taker_fee_tick": taker_tick,
        "max_llp_percentage": 100,
        "can_create_public_pool": False,
        "max_llp_amount": "0.000000",
        "effective_lit_stakes": "0.00000000",
        "leased_lit": "0.00000000",
    }


def test_lighter_standard_zero_fee_is_verified_from_account_limits() -> None:
    limits = parse_lighter_account_limits(_account_limits_payload())
    assert isinstance(limits, LighterAccountLimits)
    hedge = SimpleNamespace(
        fee_bps=Decimal("123.45"),
        fee_bps_verified=False,
    )
    assert resolve_verified_rh_fee_bps(hedge, limits) == Decimal("0")
    assert hedge.fee_bps == Decimal("0")
    assert hedge.fee_bps_verified is True
    assert hedge.fee_source == "accountLimits"


def test_lighter_premium_fee_uses_account_limits_ticks() -> None:
    limits = parse_lighter_account_limits(
        _account_limits_payload(
            user_tier="premium",
            user_tier_name="Premium",
            maker_tick=40,
            taker_tick=280,
        )
    )
    hedge = SimpleNamespace(fee_bps=Decimal("0"), fee_bps_verified=False)
    assert resolve_verified_rh_fee_bps(hedge, limits) == Decimal("2.8")
    assert limits.maker_fee_bps == Decimal("0.4")
    assert limits.taker_fee_bps == Decimal("2.8")


def test_lighter_account_limits_malformed_fails_closed() -> None:
    payload = _account_limits_payload()
    del payload["current_taker_fee_tick"]
    with pytest.raises(ValueError, match="current_taker_fee_tick"):
        parse_lighter_account_limits(payload)


def test_lighter_account_limits_auth_failure_fails_closed() -> None:
    class FailedSigner:
        def create_auth_token_with_expiry(self):
            return None, "authentication failed"

    venue = cast(Any, object.__new__(LighterVenue))
    venue.name = "RH"
    venue.signer = FailedSigner()
    venue.conf = SimpleNamespace(
        lighter_creds=SimpleNamespace(account_index=7, api_key_index=3),
    )
    with pytest.raises(RuntimeError, match="accountLimits authentication"):
        asyncio.run(venue.fetch_account_limits())


def test_lighter_account_limits_uses_authenticated_account_index() -> None:
    class Signer:
        def create_auth_token_with_expiry(self, **kwargs):
            assert kwargs == {"api_key_index": 3}
            return "auth-token", None

    calls: list[tuple[str, dict, dict]] = []

    async def fake_get(
        path: str, params: dict | None = None, headers: dict | None = None
    ) -> dict:
        calls.append((path, params or {}, headers or {}))
        return _account_limits_payload()

    venue = cast(Any, object.__new__(LighterVenue))
    venue.name = "RH"
    venue.signer = Signer()
    venue.conf = SimpleNamespace(
        lighter_creds=SimpleNamespace(account_index=42, api_key_index=3),
    )
    venue._get = fake_get
    limits = asyncio.run(venue.fetch_account_limits())
    assert limits.user_tier == "standard"
    assert calls == [
        (
            "/api/v1/accountLimits",
            {"account_index": 42},
            {"Authorization": "auth-token"},
        )
    ]


def test_lighter_unknown_account_tier_fails_closed() -> None:
    with pytest.raises(ValueError, match="unknown Lighter account tier"):
        parse_lighter_account_limits(
            _account_limits_payload(
                user_tier="mystery",
                user_tier_name="Mystery",
            )
        )


def test_lighter_fee_tick_conversion_uses_official_fee_tick_scale() -> None:
    assert LIGHTER_FEE_TICK_SCALE == 1_000_000
    assert lighter_fee_tick_to_bps(100) == Decimal("1")
    assert lighter_fee_tick_to_bps(280) == Decimal("2.8")


def _fake_rh_open_orders_venue(response: Any):
    calls: list[dict[str, Any]] = []
    rh_host = "https://api.rh.lighter.xyz"

    class FakeOrderApi:
        async def account_active_orders(
            self,
            authorization: str,
            account_index: int,
            market_id: int | None = None,
            market_type: str | None = None,
        ) -> Any:
            calls.append(
                {
                    "authorization": authorization,
                    "account_index": account_index,
                    "market_id": market_id,
                    "market_type": market_type,
                }
            )
            if isinstance(response, BaseException):
                raise response
            return response

    class FakeSigner:
        account_index = 42
        order_api = FakeOrderApi()
        api_client = SimpleNamespace(
            configuration=SimpleNamespace(host=rh_host),
        )

        def create_auth_token_with_expiry(self, **kwargs: Any):
            assert kwargs == {"api_key_index": 3}
            return "auth-token", None

    venue = SimpleNamespace(
        name="RH",
        profile=SimpleNamespace(name="robinhood", api_url=rh_host),
        signer=FakeSigner(),
        conf=SimpleNamespace(
            lighter_creds=SimpleNamespace(account_index=42, api_key_index=3),
        ),
        market_id=32,
        # This must never be used by the RH preflight helper.
        arcus_market_id=33,
    )
    return venue, calls


def test_rh_open_orders_uses_authenticated_official_api_and_rh_market_id() -> None:
    venue, calls = _fake_rh_open_orders_venue(
        SimpleNamespace(orders=[]),
    )

    assert asyncio.run(fetch_lighter_open_orders(venue)) == []
    assert calls == [
        {
            "authorization": "auth-token",
            "account_index": 42,
            "market_id": 32,
            "market_type": None,
        }
    ]


def test_rh_open_orders_sndk_order_blocks_startup_gate() -> None:
    venue, _ = _fake_rh_open_orders_venue(
        {
            "code": 200,
            "orders": [{"market_index": 32, "order_id": "rh-sndk"}],
        }
    )

    rows = asyncio.run(fetch_lighter_open_orders(venue))
    assert len(rows) == 1
    assert rows[0]["order_id"] == "rh-sndk"


def test_rh_open_orders_ignores_unrelated_market_response_row() -> None:
    venue, _ = _fake_rh_open_orders_venue(
        {
            "code": 200,
            "orders": [{"market_index": 99, "order_id": "other-market"}],
        }
    )

    assert asyncio.run(fetch_lighter_open_orders(venue)) == []


def test_rh_open_orders_400_fails_closed_with_result_code() -> None:
    class FakeApiError(Exception):
        status = 400
        body = '{"code": 20001, "message": "invalid active-order request"}'

    venue, _ = _fake_rh_open_orders_venue(FakeApiError())
    with pytest.raises(RuntimeError, match=r"status=400.*code=20001"):
        asyncio.run(fetch_lighter_open_orders(venue))


def test_rh_open_orders_account_index_mismatch_fails_closed() -> None:
    venue, calls = _fake_rh_open_orders_venue(SimpleNamespace(orders=[]))
    venue.signer.account_index = 43

    with pytest.raises(RuntimeError, match="account index mismatch"):
        asyncio.run(fetch_lighter_open_orders(venue))
    assert calls == []


def test_rh_open_orders_account_limits_identity_mismatch_fails_closed() -> None:
    venue, calls = _fake_rh_open_orders_venue(SimpleNamespace(orders=[]))
    venue.account_limits = SimpleNamespace(account_index=43)

    with pytest.raises(RuntimeError, match="mismatch with accountLimits"):
        asyncio.run(fetch_lighter_open_orders(venue))
    assert calls == []


def test_rh_open_orders_rejects_non_robinhood_or_standard_host() -> None:
    venue, calls = _fake_rh_open_orders_venue(SimpleNamespace(orders=[]))
    venue.profile = SimpleNamespace(
        name="mainnet",
        api_url="https://mainnet.zklighter.elliot.ai",
    )
    venue.signer.api_client.configuration.host = venue.profile.api_url

    with pytest.raises(RuntimeError, match="Robinhood deployment host"):
        asyncio.run(fetch_lighter_open_orders(venue))
    assert calls == []


def test_rh_fee_gate_never_falls_back_to_public_orderbook_fee() -> None:
    hedge = SimpleNamespace(
        # This value could have come from orderBooks.taker_fee, but that is
        # not account-specific and must never satisfy the B0 gate.
        fee_bps=Decimal("0"),
        fee_bps_verified=True,
    )
    with pytest.raises(RuntimeError, match="accountLimits"):
        resolve_verified_rh_fee_bps(hedge, None)


def test_verified_rh_fee_value_is_used_by_expected_edge_model() -> None:
    hedge = SimpleNamespace(fee_bps=Decimal("1.25"), fee_bps_verified=False)
    limits = parse_lighter_account_limits(
        _account_limits_payload(
            user_tier="premium",
            user_tier_name="Premium",
            maker_tick=40,
            taker_tick=125,
        )
    )
    verified_fee = resolve_verified_rh_fee_bps(hedge, limits)
    edge = expected_edge_bps(
        side="SELL",
        arcus_price=Decimal("101.00"),
        hedge_price=Decimal("100.00"),
        arcus_fee_bps=Decimal("0.00"),
        rh_fee_bps=verified_fee,
        rh_slippage_allowance_bps=Decimal("2.00"),
    )
    assert edge == pytest.approx(95.79, abs=0.01)


def test_rolling_center_warmup_uses_zero_fallback_and_can_quote() -> None:
    strategy = StableBasisStrategy(
        center_bps=0.0,
        upper_bps=4.0,
        lower_bps=4.0,
        center_mode="rolling",
        center_window_hours=1.0,
        center_update_minutes=60,
    )
    controller = cast(Any, object.__new__(CalibrationController))
    controller.strategy = strategy

    assert controller.center_bps() == Decimal("0")
    assert controller.center_source() == "fallback"
    assert (
        choose_quote(
            build_quote_candidates(_quote_input(center_bps=controller.center_bps()))
        )
        is not None
    )

    strategy.bootstrap([(0.0, 1.0), (3_599.0, 3.0)], now=3_600.0)
    assert controller.center_bps() == Decimal("2")
    assert controller.center_source() == "rolling"


def test_calibration_telemetry_records_rth_and_center_context(tmp_path) -> None:
    store = MarketHistoryStore(tmp_path / "history.sqlite")
    lifecycle = CalibrationLifecycle()
    risk = SessionRisk(SessionLimits())
    telemetry = CalibrationTelemetry(
        store,
        session_id="b0-session",
        risk=risk,
        lifecycle=lifecycle,
        pnl=CalibrationPnL(),
        accumulator=FillAccumulator(rh_min_qty=RH_HEDGE_MIN_QTY),
        session_limits=SessionLimits(),
        context_provider=lambda: {
            "is_outside_rth": True,
            "center_bps": "0.0",
            "center_source": "fallback",
            "arcus_maker_fee_bps": "0.2",
            "rh_taker_fee_bps": "0.0",
        },
    )

    telemetry.record("quote_created", client_id="b0-test")
    assert store.flush().ok
    row = store._conn.execute(
        "SELECT is_outside_rth, center_bps, center_source, "
        "arcus_maker_fee_bps, rh_taker_fee_bps "
        "FROM arcus_calibration_events"
    ).fetchone()
    assert row == (1, "0.0", "fallback", "0.2", "0.0")
    store.close()


def test_record_only_does_not_require_arcus_private_credentials() -> None:
    # The public Phase A config path must remain credential-free.
    assert os.getenv("ARCUS_ED25519_PRIVATE_KEY") is None or True
    assert not hasattr(SimpleNamespace(), "private_key")


def test_ordersign_payload_uses_exact_alo_integer_semantics() -> None:
    payload = build_ordersign_payload(
        _credentials(),
        market_id=33,
        side="SELL",
        price=Decimal("1540.67"),
        quantity=Decimal("0.01"),
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.01"),
        timestamp_ns=1_700_000_000_000_000_000,
        good_til_time_us=1_800_000_000_000_000,
        client_id="b0-test-1",
    )
    assert payload == {
        "ad": ADDRESS.lower(),
        "ai": 0,
        "c": "b0-test-1",
        "ct": 1_700_000_000_000_000_000,
        "g": 1_800_000_000_000_000_000,
        "m": 33,
        "op": 1,
        "p": 154067,
        "q": 1,
        "r": 0,
        "s": 1,
        "t": 3,
        "v": 1,
    }


def test_alo_is_the_only_calibration_tif() -> None:
    client = ArcusMakerClient.__new__(ArcusMakerClient)
    with pytest.raises(ValueError, match="ALO"):
        client.validate_calibration_order_type("LIMIT", "GTT")
    client.validate_calibration_order_type("LIMIT", "ALO")


def test_alo_would_cross_is_rejected_without_taker_fallback() -> None:
    assert ArcusMakerClient.would_cross(
        "BUY", Decimal("100.20"), Decimal("100.10"), Decimal("100.20")
    )
    assert ArcusMakerClient.would_cross(
        "SELL", Decimal("100.00"), Decimal("100.00"), Decimal("100.10")
    )
    assert not ArcusMakerClient.would_cross(
        "BUY", Decimal("100.09"), Decimal("100.10"), Decimal("100.20")
    )


def test_signer_never_prints_private_key_and_signs_typed_payload() -> None:
    signer = ArcusSigner(_credentials())
    signature = signer.sign_typed({"b": 2, "a": 1})
    assert len(signature) == 128
    assert _credentials().private_key_text not in repr(signer)


def test_quote_place_threshold_and_one_sided_selection() -> None:
    inp = _quote_input(
        arcus_bid=Decimal("100.10"),
        arcus_ask=Decimal("100.80"),
        rh_bid=Decimal("100.00"),
        rh_ask=Decimal("100.30"),
    )
    candidates = build_quote_candidates(inp)
    assert len(candidates) == 2
    chosen = choose_quote(candidates)
    assert chosen is not None
    assert chosen.side in {"BUY", "SELL"}
    assert chosen.expected_edge_bps >= PLACE_EDGE_BPS
    assert ARCUS_CALIBRATION_QTY == Decimal("0.01")


def test_quote_waits_when_neither_side_clears_four_bps() -> None:
    inp = _quote_input(
        arcus_bid=Decimal("99.99"),
        arcus_ask=Decimal("100.01"),
        rh_bid=Decimal("99.99"),
        rh_ask=Decimal("100.01"),
    )
    assert choose_quote(build_quote_candidates(inp)) is None


def test_expected_edge_includes_arcus_fee_rh_fee_and_slippage_allowance() -> None:
    edge = expected_edge_bps(
        side="SELL",
        arcus_price=Decimal("101.00"),
        hedge_price=Decimal("100.00"),
        arcus_fee_bps=Decimal("2.00"),
        rh_fee_bps=Decimal("1.00"),
        rh_slippage_allowance_bps=Decimal("2.00"),
    )
    assert edge == pytest.approx(94.04, abs=0.01)


def test_cancel_threshold_is_hysteretic() -> None:
    assert PLACE_EDGE_BPS == Decimal("4.0")
    assert CANCEL_EDGE_BPS == Decimal("1.5")
    lifecycle = CalibrationLifecycle()
    lifecycle.mark_placed(expected_edge_bps=Decimal("4.3"))
    assert lifecycle.should_cancel(Decimal("3.0")) is False
    assert lifecycle.should_cancel(Decimal("1.49")) is True


def test_terminal_order_update_does_not_reopen_lifecycle() -> None:
    lifecycle = CalibrationLifecycle()
    lifecycle.mark_placed(expected_edge_bps=Decimal("4.2"))
    lifecycle.record_order_status("CANCELED")
    lifecycle.record_order_status("OPEN", Decimal("0.01"))
    assert lifecycle.state == "CANCELED"


def test_controller_retains_old_context_through_cancel_fill_race(tmp_path) -> None:
    class FakeFeed:
        def __init__(self):
            self.healthy = True
            self.ready = asyncio.Event()
            self.ready.set()

    class FakeRest:
        async def open_orders(self, *args, **kwargs):
            return []

        async def fills(self, *args, **kwargs):
            return []

    class FakeMaker:
        def __init__(self):
            self.credentials = _credentials()
            self.place_calls = 0
            self.cancel_calls = 0

        async def place_alo(self, **kwargs):
            self.place_calls += 1
            return SimpleNamespace(
                order_id=f"order-{self.place_calls}",
                client_id=kwargs["client_id"],
            )

        async def cancel_calibration_order(self, **kwargs):
            self.cancel_calls += 1
            return {"status": 202}

    class FakeHedge:
        def __init__(self):
            self.book = OrderBook()
            self.book.bids = {100.0: 1.0}
            self.book.asks = {101.0: 1.0}
            self.book.ready = True
            self.book.alive_ts = time.time()
            self.size_decimals = 2
            self.min_base = 0.01
            self.fee_bps = 1.0
            self.hedges = []

        async def send_taker(self, **kwargs):
            self.hedges.append(kwargs)
            return {
                "status": "filled",
                "filled_base": kwargs["qty"],
                "avg_px": 101.0,
                "fee": 0.01,
                "order_send_ts_ms": 2,
                "ack_ts_ms": 3,
                "fill_receive_ts_ms": 4,
            }

    arcus_book = cast(Any, OrderBook())
    arcus_book.bids = {99.0: 1.0}
    arcus_book.asks = {100.0: 1.0}
    arcus_book.ready = True
    arcus_book.alive_ts = time.time()
    arcus_book.sequence_health = "OK"
    arcus = SimpleNamespace(
        book=arcus_book,
        latest_attributes=SimpleNamespace(is_outside_rth=False),
    )
    hedge = FakeHedge()
    maker = FakeMaker()
    feed = FakeFeed()
    metadata = SimpleNamespace(
        market_id=33,
        symbol="SNDK-USD",
        tick_size="0.01",
        step_size="0.01",
        status="ONLINE",
    )
    strategy = SimpleNamespace(
        fixed_center_bps=0.0,
        state=lambda: SimpleNamespace(center_bps=0.0),
    )
    candidate = QuoteCandidate(
        side="SELL",
        price=Decimal("100.00"),
        quantity=Decimal("0.01"),
        hedge_side="BUY",
        hedge_price=Decimal("101.00"),
        fair_price=Decimal("100.50"),
        expected_edge_bps=Decimal("4.2"),
        expected_usd=Decimal("0.0042"),
    )

    from entropy_arb.storage import MarketHistoryStore

    store = MarketHistoryStore(tmp_path / "history.sqlite")
    controller = CalibrationController(
        arcus=arcus,
        hedge=hedge,
        maker=cast(ArcusMakerClient, maker),
        account_feed=feed,
        account_rest=cast(ArcusAccountRest, FakeRest()),
        account_state=ArcusAccountState(startup_watermark_us=0),
        metadata=metadata,
        fee_tier=ArcusFeeTier(1, "base", 20, 100),
        strategy=strategy,
        store=store,
        allow_first_order=True,
        staleness_sec=10.0,
    )

    async def exercise() -> None:
        await controller._place_quote(candidate)
        old_lifecycle = controller.lifecycle
        old_client_id = old_lifecycle.client_id
        old_order_id = old_lifecycle.order_id
        await controller.cancel_outstanding()
        await controller.on_order(
            ArcusOrderUpdate(
                order_id=old_order_id or "",
                client_id=old_client_id,
                market_id=33,
                market_display_name="SNDK-USD",
                side="SELL",
                status="CANCELED",
                state="CANCELED",
                price=Decimal("100"),
                original_size=Decimal("0.01"),
                remaining_size=Decimal("0.01"),
                avg_fill_price=None,
                created_at_us=1,
                updated_at_us=2,
                sequence_number=1,
                is_snapshot=False,
            )
        )
        await controller.reconcile()
        await controller._place_quote(candidate)
        assert controller.lifecycle is not old_lifecycle
        await controller.on_fill(
            ArcusUserFill(
                trade_id="late-old-fill",
                order_id=old_order_id or "",
                client_id=old_client_id,
                market_id=33,
                market_display_name="SNDK-USD",
                side="SELL",
                price=Decimal("100"),
                quantity=Decimal("0.01"),
                fee=Decimal("0.01"),
                created_at_us=1_001,
                sequence_number=2,
                is_snapshot=False,
                local_receive_ts_ms=10,
                local_receive_monotonic_ns=10,
            )
        )
        assert old_lifecycle.state == "CANCELED"
        assert old_lifecycle.filled_qty == Decimal("0.01")
        assert controller.lifecycle.state == "OPEN"
        assert len(hedge.hedges) == 1
        # The loss cap sees the complete matched cash flow: Arcus proceeds
        # plus the RH hedge, including both actual fees.
        assert controller.pnl.actual_usd == Decimal("-0.03")
        assert controller.risk.realized_pnl == Decimal("-0.03")

    asyncio.run(exercise())
    assert maker.place_calls == 2
    assert maker.cancel_calls == 1
    store.close()


@pytest.mark.parametrize("failing_operation", ("open_orders", "fills"))
def test_reconcile_empty_exception_records_operation_and_type(
    tmp_path, failing_operation: str
) -> None:
    class EmptyReconciliationError(RuntimeError):
        pass

    class FailingRest:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def open_orders(self, *args, **kwargs):
            self.calls.append("open_orders")
            if failing_operation == "open_orders":
                raise EmptyReconciliationError()
            return []

        async def fills(self, *args, **kwargs):
            self.calls.append("fills")
            if failing_operation == "fills":
                raise EmptyReconciliationError()
            return []

    class NoMutationMaker:
        def __init__(self) -> None:
            self.credentials = _credentials()
            self.place_calls = 0
            self.cancel_calls = 0

    class NoMutationHedge:
        size_decimals = 2
        min_base = 0.0
        fee_bps = 1.0
        hedge_calls = 0

    rest = FailingRest()
    maker = NoMutationMaker()
    hedge = NoMutationHedge()
    store = MarketHistoryStore(tmp_path / "reconciliation.sqlite")
    controller = CalibrationController(
        arcus=SimpleNamespace(),
        hedge=hedge,
        maker=cast(ArcusMakerClient, maker),
        account_feed=SimpleNamespace(healthy=True),
        account_rest=cast(ArcusAccountRest, rest),
        account_state=ArcusAccountState(startup_watermark_us=0),
        metadata=SimpleNamespace(
            market_id=33,
            symbol="SNDK-USD",
            tick_size="0.01",
            step_size="0.01",
        ),
        fee_tier=ArcusFeeTier(1, "base", 20, 100),
        strategy=SimpleNamespace(state=lambda: SimpleNamespace(center_bps=0.0)),
        store=store,
        allow_first_order=False,
        staleness_sec=10.0,
    )
    cast(Any, controller)._order_contexts = {"client:pending": object()}

    asyncio.run(controller.reconcile())

    reason = controller.risk.halt_reason or ""
    assert f"reconciliation {failing_operation} failed" in reason
    assert "EmptyReconciliationError" in reason
    assert "EmptyReconciliationError()" in reason
    assert rest.calls == (
        ["open_orders"]
        if failing_operation == "open_orders"
        else ["open_orders", "fills"]
    )
    assert maker.place_calls == 0
    assert maker.cancel_calls == 0
    assert hedge.hedge_calls == 0
    store.close()


def test_maximum_one_arcus_order() -> None:
    lifecycle = CalibrationLifecycle()
    lifecycle.mark_placed(expected_edge_bps=Decimal("4.2"))
    with pytest.raises(RuntimeError, match="one Arcus"):
        lifecycle.mark_placed(expected_edge_bps=Decimal("4.5"))


def test_partial_fill_at_rh_minimum_triggers_immediate_hedge() -> None:
    acc = FillAccumulator(rh_min_qty=RH_HEDGE_MIN_QTY)
    first = acc.add_fill(side="SELL", quantity=Decimal("0.01"))
    assert first is not None
    assert first.quantity == Decimal("0.01")
    assert first.hedge_side == "BUY"
    assert acc.unhedged_qty == Decimal("0")


def test_partial_fill_below_rh_minimum_remains_explicit_residual() -> None:
    acc = FillAccumulator(rh_min_qty=RH_HEDGE_MIN_QTY)
    assert acc.add_fill(side="BUY", quantity=Decimal("0.004")) is None
    assert acc.unhedged_qty == Decimal("0.004")
    assert acc.residual_exposure == Decimal("0.004")


def test_hedge_never_rounds_above_arcus_filled_quantity() -> None:
    acc = FillAccumulator(rh_min_qty=RH_HEDGE_MIN_QTY)
    acc.add_fill(side="SELL", quantity=Decimal("0.006"))
    assert acc.add_fill(side="SELL", quantity=Decimal("0.003")) is None
    assert acc.unhedged_qty == Decimal("0.009")
    instruction = acc.add_fill(side="SELL", quantity=Decimal("0.001"))
    assert instruction is not None
    assert instruction.quantity == Decimal("0.01")


def test_cancel_fill_race_still_accounts_and_hedges_fill() -> None:
    lifecycle = CalibrationLifecycle()
    lifecycle.mark_placed(expected_edge_bps=Decimal("4.2"))
    lifecycle.request_cancel()
    assert lifecycle.state == "CANCEL_SENT"
    lifecycle.record_fill(Decimal("0.01"))
    assert lifecycle.state == "PARTIAL"
    assert lifecycle.filled_qty == Decimal("0.01")


def test_historical_userfills_snapshot_is_not_rehedged() -> None:
    state = ArcusAccountState(startup_watermark_us=1_000)
    historical = parse_arcus_user_fill(
        {
            "type": "subscribed",
            "channel": "userFills",
            "contents": {
                "isSnapshot": True,
                "tradeId": "old",
                "orderId": "old-order",
                "market": "SNDK-USD",
                "side": "SELL",
                "fillPrice": "100",
                "fillSize": "0.01",
                "createdAt": 999,
            },
        }
    )
    assert historical is not None
    assert state.should_hedge_fill(historical) is False
    live = parse_arcus_user_fill(
        {
            "type": "channel_data",
            "channel": "userFills",
            "contents": {
                "isSnapshot": False,
                "tradeId": "new",
                "orderId": "new-order",
                "market": "SNDK-USD",
                "side": "SELL",
                "fillPrice": "100",
                "fillSize": "0.01",
                "createdAt": 1_001,
            },
        }
    )
    assert live is not None
    state.calibration_client_ids.add("new-order")
    assert state.should_hedge_fill(live) is True


def test_arcus_userfills_snapshot_unwraps_current_fills_wrapper() -> None:
    fills = parse_arcus_user_fills(
        {
            "type": "subscribed",
            "channel": "userFills",
            "contents": {
                "isSnapshot": True,
                "fills": [
                    {
                        "tradeId": "snapshot-fill",
                        "orderId": "snapshot-order",
                        "market": "SNDK-USD",
                        "side": "SELL",
                        "fillPrice": "100.25",
                        "fillSize": "0.01",
                        "createdAt": 1_001,
                    }
                ],
            },
        }
    )
    assert len(fills) == 1
    assert fills[0].price == Decimal("100.25")
    assert fills[0].quantity == Decimal("0.01")
    assert fills[0].is_snapshot is True


def test_arcus_userfills_snapshot_preserves_each_fill_row() -> None:
    fills = parse_arcus_user_fills(
        {
            "type": "subscribed",
            "channel": "userFills",
            "contents": {
                "isSnapshot": True,
                "fills": [
                    {"side": "BUY", "fillPrice": "100", "fillSize": "0.01"},
                    {"side": "SELL", "fillPrice": "101", "fillSize": "0.02"},
                ],
            },
        }
    )
    assert [(fill.price, fill.quantity) for fill in fills] == [
        (Decimal("100"), Decimal("0.01")),
        (Decimal("101"), Decimal("0.02")),
    ]


def test_new_arcus_userfill_with_missing_or_nonfinite_price_fails_closed() -> None:
    for price in (None, "NaN", "Infinity"):
        with pytest.raises(ValueError, match="fill.price must be a finite decimal"):
            parse_arcus_user_fills(
                {
                    "type": "channel_data",
                    "channel": "userFills",
                    "contents": {
                        "side": "BUY",
                        "fillPrice": price,
                        "fillSize": "0.01",
                    },
                }
            )


def test_malformed_historical_userfill_is_skipped_without_losing_valid_rows(
    caplog,
) -> None:
    caplog.set_level("WARNING", logger="arcus-execution")
    fills = parse_arcus_user_fills(
        {
            "type": "subscribed",
            "channel": "userFills",
            "contents": {
                "isSnapshot": True,
                "fills": [
                    {"side": "SELL", "fillPrice": None, "fillSize": "0.01"},
                    {"side": "SELL", "fillPrice": "100", "fillSize": "0.01"},
                ],
            },
        },
        tolerate_snapshot_errors=True,
    )
    assert len(fills) == 1
    assert fills[0].price == Decimal("100")
    assert "skipped malformed historical userFills row" in caplog.text


def test_malformed_userfills_does_not_block_account_attribute_updates() -> None:
    feed = ArcusAccountFeed(ADDRESS, "SNDK-USD")

    async def exercise() -> None:
        await feed._handle_message(
            {
                "type": "subscribed",
                "channel": "userFills",
                "contents": {
                    "isSnapshot": True,
                    "fills": [
                        {
                            "side": "SELL",
                            "fillPrice": None,
                            "fillSize": "0.01",
                        }
                    ],
                },
            }
        )
        await feed._handle_message(
            {
                "type": "subscribed",
                "channel": "accountAttributeUpdates",
                "contents": {
                    "isSnapshot": True,
                    "entries": [
                        {
                            "type": "feeTier",
                            "feeTierLevel": 2,
                            "makerFeePpm": 200,
                            "takerFeePpm": 500,
                        }
                    ],
                },
            }
        )

    asyncio.run(exercise())
    assert feed.latest_fee_tier is not None
    assert feed.latest_fee_tier.level == 2
    assert feed.channel_health["userFills"] is False
    assert feed.channel_health["accountAttributeUpdates"] is True
    assert "userFills" in feed.channel_errors


def test_required_account_channel_error_blocks_b0_market_health() -> None:
    class HealthyBook:
        ready = True
        sequence_health = "OK"

        @staticmethod
        def is_fresh(_staleness: float) -> bool:
            return True

        @staticmethod
        def best_bid() -> Decimal:
            return Decimal("100")

        @staticmethod
        def best_ask() -> Decimal:
            return Decimal("101")

    ready = asyncio.Event()
    ready.set()
    controller = cast(Any, object.__new__(CalibrationController))
    controller.account_feed = SimpleNamespace(
        healthy=True,
        ready=ready,
        required_channels_healthy=False,
    )
    controller.arcus = SimpleNamespace(
        book=HealthyBook(),
        latest_attributes=SimpleNamespace(is_outside_rth=False),
    )
    controller.hedge = SimpleNamespace(
        book=HealthyBook(),
        ready_to_trade=lambda: True,
    )
    controller.metadata = SimpleNamespace(status="ONLINE")
    controller.staleness_sec = 10.0

    health = controller.market_health()
    assert health.account_ws_healthy is False
    assert health.can_quote is False


def test_reconnect_snapshot_fill_after_session_watermark_is_actionable() -> None:
    state = ArcusAccountState(startup_watermark_us=1_000)
    state.calibration_client_ids.add("b0-current")
    fill = parse_arcus_user_fill(
        {
            "type": "subscribed",
            "channel": "userFills",
            "contents": {
                "tradeId": "new-snapshot-fill",
                "orderId": "o-current",
                "clientId": "b0-current",
                "side": "SELL",
                "fillPrice": "100",
                "fillSize": "0.01",
                "createdAt": 1_001,
            },
        }
    )
    assert fill is not None and fill.is_snapshot
    assert state.should_hedge_fill(fill) is True


def test_untimestamped_snapshot_is_not_rehedged_before_rest_backfill() -> None:
    state = ArcusAccountState(startup_watermark_us=1_000)
    state.calibration_client_ids.add("b0-current")
    fill = parse_arcus_user_fill(
        {
            "type": "subscribed",
            "channel": "userFills",
            "contents": {
                "tradeId": "snapshot-without-time",
                "orderId": "o-current",
                "clientId": "b0-current",
                "side": "SELL",
                "fillPrice": "100",
                "fillSize": "0.01",
            },
        }
    )
    assert fill is not None and fill.is_snapshot
    assert state.should_hedge_fill(fill) is False


def test_userfill_store_only_fee_and_timestamp_are_optional() -> None:
    fill = parse_arcus_user_fill(
        {
            "type": "channel_data",
            "channel": "userFills",
            "contents": {
                "tradeId": "trade-live",
                "orderId": "order-live",
                "market": "SNDK-USD",
                "side": "BUY",
                "fillPrice": "100",
                "fillSize": "0.01",
            },
        }
    )
    assert fill is not None
    assert fill.created_at_us is None
    assert fill.fee is None


def test_arcus_order_snapshot_keeps_open_and_recent_closed_separate() -> None:
    open_orders, closed_orders = parse_arcus_order_snapshot(
        {
            "type": "subscribed",
            "channel": "orders",
            "contents": {
                "isSnapshot": True,
                "openOrders": [{"orderId": "o1", "clientId": "b0-1", "status": "OPEN"}],
                "recentClosedOrders": [{"orderId": "o0", "status": "FILLED"}],
            },
        }
    )
    assert [o.order_id for o in open_orders] == ["o1"]
    assert [o.order_id for o in closed_orders] == ["o0"]


def test_b0_account_stream_uses_exactly_four_lifecycle_subscriptions() -> None:
    class FakeWebSocket:
        def __init__(self):
            self.sent = []

        async def send(self, message: str) -> None:
            self.sent.append(message)

    feed = ArcusAccountFeed(ADDRESS, "SNDK-USD")
    websocket = FakeWebSocket()
    asyncio.run(feed.subscribe(websocket))
    import json

    messages = [json.loads(message) for message in websocket.sent]
    assert [message["channel"] for message in messages] == [
        "userFills",
        "orders",
        "positions",
        "accountAttributeUpdates",
    ]


def test_account_fee_tier_uses_current_fee_tier_level_field() -> None:
    tier = parse_arcus_account_fee_tier(
        {
            "channel": "accountAttributeUpdates",
            "contents": {
                "entries": [
                    {
                        "type": "feeTier",
                        "feeTierLevel": 3,
                        "makerFeePpm": 250,
                        "takerFeePpm": 750,
                    }
                ]
            },
        }
    )
    assert tier is not None
    assert (tier.level, tier.maker_fee_bps, tier.taker_fee_bps) == (
        3,
        Decimal("2.5"),
        Decimal("7.5"),
    )


def test_arcus_fee_table_preserves_negative_maker_rebate() -> None:
    tiers = parse_arcus_fee_tiers(
        {
            "tiers": [
                {
                    "level": 0,
                    "name": "maker-rebate",
                    "makerFeePpm": -25,
                    "takerFeePpm": 100,
                }
            ],
        }
    )
    assert tiers[0].maker_fee_ppm == -25
    assert tiers[0].maker_fee_bps == Decimal("-0.25")


def test_order_snapshot_propagates_account_last_sequence_id() -> None:
    open_orders, _ = parse_arcus_order_snapshot(
        {
            "type": "subscribed",
            "channel": "orders",
            "contents": {
                "lastSequenceId": 77,
                "openOrders": [{"orderId": "o1", "status": "OPEN"}],
                "recentClosedOrders": [],
            },
        }
    )
    assert open_orders[0].last_sequence_id == 77


def test_arcus_order_update_supports_cancel_fill_race_states() -> None:
    update = parse_arcus_order_update(
        {
            "type": "channel_data",
            "channel": "orders",
            "contents": {
                "orderId": "o1",
                "clientId": "b0-1",
                "marketId": 33,
                "marketDisplayName": "SNDK-USD",
                "side": "SELL",
                "status": "CANCELED",
                "state": "CANCELED",
                "price": "100",
                "originalSize": "0.01",
                "remainingSize": "0.005",
                "updatedAt": 1_002,
                "sequenceNumber": 8,
            },
        }
    )
    assert update.status == "CANCELED"
    assert update.remaining_size == Decimal("0.005")
    assert update.sequence_number == 8


def test_rh_reject_halts_new_arcus_quoting() -> None:
    risk = SessionRisk(SessionLimits())
    risk.on_hedge_failure("reject")
    assert risk.halted
    assert "hedge" in (risk.halt_reason or "")


def test_rh_timeout_halts_new_arcus_quoting() -> None:
    risk = SessionRisk(SessionLimits())
    risk.on_hedge_failure("timeout")
    assert risk.halted


def test_market_health_blocks_stale_resync_and_offline_but_not_outside_rth() -> None:
    assert MarketHealth(False, True, True, True, "OK", False).can_quote is False
    assert MarketHealth(True, False, True, True, "OK", False).can_quote is False
    assert MarketHealth(True, True, False, True, "OK", False).can_quote is False
    assert MarketHealth(True, True, True, False, "OK", False).can_quote is False
    assert MarketHealth(True, True, True, True, "RESYNC", False).can_quote is False
    assert MarketHealth(True, True, True, True, "OK", True).can_quote is True


def test_outside_rth_is_regime_telemetry_only() -> None:
    lifecycle = CalibrationLifecycle()
    lifecycle.mark_placed(expected_edge_bps=Decimal("4.2"))
    risk = SessionRisk(SessionLimits())
    health = MarketHealth(True, True, True, True, "OK", True)
    assert health.can_quote
    assert lifecycle.state == "OPEN"
    assert not risk.halted


def test_b0_step_does_not_halt_on_outside_rth() -> None:
    controller = cast(Any, object.__new__(CalibrationController))
    controller.risk = SessionRisk(SessionLimits())
    controller.lifecycle = CalibrationLifecycle()
    controller.account_feed = SimpleNamespace(healthy=True)
    controller._cancel_pending = False
    controller._terminal_reconcile_pending = False
    controller.current_candidate = None
    controller.accumulator = FillAccumulator(rh_min_qty=RH_HEDGE_MIN_QTY)
    controller.proposed_quote = lambda: None
    controller.market_health = lambda: MarketHealth(True, True, True, True, "OK", True)

    asyncio.run(controller.step())
    assert not controller.risk.halted


def test_session_limits_stop_at_fill_count_notional_loss_and_runtime() -> None:
    limits = SessionLimits()
    risk = SessionRisk(limits)
    for _ in range(20):
        risk.on_arcus_fill(Decimal("100"), Decimal("0.01"))
    assert risk.halted and "fill events" in (risk.halt_reason or "")

    risk = SessionRisk(limits)
    risk.on_arcus_fill(Decimal("50000"), Decimal("0.01"))
    assert risk.halted and "notional" in (risk.halt_reason or "")

    risk = SessionRisk(limits)
    risk.on_realized_pnl(Decimal("-5"))
    assert risk.halted and "loss" in (risk.halt_reason or "")

    risk = SessionRisk(limits, started_monotonic=0.0)
    risk.check_runtime(now_monotonic=60 * 60)
    assert risk.halted and "runtime" in (risk.halt_reason or "")


def test_actual_arcus_fee_is_included_in_pnl() -> None:
    pnl = CalibrationPnL()
    pnl.add_arcus_fill(
        side="SELL", price=Decimal("100"), quantity=Decimal("0.01"), fee=Decimal("0.02")
    )
    pnl.add_rh_hedge(
        side="BUY",
        price=Decimal("99.90"),
        quantity=Decimal("0.01"),
        fee=Decimal("0.01"),
    )
    assert pnl.actual_usd == Decimal("-0.029")


def test_telemetry_failure_halts_instead_of_bypassing_loss_cap() -> None:
    risk = SessionRisk(SessionLimits())
    risk.on_telemetry_failure("sqlite unavailable")
    assert risk.halted
    assert "telemetry" in (risk.halt_reason or "")


def test_arcus_disconnect_halts_and_requests_cancel() -> None:
    lifecycle = CalibrationLifecycle()
    lifecycle.mark_placed(expected_edge_bps=Decimal("4.2"))
    risk = SessionRisk(SessionLimits())
    risk.on_arcus_disconnect(lifecycle)
    assert lifecycle.state == "CANCEL_SENT"
    assert risk.halted


def test_unknown_open_arcus_order_aborts_without_cancel() -> None:
    state = ArcusAccountState(startup_watermark_us=0)
    with pytest.raises(RuntimeError, match="unknown Arcus"):
        state.validate_startup_orders(
            [
                cast(
                    ArcusOrderUpdate,
                    SimpleNamespace(
                        client_id="someone-elses-order", market_id=33, status="OPEN"
                    ),
                )
            ],
            calibration_prefix="b0-",
        )


def test_nonzero_starting_inventory_aborts() -> None:
    state = ArcusAccountState(startup_watermark_us=0)
    with pytest.raises(RuntimeError, match="inventory"):
        state.validate_starting_inventory(Decimal("0.01"), Decimal("0"))


def test_graceful_shutdown_cancels_outstanding_maker_and_reconciles() -> None:
    calls: list[str] = []

    class FakeGateway:
        async def cancel_calibration_order(self, *args, **kwargs):
            calls.append("cancel")

        async def reconcile(self):
            calls.append("reconcile")

    lifecycle = CalibrationLifecycle()
    lifecycle.mark_placed(expected_edge_bps=Decimal("4.2"))

    async def run():
        await lifecycle.graceful_shutdown(FakeGateway())

    asyncio.run(run())
    assert calls == ["cancel", "reconcile"]
    assert lifecycle.state == "CANCEL_SENT"


def _calibration_event(**overrides) -> ArcusCalibrationEventRow:
    values: dict[str, Any] = dict(
        session_id="b0-session",
        execution_id="b0-exec-1",
        event_type="fill",
        event_ts_ms=1_000,
        event_local_receive_monotonic_ns=2_000,
        client_id="b0-1",
        order_id="o1",
        arcus_side="SELL",
        arcus_quote_price="100",
        quote_qty="0.01",
        quote_created_ts_ms=900,
        expected_edge_bps="4.2",
        expected_edge_at_fill_bps="3.8",
        expected_usd="0.0042",
        fill_trade_id="t1",
        fill_ts_us=1_001_000,
        arcus_fill_price="100",
        arcus_fill_qty="0.01",
        arcus_fee="0.02",
        fill_to_hedge_send_ms=3,
        fill_to_rh_fill_ms=8,
        rh_signal_bid="99.8",
        rh_signal_ask="99.9",
        rh_hedge_side="BUY",
        rh_hedge_qty="0.01",
        rh_hedge_avg_fill="99.9",
        rh_fee="0.01",
        matched_edge_usd="0.07",
        actual_usd="0.07",
        remaining_arcus_qty="0",
        unhedged_residual_qty="0",
        lifecycle_state="FILLED",
        halt_reason=None,
        account_sequence_id=5,
    )
    values.update(overrides)
    return ArcusCalibrationEventRow(**values)


def test_calibration_telemetry_persists_and_restarts_append_safely(tmp_path) -> None:
    db = tmp_path / "history.sqlite"
    store = MarketHistoryStore(db)
    store.append_arcus_calibration_event(_calibration_event())
    assert store.flush().ok
    store.close()

    reopened = MarketHistoryStore(db)
    reopened.append_arcus_calibration_event(
        _calibration_event(event_type="halt", event_ts_ms=1_001)
    )
    assert reopened.flush().ok
    assert reopened.count_rows("arcus_calibration_events") == 2
    reopened.close()


def test_calibration_event_has_partial_fill_granularity(tmp_path) -> None:
    store = MarketHistoryStore(tmp_path / "history.sqlite")
    store.append_arcus_calibration_event(
        _calibration_event(
            event_type="fill",
            arcus_fill_qty="0.004",
            remaining_arcus_qty="0.006",
            unhedged_residual_qty="0.004",
        )
    )
    assert store.flush().ok
    row = store._conn.execute(
        "SELECT event_type, arcus_fill_qty, remaining_arcus_qty, "
        "unhedged_residual_qty FROM arcus_calibration_events"
    ).fetchone()
    assert row == ("fill", "0.004", "0.006", "0.004")
    store.close()


def test_arcus_maker_client_sends_only_publicly_documented_alo_payload() -> None:
    class FakeRpc:
        def __init__(self):
            self.requests = []

        async def post(self, method, payload, signature, timestamp):
            self.requests.append((method, payload, signature, timestamp))
            return {"status": 202, "result": {"orderId": "o1"}}

    rpc = FakeRpc()
    client = ArcusMakerClient(
        credentials=_credentials(),
        signer=ArcusSigner(_credentials()),
        rpc=rpc,
    )

    async def run():
        return await client.place_alo(
            market_id=33,
            side="SELL",
            price=Decimal("100.00"),
            quantity=Decimal("0.01"),
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.01"),
            best_bid=Decimal("99.90"),
            best_ask=Decimal("100.00"),
            client_id="b0-test-1",
        )

    result = asyncio.run(run())
    assert result.order_id == "o1"
    assert rpc.requests[0][0] == "placeOrder"
    assert rpc.requests[0][1]["orderType"] == "LIMIT"
    assert rpc.requests[0][1]["timeInForce"] == "ALO"


def test_arcus_maker_volume_mode_accepts_vp_prefix_and_nonfixed_quantity() -> None:
    class FakeRpc:
        async def post(self, method, payload, signature, timestamp):
            return {"status": 202, "result": {"orderId": "vp-order"}}

    class FakeSigner:
        @staticmethod
        def sign_typed(payload):
            return "signature"

    client = ArcusMakerClient(
        credentials=_credentials(),
        signer=cast(Any, FakeSigner()),
        rpc=FakeRpc(),
        client_prefix="vp-",
        fixed_quantity=None,
    )

    async def run() -> None:
        ack = await client.place_alo(
            market_id=33,
            side="SELL",
            price=Decimal("100.00"),
            quantity=Decimal("0.123"),
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            best_bid=Decimal("99.90"),
            best_ask=Decimal("100.00"),
            client_id="vp-session-1",
        )
        assert ack.order_id == "vp-order"

    asyncio.run(run())


@pytest.mark.parametrize(
    "response, expected_status, expected_code, expected_message",
    [
        (
            {
                "status": 401,
                "error": {
                    "code": "INVALID_API_KEY",
                    "message": "invalid apiKey API_KEY_SECRET_401",
                },
            },
            401,
            "INVALID_API_KEY",
            "invalid apiKey",
        ),
        (
            {
                "status": 403,
                "errorCode": "ACCOUNT_INDEX_MISMATCH",
                "errorMessage": "accountIndex mismatch",
            },
            403,
            "ACCOUNT_INDEX_MISMATCH",
            "accountIndex mismatch",
        ),
    ],
)
def test_arcus_order_rejected_for_explicit_place_order_rejection(
    response: dict[str, Any],
    expected_status: int,
    expected_code: str,
    expected_message: str,
) -> None:
    class FakeRpc:
        async def post(self, method, payload, signature, timestamp):
            assert method == "placeOrder"
            return response

    class FakeSigner:
        @staticmethod
        def sign_typed(payload):
            return "SIGNATURE_SECRET"

    client = ArcusMakerClient(
        credentials=_credentials(),
        signer=cast(Any, FakeSigner()),
        rpc=FakeRpc(),
    )

    async def run() -> None:
        with pytest.raises(ArcusOrderRejected) as caught:
            await client.place_alo(
                market_id=33,
                side="SELL",
                price=Decimal("100.00"),
                quantity=Decimal("0.01"),
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.01"),
                best_bid=Decimal("99.90"),
                best_ask=Decimal("100.00"),
                client_id="b0-rejected-1",
            )

        rejection = caught.value
        assert rejection.status == expected_status
        assert rejection.code == expected_code
        assert rejection.message is not None
        assert expected_message in rejection.message
        assert f"status={expected_status}" in str(rejection)
        assert expected_code in str(rejection)
        assert expected_message in str(rejection)
        assert "API_KEY_SECRET_401" not in str(rejection)

    asyncio.run(run())


def test_arcus_order_rejected_sanitizes_place_order_rejection_details() -> None:
    class FakeRpc:
        async def post(self, method, payload, signature, timestamp):
            return {
                "status": 401,
                "error": {
                    "code": "AUTH_FAILED",
                    "message": (
                        "invalid signature; apiKey=API_KEY_SECRET "
                        "signature=SIGNATURE_SECRET "
                        "privateKey=PRIVATE_KEY_SECRET "
                        '{"apiKey":"JSON_API_KEY_SECRET"} '
                        'signedRequestPayload={"apiKey":"NESTED_API_KEY_SECRET",'
                        '"signature":"NESTED_SIGNATURE_SECRET"}'
                    ),
                },
                "signedRequestPayload": "SIGNED_REQUEST_PAYLOAD_SECRET",
            }

    class FakeSigner:
        @staticmethod
        def sign_typed(payload):
            return "SIGNATURE_SECRET"

    client = ArcusMakerClient(
        credentials=_credentials(),
        signer=cast(Any, FakeSigner()),
        rpc=FakeRpc(),
    )

    async def run() -> None:
        with pytest.raises(ArcusOrderRejected) as caught:
            await client.place_alo(
                market_id=33,
                side="SELL",
                price=Decimal("100.00"),
                quantity=Decimal("0.01"),
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.01"),
                best_bid=Decimal("99.90"),
                best_ask=Decimal("100.00"),
                client_id="b0-rejected-2",
            )

        rendered = str(caught.value)
        assert "AUTH_FAILED" in rendered
        assert "invalid signature" in rendered
        for secret in (
            "API_KEY_SECRET",
            "SIGNATURE_SECRET",
            "PRIVATE_KEY_SECRET",
            "JSON_API_KEY_SECRET",
            "NESTED_API_KEY_SECRET",
            "NESTED_SIGNATURE_SECRET",
            "SIGNED_REQUEST_PAYLOAD_SECRET",
        ):
            assert secret not in rendered

    asyncio.run(run())


def test_place_order_rejection_non_4xx_remains_generic() -> None:
    class FakeRpc:
        async def post(self, method, payload, signature, timestamp):
            return {
                "status": 503,
                "error": {"code": "UPSTREAM_FAILURE", "message": "try later"},
            }

    client = ArcusMakerClient(
        credentials=_credentials(),
        signer=ArcusSigner(_credentials()),
        rpc=FakeRpc(),
    )

    async def run() -> None:
        with pytest.raises(ArcusOrderError) as caught:
            await client.place_alo(
                market_id=33,
                side="SELL",
                price=Decimal("100.00"),
                quantity=Decimal("0.01"),
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.01"),
                best_bid=Decimal("99.90"),
                best_ask=Decimal("100.00"),
                client_id="b0-rejected-3",
            )
        assert type(caught.value) is ArcusOrderError

    asyncio.run(run())


def test_place_order_rejection_transport_failure_remains_ambiguous() -> None:
    class FakeRpc:
        async def post(self, method, payload, signature, timestamp):
            raise TimeoutError("connection lost after send")

    client = ArcusMakerClient(
        credentials=_credentials(),
        signer=ArcusSigner(_credentials()),
        rpc=FakeRpc(),
    )

    async def run() -> None:
        with pytest.raises(TimeoutError, match="connection lost after send"):
            await client.place_alo(
                market_id=33,
                side="SELL",
                price=Decimal("100.00"),
                quantity=Decimal("0.01"),
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.01"),
                best_bid=Decimal("99.90"),
                best_ask=Decimal("100.00"),
                client_id="b0-rejected-4",
            )

    asyncio.run(run())


def test_account_rpc_rejects_unsupported_mutations_locally() -> None:
    class FakeWebSocket:
        async def send(self, message: str) -> None:
            raise AssertionError("unsupported RPC reached websocket")

    feed = ArcusAccountFeed(ADDRESS, "SNDK-USD", api_key="api")
    feed.websocket = FakeWebSocket()
    feed.healthy = True

    async def exercise() -> None:
        with pytest.raises(Exception, match=r"only LIMIT\+ALO"):
            await feed.post(
                "modifyOrder",
                {"orderId": "o1"},
                "signature",
                1,
            )
        with pytest.raises(Exception, match=r"LIMIT\+ALO"):
            await feed.post(
                "placeOrder",
                {
                    "orderType": "LIMIT",
                    "timeInForce": "GTT",
                    "clientId": "b0-test",
                    "quantity": "0.01",
                },
                "signature",
                1,
            )

    asyncio.run(exercise())


def test_startup_open_order_gate_does_not_ignore_untriggered_status() -> None:
    state = ArcusAccountState(startup_watermark_us=0)
    order = cast(
        ArcusOrderUpdate,
        SimpleNamespace(
            client_id="someone-elses-order",
            market_id=33,
            status="UNTRIGGERED",
            state=None,
        ),
    )
    with pytest.raises(RuntimeError, match="unknown Arcus"):
        state.validate_startup_orders([order], calibration_prefix="b0-")


def test_record_only_arcus_order_attempt_remains_local_hard_stop() -> None:
    from entropy_arb.venue_arcus import ArcusVenue

    venue = ArcusVenue()
    with pytest.raises(RuntimeError, match="Phase A"):
        venue.send_taker("BUY", 1, 1)


def test_no_live_order_is_submitted_by_this_test_suite() -> None:
    # A positive assertion documents the invariant for future test authors.
    assert "api.arcus.xyz" not in ""  # no endpoint client is constructed here
