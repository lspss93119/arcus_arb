"""Engine signal math: midline band directions, inventory ladder, scan.

Run:  python3 -m pytest tests/  (or  python3 tests/test_engine.py)
"""

import asyncio
import csv
import logging
import os
import sys
import tempfile
import time
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.arcus import ArcusBBO, ArcusBookSnapshot, ArcusBookUpdate  # noqa: E402
from entropy_arb.arcus_book import ArcusOrderBook  # noqa: E402
from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.config import load_config  # noqa: E402
from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.premium import calculate_premiums  # noqa: E402
from entropy_arb.storage import MarketHistoryStore  # noqa: E402

NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def make_cfg(
    midline=5.0,
    upper=4.0,
    lower=3.0,
    *,
    hedge_venue="lighter-rh",
    recorder_enabled=True,
    strategy_name="stable_basis",
    window_minutes=60,
    center_mode="fixed",
    center_window_hours=12,
    center_update_minutes=60,
):
    if strategy_name == "stable_basis":
        rolling_params = ""
        if center_mode != "fixed":
            rolling_params = f"""    center_mode: {center_mode}
    center_window_hours: {center_window_hours}
    center_update_minutes: {center_update_minutes}
"""
        strategy_yaml = f"""
strategy:
  name: stable_basis
  params:
{rolling_params}    center_bps: {midline}
    upper_bps: {upper}
    lower_bps: {lower}
"""
    elif strategy_name == "drifting_basis":
        strategy_yaml = f"""
strategy:
  name: drifting_basis
  params:
    window_minutes: {window_minutes}
    upper_bps: {upper}
    lower_bps: {lower}
"""
    else:
        raise ValueError(strategy_name)

    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(
        strategy_yaml
        + f"""
execution:
  premium_persist_sec: 0.0
recorder:
  enabled: {str(recorder_enabled).lower()}
  database: {os.path.join(tempfile.gettempdir(), "engine-market-history.sqlite")}
"""
    )
    f.close()
    return load_config(f.name, NO_ENV, symbol="SNDK", hedge_venue=hedge_venue)


class StubVenue:
    def __init__(self, key, label, cap=10000.0, fee=0.0):
        self.key, self.name = key, label
        self.cap_usd, self.fee_bps = cap, fee
        self.size_decimals, self.min_base, self.min_quote = 4, 1e-4, 10.0
        self.position, self.cash = 0.0, 0.0
        self.orders_per_min = 30
        self.last_traded_ts = 0.0
        self.book = OrderBook()

    def ready_to_trade(self):
        return True

    def set_book(self, bid, ask, sz=50.0):
        self.book.apply_hl(
            [[{"px": str(bid), "sz": str(sz)}], [{"px": str(ask), "sz": str(sz)}]]
        )


class SettlementVenue(StubVenue):
    def __init__(self, key, label, responses, *, fee=0.0):
        super().__init__(key, label, fee=fee)
        self.volume_usd = 0.0
        self.responses = list(responses)

    def px_round(self, px, round_up):
        return px

    async def send_taker(self, **kwargs):
        assert self.responses, f"unexpected send_taker on {self.key}"
        return self.responses.pop(0)


def execution_plan(qty=1.0, buy_px=100.0, sell_px=101.0, buy_fee=0.001, sell_fee=0.002):
    return SimpleNamespace(
        qty=qty,
        buy_limit=buy_px,
        sell_limit=sell_px,
        buy_notional=qty * buy_px,
        sell_notional=qty * sell_px,
        q_max=qty,
        q_max_notional=qty * buy_px,
        marginal_premium_bps=(sell_px / buy_px - 1.0) * 1e4,
        buy_fee=buy_fee,
        sell_fee=sell_fee,
        exp_edge_usd=(
            qty * sell_px * (1.0 - sell_fee) - qty * buy_px * (1.0 + buy_fee)
        ),
        gross_edge_usd=qty * (sell_px - buy_px),
    )


def settlement_info(status, filled_base, avg_px=None, *, unresolved=False, err=None):
    return {
        "status": status,
        "filled_base": filled_base,
        "avg_px": avg_px,
        "err": err,
        "unresolved": unresolved,
    }


def make_settlement_engine(
    buy_responses, sell_responses, *, buy_fee=10.0, sell_fee=20.0, telemetry_path=None
):
    eng = Engine(make_cfg())
    if telemetry_path is not None:
        eng.execution_telemetry_csv = str(telemetry_path)
    eng.hedge = SettlementVenue("hedge", "RH", buy_responses, fee=buy_fee)
    eng.entropy = SettlementVenue("entropy", "ENTROPY", sell_responses, fee=sell_fee)
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    eng.hedge.set_book(99.0, 100.0)
    eng.entropy.set_book(101.0, 102.0)
    setattr(eng, "_log_csv", lambda *args, **kwargs: None)
    return eng


def execution_rows(path):
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def make_engine(**thr):
    cfg = make_cfg(**thr)
    eng = Engine(cfg)
    eng.entropy = StubVenue("entropy", "ENTROPY")
    eng.hedge = StubVenue("hedge", "RH")
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    return eng


def approx(a, b, tol=1e-9):
    assert abs(a - b) <= tol, f"{a} != {b}"


class _VolumeProbeStartupBook:
    def __init__(
        self,
        *,
        ready: bool = True,
        sequence_health: str = "OK",
        first_delta_after_snapshot: bool = False,
        bid: str = "100",
        ask: str = "101",
        fresh: bool = True,
    ) -> None:
        self.ready = ready
        self.sequence_health = sequence_health
        self.first_delta_after_snapshot = first_delta_after_snapshot
        self.bid = Decimal(bid)
        self.ask = Decimal(ask)
        self.fresh = fresh

    def best_bid(self) -> Decimal:
        return self.bid

    def best_ask(self) -> Decimal:
        return self.ask

    def is_fresh(self, _max_age_sec: float) -> bool:
        return self.fresh


def _volume_probe_startup_engine(
    *, arcus_book: Any | None = None
) -> tuple[Engine, Any]:
    engine = Engine(make_cfg(recorder_enabled=False))
    engine.arcus = SimpleNamespace(
        book=arcus_book or _VolumeProbeStartupBook(),
    )
    engine.hedge = SimpleNamespace(book=_VolumeProbeStartupBook())
    ready = asyncio.Event()
    ready.set()
    account_feed = SimpleNamespace(
        healthy=True,
        ready=ready,
        required_channels_healthy=True,
    )
    return engine, account_feed


def _arcus_startup_snapshot() -> ArcusOrderBook:
    book = ArcusOrderBook()
    book.apply_snapshot(
        ArcusBookSnapshot(
            bids=(("100", "1"),),
            asks=(("101", "1"),),
            last_sequence_id=100,
            global_sequence_id=1,
            exchange_timestamp_us=None,
        ),
        local_receive_ts_ms=int(time.time() * 1000),
        local_receive_monotonic_ns=1,
    )
    return book


def _arcus_startup_delta(sequence: int) -> ArcusBookUpdate:
    return ArcusBookUpdate(
        bids=(("100", "1"),),
        asks=(("101", "1"),),
        last_sequence_id=sequence,
        global_sequence_id=sequence,
        exchange_timestamp_us=None,
    )


def test_volume_probe_startup_snapshot_alone_does_not_satisfy_barrier() -> None:
    arcus_book = _arcus_startup_snapshot()
    assert arcus_book.first_delta_after_snapshot
    engine, account_feed = _volume_probe_startup_engine(arcus_book=arcus_book)

    with pytest.raises(RuntimeError, match="first_delta_after_snapshot=True"):
        asyncio.run(engine._wait_volume_probe_book_stable(account_feed, timeout=0.0))


def test_volume_probe_startup_contiguous_first_delta_satisfies_barrier() -> None:
    arcus_book = _arcus_startup_snapshot()
    assert arcus_book.apply_update(
        _arcus_startup_delta(101),
        local_receive_ts_ms=int(time.time() * 1000),
        local_receive_monotonic_ns=2,
    )
    engine, account_feed = _volume_probe_startup_engine(arcus_book=arcus_book)

    asyncio.run(engine._wait_volume_probe_book_stable(account_feed, timeout=0.0))


def test_volume_probe_startup_boundary_does_not_satisfy_barrier() -> None:
    arcus_book = _arcus_startup_snapshot()
    assert arcus_book.apply_update(
        _arcus_startup_delta(105),
        local_receive_ts_ms=int(time.time() * 1000),
        local_receive_monotonic_ns=2,
    )
    assert arcus_book.sequence_health == "BOUNDARY"
    engine, account_feed = _volume_probe_startup_engine(arcus_book=arcus_book)

    with pytest.raises(RuntimeError, match="arcus_sequence=BOUNDARY"):
        asyncio.run(engine._wait_volume_probe_book_stable(account_feed, timeout=0.0))


def test_volume_probe_startup_matching_bbo_after_boundary_allows_barrier() -> None:
    arcus_book = _arcus_startup_snapshot()
    assert arcus_book.apply_update(
        _arcus_startup_delta(105),
        local_receive_ts_ms=int(time.time() * 1000),
        local_receive_monotonic_ns=2,
    )
    assert arcus_book.sequence_health == "BOUNDARY"
    assert arcus_book.apply_bbo(
        ArcusBBO(
            best_bid=("100", "2"),
            best_ask=("101", "2"),
            last_sequence_id=105,
            global_sequence_id=105,
            exchange_timestamp_us=None,
        ),
        local_receive_ts_ms=int(time.time() * 1000),
        local_receive_monotonic_ns=3,
    )
    assert arcus_book.sequence_health == "OK"
    assert arcus_book.ready
    engine, account_feed = _volume_probe_startup_engine(arcus_book=arcus_book)

    asyncio.run(engine._wait_volume_probe_book_stable(account_feed, timeout=0.0))


def test_volume_probe_startup_timeout_has_no_place_order_side_effect() -> None:
    arcus_book = _VolumeProbeStartupBook(
        ready=False,
        sequence_health="BOUNDARY",
        first_delta_after_snapshot=False,
    )
    engine, account_feed = _volume_probe_startup_engine(arcus_book=arcus_book)
    place_calls = 0

    class Maker:
        async def place_alo(self, **_kwargs: Any) -> None:
            nonlocal place_calls
            place_calls += 1

    setattr(engine, "maker", Maker())

    with pytest.raises(RuntimeError, match="volume-probe startup book stability"):
        asyncio.run(engine._wait_volume_probe_book_stable(account_feed, timeout=0.0))
    assert place_calls == 0


def test_volume_probe_rereads_fresh_bbo_after_startup_barrier() -> None:
    engine, account_feed = _volume_probe_startup_engine()
    asyncio.run(engine._wait_volume_probe_book_stable(account_feed, timeout=0.0))
    engine.arcus.book.bid = Decimal("102")
    engine.arcus.book.ask = Decimal("103")
    engine.hedge.book.bid = Decimal("99")
    engine.hedge.book.ask = Decimal("100")

    assert engine._read_volume_probe_bbo() == (
        Decimal("102"),
        Decimal("103"),
        Decimal("99"),
        Decimal("100"),
    )


def test_eff_threshold_directions():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    e, h = eng.entropy, eng.hedge
    state = eng.strategy.state()
    # sell entropy: hurdle = midline + upper = 9
    approx(eng._eff_threshold(buy=h, sell=e, state=state), 9.0)
    # buy entropy: hurdle = lower - midline = -2 (unwind side of a positive
    # midline is deliberately cheap — that's what completes the round trip)
    approx(eng._eff_threshold(buy=e, sell=h, state=state), -2.0)
    # round trip nets upper + lower regardless of midline sign
    for m in (-7.0, 0.0, 12.5):
        eng = make_engine(midline=m, upper=4.0, lower=3.0)
        state = eng.strategy.state()
        total = eng._eff_threshold(
            buy=eng.hedge, sell=eng.entropy, state=state
        ) + eng._eff_threshold(buy=eng.entropy, sell=eng.hedge, state=state)
        approx(total, 7.0)


def test_stable_strategy_preserves_legacy_hurdle_math():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    state = eng.strategy.state()
    e, h = eng.entropy, eng.hedge
    approx(eng._eff_threshold(h, e, state), 9.0)
    approx(eng._eff_threshold(e, h, state), -2.0)
    approx(
        eng._eff_threshold(h, e, state) + eng._eff_threshold(e, h, state),
        7.0,
    )


def test_inventory_ladder():
    eng = make_engine()
    eng.cfg.inventory_scale_bps, eng.cfg.inventory_floor_frac = 10.0, 0.5
    e, h = eng.entropy, eng.hedge
    e.set_book(99.9, 100.1)  # mid 100
    h.set_book(99.9, 100.1)
    approx(eng._inv_add_bps(e, h), 0.0)  # flat: dead zone
    e.position = 90.0  # long $9k of $10k cap
    v = eng._inv_add_bps(e, h)  # buying entropy adds long
    assert 7.5 < v < 8.5, v  # u=0.9 -> ~+8
    approx(eng._inv_add_bps(h, e), 0.0)  # selling entropy reduces
    h.position = -90.0  # hedge short $9k too
    v2 = eng._inv_add_bps(e, h)  # both legs add -> max()
    assert abs(v2 - v) < 0.6, (v, v2)  # max, not sum


def run_scan(eng):
    async def go():
        # first pass arms the direction, second passes the persistence gate
        # (premium_persist_sec is 0 in the test config)
        eng._scan(time.time())
        return eng._scan(time.time())

    return asyncio.run(go())


async def run_one_status_cycle(eng):
    eng.cfg.status_interval_sec = 0.01
    task = asyncio.create_task(eng._status_loop())
    await asyncio.sleep(0.02)
    eng.stop.set()
    await task


def test_scan_fires_sell_entropy_above_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 15 bps rich vs hedge: above midline+upper=9 -> sell entropy
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan, state = best
    assert state.center_bps == 5.0
    assert sell.key == "entropy" and buy.key == "hedge"
    assert plan.exp_edge_usd > 0


def test_scan_quiet_inside_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 5 bps rich = exactly on the midline: inside the band, no trade
    eng.entropy.set_book(100.04, 100.06)
    eng.hedge.set_book(99.99, 100.01)
    assert run_scan(eng) is None


def test_scan_fires_buy_entropy_below_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 5 bps CHEAP (premium -5): below midline-lower=+2 -> buy entropy
    eng.entropy.set_book(99.94, 99.96)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan, state = best
    assert state.center_bps == 5.0
    assert buy.key == "entropy" and sell.key == "hedge"


def test_scan_respects_position_caps():
    eng = make_engine(midline=0.0, upper=1.0, lower=1.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    eng.entropy.position = -100.0  # entropy already short at its cap
    eng.entropy.cap_usd = 10000.0
    eng.hedge.position = 100.0
    eng.hedge.cap_usd = 10000.0
    assert run_scan(eng) is None


def test_unready_drifting_strategy_blocks_scan_and_clears_arming():
    eng = make_engine(
        upper=4.0,
        lower=3.0,
        strategy_name="drifting_basis",
        window_minutes=60,
    )
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    eng._armed["sell_entropy"] = 111.0
    eng._armed["buy_entropy"] = 222.0

    assert eng._scan(time.time()) is None
    assert eng._armed == {
        "sell_entropy": None,
        "buy_entropy": None,
    }


def test_stable_strategy_observer_helper_is_noop():
    eng = make_engine(strategy_name="stable_basis")
    assert eng.strategy.requires_observations is False
    eng.entropy.set_book(100.10, 100.20)
    eng.hedge.set_book(99.90, 100.00)

    before = eng.strategy.state()
    assert eng._sample_strategy_observation(now=1000.0) is False
    assert eng.strategy.state() == before
    assert not eng._update_evt.is_set()


def test_drifting_observation_uses_mid_premium_and_fresh_books():
    eng = make_engine(strategy_name="drifting_basis", window_minutes=1)
    eng.entropy.set_book(100.10, 100.20)
    eng.hedge.set_book(99.90, 100.00)

    assert eng._sample_strategy_observation(now=1000.0) is True
    state = eng.strategy.state()
    assert state.ready is False
    assert state.center_bps is None
    assert state.coverage_ratio > 0
    assert eng._update_evt.is_set()


def test_drifting_observation_rejects_unready_empty_and_stale_books():
    eng = make_engine(strategy_name="drifting_basis", window_minutes=1)
    before = eng.strategy.state()

    assert eng._sample_strategy_observation(now=1000.0) is False

    eng.entropy.set_book(100.10, 100.20)
    eng.hedge.book.ready = True
    eng.hedge.book.touch()
    assert eng._sample_strategy_observation(now=1001.0) is False

    eng.hedge.set_book(99.90, 100.00)
    eng.entropy.book.alive_ts = time.time() - eng.cfg.staleness_sec - 1.0
    assert eng._sample_strategy_observation(now=1002.0) is False

    assert eng.strategy.state() == before
    assert not eng._update_evt.is_set()


def test_engine_premium_bps_matches_shared_helper():
    eng = make_engine()
    eng.entropy.set_book(100.10, 100.20)
    eng.hedge.set_book(99.90, 100.00)

    values = calculate_premiums(
        entropy_bid=100.10,
        entropy_ask=100.20,
        hedge_bid=99.90,
        hedge_ask=100.00,
    )
    assert eng.premium_bps() == pytest.approx(values.premium_bps)


def test_rolling_center_bootstrap_from_history_updates_effective_thresholds():
    eng = make_engine(
        midline=-1.8,
        upper=0.75,
        lower=0.75,
        center_mode="rolling",
    )
    observations = [
        (1.0, -10.0),
        (6 * 3600.0, -2.0),
        (12 * 3600.0, -6.0),
    ]
    eng.market_history = SimpleNamespace(
        recent_premium_observations=lambda *args: observations
    )

    asyncio.run(eng._bootstrap_rolling_center(now=12 * 3600.0 + 1.0))
    state = eng.strategy.state()

    assert state.center_bps == pytest.approx(-6.0)
    assert eng._eff_threshold(eng.hedge, eng.entropy, state) == pytest.approx(-5.25)
    assert eng._eff_threshold(eng.entropy, eng.hedge, state) == pytest.approx(6.75)


def test_rolling_center_thresholds_keep_existing_inventory_surcharge():
    eng = make_engine(
        midline=-1.8,
        upper=0.75,
        lower=0.75,
        center_mode="rolling",
    )
    eng.entropy.set_book(100.0, 100.1)
    eng.hedge.set_book(100.0, 100.1)
    eng.strategy.bootstrap(
        [(1.0, -10.0), (6 * 3600.0, -2.0), (12 * 3600.0, -6.0)],
        now=12 * 3600.0 + 1.0,
    )

    eng.hedge.position = 90.0
    eng.entropy.position = -90.0
    surcharge = eng._inv_add_bps(eng.hedge, eng.entropy)
    state = eng.strategy.state()

    assert surcharge > 0
    assert eng._eff_threshold(eng.hedge, eng.entropy, state) == pytest.approx(
        state.center_bps + state.upper_bps + surcharge
    )

    buy_eng = make_engine(
        midline=-1.8,
        upper=0.75,
        lower=0.75,
        center_mode="rolling",
    )
    buy_eng.entropy.set_book(100.0, 100.1)
    buy_eng.hedge.set_book(100.0, 100.1)
    buy_eng.strategy.bootstrap(
        [(1.0, -10.0), (6 * 3600.0, -2.0), (12 * 3600.0, -6.0)],
        now=12 * 3600.0 + 1.0,
    )
    buy_eng.entropy.position = 90.0
    buy_eng.hedge.position = -90.0
    buy_surcharge = buy_eng._inv_add_bps(buy_eng.entropy, buy_eng.hedge)
    buy_state = buy_eng.strategy.state()
    assert buy_surcharge > 0
    assert buy_eng._eff_threshold(
        buy_eng.entropy, buy_eng.hedge, buy_state
    ) == pytest.approx(buy_state.lower_bps - buy_state.center_bps + buy_surcharge)


def test_rolling_center_status_exposes_effective_center(caplog):
    eng = make_engine(
        midline=-1.8,
        upper=0.75,
        lower=0.75,
        center_mode="rolling",
    )
    eng.strategy.bootstrap(
        [(1.0, -10.0), (6 * 3600.0, -2.0), (12 * 3600.0, -6.0)],
        now=12 * 3600.0 + 1.0,
    )
    eng.entropy.set_book(100.0, 100.1)
    eng.hedge.set_book(100.0, 100.1)

    caplog.set_level(logging.INFO, logger="engine")
    asyncio.run(run_one_status_cycle(eng))

    assert "strategy=stable_basis" in caplog.text
    assert "center=-6.00" in caplog.text


def test_rolling_center_history_failure_keeps_fixed_fallback(caplog):
    eng = make_engine(
        midline=-1.8,
        upper=0.75,
        lower=0.75,
        center_mode="rolling",
    )

    def fail(*args):
        raise OSError("history unavailable")

    eng.market_history = SimpleNamespace(recent_premium_observations=fail)
    caplog.set_level(logging.INFO, logger="engine")

    asyncio.run(eng._bootstrap_rolling_center(now=12 * 3600.0 + 1.0))

    assert eng.strategy.state().center_bps == pytest.approx(-1.8)
    assert "using fallback=-1.80bps" in caplog.text


def test_drifting_observations_reach_ready_from_live_books():
    eng = make_engine(strategy_name="drifting_basis", window_minutes=1)
    eng.entropy.set_book(100.10, 100.20)
    eng.hedge.set_book(99.90, 100.00)

    for offset in range(61):
        assert eng._sample_strategy_observation(now=1000.0 + offset) is True

    state = eng.strategy.state()
    values = calculate_premiums(
        entropy_bid=100.10,
        entropy_ask=100.20,
        hedge_bid=99.90,
        hedge_ask=100.00,
    )
    assert state.ready is True
    assert state.center_bps == pytest.approx(values.premium_bps)


def test_strategy_observation_loop_cancels_cleanly():
    async def scenario():
        eng = make_engine(strategy_name="drifting_basis", window_minutes=1)
        samples = 0

        def fake_sample(now=None):
            nonlocal samples
            samples += 1
            return True

        eng._sample_strategy_observation = fake_sample
        task = asyncio.create_task(eng._strategy_observation_loop())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert samples == 1

    asyncio.run(scenario())


def test_status_reports_stable_strategy(caplog):
    eng = make_engine(strategy_name="stable_basis", midline=-1.0, upper=3.0, lower=3.5)
    eng.entropy.set_book(100.0, 100.1)
    eng.hedge.set_book(100.0, 100.1)

    caplog.set_level(logging.INFO, logger="engine")
    asyncio.run(run_one_status_cycle(eng))

    assert "strategy=stable_basis" in caplog.text
    assert "center=-1.00" in caplog.text


def test_status_reports_drifting_warmup_without_mutation(caplog):
    eng = make_engine(strategy_name="drifting_basis", window_minutes=1)
    eng.entropy.set_book(100.0, 100.1)
    eng.hedge.set_book(100.0, 100.1)
    eng.strategy.update(1000.0, 0.0)
    before = eng.strategy.state()

    caplog.set_level(logging.INFO, logger="engine")
    asyncio.run(run_one_status_cycle(eng))

    after = eng.strategy.state()
    assert "strategy=drifting_basis" in caplog.text
    assert "WARMING_UP" in caplog.text
    assert before == after


def test_log_csv_records_captured_strategy_center(tmp_path):
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    state = eng.strategy.state()
    trades_csv = tmp_path / "trades.csv"
    eng.cfg.trades_csv = str(trades_csv)
    plan = SimpleNamespace(
        qty=1.25,
        buy_limit=99.5,
        sell_limit=100.5,
        buy_notional=124.38,
        sell_notional=125.62,
        exp_edge_usd=1.23,
        gross_edge_usd=1.56,
        marginal_premium_bps=12.345,
    )
    captured = SimpleNamespace(
        ready=True,
        center_bps=7.5,
        upper_bps=state.upper_bps,
        lower_bps=state.lower_bps,
    )

    eng._log_csv(
        "sell_entropy",
        eng.hedge,
        eng.entropy,
        plan,
        True,
        1.25,
        1.25,
        "filled",
        "filled",
        1.11,
        0.0,
        captured,
    )

    rows = trades_csv.read_text().strip().splitlines()
    assert rows[0].split(",")[12] == "midline_bps"
    assert rows[1].split(",")[12] == "7.500"


def test_execution_actual_full_fill_equals_realized_fill_result():
    eng = make_settlement_engine(
        [settlement_info("filled", 1.0, 100.0)],
        [settlement_info("filled", 1.0, 101.0)],
    )
    plan = execution_plan()

    async def scenario():
        await eng._execute(
            eng.hedge, eng.entropy, plan, eng.strategy.state(), execution_id="exec-full"
        )

    asyncio.run(scenario())
    trade = eng.recent_trades[-1]
    expected = 101.0 * (1.0 - plan.sell_fee) - 100.0 * (1.0 + plan.buy_fee)
    assert trade["actual"] == pytest.approx(expected)
    assert trade["actual"] == pytest.approx(trade["fill"])
    assert eng.total_fill_edge == pytest.approx(expected)
    assert trade["status"] == "filled/filled"


def test_execution_telemetry_persists_filled_filled_actual(tmp_path):
    telemetry = tmp_path / "executions.csv"
    eng = make_settlement_engine(
        [settlement_info("filled", 1.0, 100.0)],
        [settlement_info("filled", 1.0, 101.0)],
        telemetry_path=telemetry,
    )

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            execution_plan(),
            eng.strategy.state(),
            execution_id="exec-persist",
        )

    asyncio.run(scenario())
    rows = execution_rows(telemetry)
    assert len(rows) == 1
    row = rows[-1]
    assert row["event_type"] == "execution_finalized"
    assert row["execution_id"] == "exec-persist"
    assert float(row["actual_usd"]) == pytest.approx(101.0 * 0.998 - 100.0 * 1.001)
    assert row["lifecycle_status"] == "filled/filled"
    assert row["buy_venue"] == "RH"
    assert row["sell_venue"] == "ENTROPY"
    assert float(row["requested_qty"]) == pytest.approx(1.0)
    assert float(row["buy_filled_qty"]) == pytest.approx(1.0)
    assert float(row["sell_filled_qty"]) == pytest.approx(1.0)
    assert float(row["buy_avg_px"]) == pytest.approx(100.0)
    assert float(row["sell_avg_px"]) == pytest.approx(101.0)
    assert row["hedge_status"] == "not_required"


def test_execution_actual_no_fill_is_zero(tmp_path):
    telemetry = tmp_path / "executions.csv"
    eng = make_settlement_engine(
        [settlement_info("canceled", 0.0)],
        [settlement_info("canceled", 0.0)],
        telemetry_path=telemetry,
    )

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            execution_plan(),
            eng.strategy.state(),
            execution_id="exec-none",
        )

    asyncio.run(scenario())
    trade = eng.recent_trades[-1]
    assert trade["actual"] == 0.0
    assert trade["fill"] == 0.0
    assert trade["status"] == "canceled/canceled"
    row = execution_rows(telemetry)[-1]
    assert row["event_type"] == "execution_finalized"
    assert float(row["actual_usd"]) == pytest.approx(0.0)


def test_execution_telemetry_one_leg_is_pending_not_zero_before_hedge(tmp_path):
    telemetry = tmp_path / "executions.csv"
    eng = make_settlement_engine(
        [settlement_info("filled", 1.0, 100.0)],
        [settlement_info("canceled", 0.0)],
        telemetry_path=telemetry,
    )

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            execution_plan(),
            eng.strategy.state(),
            execution_id="exec-open",
        )

    asyncio.run(scenario())
    row = execution_rows(telemetry)[-1]
    assert row["event_type"] == "execution_opened"
    assert row["execution_id"] == "exec-open"
    assert row["actual_usd"] == ""
    assert row["lifecycle_status"] == "filled/canceled → hedging"


def test_execution_actual_pending_then_includes_successful_hedge():
    eng = make_settlement_engine(
        [settlement_info("filled", 1.0, 100.0), settlement_info("filled", 1.0, 99.0)],
        [settlement_info("canceled", 0.0)],
    )
    plan = execution_plan()
    execution_id = "exec-hedge"

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            plan,
            eng.strategy.state(),
            execution_id=execution_id,
        )
        initial = eng.recent_trades[-1]
        assert initial["execution_id"] == execution_id
        assert initial["actual"] is None
        assert initial["status"] == "filled/canceled → hedging"
        await eng._maybe_hedge(execution_id)

    asyncio.run(scenario())
    trade = eng.recent_trades[-1]
    expected = -100.0 * (1.0 + plan.buy_fee) + 99.0 * (1.0 - eng.hedge.fee_bps / 1e4)
    assert trade["actual"] == pytest.approx(expected)
    assert trade["actual"] != 0.0
    assert trade["status"] == "hedged"
    assert trade["hedge_venue"] == "RH"
    assert trade["hedge_side"] == "SELL"
    assert trade["hedge_filled_qty"] == pytest.approx(1.0)
    assert trade["hedge_avg_px"] == pytest.approx(99.0)
    assert eng.total_fill_edge == 0.0


def test_execution_telemetry_hedge_finalizes_same_execution_id(tmp_path):
    telemetry = tmp_path / "executions.csv"
    eng = make_settlement_engine(
        [settlement_info("filled", 1.0, 100.0), settlement_info("filled", 1.0, 99.0)],
        [settlement_info("canceled", 0.0)],
        telemetry_path=telemetry,
    )

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            execution_plan(),
            eng.strategy.state(),
            execution_id="exec-hedged",
        )
        await eng._maybe_hedge("exec-hedged")

    asyncio.run(scenario())
    rows = execution_rows(telemetry)
    assert {row["execution_id"] for row in rows} == {"exec-hedged"}
    final = rows[-1]
    assert final["event_type"] == "execution_finalized"
    assert float(final["actual_usd"]) == pytest.approx(-100.0 * 1.001 + 99.0 * 0.999)
    assert final["lifecycle_status"] == "hedged"
    assert final["hedge_venue"] == "RH"
    assert final["hedge_side"] == "SELL"
    assert float(final["hedge_filled_qty"]) == pytest.approx(1.0)
    assert float(final["hedge_avg_px"]) == pytest.approx(99.0)
    assert final["hedge_status"] == "filled"


def test_execution_actual_partial_fill_includes_residual_hedge():
    eng = make_settlement_engine(
        [settlement_info("filled", 2.0, 100.0), settlement_info("filled", 1.0, 98.0)],
        [settlement_info("filled", 1.0, 101.0)],
    )
    plan = execution_plan(qty=2.0)
    execution_id = "exec-partial"

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            plan,
            eng.strategy.state(),
            execution_id=execution_id,
        )
        assert eng.recent_trades[-1]["actual"] is None
        await eng._maybe_hedge(execution_id)

    asyncio.run(scenario())
    trade = eng.recent_trades[-1]
    matched = 101.0 * (1.0 - plan.sell_fee) - 100.0 * (1.0 + plan.buy_fee)
    residual = -100.0 * (1.0 + plan.buy_fee) + 98.0 * (1.0 - eng.hedge.fee_bps / 1e4)
    assert trade["actual"] == pytest.approx(matched + residual)
    assert trade["fill"] == pytest.approx(matched)
    assert eng.total_fill_edge == pytest.approx(matched)
    assert trade["status"] == "hedged"


def test_execution_telemetry_partial_hedge_persists_all_in_result(tmp_path):
    telemetry = tmp_path / "executions.csv"
    eng = make_settlement_engine(
        [settlement_info("filled", 2.0, 100.0), settlement_info("filled", 1.0, 98.0)],
        [settlement_info("filled", 1.0, 101.0)],
        telemetry_path=telemetry,
    )

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            execution_plan(qty=2.0),
            eng.strategy.state(),
            execution_id="exec-partial",
        )
        await eng._maybe_hedge("exec-partial")

    asyncio.run(scenario())
    final = execution_rows(telemetry)[-1]
    expected = 101.0 * 0.998 - 100.0 * 1.001 - 100.0 * 1.001 + 98.0 * 0.999
    assert float(final["actual_usd"]) == pytest.approx(expected)
    assert float(final["buy_filled_qty"]) == pytest.approx(2.0)
    assert float(final["sell_filled_qty"]) == pytest.approx(1.0)
    assert float(final["hedge_filled_qty"]) == pytest.approx(1.0)
    assert final["lifecycle_status"] == "hedged"


def test_finalized_execution_telemetry_survives_shutdown(tmp_path):
    telemetry = tmp_path / "executions.csv"
    eng = make_settlement_engine(
        [settlement_info("filled", 1.0, 100.0)],
        [settlement_info("filled", 1.0, 101.0)],
        telemetry_path=telemetry,
    )

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            execution_plan(),
            eng.strategy.state(),
            execution_id="exec-shutdown",
        )

    asyncio.run(scenario())
    eng.request_stop()
    assert execution_rows(telemetry)[-1]["execution_id"] == "exec-shutdown"


def test_execution_actual_stays_pending_when_hedge_unresolved():
    eng = make_settlement_engine(
        [
            settlement_info("filled", 1.0, 100.0),
            settlement_info("timeout", 0.0, unresolved=True),
        ],
        [settlement_info("canceled", 0.0)],
    )
    execution_id = "exec-unresolved"

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            execution_plan(),
            eng.strategy.state(),
            execution_id=execution_id,
        )
        await eng._maybe_hedge(execution_id)

    asyncio.run(scenario())
    trade = eng.recent_trades[-1]
    assert trade["actual"] is None
    assert trade["status"] == "hedge-unresolved"
    assert trade["actual"] != 0.0
    assert eng.total_fill_edge == 0.0


def test_execution_telemetry_unresolved_hedge_is_not_zero(tmp_path):
    telemetry = tmp_path / "executions.csv"
    eng = make_settlement_engine(
        [
            settlement_info("filled", 1.0, 100.0),
            settlement_info("timeout", 0.0, unresolved=True),
        ],
        [settlement_info("canceled", 0.0)],
        telemetry_path=telemetry,
    )

    async def scenario():
        await eng._execute(
            eng.hedge,
            eng.entropy,
            execution_plan(),
            eng.strategy.state(),
            execution_id="exec-unresolved",
        )
        await eng._maybe_hedge("exec-unresolved")

    asyncio.run(scenario())
    final = execution_rows(telemetry)[-1]
    assert final["event_type"] == "execution_finalized"
    assert final["actual_usd"] == ""
    assert final["lifecycle_status"] == "hedge-unresolved"
    assert final["hedge_status"] == "unresolved"


def test_run_inner_logs_selected_stable_strategy_and_no_auto_selection(
    monkeypatch, caplog
):
    from entropy_arb import engine as engine_module

    class LifecycleVenue:
        def __init__(self, key, name):
            self.kind = "hl"
            self.key = key
            self.name = name
            self.conf = SimpleNamespace(symbol="SNDK")
            self.size_decimals = 4
            self.min_base = 0.0001
            self.min_quote = 10.0
            self.fee_bps = 0.0
            self.book = OrderBook()
            self.position = 0.0
            self.orders_per_min = 30

        async def load_market(self):
            return None

        def start_tasks(self, stop, notify, live):
            return []

        async def close(self):
            return None

        def _query_address(self):
            return None

    class QuietMinuteRecorder:
        rows_written = 0

        def __init__(self, *args, **kwargs):
            return None

        async def run(self, stop):
            await stop.wait()

    async def scenario():
        cfg = make_cfg(
            midline=-1.0,
            upper=3.0,
            lower=3.5,
            hedge_venue="tradexyz",
            recorder_enabled=False,
        )
        eng = Engine(cfg, record_only=True)
        venues = iter(
            [
                LifecycleVenue("entropy", "ENTROPY"),
                LifecycleVenue("hedge", "XYZ"),
            ]
        )
        monkeypatch.setattr(eng, "_make_venue", lambda conf: next(venues))
        monkeypatch.setattr(engine_module, "MinuteRecorder", QuietMinuteRecorder)
        run_task = asyncio.create_task(eng._run_inner())
        await asyncio.sleep(0)
        eng.request_stop()
        await asyncio.wait_for(run_task, timeout=1.0)

    caplog.set_level(logging.INFO, logger="engine")
    asyncio.run(scenario())
    assert "strategy=stable_basis center=-1.00bps band=[-4.50,+2.00]" in caplog.text
    assert "No automatic strategy selection." in caplog.text


def test_run_inner_logs_drifting_warmup_strategy_and_no_auto_selection(
    monkeypatch, caplog
):
    from entropy_arb import engine as engine_module

    class LifecycleVenue:
        def __init__(self, key, name):
            self.kind = "hl"
            self.key = key
            self.name = name
            self.conf = SimpleNamespace(symbol="SNDK")
            self.size_decimals = 4
            self.min_base = 0.0001
            self.min_quote = 10.0
            self.fee_bps = 0.0
            self.book = OrderBook()
            self.position = 0.0
            self.orders_per_min = 30

        async def load_market(self):
            return None

        def start_tasks(self, stop, notify, live):
            return []

        async def close(self):
            return None

        def _query_address(self):
            return None

    class QuietMinuteRecorder:
        rows_written = 0

        def __init__(self, *args, **kwargs):
            return None

        async def run(self, stop):
            await stop.wait()

    async def scenario():
        cfg = make_cfg(
            upper=3.0,
            lower=3.5,
            hedge_venue="tradexyz",
            recorder_enabled=False,
            strategy_name="drifting_basis",
            window_minutes=60,
        )
        eng = Engine(cfg, record_only=True)
        venues = iter(
            [
                LifecycleVenue("entropy", "ENTROPY"),
                LifecycleVenue("hedge", "XYZ"),
            ]
        )
        monkeypatch.setattr(eng, "_make_venue", lambda conf: next(venues))
        monkeypatch.setattr(engine_module, "MinuteRecorder", QuietMinuteRecorder)
        run_task = asyncio.create_task(eng._run_inner())
        await asyncio.sleep(0)
        eng.request_stop()
        await asyncio.wait_for(run_task, timeout=1.0)

    caplog.set_level(logging.INFO, logger="engine")
    asyncio.run(scenario())
    assert (
        "strategy=drifting_basis window=60m center=WARMING_UP band-offset=[-3.50,+3.00]"
    ) in caplog.text
    assert "No automatic strategy selection." in caplog.text


class LifecycleVenue:
    def __init__(self, key, name, *, kind="hl"):
        self.kind = kind
        self.key = key
        self.name = name
        self.conf = SimpleNamespace(symbol="SNDK")
        self.size_decimals = 4
        self.min_base = 0.0001
        self.min_quote = 10.0
        self.fee_bps = 0.0
        self.book = OrderBook()
        self.position = 0.0
        self.orders_per_min = 30

    async def load_market(self):
        return None

    def start_tasks(self, stop, notify, live):
        return []

    async def close(self):
        return None

    def _query_address(self):
        return None

    def init_signer(self):
        return None

    def share_nonces_with(self, other):
        return None


async def run_strategy_wiring_scenario(
    monkeypatch,
    *,
    strategy_name,
    record_only,
    execution_overrides=None,
):
    cfg = make_cfg(
        strategy_name=strategy_name,
        window_minutes=1,
        hedge_venue="tradexyz",
        recorder_enabled=False,
    )
    if not record_only:
        monkeypatch.setattr(type(cfg), "creds_complete", property(lambda self: True))
    for key, value in (execution_overrides or {}).items():
        setattr(cfg, key, value)
    eng = Engine(cfg, record_only=record_only)
    venues = iter(
        [
            LifecycleVenue("entropy", "ENTROPY"),
            LifecycleVenue("hedge", "XYZ"),
        ]
    )
    monkeypatch.setattr(eng, "_make_venue", lambda conf: next(venues))

    started = {
        "strategy": asyncio.Event(),
        "observer": asyncio.Event(),
    }

    async def strategy_loop():
        started["strategy"].set()
        await eng.stop.wait()

    async def observer_loop():
        started["observer"].set()
        await eng.stop.wait()

    async def idle_loop():
        await eng.stop.wait()

    async def fake_reconcile_positions(*args, **kwargs):
        return None

    monkeypatch.setattr(eng, "_strategy_loop", strategy_loop)
    monkeypatch.setattr(
        eng,
        "_strategy_observation_loop",
        observer_loop,
        raising=False,
    )
    monkeypatch.setattr(eng, "_balance_loop", idle_loop)
    monkeypatch.setattr(eng, "_http_keepalive_loop", idle_loop)
    monkeypatch.setattr(eng, "_status_loop", idle_loop)
    monkeypatch.setattr(eng, "_reconcile_loop", idle_loop)
    monkeypatch.setattr(eng, "_reconcile_positions", fake_reconcile_positions)

    run_task = asyncio.create_task(eng._run_inner())
    for _ in range(20):
        await asyncio.sleep(0.01)
        if (
            run_task.done()
            or started["strategy"].is_set()
            or started["observer"].is_set()
        ):
            break
    snapshot = {
        "strategy": started["strategy"].is_set(),
        "observer": started["observer"].is_set(),
    }
    eng.request_stop()
    await asyncio.wait_for(run_task, timeout=1.0)
    return snapshot


def test_live_stable_run_inner_starts_strategy_without_observer(monkeypatch):
    started = asyncio.run(
        run_strategy_wiring_scenario(
            monkeypatch,
            strategy_name="stable_basis",
            record_only=False,
        )
    )
    assert started == {
        "strategy": True,
        "observer": False,
    }


def test_live_startup_logs_effective_execution_config(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="engine")
    asyncio.run(
        run_strategy_wiring_scenario(
            monkeypatch,
            strategy_name="stable_basis",
            record_only=False,
            execution_overrides={
                "leg_slippage_bps": 7.5,
                "hedge_slippage_bps": 20.0,
                "premium_persist_sec": 1.0,
                "cooldown_sec": 1.0,
                "max_order_notional": 100.0,
                "inventory_scale_bps": 7.5,
            },
        )
    )
    assert (
        "execution config: leg_slippage=7.50bps hedge_slippage=20.00bps "
        "persistence=1.00s cooldown=1.00s max_order=$100 "
        "inventory_scale=7.50bps"
    ) in caplog.text


def test_live_drifting_run_inner_starts_strategy_and_observer(monkeypatch):
    started = asyncio.run(
        run_strategy_wiring_scenario(
            monkeypatch,
            strategy_name="drifting_basis",
            record_only=False,
        )
    )
    assert started == {
        "strategy": True,
        "observer": True,
    }


def test_record_only_drifting_run_inner_skips_strategy_and_observer(monkeypatch):
    started = asyncio.run(
        run_strategy_wiring_scenario(
            monkeypatch,
            strategy_name="drifting_basis",
            record_only=True,
        )
    )
    assert started == {
        "strategy": False,
        "observer": False,
    }


def test_volume_probe_disables_arcus_minute_output(monkeypatch) -> None:
    from entropy_arb import engine as engine_module

    captured = {}

    class StopAfterRecorderConstruction(RuntimeError):
        pass

    class SpyArcusMarketRecorder:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        def record_metadata(self, metadata):
            raise StopAfterRecorderConstruction

    class NoopRoundWriter:
        def append(self, metrics):
            return None

    class FakeVenue:
        def __init__(self, *, metadata=None):
            self.book = SimpleNamespace()
            self.metadata = metadata

        async def load_market(self):
            return self.metadata

        async def close(self):
            return None

    metadata = SimpleNamespace(symbol="SNDK", status="ONLINE")
    arcus = FakeVenue(metadata=metadata)
    hedge = FakeVenue()
    eng = Engine(
        make_cfg(recorder_enabled=False),
        confirm_mainnet=True,
        volume_probe=True,
        probe_clip_usd=20.0,
        probe_side="sell",
    )
    eng.session = cast(Any, object())
    monkeypatch.setattr(eng, "_make_venue", lambda _conf: hedge)
    monkeypatch.setattr(engine_module, "ArcusMarketRecorder", SpyArcusMarketRecorder)
    monkeypatch.setattr(engine_module, "VolumeProbeRoundWriter", NoopRoundWriter)

    with pytest.raises(StopAfterRecorderConstruction):
        asyncio.run(eng._run_arcus_volume_probe(cast(Any, arcus)))

    assert captured["write_minutes"] is False


def attach_reference_venues(
    eng,
    *,
    market_id=32,
    hedge_ws_url="wss://api.rh.lighter.xyz/stream",
):
    eng.entropy = StubVenue("entropy", "ENTROPY")
    eng.entropy.ws_url = "wss://api.hyperliquid.xyz/ws"
    eng.entropy.coin = "io:SNDK"
    eng.hedge = StubVenue("hedge", "RH")
    eng.hedge.kind = "lighter"
    eng.hedge.profile = SimpleNamespace(ws_url=hedge_ws_url)
    eng.hedge.market_id = market_id
    eng.market_history = MarketHistoryStore(":memory:")


@pytest.mark.parametrize(
    ("record_only", "recorder_enabled", "expected"),
    [
        (True, False, True),
        (False, True, True),
        (False, False, False),
    ],
)
def test_reference_lifecycle_matrix(
    monkeypatch, record_only, recorder_enabled, expected
):
    from entropy_arb import engine as engine_module

    captured = []

    class SpyReferenceRecorder:
        def __init__(self, **kwargs):
            captured.append(kwargs)

    monkeypatch.setattr(engine_module, "ReferenceRecorder", SpyReferenceRecorder)
    eng = Engine(
        make_cfg(recorder_enabled=recorder_enabled),
        record_only=record_only,
    )
    attach_reference_venues(eng)
    recorder = eng._build_reference_recorder()
    assert (recorder is not None) is expected
    if expected:
        assert captured[0] == {
            "symbol": "SNDK",
            "hedge_key": "lighter-rh",
            "entropy_ws_url": "wss://api.hyperliquid.xyz/ws",
            "entropy_coin": "io:SNDK",
            "hedge_ws_url": "wss://api.rh.lighter.xyz/stream",
            "hedge_market_id": 32,
            "store": eng.market_history,
            "quota_coordinator": eng.entropy_quota,
        }


@pytest.mark.parametrize(
    ("hedge_key", "hedge_ws_url", "market_id"),
    [
        (
            "lighter",
            "wss://mainnet.zklighter.elliot.ai/stream",
            139,
        ),
        (
            "lighter-rh",
            "wss://api.rh.lighter.xyz/stream",
            32,
        ),
    ],
)
def test_reference_factory_uses_runtime_resolved_metadata(
    monkeypatch, hedge_key, hedge_ws_url, market_id
):
    from entropy_arb import engine as engine_module

    captured = []

    class SpyReferenceRecorder:
        def __init__(self, **kwargs):
            captured.append(kwargs)

    monkeypatch.setattr(engine_module, "ReferenceRecorder", SpyReferenceRecorder)
    eng = Engine(make_cfg(hedge_venue=hedge_key, recorder_enabled=True))
    attach_reference_venues(
        eng,
        market_id=market_id,
        hedge_ws_url=hedge_ws_url,
    )
    eng._build_reference_recorder()
    assert captured[0]["entropy_coin"] == "io:SNDK"
    assert captured[0]["hedge_key"] == hedge_key
    assert captured[0]["hedge_ws_url"] == hedge_ws_url
    assert captured[0]["hedge_market_id"] == market_id


def test_tradexyz_does_not_build_reference_collector():
    eng = Engine(
        make_cfg(hedge_venue="tradexyz", recorder_enabled=True),
        record_only=True,
    )
    eng.entropy = SimpleNamespace(
        ws_url="wss://api.hyperliquid.xyz/ws",
        coin="io:SNDK",
    )
    eng.hedge = SimpleNamespace(kind="hl")
    assert eng._build_reference_recorder() is None


def test_reference_factory_has_no_strategy_wakeup_dependency(monkeypatch):
    from entropy_arb import engine as engine_module

    captured = {}

    class SpyReferenceRecorder:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(engine_module, "ReferenceRecorder", SpyReferenceRecorder)
    eng = Engine(make_cfg(), record_only=True)
    attach_reference_venues(eng)
    eng.reference = eng._build_reference_recorder()
    assert "notify" not in captured
    assert "update_evt" not in captured
    assert not eng._update_evt.is_set()


def test_reference_failure_is_nonfatal_and_does_not_set_engine_events(caplog):
    class BrokenReference:
        async def run(self, stop):
            raise RuntimeError("reference failed")

    async def scenario():
        eng = Engine(make_cfg(), record_only=True)
        eng.reference = BrokenReference()
        await eng._run_reference()
        assert not eng.stop.is_set()
        assert not eng._update_evt.is_set()
        assert not eng._reconcile_evt.is_set()

    asyncio.run(scenario())
    assert "reference recorder failed" in caplog.text


def test_no_strategy_wakeup_after_successful_reference_run():
    class SuccessfulReference:
        async def run(self, stop):
            return None

    async def scenario():
        eng = Engine(make_cfg(), record_only=False)
        evaluations = 0

        async def tracked_evaluate():
            nonlocal evaluations
            evaluations += 1

        setattr(eng, "_evaluate", tracked_evaluate)
        eng.reference = SuccessfulReference()
        strategy_task = asyncio.create_task(eng._strategy_loop())
        await asyncio.sleep(0)
        await eng._run_reference()
        await asyncio.sleep(0)
        assert not eng._update_evt.is_set()
        assert evaluations == 0
        eng.stop.set()
        eng._update_evt.set()
        await strategy_task

    asyncio.run(scenario())


def test_engine_awaits_reference_shutdown_without_cancelling_it(monkeypatch):
    from entropy_arb import engine as engine_module

    async def scenario():
        started = asyncio.Event()
        closed = asyncio.Event()
        was_cancelled = False
        recorder_stores = []

        class LifecycleVenue:
            def __init__(self, kind, *, coin=None, market_id=None, ws_url=None):
                self.kind = kind
                self.key = "entropy" if coin else "hedge"
                self.name = self.key.upper()
                self.conf = SimpleNamespace(symbol="SNDK")
                self.ws_url = ws_url
                self.coin = coin
                self.market_id = market_id
                self.profile = SimpleNamespace(ws_url=ws_url)
                self.size_decimals = 4
                self.min_base = 0.0001
                self.min_quote = 10.0
                self.fee_bps = 0.0
                self.book = OrderBook()

            async def load_market(self):
                return None

            def start_tasks(self, stop, notify, live):
                return []

            async def close(self):
                return None

        class SpyReferenceRecorder:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            async def run(self, stop):
                nonlocal was_cancelled
                started.set()
                try:
                    await stop.wait()
                    await asyncio.sleep(0)
                    closed.set()
                except asyncio.CancelledError:
                    was_cancelled = True
                    raise

        class QuietMinuteRecorder:
            rows_written = 0

            def __init__(self, *args, **kwargs):
                recorder_stores.append(args[0])

            async def run(self, stop):
                await stop.wait()

        cfg = make_cfg(recorder_enabled=False)
        eng = Engine(cfg, record_only=True)
        venues = iter(
            [
                LifecycleVenue(
                    "hl",
                    coin="io:SNDK",
                    ws_url="wss://api.hyperliquid.xyz/ws",
                ),
                LifecycleVenue(
                    "lighter",
                    market_id=32,
                    ws_url="wss://api.rh.lighter.xyz/stream",
                ),
            ]
        )
        monkeypatch.setattr(eng, "_make_venue", lambda conf: next(venues))
        monkeypatch.setattr(engine_module, "ReferenceRecorder", SpyReferenceRecorder)
        monkeypatch.setattr(engine_module, "MinuteRecorder", QuietMinuteRecorder)
        run_task = asyncio.create_task(eng._run_inner())
        await asyncio.wait_for(started.wait(), timeout=0.2)
        assert eng.market_history is not None
        assert recorder_stores == [eng.market_history]
        assert eng.reference.kwargs["store"] is eng.market_history
        eng.request_stop()
        await asyncio.wait_for(run_task, timeout=1.0)
        assert closed.is_set()
        assert was_cancelled is False

    asyncio.run(scenario())


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
