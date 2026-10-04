from datetime import UTC, datetime

import pytest

from entropy_arb.lot_ledger import LotLedger, LotLedgerError
from entropy_arb.market_state import (
    MarketState,
    allow_new_add,
    classify_market_state,
    research_profile,
)
from entropy_arb.rolling import RollingConfig, RollingWindow
from entropy_arb.rolling_policy import RollingPolicy


def _row(minute_ts, premium, samples=1):
    return {
        "minute_ts": str(minute_ts),
        "samples": str(samples),
        "premium_close_bps": str(premium),
    }


def _filled_window(config=None, value=5.0):
    config = config or RollingConfig(window_hours=1, min_coverage_pct=80)
    window = RollingWindow(config)
    for minute in range(60):
        window.ingest_row(_row(minute * 60, value))
    return window


def test_rolling_snapshot_is_causal_and_uses_median():
    window = RollingWindow(RollingConfig(window_hours=1, min_coverage_pct=80))
    for minute in range(60):
        window.ingest_row(_row(minute * 60, float(minute)))
    window.ingest_row(_row(3600, 10_000.0))

    snapshot = window.snapshot_for(3600)

    assert snapshot.valid
    assert snapshot.median_bps == 29.5
    assert snapshot.valid_minutes == 60


def test_invalid_coverage_fails_closed():
    window = RollingWindow(RollingConfig(window_hours=1, min_coverage_pct=80))
    for minute in range(10):
        window.ingest_row(_row(minute * 60, 1.0))

    assert window.signal(10.0, 3600, upper_bps=1, lower_bps=1) is None


def test_signal_names_are_arcus_native():
    window = _filled_window()
    assert window.signal(6.0, 3600, upper_bps=1, lower_bps=1).direction == "sell_arcus"
    assert window.signal(4.0, 3600, upper_bps=1, lower_bps=1).direction == "buy_arcus"


def test_research_profiles_match_selected_canary_parameters():
    assert research_profile("SPY").upper_bps == pytest.approx(0.50)
    assert research_profile("QQQ").upper_bps == pytest.approx(0.75)
    assert research_profile("NVDA").upper_bps == pytest.approx(2.50)
    assert research_profile("NVDA").max_lots == 5
    assert not research_profile("SNDK").enabled


def test_open_transition_filter_is_symbol_specific_and_weekend_not_blocked():
    open_transition = datetime(2026, 10, 1, 13, 45, tzinfo=UTC)
    after_filter = datetime(2026, 10, 1, 14, 0, tzinfo=UTC)
    weekend = datetime(2026, 10, 3, 16, 0, tzinfo=UTC)

    assert classify_market_state(open_transition) is MarketState.OPEN_TRANSITION
    assert not allow_new_add(research_profile("SPY"), open_transition)
    assert not allow_new_add(research_profile("QQQ"), open_transition)
    assert allow_new_add(research_profile("NVDA"), open_transition)
    assert allow_new_add(research_profile("SPY"), after_filter)
    assert classify_market_state(weekend) is MarketState.WEEKEND_CLOSED
    assert allow_new_add(research_profile("SPY"), weekend)


def test_entry_requires_persistence_but_opposite_reduce_is_immediate():
    config = RollingConfig(
        window_hours=1,
        update_minutes=15,
        min_coverage_pct=80,
        persistence_sec=10,
    )
    policy = RollingPolicy(research_profile("SPY"), config)
    for minute in range(60):
        policy.window.ingest_row(_row(minute * 60, 0.0))

    t0 = 3600.0
    assert policy.evaluate(
        premium_bps=1.0, now=t0, open_direction=None, open_lot_count=0
    ) is None
    assert policy.evaluate(
        premium_bps=1.0, now=t0 + 9, open_direction=None, open_lot_count=0
    ) is None
    add = policy.evaluate(
        premium_bps=1.0, now=t0 + 10, open_direction=None, open_lot_count=0
    )
    assert add is not None and add.action == "add"
    assert add.direction == "sell_arcus"

    reduce = policy.evaluate(
        premium_bps=-1.0,
        now=t0 + 11,
        open_direction="sell_arcus",
        open_lot_count=1,
    )
    assert reduce is not None and reduce.action == "reduce"
    assert reduce.direction == "buy_arcus"


def test_open_transition_never_blocks_reduce():
    config = RollingConfig(window_hours=1, min_coverage_pct=80)
    policy = RollingPolicy(research_profile("SPY"), config)
    base = datetime(2026, 10, 1, 13, 45, tzinfo=UTC).timestamp()
    block_start = (int(base) // 900) * 900
    for minute in range(60):
        policy.window.ingest_row(_row(block_start - 3600 + minute * 60, 0.0))

    decision = policy.evaluate(
        premium_bps=-1.0,
        now=base,
        open_direction="sell_arcus",
        open_lot_count=1,
    )
    assert decision is not None and decision.action == "reduce"


def _lot(ledger, lot_id, direction, qty, buy_px, sell_px):
    return ledger.add_lot(
        lot_id=lot_id,
        source_event_id=lot_id,
        direction=direction,
        open_qty=qty,
        entry_ts=100.0,
        buy_venue="ARCUS" if direction == "buy_arcus" else "RH",
        sell_venue="RH" if direction == "buy_arcus" else "ARCUS",
        buy_avg_px=buy_px,
        sell_avg_px=sell_px,
        buy_fee_bps=0.0,
        sell_fee_bps=0.0,
    )


def test_lot_ledger_uses_actual_fills_for_break_even_and_position_validation():
    ledger = LotLedger(tolerance=1e-6)
    lot = _lot(ledger, "lot-1", "sell_arcus", 1.0, 100.0, 102.0)

    assert lot.entry_cash_per_base == pytest.approx(2.0)
    assert lot.required_exit_cash_per_base(0.0) == pytest.approx(-2.0)
    ledger.validate_positions({"arcus": -1.0, "hedge": 1.0})
    with pytest.raises(LotLedgerError, match="position mismatch"):
        ledger.validate_positions({"arcus": -0.5, "hedge": 0.5})


def test_lot_ledger_persists_and_reloads(tmp_path):
    path = tmp_path / "lots.json"
    ledger = LotLedger(str(path), symbol="SPY", hedge="lighter-rh")
    _lot(ledger, "lot-1", "buy_arcus", 0.5, 100.0, 101.0)

    loaded = LotLedger(str(path), symbol="SPY", hedge="lighter-rh")
    assert loaded.load() == 1
    assert loaded.direction == "buy_arcus"
    assert loaded.total_qty == pytest.approx(0.5)
