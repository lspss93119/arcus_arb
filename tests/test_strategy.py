import math

import pytest

from entropy_arb.strategy import DriftingBasisStrategy, StableBasisStrategy


def feed_seconds(strategy, start, seconds, value_fn=lambda i: float(i)):
    for i in range(seconds):
        strategy.update(start + i, value_fn(i))


def test_stable_basis_is_immediately_ready_and_fixed():
    s = StableBasisStrategy(center_bps=-1.0, upper_bps=3.0, lower_bps=3.5)
    before = s.state()
    assert before.ready is True
    assert before.center_bps == -1.0
    assert before.upper_bps == 3.0
    assert before.lower_bps == 3.5

    s.update(1000.0, 25.0)
    after = s.state()
    assert after == before


def _rolling_stable():
    return StableBasisStrategy(
        center_bps=-1.8,
        upper_bps=0.75,
        lower_bps=0.75,
        center_mode="rolling",
        center_window_hours=12,
        center_update_minutes=60,
    )


def test_rolling_center_with_insufficient_history_uses_fixed_fallback():
    strategy = _rolling_stable()

    update = strategy.bootstrap(
        [(0.0, -10.0), (6 * 3600.0, -3.0)],
        now=6 * 3600.0 + 1.0,
    )

    assert update is None
    assert strategy.state().ready is True
    assert strategy.state().center_bps == pytest.approx(-1.8)
    assert strategy.requires_observations is True


def test_rolling_center_bootstrap_uses_causal_twelve_hour_median():
    strategy = _rolling_stable()

    update = strategy.bootstrap(
        [(1.0, -10.0), (6 * 3600.0, -2.0), (12 * 3600.0, -6.0)],
        now=12 * 3600.0 + 1.0,
    )

    assert update is not None
    assert update.new_center_bps == pytest.approx(-6.0)
    assert strategy.state().center_bps == pytest.approx(-6.0)


def test_rolling_center_excludes_future_bootstrap_samples():
    strategy = _rolling_stable()

    strategy.bootstrap(
        [
            (1.0, -10.0),
            (6 * 3600.0, -2.0),
            (12 * 3600.0, -6.0),
            (12 * 3600.0 + 2.0, 1000.0),
        ],
        now=12 * 3600.0 + 1.0,
    )

    assert strategy.state().center_bps == pytest.approx(-6.0)


def test_rolling_center_updates_at_most_once_per_hour():
    strategy = _rolling_stable()
    history = (
        [
            (1.0, -10.0),
        ]
        + [(3601.0 + i * 3600.0, float(i)) for i in range(9)]
        + [(12 * 3600.0, -6.0)]
    )
    strategy.bootstrap(
        history,
        now=12 * 3600.0 + 1.0,
    )
    initial = strategy.state().center_bps

    assert strategy.update(12 * 3600.0 + 3600.0, 20.0) is None
    assert strategy.state().center_bps == initial

    changed = strategy.update(12 * 3600.0 + 3600.0 + 1.0, 20.0)
    assert changed is not None
    assert strategy.state().center_bps != initial


def test_rolling_center_keeps_last_valid_value_when_update_has_no_history():
    strategy = _rolling_stable()
    strategy.bootstrap(
        [(1.0, -10.0), (6 * 3600.0, -2.0), (12 * 3600.0, -6.0)],
        now=12 * 3600.0 + 1.0,
    )
    initial = strategy.state().center_bps

    # A non-finite live observation is ignored; the previously valid center
    # remains effective and no exception escapes the strategy boundary.
    assert strategy.update(12 * 3600.0 + 3600.0 + 1.0, math.nan) is None
    assert strategy.state().center_bps == initial


def test_rolling_center_restart_bootstraps_from_the_same_history():
    observations = [
        (1.0, -10.0),
        (6 * 3600.0, -2.0),
        (12 * 3600.0, -6.0),
    ]
    first = _rolling_stable()
    second = _rolling_stable()

    first.bootstrap(observations, now=12 * 3600.0 + 1.0)
    second.bootstrap(observations, now=12 * 3600.0 + 1.0)

    assert second.state() == first.state()


def test_drifting_not_ready_before_full_window():
    s = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    feed_seconds(s, 1000.0, 60, lambda i: 1.0)
    assert s.state().ready is False
    s.update(1060.0, 1.0)
    assert s.state().ready is True
    assert s.state().center_bps == pytest.approx(1.0)


def test_drifting_requires_90_percent_coverage():
    s = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    for i in range(0, 61, 2):
        s.update(1000.0 + i, 2.0)
    state = s.state()
    assert state.warmup_span_sec is not None
    assert state.coverage_ratio is not None
    assert state.warmup_span_sec >= 60
    assert state.coverage_ratio < 0.90
    assert state.ready is False


def test_drifting_uses_timestamp_window_and_causal_median():
    s = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    for i in range(61):
        s.update(1000.0 + i, 1.0 if i < 60 else 9.0)
    state = s.state()
    assert state.ready is True
    assert state.center_bps is not None
    first_center = state.center_bps
    s.update(1061.0, 9.0)
    updated = s.state()
    assert updated.center_bps is not None
    assert updated.center_bps >= first_center


def test_short_gap_does_not_reset_history():
    s = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    feed_seconds(s, 1000.0, 40, lambda i: 1.0)
    s.update(1060.0, 1.0)
    state = s.state()
    assert state.warmup_span_sec is not None
    assert state.warmup_span_sec >= 60


def test_exactly_30_second_gap_preserves_history_without_reset():
    s = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    feed_seconds(s, 1000.0, 61, lambda i: 1.0)
    assert s.state().ready is True

    s.update(1090.0, 5.0)  # exactly 30s since the last valid observation
    state = s.state()
    assert state.ready is False  # coverage falls, but the segment is retained
    assert state.warmup_span_sec == pytest.approx(90.0)
    assert state.coverage_ratio == pytest.approx(31 / 60)


def test_gap_over_30_seconds_resets_history():
    s = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    feed_seconds(s, 1000.0, 61, lambda i: 1.0)
    assert s.state().ready is True
    s.update(1091.1, 5.0)
    state = s.state()
    assert state.ready is False
    assert state.warmup_span_sec == pytest.approx(0.0)
    assert state.center_bps is None


def test_nonfinite_premium_is_ignored():
    s = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    s.update(1000.0, 1.0)
    before = s.state()
    s.update(1001.0, math.nan)
    s.update(1002.0, math.inf)
    assert s.state() == before


def test_new_drifting_instance_never_restores_old_state():
    old = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    feed_seconds(old, 1000.0, 61, lambda i: 2.0)
    assert old.state().ready is True

    restarted = DriftingBasisStrategy(
        window_minutes=1,
        upper_bps=3.0,
        lower_bps=3.5,
    )
    assert restarted.state().ready is False
    assert restarted.state().center_bps is None


def test_same_observation_stream_produces_deterministic_replay_state_sequence():
    observations = [(1000.0 + i, -2.0 if i < 30 else 2.0) for i in range(61)]

    live_style = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)
    replay_style = DriftingBasisStrategy(window_minutes=1, upper_bps=3.0, lower_bps=3.5)

    live_states = []
    replay_states = []
    for ts, premium in observations:
        live_style.update(ts, premium)
        live_states.append(live_style.state())
    for ts, premium in observations:
        replay_style.update(ts, premium)
        replay_states.append(replay_style.state())

    assert replay_states == live_states
