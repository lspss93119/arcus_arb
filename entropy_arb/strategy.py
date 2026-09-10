from __future__ import annotations

import math
import statistics
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass

from .config import StrategyConf

OBSERVATION_INTERVAL_SEC = 1.0
MIN_COVERAGE_RATIO = 0.90
DISCONTINUITY_RESET_SEC = 30.0


@dataclass(frozen=True)
class StrategyState:
    ready: bool
    center_bps: float | None
    upper_bps: float
    lower_bps: float
    window_minutes: int | None = None
    warmup_span_sec: float | None = None
    coverage_ratio: float | None = None


@dataclass(frozen=True)
class RollingCenterUpdate:
    """A valid center recalculation, for startup/runtime observability."""

    old_center_bps: float
    new_center_bps: float
    window_sec: float
    samples: int
    range_min_bps: float
    range_max_bps: float
    observed_at: float


class StableBasisStrategy:
    name = "stable_basis"
    requires_observations = False

    def __init__(
        self,
        *,
        center_bps: float,
        upper_bps: float,
        lower_bps: float,
        center_mode: str = "fixed",
        center_window_hours: float = 12.0,
        center_update_minutes: int = 60,
    ):
        if center_mode not in ("fixed", "rolling"):
            raise ValueError("center_mode must be 'fixed' or 'rolling'")
        if not math.isfinite(center_bps):
            raise ValueError("center_bps must be finite")
        if not math.isfinite(center_window_hours) or center_window_hours <= 0:
            raise ValueError("center_window_hours must be > 0")
        if not isinstance(center_update_minutes, int) or center_update_minutes <= 0:
            raise ValueError("center_update_minutes must be a positive integer")

        self.center_mode = center_mode
        self.fixed_center_bps = center_bps
        self.center_window_hours = float(center_window_hours)
        self.center_update_minutes = center_update_minutes
        self.window_sec = self.center_window_hours * 3600.0
        self.update_sec = float(center_update_minutes * 60)
        self.requires_observations = center_mode == "rolling"
        self.upper_bps = upper_bps
        self.lower_bps = lower_bps
        self._effective_center_bps = center_bps
        self._rolling_samples: deque[tuple[float, float]] = deque()
        self._rolling_ready = False
        self._next_update_ts: float | None = None

    def _window_samples(self, timestamp: float) -> list[tuple[float, float]]:
        cutoff = timestamp - self.window_sec
        return [
            (ts, value)
            for ts, value in self._rolling_samples
            if cutoff <= ts < timestamp and math.isfinite(ts) and math.isfinite(value)
        ]

    def _valid_window_values(
        self, timestamp: float
    ) -> list[tuple[float, float]] | None:
        samples = self._window_samples(timestamp)
        if len(samples) < 2:
            return None
        span = max(ts for ts, _ in samples) - min(ts for ts, _ in samples)
        # A one-second tolerance accounts for the strict causal right edge
        # when observations arrive at the configured one-second cadence.
        if span + OBSERVATION_INTERVAL_SEC < self.window_sec:
            return None
        return samples

    def _prune(self, timestamp: float) -> None:
        cutoff = timestamp - self.window_sec
        while self._rolling_samples and self._rolling_samples[0][0] < cutoff:
            self._rolling_samples.popleft()

    def bootstrap(
        self,
        observations: Iterable[tuple[float, float]],
        *,
        now: float,
    ) -> RollingCenterUpdate | None:
        """Seed a rolling center from bounded historical observations.

        ``observations`` are expected to be chronological, as returned by the
        SQLite history query.  Values at or after ``now`` are deliberately
        excluded so bootstrap has the same causal boundary as live updates.
        """
        if self.center_mode != "rolling":
            return None
        self._rolling_samples.clear()
        self._rolling_ready = False
        self._effective_center_bps = self.fixed_center_bps
        if not math.isfinite(now):
            self._next_update_ts = None
            return None

        valid = sorted(
            (float(timestamp), float(value))
            for timestamp, value in observations
            if math.isfinite(timestamp) and math.isfinite(value) and timestamp < now
        )
        self._rolling_samples.extend(valid)
        self._prune(now)
        self._next_update_ts = now + self.update_sec
        samples = self._valid_window_values(now)
        if samples is None:
            return None
        values = [value for _, value in samples]
        new_center = statistics.median(values)
        old_center = self._effective_center_bps
        self._effective_center_bps = new_center
        self._rolling_ready = True
        return RollingCenterUpdate(
            old_center_bps=old_center,
            new_center_bps=new_center,
            window_sec=self.window_sec,
            samples=len(values),
            range_min_bps=min(values),
            range_max_bps=max(values),
            observed_at=now,
        )

    def update(
        self, timestamp: float, premium_bps: float
    ) -> RollingCenterUpdate | None:
        if self.center_mode != "rolling":
            return None
        if not (math.isfinite(timestamp) and math.isfinite(premium_bps)):
            return None

        self._rolling_samples.append((timestamp, premium_bps))
        self._prune(timestamp)
        if self._next_update_ts is None:
            self._next_update_ts = timestamp + self.update_sec
            return None
        if timestamp < self._next_update_ts:
            return None

        samples = self._valid_window_values(timestamp)
        self._next_update_ts = timestamp + self.update_sec
        if samples is None:
            # Keep the last valid rolling value, or the fixed fallback during
            # warm-up.  Missing history is never converted into a new center.
            return None
        values = [value for _, value in samples]
        new_center = statistics.median(values)
        old_center = self._effective_center_bps
        self._effective_center_bps = new_center
        self._rolling_ready = True
        return RollingCenterUpdate(
            old_center_bps=old_center,
            new_center_bps=new_center,
            window_sec=self.window_sec,
            samples=len(values),
            range_min_bps=min(values),
            range_max_bps=max(values),
            observed_at=timestamp,
        )

    def state(self) -> StrategyState:
        return StrategyState(
            ready=True,
            center_bps=self._effective_center_bps,
            upper_bps=self.upper_bps,
            lower_bps=self.lower_bps,
        )

    def rolling_history_summary(self, *, now: float) -> tuple[int, float]:
        samples = self._window_samples(now)
        if not samples:
            return 0, 0.0
        return (
            len(samples),
            max(ts for ts, _ in samples) - min(ts for ts, _ in samples),
        )


class DriftingBasisStrategy:
    name = "drifting_basis"
    requires_observations = True

    def __init__(self, *, window_minutes: int, upper_bps: float, lower_bps: float):
        self.window_minutes = window_minutes
        self.window_sec = float(window_minutes * 60)
        self.upper_bps = upper_bps
        self.lower_bps = lower_bps
        self._samples: deque[tuple[float, float]] = deque()
        self._segment_start_ts: float | None = None
        self._last_valid_ts: float | None = None

    def update(self, timestamp: float, premium_bps: float) -> None:
        if not (math.isfinite(timestamp) and math.isfinite(premium_bps)):
            return

        if (
            self._last_valid_ts is not None
            and timestamp - self._last_valid_ts > DISCONTINUITY_RESET_SEC
        ):
            self._samples.clear()
            self._segment_start_ts = None

        if self._segment_start_ts is None:
            self._segment_start_ts = timestamp

        self._last_valid_ts = timestamp
        self._samples.append((timestamp, premium_bps))

        cutoff = timestamp - self.window_sec
        while self._samples and self._samples[0][0] <= cutoff:
            self._samples.popleft()

    def state(self) -> StrategyState:
        if self._last_valid_ts is None or self._segment_start_ts is None:
            return StrategyState(
                ready=False,
                center_bps=None,
                upper_bps=self.upper_bps,
                lower_bps=self.lower_bps,
                window_minutes=self.window_minutes,
                warmup_span_sec=0.0,
                coverage_ratio=0.0,
            )

        span = max(self._last_valid_ts - self._segment_start_ts, 0.0)
        expected = self.window_sec / OBSERVATION_INTERVAL_SEC
        coverage = min(len(self._samples) / expected, 1.0)
        ready = span >= self.window_sec and coverage >= MIN_COVERAGE_RATIO
        center = (
            statistics.median(value for _, value in self._samples) if ready else None
        )
        return StrategyState(
            ready=ready,
            center_bps=center,
            upper_bps=self.upper_bps,
            lower_bps=self.lower_bps,
            window_minutes=self.window_minutes,
            warmup_span_sec=span,
            coverage_ratio=coverage,
        )


def build_strategy(conf: StrategyConf):
    if conf.name == "stable_basis":
        if conf.center_bps is None:
            raise ValueError("stable_basis requires center_bps")
        return StableBasisStrategy(
            center_bps=conf.center_bps,
            upper_bps=conf.upper_bps,
            lower_bps=conf.lower_bps,
            center_mode=conf.center_mode,
            center_window_hours=conf.center_window_hours,
            center_update_minutes=conf.center_update_minutes,
        )
    if conf.name == "drifting_basis":
        if conf.window_minutes is None:
            raise ValueError("drifting_basis requires window_minutes")
        return DriftingBasisStrategy(
            window_minutes=conf.window_minutes,
            upper_bps=conf.upper_bps,
            lower_bps=conf.lower_bps,
        )
    raise ValueError(f"unknown strategy {conf.name!r}")
