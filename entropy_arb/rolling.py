"""Causal rolling-median signal model for Arcus × hedge strategies.

This is the venue-agnostic strategy core ported from ``entropy-arb``.  It
consumes completed minute rows only and never lets the current update block
influence its own center.
"""

from __future__ import annotations

import csv
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from statistics import median


@dataclass(frozen=True)
class RollingConfig:
    window_hours: float = 12.0
    update_minutes: int = 15
    min_coverage_pct: float = 80.0
    min_exit_capture_bps: float = 0.0
    persistence_sec: float = 10.0

    def __post_init__(self) -> None:
        if not math.isfinite(self.window_hours) or self.window_hours <= 0:
            raise ValueError("window_hours must be finite and > 0")
        if not isinstance(self.update_minutes, int) or self.update_minutes <= 0:
            raise ValueError("update_minutes must be a positive integer")
        if (
            not math.isfinite(self.min_coverage_pct)
            or self.min_coverage_pct <= 0
            or self.min_coverage_pct > 100
        ):
            raise ValueError("min_coverage_pct must be in (0, 100]")
        if (
            not math.isfinite(self.min_exit_capture_bps)
            or self.min_exit_capture_bps < 0
        ):
            raise ValueError("min_exit_capture_bps must be finite and >= 0")
        if not math.isfinite(self.persistence_sec) or self.persistence_sec < 0:
            raise ValueError("persistence_sec must be finite and >= 0")


@dataclass(frozen=True)
class RollingSnapshot:
    block_start_ts: float
    valid: bool
    median_bps: float | None
    valid_minutes: int
    coverage_pct: float
    reason: str


@dataclass(frozen=True)
class RollingSignal:
    direction: str
    reason: str
    center_bps: float | None
    snapshot_ts: float
    coverage_pct: float
    valid_minutes: int


class RollingWindow:
    """Store completed minute closes and calculate strictly walk-forward gates."""

    def __init__(self, config: RollingConfig) -> None:
        self.config = config
        self._points: dict[float, float] = {}
        self._snapshots: dict[float, RollingSnapshot] = {}

    @property
    def points(self) -> tuple[tuple[float, float], ...]:
        return tuple(sorted(self._points.items()))

    @property
    def expected_minutes(self) -> int:
        return max(1, int(math.ceil(self.config.window_hours * 60.0)))

    @property
    def min_valid_minutes(self) -> int:
        return max(
            1,
            int(
                math.ceil(self.expected_minutes * self.config.min_coverage_pct / 100.0)
            ),
        )

    def ingest_row(self, row: Mapping[str, object]) -> bool:
        """Add one completed recorder row; return false for unusable rows."""
        try:
            minute_ts = float(str(row["minute_ts"]))
            samples = int(float(str(row.get("samples", 0))))
            premium = float(str(row["premium_close_bps"]))
        except (KeyError, TypeError, ValueError):
            return False
        if not math.isfinite(minute_ts) or not math.isfinite(premium) or samples <= 0:
            return False
        self._points[minute_ts] = premium
        self._snapshots.clear()
        return True

    def load_csv(self, path: str) -> int:
        """Seed from an existing recorder CSV, returning accepted row count."""
        if not path or not os.path.exists(path):
            return 0
        accepted = 0
        with open(path, newline="") as fh:
            for row in csv.DictReader(fh):
                accepted += int(self.ingest_row(row))
        return accepted

    def _block_start(self, now: float) -> float:
        interval = self.config.update_minutes * 60
        return math.floor(float(now) / interval) * interval

    def snapshot_for(self, block_start_ts: float) -> RollingSnapshot:
        """Calculate the snapshot from rows strictly before this block."""
        block_start_ts = float(block_start_ts)
        cached = self._snapshots.get(block_start_ts)
        if cached is not None:
            return cached
        start = block_start_ts - self.config.window_hours * 3600.0
        values = [
            value
            for ts, value in self._points.items()
            if start <= ts < block_start_ts and math.isfinite(value)
        ]
        valid_minutes = len(values)
        coverage_pct = min(100.0, valid_minutes / self.expected_minutes * 100.0)
        gap_count = max(self.expected_minutes - valid_minutes, 0)
        if valid_minutes < self.min_valid_minutes:
            snapshot = RollingSnapshot(
                block_start_ts,
                False,
                None,
                valid_minutes,
                coverage_pct,
                f"insufficient coverage ({gap_count} missing)",
            )
        else:
            snapshot = RollingSnapshot(
                block_start_ts,
                True,
                median(values),
                valid_minutes,
                coverage_pct,
                "quality ok",
            )
        self._snapshots[block_start_ts] = snapshot
        return snapshot

    def signal(
        self,
        premium_bps: float,
        now: float,
        *,
        upper_bps: float,
        lower_bps: float,
    ) -> RollingSignal | None:
        """Return the current directional signal around the causal median."""
        try:
            premium = float(premium_bps)
            upper = float(upper_bps)
            lower = float(lower_bps)
        except (TypeError, ValueError):
            return None
        if (
            not math.isfinite(premium)
            or not math.isfinite(upper)
            or not math.isfinite(lower)
            or upper <= 0
            or lower <= 0
        ):
            return None
        snapshot = self.snapshot_for(self._block_start(now))
        center = snapshot.median_bps
        if not snapshot.valid or center is None:
            return None
        if premium >= center + upper:
            direction = "sell_arcus"
        elif premium <= center - lower:
            direction = "buy_arcus"
        else:
            return None
        return RollingSignal(
            direction=direction,
            reason="entry",
            center_bps=center,
            snapshot_ts=snapshot.block_start_ts,
            coverage_pct=snapshot.coverage_pct,
            valid_minutes=snapshot.valid_minutes,
        )
