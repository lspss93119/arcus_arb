"""Fail-closed local L2 state for Arcus ``l2OrderbookUpdates``."""
from __future__ import annotations

import math
import time
from typing import Optional

from .arcus import ArcusBookSnapshot, ArcusBookUpdate
from .book import OrderBook


class ArcusOrderBook(OrderBook):
    """Small local Arcus book synchronized by per-market sequence IDs.

    Arcus deltas contain absolute level sizes.  A gap clears the book and
    leaves it unusable until a later subscribe-time snapshot is applied.
    ``globalSequenceId`` is retained as telemetry only; it is not used as the
    per-market gap anchor.
    """

    def __init__(self) -> None:
        super().__init__()
        self.health = "STALE"
        self.sequence_id: Optional[int] = None
        self.global_sequence_id: Optional[int] = None
        self.exchange_timestamp_us: Optional[int] = None
        self.local_receive_ts_ms: Optional[int] = None
        self.local_receive_monotonic_ns: Optional[int] = None
        self.sequence_gap_count = 0

    @property
    def sequence_health(self) -> str:
        return self.health

    @property
    def best_bid_size(self) -> Optional[float]:
        price = self.best_bid()
        return self.bids.get(price) if price is not None else None

    @property
    def best_ask_size(self) -> Optional[float]:
        price = self.best_ask()
        return self.asks.get(price) if price is not None else None

    def clear(self) -> None:
        super().clear()
        self.sequence_id = None
        self.global_sequence_id = None
        self.exchange_timestamp_us = None
        self.local_receive_ts_ms = None
        self.local_receive_monotonic_ns = None

    def mark_stale(self) -> None:
        self.clear()
        self.health = "STALE"

    def mark_resync(self) -> None:
        self.clear()
        self.health = "RESYNC"

    def _receive(self, wall_ms: Optional[int], monotonic_ns: Optional[int]) -> None:
        now_ms = int(time.time() * 1000) if wall_ms is None else int(wall_ms)
        self.local_receive_ts_ms = now_ms
        self.local_receive_monotonic_ns = monotonic_ns
        self.last_update_ts = now_ms / 1000.0
        self.alive_ts = self.last_update_ts

    @staticmethod
    def _apply_levels(target: dict[float, float], levels: tuple[tuple[str, str], ...]) -> None:
        for price_text, size_text in levels:
            price = float(price_text)
            size = float(size_text)
            if not math.isfinite(price) or not math.isfinite(size):
                raise ValueError("Arcus book level must be finite")
            if size <= 0:
                target.pop(price, None)
            else:
                target[price] = size

    def apply_snapshot(
        self,
        snapshot: ArcusBookSnapshot,
        local_receive_ts_ms: Optional[int] = None,
        local_receive_monotonic_ns: Optional[int] = None,
    ) -> None:
        self.bids.clear()
        self.asks.clear()
        self._apply_levels(self.bids, snapshot.bids)
        self._apply_levels(self.asks, snapshot.asks)
        self.sequence_id = snapshot.last_sequence_id
        self.global_sequence_id = snapshot.global_sequence_id
        self.exchange_timestamp_us = snapshot.exchange_timestamp_us
        self._receive(local_receive_ts_ms, local_receive_monotonic_ns)
        # Synchronization readiness is independent from whether either side
        # currently has a level; ``is_fresh`` separately requires both BBO
        # sides before a premium sample is accepted.
        self.ready = True
        self.health = "OK"

    def apply_update(
        self,
        update: ArcusBookUpdate,
        local_receive_ts_ms: Optional[int] = None,
        local_receive_monotonic_ns: Optional[int] = None,
    ) -> bool:
        if not self.ready or self.health != "OK" or self.sequence_id is None:
            return False
        expected = self.sequence_id + 1
        if update.last_sequence_id < expected:
            # A replayed frame is harmless and must not make a valid book stale.
            return True
        if update.last_sequence_id != expected:
            self.sequence_gap_count += 1
            self.mark_resync()
            return False
        self._apply_levels(self.bids, update.bids)
        self._apply_levels(self.asks, update.asks)
        self.sequence_id = update.last_sequence_id
        self.global_sequence_id = update.global_sequence_id
        self.exchange_timestamp_us = update.exchange_timestamp_us
        self._receive(local_receive_ts_ms, local_receive_monotonic_ns)
        self.ready = True
        self.health = "OK"
        return self.ready
