"""Fail-closed local L2 state for Arcus ``l2OrderbookUpdates``."""

from __future__ import annotations

import math
import time

from .arcus import ArcusBBO, ArcusBookSnapshot, ArcusBookUpdate
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
        self.book_epoch = 0
        self.sequence_id: int | None = None
        self.global_sequence_id: int | None = None
        self.exchange_timestamp_us: int | None = None
        self.local_receive_ts_ms: int | None = None
        self.local_receive_monotonic_ns: int | None = None
        self.sequence_gap_count = 0
        self.boundary_gap_count = 0
        self.first_delta_after_snapshot = False
        self.boundary_bbo_pending = False
        self.bbo_sequence_id: int | None = None
        self.bbo_global_sequence_id: int | None = None
        self.bbo_exchange_timestamp_us: int | None = None
        self._latest_bbo: ArcusBBO | None = None
        self._latest_bbo_receive_ts_ms: int | None = None
        self._latest_bbo_receive_monotonic_ns: int | None = None

    @property
    def sequence_health(self) -> str:
        return self.health

    @property
    def best_bid_size(self) -> float | None:
        price = self.best_bid()
        return self.bids.get(price) if price is not None else None

    @property
    def best_ask_size(self) -> float | None:
        price = self.best_ask()
        return self.asks.get(price) if price is not None else None

    def clear(self) -> None:
        super().clear()
        self.sequence_id = None
        self.global_sequence_id = None
        self.exchange_timestamp_us = None
        self.local_receive_ts_ms = None
        self.local_receive_monotonic_ns = None
        self.first_delta_after_snapshot = False
        self.boundary_bbo_pending = False
        self.bbo_sequence_id = None
        self.bbo_global_sequence_id = None
        self.bbo_exchange_timestamp_us = None
        self._latest_bbo = None
        self._latest_bbo_receive_ts_ms = None
        self._latest_bbo_receive_monotonic_ns = None

    def mark_stale(self) -> None:
        self.clear()
        self.health = "STALE"

    def mark_resync(self) -> None:
        self.clear()
        self.health = "RESYNC"

    def _receive(self, wall_ms: int | None, monotonic_ns: int | None) -> None:
        now_ms = int(time.time() * 1000) if wall_ms is None else int(wall_ms)
        self.local_receive_ts_ms = now_ms
        self.local_receive_monotonic_ns = monotonic_ns
        self.last_update_ts = now_ms / 1000.0
        self.alive_ts = self.last_update_ts

    @staticmethod
    def _apply_levels(
        target: dict[float, float], levels: tuple[tuple[str, str], ...]
    ) -> None:
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
        local_receive_ts_ms: int | None = None,
        local_receive_monotonic_ns: int | None = None,
    ) -> None:
        # Every subscribe-time snapshot starts a new replayable continuous
        # segment, including the first snapshot after startup/reconnect or a
        # sequence-gap resync.
        self.book_epoch += 1
        self.bids.clear()
        self.asks.clear()
        self._apply_levels(self.bids, snapshot.bids)
        self._apply_levels(self.asks, snapshot.asks)
        self.sequence_id = snapshot.last_sequence_id
        self.global_sequence_id = snapshot.global_sequence_id
        self.exchange_timestamp_us = snapshot.exchange_timestamp_us
        self._receive(local_receive_ts_ms, local_receive_monotonic_ns)
        self.first_delta_after_snapshot = True
        self.boundary_bbo_pending = False
        self.bbo_sequence_id = None
        self.bbo_global_sequence_id = None
        self.bbo_exchange_timestamp_us = None
        if (
            self._latest_bbo is not None
            and self._latest_bbo.last_sequence_id < snapshot.last_sequence_id
        ):
            self._latest_bbo = None
            self._latest_bbo_receive_ts_ms = None
            self._latest_bbo_receive_monotonic_ns = None
        # Synchronization readiness is independent from whether either side
        # currently has a level; ``is_fresh`` separately requires both BBO
        # sides before a premium sample is accepted.
        self.ready = True
        self.health = "OK"
        if self._latest_bbo is not None:
            self._reconcile_matching_bbo()

    def apply_update(
        self,
        update: ArcusBookUpdate,
        local_receive_ts_ms: int | None = None,
        local_receive_monotonic_ns: int | None = None,
    ) -> bool:
        if self.sequence_id is None or self.health in ("STALE", "RESYNC"):
            return False
        expected = self.sequence_id + 1
        if update.last_sequence_id < expected:
            # A replayed frame is harmless and must not make a valid book stale.
            return True
        if self.first_delta_after_snapshot:
            if update.last_sequence_id > expected:
                self.boundary_gap_count += 1
                self.boundary_bbo_pending = True
                self.ready = False
                self.health = "BOUNDARY"
            self.first_delta_after_snapshot = False
        elif update.last_sequence_id != expected:
            self.sequence_gap_count += 1
            self.mark_resync()
            return False
        self._apply_levels(self.bids, update.bids)
        self._apply_levels(self.asks, update.asks)
        self.sequence_id = update.last_sequence_id
        self.global_sequence_id = update.global_sequence_id
        self.exchange_timestamp_us = update.exchange_timestamp_us
        self._receive(local_receive_ts_ms, local_receive_monotonic_ns)
        if self.boundary_bbo_pending:
            self._reconcile_matching_bbo()
        elif self.health != "BOUNDARY":
            self.ready = True
            self.health = "OK"
        return True

    @staticmethod
    def _apply_authoritative_top(
        target: dict[float, float],
        level: tuple[str, str] | None,
        *,
        is_bid: bool,
    ) -> None:
        if level is None:
            target.clear()
            return
        price = float(level[0])
        size = float(level[1])
        if not math.isfinite(price) or not math.isfinite(size) or size <= 0:
            raise ValueError("Arcus BBO level must be finite and positive")
        for existing_price in tuple(target):
            if (is_bid and existing_price > price) or (
                not is_bid and existing_price < price
            ):
                target.pop(existing_price, None)
        target[price] = size

    def _reconcile_matching_bbo(self) -> bool:
        bbo = self._latest_bbo
        if bbo is None or self.sequence_id != bbo.last_sequence_id:
            return False
        self._apply_authoritative_top(self.bids, bbo.best_bid, is_bid=True)
        self._apply_authoritative_top(self.asks, bbo.best_ask, is_bid=False)
        self.bbo_sequence_id = bbo.last_sequence_id
        self.bbo_global_sequence_id = bbo.global_sequence_id
        self.bbo_exchange_timestamp_us = bbo.exchange_timestamp_us
        self._receive(
            self._latest_bbo_receive_ts_ms,
            self._latest_bbo_receive_monotonic_ns,
        )
        if self.boundary_bbo_pending:
            self.boundary_bbo_pending = False
            self.ready = True
            self.health = "OK"
        return True

    def apply_bbo(
        self,
        bbo: ArcusBBO,
        local_receive_ts_ms: int | None = None,
        local_receive_monotonic_ns: int | None = None,
    ) -> bool:
        """Apply a matching-sequence BBO without fabricating L2 events."""
        if self.sequence_id is None:
            self._latest_bbo = bbo
            self._latest_bbo_receive_ts_ms = local_receive_ts_ms
            self._latest_bbo_receive_monotonic_ns = local_receive_monotonic_ns
            return False
        if bbo.last_sequence_id == self.sequence_id:
            self._latest_bbo = bbo
            self._latest_bbo_receive_ts_ms = local_receive_ts_ms
            self._latest_bbo_receive_monotonic_ns = local_receive_monotonic_ns
            return self._reconcile_matching_bbo()
        if bbo.last_sequence_id < self.sequence_id:
            return False
        if self._latest_bbo is not None and (
            bbo.last_sequence_id < self._latest_bbo.last_sequence_id
        ):
            return False
        self._latest_bbo = bbo
        self._latest_bbo_receive_ts_ms = local_receive_ts_ms
        self._latest_bbo_receive_monotonic_ns = local_receive_monotonic_ns
        return self._reconcile_matching_bbo()
