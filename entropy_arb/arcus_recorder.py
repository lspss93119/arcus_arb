"""Record-only Arcus × Lighter-RH market history."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import Optional

from .arcus import (
    ArcusL2Event,
    ArcusMarketAttributes,
    ArcusMarketMetadata,
    ArcusTrade,
)
from .arcus_book import ArcusOrderBook
from .premium import calculate_premiums
from .storage import (
    ArcusMarketAttributesRow,
    ArcusL2EventRow,
    ArcusMarketMetadataRow,
    ArcusMinuteRow,
    ArcusSampleRow,
    ArcusTradeRow,
    MarketHistoryStore,
)

log = logging.getLogger("arcus-recorder")


def _book_size(book, side: str, price: Optional[float]) -> float:
    if price is None:
        return 0.0
    value = getattr(book, f"best_{side}_size", None)
    if value is not None:
        return float(value)
    levels = getattr(book, f"{side}s", {})
    return float(levels.get(price, 0.0))


class _ArcusMinuteAgg:
    __slots__ = (
        "minute", "n", "p_open", "p_high", "p_low", "p_close", "p_sum",
        "p_sumsq", "arcus_bid", "arcus_ask", "rh_bid", "rh_ask",
    )

    def __init__(self, minute: int) -> None:
        self.minute = minute
        self.n = 0
        self.p_open = self.p_high = self.p_low = self.p_close = 0.0
        self.p_sum = self.p_sumsq = 0.0
        self.arcus_bid = self.arcus_ask = self.rh_bid = self.rh_ask = 0.0

    def add(self, arcus_bid: float, arcus_ask: float,
            rh_bid: float, rh_ask: float) -> float:
        premium = calculate_premiums(
            arcus_bid, arcus_ask, rh_bid, rh_ask
        ).premium_bps
        if self.n == 0:
            self.p_open = self.p_high = self.p_low = premium
        self.n += 1
        self.p_high = max(self.p_high, premium)
        self.p_low = min(self.p_low, premium)
        self.p_close = premium
        self.p_sum += premium
        self.p_sumsq += premium * premium
        self.arcus_bid, self.arcus_ask = arcus_bid, arcus_ask
        self.rh_bid, self.rh_ask = rh_bid, rh_ask
        return premium

    def row(self, symbol: str, hedge: str) -> ArcusMinuteRow:
        mean = self.p_sum / self.n
        variance = max(self.p_sumsq / self.n - mean * mean, 0.0)
        return ArcusMinuteRow(
            minute_ts=self.minute * 60,
            symbol=symbol,
            hedge=hedge,
            arcus_bid=float(f"{self.arcus_bid:.10g}"),
            arcus_ask=float(f"{self.arcus_ask:.10g}"),
            rh_bid=float(f"{self.rh_bid:.10g}"),
            rh_ask=float(f"{self.rh_ask:.10g}"),
            premium_open_bps=float(f"{self.p_open:.3f}"),
            premium_high_bps=float(f"{self.p_high:.3f}"),
            premium_low_bps=float(f"{self.p_low:.3f}"),
            premium_close_bps=float(f"{self.p_close:.3f}"),
            premium_mean_bps=float(f"{mean:.3f}"),
            premium_std_bps=float(f"{math.sqrt(variance):.3f}"),
            samples=self.n,
        )


class ArcusMarketRecorder:
    """Persist one-second BBO samples plus separate events and metadata."""

    def __init__(
        self,
        store: MarketHistoryStore,
        *,
        symbol: str,
        arcus_book: ArcusOrderBook,
        rh_book,
        hedge: str = "lighter-rh",
        is_fresh_seconds: float = 10.0,
        interval_sec: float = 1.0,
    ) -> None:
        self.store = store
        self.symbol = symbol
        self.hedge = hedge
        self.arcus_book = arcus_book
        self.rh_book = rh_book
        self.is_fresh_seconds = is_fresh_seconds
        self.interval_sec = interval_sec
        self.rows_written = 0
        self.minute_rows_written = 0
        self.l2_events_written = 0
        self._agg: Optional[_ArcusMinuteAgg] = None
        self.attributes: Optional[ArcusMarketAttributes] = None
        self.market_metadata: Optional[ArcusMarketMetadata] = None

    def record_metadata(
        self, metadata: ArcusMarketMetadata, discovered_at_ms: Optional[int] = None
    ) -> None:
        self.market_metadata = metadata
        self.store.append_arcus_market_metadata(ArcusMarketMetadataRow(
            discovered_at_ms=(int(time.time() * 1000)
                              if discovered_at_ms is None else int(discovered_at_ms)),
            symbol=self.symbol,
            market_id=metadata.market_id,
            market_display_name=metadata.symbol,
            status=metadata.status,
            tick_size=metadata.tick_size,
            step_size=metadata.step_size,
            min_order_size=metadata.min_order_size,
            min_order_notional=metadata.min_order_notional,
            max_order_size=metadata.max_order_size,
            is_outside_rth=metadata.is_outside_rth,
            current_settlement_price=metadata.current_settlement_price,
            upper_trading_bound=metadata.upper_trading_bound,
            lower_trading_bound=metadata.lower_trading_bound,
            next_upper_trading_bound=metadata.next_upper_trading_bound,
            next_lower_trading_bound=metadata.next_lower_trading_bound,
            regular_trading_hours=(
                json.dumps(metadata.regular_trading_hours, sort_keys=True)
                if metadata.regular_trading_hours is not None else None
            ),
        ))

    def record_attributes(
        self,
        attributes: ArcusMarketAttributes,
        local_receive_ts_ms: int,
        local_receive_monotonic_ns: Optional[int],
        market_status: str = "UNKNOWN",
    ) -> None:
        self.attributes = attributes
        self.store.append_arcus_market_attributes(ArcusMarketAttributesRow(
            symbol=self.symbol,
            market_id=attributes.market_id,
            market_display_name=attributes.market_display_name,
            market_status=market_status,
            local_receive_ts_ms=int(local_receive_ts_ms),
            local_receive_monotonic_ns=local_receive_monotonic_ns,
            event_timestamp_us=attributes.event_timestamp_us,
            market_sequence_num=attributes.market_sequence_num,
            is_outside_rth=attributes.is_outside_rth,
            current_settlement_price=attributes.current_settlement_price,
            upper_trading_bound=attributes.upper_trading_bound,
            lower_trading_bound=attributes.lower_trading_bound,
            next_upper_trading_bound=attributes.next_upper_trading_bound,
            next_lower_trading_bound=attributes.next_lower_trading_bound,
        ))

    def record_trade(
        self,
        trade: ArcusTrade,
        local_receive_ts_ms: int,
        local_receive_monotonic_ns: Optional[int],
    ) -> None:
        market_id = trade.market_id
        if market_id is None and self.market_metadata is not None:
            market_id = self.market_metadata.market_id
        if market_id is None:
            log.warning("[ARCUS] dropping trade without a market id")
            return
        self.store.append_arcus_trade(ArcusTradeRow(
            symbol=self.symbol,
            market_id=market_id,
            market_display_name=trade.market_display_name or (
                self.market_metadata.symbol if self.market_metadata else ""
            ),
            trade_id=trade.trade_id or "",
            exchange_timestamp_us=trade.exchange_timestamp_us,
            local_receive_ts_ms=int(local_receive_ts_ms),
            local_receive_monotonic_ns=local_receive_monotonic_ns,
            price=trade.price,
            quantity=trade.quantity,
            aggressor_side=trade.aggressor_side,
            sequence_number=trade.sequence_number,
        ))

    def record_l2_event(self, event: ArcusL2Event) -> None:
        """Append one raw Arcus L2 level without committing synchronously."""
        market_id = event.market_id
        if market_id is None and self.market_metadata is not None:
            market_id = self.market_metadata.market_id
        if market_id is None:
            # ArcusVenue always resolves a market id before starting this
            # feed. Keep a malformed/direct feed observable rather than
            # losing an otherwise replayable event.
            market_id = -1
            log.warning("[ARCUS] L2 event has no market id; storing -1")
        self.store.append_arcus_l2_event(ArcusL2EventRow(
            symbol=self.symbol,
            market_id=int(market_id),
            event_type=event.event_type,
            book_epoch=event.book_epoch,
            local_receive_ts_ms=int(event.local_receive_ts_ms),
            local_receive_monotonic_ns=int(event.local_receive_monotonic_ns),
            last_sequence_id=event.last_sequence_id,
            global_sequence_id=event.global_sequence_id,
            side=event.side,
            price=event.price,
            absolute_size=event.absolute_size,
            event_index=event.event_index,
            exchange_timestamp_us=event.exchange_timestamp_us,
        ))
        self.l2_events_written += 1

    def _flush_minute(self) -> None:
        if self._agg is None or self._agg.n == 0:
            self._agg = None
            return
        self.store.append_arcus_minute(self._agg.row(self.symbol, self.hedge))
        self.minute_rows_written += 1
        self._agg = None

    def record_sample(
        self,
        *,
        timestamp_ms: Optional[int] = None,
        monotonic_ns: Optional[int] = None,
    ) -> bool:
        now_ms = int(time.time() * 1000) if timestamp_ms is None else int(timestamp_ms)
        now_s = now_ms / 1000.0
        minute = int(now_s // 60)
        if self._agg is not None and self._agg.minute != minute:
            self._flush_minute()
        if not (
            self.arcus_book.is_fresh(self.is_fresh_seconds)
            and self.rh_book.is_fresh(self.is_fresh_seconds)
        ):
            return False
        arcus_bid, arcus_ask = self.arcus_book.best_bid(), self.arcus_book.best_ask()
        rh_bid, rh_ask = self.rh_book.best_bid(), self.rh_book.best_ask()
        if None in (arcus_bid, arcus_ask, rh_bid, rh_ask):
            return False
        assert arcus_bid is not None and arcus_ask is not None
        assert rh_bid is not None and rh_ask is not None
        if self._agg is None:
            self._agg = _ArcusMinuteAgg(minute)
        premium = self._agg.add(arcus_bid, arcus_ask, rh_bid, rh_ask)
        arcus_receive_ms = self.arcus_book.local_receive_ts_ms
        if arcus_receive_ms is None:
            arcus_receive_ms = int(self.arcus_book.last_update_ts * 1000)
        rh_receive_ms = int(getattr(self.rh_book, "last_update_ts", now_s) * 1000)
        attrs = self.attributes
        self.store.append_arcus_sample(ArcusSampleRow(
            timestamp_ms=now_ms,
            symbol=self.symbol,
            arcus_bid=arcus_bid,
            arcus_ask=arcus_ask,
            arcus_bid_size=_book_size(self.arcus_book, "bid", arcus_bid),
            arcus_ask_size=_book_size(self.arcus_book, "ask", arcus_ask),
            arcus_mid=(arcus_bid + arcus_ask) / 2.0,
            rh_bid=rh_bid,
            rh_ask=rh_ask,
            rh_bid_size=_book_size(self.rh_book, "bid", rh_bid),
            rh_ask_size=_book_size(self.rh_book, "ask", rh_ask),
            rh_mid=(rh_bid + rh_ask) / 2.0,
            premium_bps=premium,
            arcus_book_sequence_id=self.arcus_book.sequence_id,
            arcus_global_sequence_id=self.arcus_book.global_sequence_id,
            arcus_exchange_timestamp_us=self.arcus_book.exchange_timestamp_us,
            arcus_local_receive_ts_ms=arcus_receive_ms,
            arcus_local_receive_monotonic_ns=self.arcus_book.local_receive_monotonic_ns,
            rh_local_receive_ts_ms=rh_receive_ms,
            is_outside_rth=attrs.is_outside_rth if attrs else None,
            current_settlement_price=(attrs.current_settlement_price if attrs else None),
            upper_trading_bound=attrs.upper_trading_bound if attrs else None,
            lower_trading_bound=attrs.lower_trading_bound if attrs else None,
            next_upper_trading_bound=attrs.next_upper_trading_bound if attrs else None,
            next_lower_trading_bound=attrs.next_lower_trading_bound if attrs else None,
            hedge=self.hedge,
        ))
        self.rows_written += 1
        return True

    def close(self) -> None:
        self._flush_minute()

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    self.record_sample()
                except Exception:
                    log.exception("Arcus recorder sample failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval_sec)
                except asyncio.TimeoutError:
                    pass
        finally:
            self.close()
            log.info(
                "[ARCUS] recorder stopped — %d samples, %d minute rows, "
                "%d L2 events buffered",
                self.rows_written,
                self.minute_rows_written,
                self.l2_events_written,
            )
