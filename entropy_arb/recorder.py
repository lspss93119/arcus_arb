"""Automatic 1-minute orderbook data recorder.

While the bot runs (live or --record-only), both venues' actual order books
are sampled once per second. Each valid BBO sample is persisted for persistence
research and aggregated into one SQLite minute row. These datasets support
offline market analysis and explicit strategy parameter selection in
config.yaml; the recorder never selects a strategy.

Definitions (all in bps, fees NOT included — the engine adds fees on top):

    premium    = (entropy_mid / hedge_mid - 1) * 1e4
                 the mid-to-mid premium of Entropy over the hedge venue;
                 its observed center informs a human-selected strategy center.
    sell_edge  = (entropy_bid / hedge_ask - 1) * 1e4
                 the EXECUTABLE premium for SELL-entropy/BUY-hedge; the
                 engine fires this direction when sell_edge clears the
                 selected strategy's center plus upper_bps (plus fees).
    buy_edge   = (hedge_bid / entropy_ask - 1) * 1e4
                 the executable premium for BUY-entropy/SELL-hedge; fires
                 when buy_edge clears lower_bps minus the selected center
                 (plus fees).

Bid/ask columns are the minute's last fresh sample (close). A row is only
written for minutes with at least one sample where both books were fresh;
`samples` says how many of the ~60 seconds qualified.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time

from .book import OrderBook
from .premium import calculate_premiums
from .storage import MarketHistoryStore, MinuteRow, SampleRow

log = logging.getLogger("recorder")


class _MinuteAgg:
    __slots__ = (
        "minute",
        "n",
        "p_open",
        "p_high",
        "p_low",
        "p_close",
        "p_sum",
        "p_sumsq",
        "s_sum",
        "s_max",
        "b_sum",
        "b_max",
        "e_bid",
        "e_ask",
        "h_bid",
        "h_ask",
    )

    def __init__(self, minute: int) -> None:
        self.minute = minute
        self.n = 0
        self.p_open = self.p_high = self.p_low = self.p_close = 0.0
        self.p_sum = self.p_sumsq = 0.0
        self.s_sum = 0.0
        self.s_max = -math.inf
        self.b_sum = 0.0
        self.b_max = -math.inf
        self.e_bid = self.e_ask = self.h_bid = self.h_ask = 0.0

    def add(
        self, e_bid: float, e_ask: float, h_bid: float, h_ask: float
    ) -> tuple[float, float, float]:
        values = calculate_premiums(e_bid, e_ask, h_bid, h_ask)
        prem = values.premium_bps
        sell_edge = values.sell_edge_bps
        buy_edge = values.buy_edge_bps
        if self.n == 0:
            self.p_open = self.p_high = self.p_low = prem
        self.n += 1
        self.p_high = max(self.p_high, prem)
        self.p_low = min(self.p_low, prem)
        self.p_close = prem
        self.p_sum += prem
        self.p_sumsq += prem * prem
        self.s_sum += sell_edge
        self.s_max = max(self.s_max, sell_edge)
        self.b_sum += buy_edge
        self.b_max = max(self.b_max, buy_edge)
        self.e_bid, self.e_ask, self.h_bid, self.h_ask = e_bid, e_ask, h_bid, h_ask
        return prem, sell_edge, buy_edge

    def row(self, symbol: str, hedge: str) -> MinuteRow:
        mean = self.p_sum / self.n
        var = max(self.p_sumsq / self.n - mean * mean, 0.0)
        return MinuteRow(
            minute_ts=self.minute * 60,
            symbol=symbol,
            hedge=hedge,
            entropy_bid=float(f"{self.e_bid:.10g}"),
            entropy_ask=float(f"{self.e_ask:.10g}"),
            hedge_bid=float(f"{self.h_bid:.10g}"),
            hedge_ask=float(f"{self.h_ask:.10g}"),
            premium_open_bps=float(f"{self.p_open:.3f}"),
            premium_high_bps=float(f"{self.p_high:.3f}"),
            premium_low_bps=float(f"{self.p_low:.3f}"),
            premium_close_bps=float(f"{self.p_close:.3f}"),
            premium_mean_bps=float(f"{mean:.3f}"),
            premium_std_bps=float(f"{math.sqrt(var):.3f}"),
            sell_edge_mean_bps=float(f"{self.s_sum / self.n:.3f}"),
            sell_edge_max_bps=float(f"{self.s_max:.3f}"),
            buy_edge_mean_bps=float(f"{self.b_sum / self.n:.3f}"),
            buy_edge_max_bps=float(f"{self.b_max:.3f}"),
            samples=self.n,
        )


class MinuteRecorder:
    def __init__(
        self,
        store: MarketHistoryStore,
        entropy_book: OrderBook,
        hedge_book: OrderBook,
        staleness_sec: float,
        interval_sec: float = 1.0,
        *,
        symbol: str,
        hedge: str,
    ) -> None:
        self.store = store
        self.symbol = symbol
        self.hedge = hedge
        self.entropy_book = entropy_book
        self.hedge_book = hedge_book
        self.staleness_sec = staleness_sec
        self.interval_sec = interval_sec
        self.rows_written = 0
        self._agg: _MinuteAgg | None = None

    def _flush_agg(self) -> None:
        if self._agg is None or self._agg.n == 0:
            self._agg = None
            return
        self.store.append_minute(self._agg.row(self.symbol, self.hedge))
        self.rows_written += 1
        self._agg = None

    def sample(self, now: float | None = None) -> None:
        """Take one sample; call ~1/sec. Rolls the minute over as needed."""
        now = time.time() if now is None else now
        minute = int(now // 60)
        if self._agg is not None and self._agg.minute != minute:
            self._flush_agg()
        if not (
            self.entropy_book.is_fresh(self.staleness_sec)
            and self.hedge_book.is_fresh(self.staleness_sec)
        ):
            return
        e_bid, e_ask = self.entropy_book.best_bid(), self.entropy_book.best_ask()
        h_bid, h_ask = self.hedge_book.best_bid(), self.hedge_book.best_ask()
        if None in (e_bid, e_ask, h_bid, h_ask):
            return
        if self._agg is None:
            self._agg = _MinuteAgg(minute)
        premium, sell_edge, buy_edge = self._agg.add(e_bid, e_ask, h_bid, h_ask)
        self.store.append_sample(
            SampleRow(
                timestamp_ms=int(now * 1000),
                symbol=self.symbol,
                hedge=self.hedge,
                premium_bps=premium,
                sell_edge_bps=sell_edge,
                buy_edge_bps=buy_edge,
                entropy_bid=e_bid,
                entropy_ask=e_ask,
                hedge_bid=h_bid,
                hedge_ask=h_ask,
                entropy_book_update_ms=int(self.entropy_book.last_update_ts * 1000),
                hedge_book_update_ms=int(self.hedge_book.last_update_ts * 1000),
            )
        )

    def close(self) -> None:
        """Flush the partial minute; Engine owns the shared store lifecycle."""
        self._flush_agg()

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    self.sample()
                except Exception:
                    log.exception("recorder sample failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval_sec)
                except TimeoutError:
                    pass
        finally:
            self.close()
            log.info("recorder stopped — %d minute row(s) buffered", self.rows_written)
