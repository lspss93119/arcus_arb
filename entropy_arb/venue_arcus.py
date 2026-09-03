"""Arcus public market-data venue for Phase A.

This adapter intentionally has no order, wallet, credential, or signing
implementation.  Its public surface is market discovery plus the read-only
book/trade/attribute feed.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Optional

import aiohttp

from .arcus import ARCUS_REST_URL, ARCUS_WS_URL, ArcusMarketMetadata, resolve_arcus_market
from .arcus_book import ArcusOrderBook
from .arcus_feed import ArcusBookFeed

log = logging.getLogger("arcus")
REST_TIMEOUT = 10.0


class ArcusVenue:
    """Read-only Arcus venue; execution is a deliberate local hard stop."""

    kind = "arcus"
    supports_trading = False

    def __init__(
        self,
        conf: Any = None,
        session: Optional[aiohttp.ClientSession] = None,
        *,
        rest_url: str = ARCUS_REST_URL,
        ws_url: str = ARCUS_WS_URL,
    ) -> None:
        self.conf = conf
        self.key = "arcus"
        self.name = "ARCUS"
        self.session = session
        self.rest_url = rest_url.rstrip("/")
        self.ws_url = ws_url
        self.book = ArcusOrderBook()
        self.market: Optional[ArcusMarketMetadata] = None
        self.market_id = -1
        self.exchange_symbol = ""
        self.price_tick = 0.0
        self.size_step = 0.0
        self.min_base = 0.0
        self.min_quote = 0.0
        self.size_decimals = 0
        self.price_decimals = 0
        self.fee_bps = float(getattr(conf, "fee_bps", 0.0))
        self.cap_usd = float(getattr(conf, "cap_usd", 0.0))
        self.orders_per_min = int(getattr(conf, "orders_per_min", 0))
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0
        self.equity = None
        self.free = None
        self.start_equity = None
        self.last_traded_ts = 0.0
        self.latest_attributes = None
        self._l2_event_sink = None
        self._trade_sink = None
        self._attribute_sink = None

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        if self.session is None:
            raise RuntimeError("Arcus public HTTP session is not configured")
        async with self.session.get(
            self.rest_url + path,
            params=params,
            timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT),
        ) as response:
            response.raise_for_status()
            payload = await response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Arcus market discovery returned a non-object response")
        return payload

    async def load_market(self) -> ArcusMarketMetadata:
        """Resolve the CLI symbol from current public Arcus metadata."""
        requested = getattr(self.conf, "symbol", "")
        payload = await self._get("/v1/markets")
        markets = payload.get("markets")
        if not isinstance(markets, list):
            raise RuntimeError("Arcus market discovery response has no markets array")
        try:
            metadata = resolve_arcus_market(markets, requested)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        self.market = metadata
        self.market_id = metadata.market_id
        self.exchange_symbol = metadata.symbol
        self.price_tick = float(Decimal(metadata.tick_size))
        self.size_step = float(Decimal(metadata.step_size))
        self.price_decimals = max(0, -Decimal(metadata.tick_size).as_tuple().exponent)
        self.size_decimals = max(0, -Decimal(metadata.step_size).as_tuple().exponent)
        self.min_base = float(Decimal(metadata.min_order_size or "0"))
        self.min_quote = float(Decimal(metadata.min_order_notional or "0"))
        self.name = "ARCUS"
        log.info(
            "[ARCUS] market id=%d symbol=%s tick_size=%s step_size=%s "
            "min_order_size=%s min_order_notional=%s status=%s "
            "isOutsideRth=%s currentSettlementPrice=%s "
            "upperTradingBound=%s lowerTradingBound=%s "
            "nextUpperTradingBound=%s nextLowerTradingBound=%s",
            metadata.market_id,
            metadata.symbol,
            metadata.tick_size,
            metadata.step_size,
            metadata.min_order_size,
            metadata.min_order_notional,
            metadata.status,
            metadata.is_outside_rth,
            metadata.current_settlement_price,
            metadata.upper_trading_bound,
            metadata.lower_trading_bound,
            metadata.next_upper_trading_bound,
            metadata.next_lower_trading_bound,
        )
        return metadata

    def set_market_data_sinks(
        self, trade_sink=None, attribute_sink=None, l2_event_sink=None
    ) -> None:
        self._l2_event_sink = l2_event_sink
        self._trade_sink = trade_sink
        self._attribute_sink = attribute_sink

    def start_tasks(self, stop, notify, live: bool = False) -> list:
        if live:
            raise RuntimeError("Arcus trading is not implemented in Phase A")
        if self.market is None:
            raise RuntimeError("Arcus market must be resolved before starting feed")
        feed = ArcusBookFeed(
            self.market.symbol,
            self.book,
            ws_url=self.ws_url,
            market_id=self.market.market_id,
            notify=notify,
            l2_event_sink=self._l2_event_sink,
            trade_sink=self._trade_sink,
            attribute_sink=self._attribute_sink,
        )
        self.feed = feed
        return [asyncio.create_task(feed.run(stop), name="book-arcus")]

    def ready_to_trade(self) -> bool:
        return False

    def init_signer(self, *args, **kwargs) -> None:
        raise RuntimeError("Arcus trading is not implemented in Phase A")

    def send_taker(self, *args, **kwargs) -> None:
        raise RuntimeError("Arcus trading is not implemented in Phase A")

    async def close(self) -> None:
        return None
