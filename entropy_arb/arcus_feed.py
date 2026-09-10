"""Public Arcus websocket feed for one market.

One multiplexed public socket carries exactly three Phase A subscriptions:
the incremental L2 book, public trades, and global market attributes.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections.abc import Callable
from typing import Any

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect

from .arcus import (
    ARCUS_BOOK_LEVELS,
    ARCUS_PUBLIC_SUBSCRIPTIONS,
    ARCUS_WS_URL,
    ArcusL2Event,
    ArcusMarketAttributes,
    ArcusTrade,
    parse_arcus_book_snapshot,
    parse_arcus_book_update,
    parse_arcus_market_attributes,
    parse_arcus_trades,
)
from .arcus_book import ArcusOrderBook

log = logging.getLogger("arcus")

Callback = Callable[..., Any]


class ArcusBookFeed:
    """Consume Arcus public book, raw L2 levels, trades, and attributes."""

    def __init__(
        self,
        market_display_name: str,
        book: ArcusOrderBook,
        *,
        ws_url: str = ARCUS_WS_URL,
        market_id: int | None = None,
        notify: Callable[[], None] | None = None,
        l2_event_sink: Callback | None = None,
        trade_sink: Callback | None = None,
        attribute_sink: Callback | None = None,
        n_levels: int = ARCUS_BOOK_LEVELS,
    ) -> None:
        if not 1 <= n_levels <= 100:
            raise ValueError("Arcus n_levels must be between 1 and 100")
        self.market_display_name = market_display_name
        self.market_id = market_id
        self.ws_url = ws_url
        self.book = book
        self.notify = notify or (lambda: None)
        self.l2_event_sink = l2_event_sink
        self.trade_sink = trade_sink
        self.attribute_sink = attribute_sink
        self.n_levels = n_levels
        self.latest_attributes: ArcusMarketAttributes | None = None
        self._resync_requested = False
        self.l2_event_rows = 0

    async def _call(self, callback: Callback | None, *args: Any) -> None:
        if callback is None:
            return
        result = callback(*args)
        if inspect.isawaitable(result):
            await result

    async def subscribe_public(self, websocket) -> None:
        """Subscribe to the three required public channels, once each."""
        messages: list[dict[str, Any]] = [
            {
                "type": "subscribe",
                "channel": "l2OrderbookUpdates",
                "id": self.market_display_name,
                "nLevels": self.n_levels,
            },
            {
                "type": "subscribe",
                "channel": "trades",
                "id": self.market_display_name,
            },
            {"type": "subscribe", "channel": "marketAttributes"},
        ]
        assert tuple(item["channel"] for item in messages) == ARCUS_PUBLIC_SUBSCRIPTIONS
        for message in messages:
            await websocket.send(json.dumps(message, separators=(",", ":")))

    async def _resubscribe_book(self, websocket) -> None:
        await websocket.send(
            json.dumps(
                {
                    "type": "unsubscribe",
                    "channel": "l2OrderbookUpdates",
                    "id": self.market_display_name,
                },
                separators=(",", ":"),
            )
        )
        await websocket.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "channel": "l2OrderbookUpdates",
                    "id": self.market_display_name,
                    "nLevels": self.n_levels,
                },
                separators=(",", ":"),
            )
        )

    async def _emit_l2_events(
        self,
        *,
        event_type: str,
        bids: tuple[tuple[str, str], ...],
        asks: tuple[tuple[str, str], ...],
        last_sequence_id: int,
        global_sequence_id: int | None,
        exchange_timestamp_us: int | None,
        local_receive_ts_ms: int,
        local_receive_monotonic_ns: int,
    ) -> None:
        """Emit one callback per wire level, retaining frame-local order."""
        levels = [("bid", price, size) for price, size in bids]
        levels.extend(("ask", price, size) for price, size in asks)
        for event_index, (side, price, size) in enumerate(levels):
            await self._call(
                self.l2_event_sink,
                ArcusL2Event(
                    market_id=self.market_id,
                    market_display_name=self.market_display_name,
                    event_type=event_type,
                    book_epoch=self.book.book_epoch,
                    local_receive_ts_ms=local_receive_ts_ms,
                    local_receive_monotonic_ns=local_receive_monotonic_ns,
                    last_sequence_id=last_sequence_id,
                    global_sequence_id=global_sequence_id,
                    side=side,
                    price=price,
                    absolute_size=size,
                    event_index=event_index,
                    exchange_timestamp_us=exchange_timestamp_us,
                ),
            )
            self.l2_event_rows += 1

    def _is_target(self, message: dict) -> bool:
        identifier = message.get("id")
        return (
            identifier is None
            or str(identifier).upper() == self.market_display_name.upper()
        )

    async def handle_message(
        self,
        websocket,
        message: dict,
        local_receive_ts_ms: int | None = None,
        local_receive_monotonic_ns: int | None = None,
    ) -> None:
        channel = message.get("channel")
        if channel in ("l2OrderbookUpdates", "trades") and not self._is_target(message):
            return
        wall_ms = (
            int(time.time() * 1000)
            if local_receive_ts_ms is None
            else int(local_receive_ts_ms)
        )
        mono_ns = (
            time.monotonic_ns()
            if local_receive_monotonic_ns is None
            else int(local_receive_monotonic_ns)
        )

        if channel == "l2OrderbookUpdates":
            if message.get("type") == "subscribed":
                snapshot = parse_arcus_book_snapshot(message)
                self.book.apply_snapshot(snapshot, wall_ms, mono_ns)
                await self._emit_l2_events(
                    event_type="snapshot",
                    bids=snapshot.bids,
                    asks=snapshot.asks,
                    last_sequence_id=snapshot.last_sequence_id,
                    global_sequence_id=snapshot.global_sequence_id,
                    exchange_timestamp_us=snapshot.exchange_timestamp_us,
                    local_receive_ts_ms=wall_ms,
                    local_receive_monotonic_ns=mono_ns,
                )
                self._resync_requested = False
                log.info(
                    "[ARCUS] snapshot market=%s seq=%s global=%s bids=%d asks=%d",
                    self.market_display_name,
                    self.book.sequence_id,
                    self.book.global_sequence_id,
                    len(self.book.bids),
                    len(self.book.asks),
                )
                self.notify()
                return
            if message.get("type") != "channel_data":
                return
            update = parse_arcus_book_update(message)
            # Persist the raw delta even when its sequence is gapped.  The
            # local book rejects that update; the row remains evidence of the
            # discontinuity and is never replaced with a fabricated event.
            await self._emit_l2_events(
                event_type="delta",
                bids=update.bids,
                asks=update.asks,
                last_sequence_id=update.last_sequence_id,
                global_sequence_id=update.global_sequence_id,
                exchange_timestamp_us=update.exchange_timestamp_us,
                local_receive_ts_ms=wall_ms,
                local_receive_monotonic_ns=mono_ns,
            )
            applied = self.book.apply_update(update, wall_ms, mono_ns)
            if not applied and self.book.health == "RESYNC":
                if not self._resync_requested:
                    self._resync_requested = True
                    log.warning(
                        "[ARCUS] sequence gap market=%s; book RESYNC required",
                        self.market_display_name,
                    )
                    await self._resubscribe_book(websocket)
                self.notify()
                return
            self.notify()
            return

        if channel == "trades":
            for trade in parse_arcus_trades(message):
                if trade.market_id is None and self.market_id is not None:
                    trade = ArcusTrade(
                        market_id=self.market_id,
                        market_display_name=self.market_display_name,
                        taker_order_id=trade.taker_order_id,
                        maker_order_id=trade.maker_order_id,
                        taker_address=trade.taker_address,
                        maker_address=trade.maker_address,
                        trade_id=trade.trade_id,
                        exchange_timestamp_us=trade.exchange_timestamp_us,
                        price=trade.price,
                        quantity=trade.quantity,
                        aggressor_side=trade.aggressor_side,
                        sequence_number=trade.sequence_number,
                    )
                await self._call(self.trade_sink, trade, wall_ms, mono_ns)
            return

        if channel == "marketAttributes":
            if self.market_id is None:
                return
            attributes = parse_arcus_market_attributes(message, self.market_id)
            if attributes is not None:
                self.latest_attributes = attributes
                await self._call(self.attribute_sink, attributes, wall_ms, mono_ns)

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                self.book.mark_stale()
                async with ws_connect(
                    self.ws_url,
                    max_size=2**23,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    log.info("[ARCUS] connected public ws=%s", self.ws_url)
                    await self.subscribe_public(websocket)
                    async for raw in websocket:
                        backoff = 1.0
                        wall_ms = int(time.time() * 1000)
                        mono_ns = time.monotonic_ns()
                        message = json.loads(raw)
                        await self.handle_message(websocket, message, wall_ms, mono_ns)
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "[ARCUS] public ws error: %s; reconnect in %.0fs", exc, backoff
                )
            self.book.mark_stale()
            self.notify()
            if stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
