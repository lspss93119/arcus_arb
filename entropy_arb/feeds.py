"""Websocket order-book feeds, writing into entropy_arb.book.OrderBook.

Two protocols, one per exchange family:

LighterBookFeed: zkLighter order_book channel (snapshot + diffs, server
    pings, diff-nonce gap detection — a gapped book is dropped and
    resubscribed rather than traded as a fiction).
HLBookFeed: the official Hyperliquid websocket (wss://api.hyperliquid.xyz/ws)
    l2Book channel with fast snapshots and client app-pings. Every price this
    bot trades on comes straight from the exchange that will fill the order.

Both touch the book on any inbound frame (connection-based freshness: a quiet
market is not stale, only a dead feed is) and reconnect with backoff.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect

from .book import OrderBook
from .entropy_quota import EntropyQuotaCoordinator, is_entropy_quota_error
from .ws_lifecycle import EntropyWebSocketLifecycle

log = logging.getLogger("feeds")


def _chan_id(channel: str) -> int | None:
    """'order_book:32' / 'order_book/32' -> 32."""
    for sep in (":", "/"):
        if sep in channel:
            try:
                return int(channel.rsplit(sep, 1)[1])
            except ValueError:
                return None
    return None


class LighterBookFeed:
    """zkLighter order book for one market over one connection."""

    def __init__(
        self,
        name: str,
        ws_url: str,
        market_id: int,
        book: OrderBook,
        notify: Callable[[], None],
    ) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.book = book
        self.notify = notify
        self._nonce: int | None = None
        self._synced = False

    async def _subscribe(self, ws) -> None:
        await ws.send(
            json.dumps({"type": "subscribe", "channel": f"order_book/{self.market_id}"})
        )

    async def _handle_book(self, ws, msg: dict, snapshot: bool) -> None:
        if _chan_id(msg.get("channel", "")) != self.market_id:
            return
        ob = msg["order_book"]
        if snapshot:
            self._nonce = ob.get("nonce")
            self._synced = True
            self.book.apply_lighter(ob, snapshot=True)
            log.info(
                "[%s] snapshot: %d bids / %d asks",
                self.name,
                len(self.book.bids),
                len(self.book.asks),
            )
            self.notify()
            return
        # diff: a skipped nonce means we lost a level update — the book is now
        # a fiction. Drop it and resubscribe rather than quote off a ghost.
        if not self._synced:
            return  # no snapshot yet (fresh connection, or one pending after a gap)
        prev, begin, end = self._nonce, ob.get("begin_nonce"), ob.get("nonce")
        if prev is not None and begin is not None and begin > prev + 1:
            log.warning(
                "[%s] diff gap (had %s, got %s) — resubscribing", self.name, prev, begin
            )
            self._nonce = None
            self._synced = False
            self.book.clear()
            self.notify()
            await ws.send(
                json.dumps(
                    {"type": "unsubscribe", "channel": f"order_book/{self.market_id}"}
                )
            )
            await self._subscribe(ws)
            return
        if end is not None:
            self._nonce = end
        self.book.apply_lighter(ob, snapshot=False)
        self.notify()

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with ws_connect(
                    self.ws_url,
                    max_size=2**23,
                    open_timeout=10,
                    ping_interval=15,
                    ping_timeout=15,
                ) as ws:
                    log.info("[%s] connected (%s)", self.name, self.ws_url)
                    self.book.clear()
                    self._nonce = None
                    self._synced = False
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        t = msg.get("type")
                        self.book.touch()
                        if t == "update/order_book":
                            await self._handle_book(ws, msg, snapshot=False)
                        elif t == "subscribed/order_book":
                            await self._handle_book(ws, msg, snapshot=True)
                        elif t == "connected":
                            await self._subscribe(ws)
                        elif t == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(
                    "[%s] ws error: %s — reconnect in %.0fs", self.name, e, backoff
                )
            self.book.ready = False
            self.notify()
            if stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


class HLBookFeed:
    """Official Hyperliquid l2Book consumer for one coin (e.g. 'io:SNDK')."""

    def __init__(
        self,
        name: str,
        ws_url: str,
        coin: str,
        book: OrderBook,
        notify: Callable[[], None],
        ping_sec: float = 5.0,
        *,
        purpose: str = "entropy-market-data",
        count_active: bool = True,
        quota_coordinator: EntropyQuotaCoordinator | None = None,
    ) -> None:
        self.name = name
        self.ws_url = ws_url
        self.coin = coin
        self.book = book
        self.notify = notify
        self.ping_sec = ping_sec
        self.purpose = purpose
        self.count_active = count_active
        self.quota_coordinator = quota_coordinator
        self._snapped = False

    def _on_frame(self, msg: dict) -> None:
        self.book.touch()
        if msg.get("channel") == "l2Book":
            d = msg.get("data") or {}
            if d.get("coin") == self.coin:
                self.book.apply_hl(d["levels"])
                if not self._snapped:
                    self._snapped = True
                    log.info(
                        "[%s] snapshot: %d bids / %d asks",
                        self.name,
                        len(self.book.bids),
                        len(self.book.asks),
                    )
                self.notify()

    async def _pinger(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(self.ping_sec)
                await ws.send(json.dumps({"method": "ping"}))
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await ws.close()
            except Exception:
                pass

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        attempt = 0
        quota_attempt = 0
        coordinator = self.quota_coordinator
        while not stop.is_set():
            attempt += 1
            lifecycle = EntropyWebSocketLifecycle(
                self.purpose,
                attempt=attempt,
                logger=log,
                count_active=self.count_active,
            )
            lifecycle.attempt_started()
            ptask = None
            connected_for = 0.0
            quota_failure = False
            reconnect_delay = backoff
            try:
                try:
                    async with ws_connect(
                        self.ws_url,
                        max_size=2**23,
                        open_timeout=10,
                        ping_interval=15,
                        ping_timeout=15,
                    ) as ws:
                        lifecycle.opened()
                        lifecycle.connected()
                        log.info(
                            "[%s] connected (official ws, %s)", self.name, self.coin
                        )
                        self.book.clear()
                        self._snapped = False
                        await ws.send(
                            json.dumps(
                                {
                                    "method": "subscribe",
                                    "subscription": {
                                        "type": "l2Book",
                                        "coin": self.coin,
                                        "fast": True,
                                    },
                                }
                            )
                        )
                        if coordinator is not None:
                            coordinator.mark_main_connected()
                        ptask = asyncio.create_task(self._pinger(ws))
                        async for raw in ws:
                            backoff = 1.0
                            self._on_frame(json.loads(raw))
                            if stop.is_set():
                                break
                finally:
                    if coordinator is not None:
                        connected_for = coordinator.main_healthy_for_s()
                        coordinator.mark_main_disconnected()
                    if ptask is not None:
                        ptask.cancel()
                        await asyncio.gather(ptask, return_exceptions=True)
                    lifecycle.closing()
                    lifecycle.closed()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                quota_failure = coordinator is not None and is_entropy_quota_error(e)
                reconnect_delay = backoff
                if quota_failure:
                    assert coordinator is not None
                    if connected_for >= coordinator.main_healthy_required_sec:
                        quota_attempt = 0
                    quota_attempt += 1
                    coordinator.note_quota_error("main")
                    reconnect_delay = coordinator.quota_reconnect_delay(quota_attempt)
                    log.warning(
                        "[entropy-quota] main quota reconnect attempt=%d delay=%.0fs",
                        quota_attempt,
                        reconnect_delay,
                    )
                else:
                    quota_attempt = 0
                lifecycle.error(e, reconnect_delay=reconnect_delay)
                log.warning(
                    "[%s] ws error: %s — reconnect in %.0fs",
                    self.name,
                    e,
                    reconnect_delay,
                )
            self.book.ready = False
            self.notify()
            if stop.is_set():
                break
            reconnect_delay = reconnect_delay if quota_failure else backoff
            if coordinator is not None:
                if not await coordinator.wait_or_stop(stop, reconnect_delay):
                    break
            else:
                await asyncio.sleep(reconnect_delay)
            backoff = min(backoff * 2, 30.0)
