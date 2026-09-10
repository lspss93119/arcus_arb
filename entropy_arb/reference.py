from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import Any

from .entropy_quota import EntropyQuotaCoordinator, is_entropy_quota_error
from .storage import EntropyReferenceRow, HedgeReferenceRow, MarketHistoryStore
from .ws_lifecycle import EntropyWebSocketLifecycle

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

log = logging.getLogger("reference")

ENTROPY_REFERENCE_HEADER: tuple[str, ...] = ("recv_ms", "oracle_px", "mark_px")
LIGHTER_REFERENCE_HEADER: tuple[str, ...] = (
    "recv_ms",
    "server_ms",
    "index_px",
    "mark_px",
)
REFERENCE_FEED_STOP_TIMEOUT_SEC = 5.0


def reference_paths(
    symbol: str, hedge_key: str, directory: str = "logs"
) -> tuple[str, str]:
    """Legacy export naming helper; live recording no longer writes these paths."""
    return (
        f"{directory}/reference-{symbol}-{hedge_key}-entropy.csv",
        f"{directory}/reference-{symbol}-{hedge_key}.csv",
    )


class ReferenceParseError(ValueError):
    """A relevant reference frame cannot produce a complete valid row."""


def _positive_float(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ReferenceParseError(f"{field} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ReferenceParseError(f"{field} must be numeric") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ReferenceParseError(f"{field} must be positive and finite")
    return parsed


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ReferenceParseError(f"{field} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ReferenceParseError(f"{field} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ReferenceParseError(f"{field} must be an integer")
    if parsed <= 0:
        raise ReferenceParseError(f"{field} must be positive")
    return parsed


def _market_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    return parsed


def _channel_market_id(channel: object) -> int | None:
    if not isinstance(channel, str):
        return None
    for separator in (":", "/"):
        prefix = f"market_stats{separator}"
        if channel.startswith(prefix):
            return _market_id(channel.removeprefix(prefix))
    return None


def parse_hl_reference(
    msg: dict,
    *,
    coin: str,
) -> tuple[float, float] | None:
    """Return a Hyperliquid oracle/mark pair for the configured coin."""
    if msg.get("channel") != "activeAssetCtx":
        return None
    data = msg.get("data")
    if not isinstance(data, dict):
        return None
    if data.get("coin") != coin:
        return None
    ctx = data.get("ctx")
    if not isinstance(ctx, dict):
        raise ReferenceParseError("activeAssetCtx ctx must be an object")
    try:
        oracle_px = _positive_float(ctx["oraclePx"], "oraclePx")
        mark_px = _positive_float(ctx["markPx"], "markPx")
    except KeyError as exc:
        raise ReferenceParseError(f"missing required field {exc.args[0]}") from exc
    return oracle_px, mark_px


def parse_lighter_reference(
    msg: dict,
    *,
    market_id: int,
) -> tuple[int, float, float] | None:
    """Return a Lighter server timestamp, index, and mark for one market."""
    if msg.get("type") not in {
        "subscribed/market_stats",
        "update/market_stats",
    }:
        return None

    channel_market_id = _channel_market_id(msg.get("channel"))
    if channel_market_id is not None and channel_market_id != market_id:
        return None

    stats = msg.get("market_stats")
    if not isinstance(stats, dict):
        if channel_market_id == market_id:
            raise ReferenceParseError("market_stats must be an object")
        return None

    payload_market_id = _market_id(stats.get("market_id"))
    if payload_market_id is None:
        if channel_market_id == market_id:
            raise ReferenceParseError("market_id must be an integer")
        return None
    if payload_market_id != market_id:
        return None
    if channel_market_id is not None and channel_market_id != payload_market_id:
        return None

    return (
        _positive_int(msg.get("timestamp"), "timestamp"),
        _positive_float(stats.get("index_price"), "index_price"),
        _positive_float(stats.get("mark_price"), "mark_price"),
    )


class _ReferenceStoreWriter:
    def __init__(self, store: MarketHistoryStore, symbol: str, hedge: str) -> None:
        self.store = store
        self.symbol = symbol
        self.hedge = hedge
        self._enabled = True
        self._accepting = False
        self._closed = False
        self._accepting = True

    @property
    def enabled(self) -> bool:
        return self._enabled and self._accepting and not self._closed

    def stop_accepting(self) -> None:
        self._accepting = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.stop_accepting()
        self._enabled = False


class EntropyReferenceStoreWriter(_ReferenceStoreWriter):
    def write(self, row: tuple[object, ...]) -> None:
        if self.enabled:
            recv_ms, oracle_px, mark_px = row
            self.store.append_entropy_reference(
                EntropyReferenceRow(
                    self.symbol,
                    self.hedge,
                    int(recv_ms),
                    float(oracle_px),
                    float(mark_px),
                )
            )


class HedgeReferenceStoreWriter(_ReferenceStoreWriter):
    def write(self, row: tuple[object, ...]) -> None:
        if self.enabled:
            recv_ms, server_ms, index_px, mark_px = row
            self.store.append_hedge_reference(
                HedgeReferenceRow(
                    self.symbol,
                    self.hedge,
                    int(recv_ms),
                    int(server_ms),
                    float(index_px),
                    float(mark_px),
                )
            )


class HLReferenceFeed:
    def __init__(
        self,
        name: str,
        ws_url: str,
        coin: str,
        writer: _ReferenceStoreWriter,
        *,
        connect=ws_connect,
        clock_ns=time.time_ns,
        sleep=asyncio.sleep,
        quota_coordinator: EntropyQuotaCoordinator | None = None,
    ) -> None:
        self.name = name
        self.ws_url = ws_url
        self.coin = coin
        self.writer = writer
        self._connect = connect
        self._clock_ns = clock_ns
        self._sleep = sleep
        self.quota_coordinator = quota_coordinator

    async def _pinger(self, ws) -> None:
        """Send reference-connection application pings every five seconds."""
        while True:
            await self._sleep(5.0)
            await ws.send(json.dumps({"method": "ping"}))

    async def run(self, stop: asyncio.Event) -> None:
        """Consume activeAssetCtx frames with isolated reconnect handling."""
        backoff = 1.0
        attempt = 0
        coordinator = self.quota_coordinator
        while not stop.is_set():
            if coordinator is not None:
                if not await coordinator.wait_reference_recovery(stop):
                    break
            attempt += 1
            lifecycle = EntropyWebSocketLifecycle(
                "entropy-reference",
                attempt=attempt,
                logger=log,
            )
            lifecycle.attempt_started()
            pinger = None
            reconnect_delay = backoff
            try:
                try:
                    async with self._connect(
                        self.ws_url,
                        max_size=2**23,
                        open_timeout=10,
                        ping_interval=15,
                        ping_timeout=15,
                    ) as ws:
                        lifecycle.opened()
                        lifecycle.connected()
                        backoff = 1.0
                        await ws.send(
                            json.dumps(
                                {
                                    "method": "subscribe",
                                    "subscription": {
                                        "type": "activeAssetCtx",
                                        "coin": self.coin,
                                    },
                                }
                            )
                        )
                        pinger = asyncio.create_task(self._pinger(ws))
                        async for raw in ws:
                            recv_ms = self._clock_ns() // 1_000_000
                            backoff = 1.0
                            try:
                                msg = json.loads(raw)
                            except (json.JSONDecodeError, TypeError) as exc:
                                log.warning(
                                    "[%s] malformed reference JSON: %s",
                                    self.name,
                                    exc,
                                )
                                continue
                            if not isinstance(msg, dict):
                                continue
                            try:
                                parsed = parse_hl_reference(msg, coin=self.coin)
                            except Exception as exc:
                                log.warning(
                                    "[%s] malformed relevant reference frame: %s",
                                    self.name,
                                    exc,
                                )
                                continue
                            if parsed is not None and self.writer.enabled:
                                self.writer.write((recv_ms, *parsed))
                            if stop.is_set():
                                break
                finally:
                    if pinger is not None:
                        pinger.cancel()
                        await asyncio.gather(pinger, return_exceptions=True)
                    lifecycle.closing()
                    lifecycle.closed()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if coordinator is not None and is_entropy_quota_error(exc):
                    coordinator.note_quota_error("reference")
                lifecycle.error(exc, reconnect_delay=reconnect_delay)
                log.warning(
                    "[%s] reference ws error: %s — reconnect in %.0fs",
                    self.name,
                    exc,
                    reconnect_delay,
                )
            if stop.is_set():
                break
            if coordinator is not None:
                if not await coordinator.wait_or_stop(stop, reconnect_delay):
                    break
            else:
                await self._sleep(reconnect_delay)
            backoff = min(backoff * 2, 30.0)


class LighterReferenceFeed:
    def __init__(
        self,
        name: str,
        ws_url: str,
        market_id: int,
        writer: _ReferenceStoreWriter,
        *,
        connect=ws_connect,
        clock_ns=time.time_ns,
        sleep=asyncio.sleep,
    ) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.writer = writer
        self._connect = connect
        self._clock_ns = clock_ns
        self._sleep = sleep

    async def _subscribe(self, ws) -> None:
        """Subscribe to the runtime market_stats channel."""
        await ws.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "channel": f"market_stats/{self.market_id}",
                }
            )
        )

    async def run(self, stop: asyncio.Event) -> None:
        """Consume market_stats frames with isolated reconnect handling."""
        backoff = 1.0
        while not stop.is_set():
            try:
                async with self._connect(
                    self.ws_url,
                    max_size=2**23,
                    open_timeout=10,
                    ping_interval=15,
                    ping_timeout=15,
                ) as ws:
                    backoff = 1.0
                    async for raw in ws:
                        recv_ms = self._clock_ns() // 1_000_000
                        backoff = 1.0
                        try:
                            msg = json.loads(raw)
                        except (json.JSONDecodeError, TypeError) as exc:
                            log.warning(
                                "[%s] malformed reference JSON: %s",
                                self.name,
                                exc,
                            )
                            continue
                        if not isinstance(msg, dict):
                            continue
                        message_type = msg.get("type")
                        if message_type == "connected":
                            await self._subscribe(ws)
                        elif message_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        try:
                            parsed = parse_lighter_reference(
                                msg,
                                market_id=self.market_id,
                            )
                        except Exception as exc:
                            log.warning(
                                "[%s] malformed relevant reference frame: %s",
                                self.name,
                                exc,
                            )
                            continue
                        if parsed is not None and self.writer.enabled:
                            self.writer.write((recv_ms, *parsed))
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "[%s] reference ws error: %s — reconnect in %.0fs",
                    self.name,
                    exc,
                    backoff,
                )
            if stop.is_set():
                break
            await self._sleep(backoff)
            backoff = min(backoff * 2, 30.0)


class ReferenceRecorder:
    def __init__(
        self,
        *,
        symbol: str,
        hedge_key: str,
        entropy_ws_url: str,
        entropy_coin: str,
        hedge_ws_url: str,
        hedge_market_id: int,
        store: MarketHistoryStore,
        feed_stop_timeout_sec: float = REFERENCE_FEED_STOP_TIMEOUT_SEC,
        quota_coordinator: EntropyQuotaCoordinator | None = None,
    ) -> None:
        self.entropy_writer = EntropyReferenceStoreWriter(store, symbol, hedge_key)
        self.hedge_writer = HedgeReferenceStoreWriter(store, symbol, hedge_key)
        self.entropy_feed = HLReferenceFeed(
            "entropy-reference",
            entropy_ws_url,
            entropy_coin,
            self.entropy_writer,
            quota_coordinator=quota_coordinator,
        )
        self.hedge_feed = LighterReferenceFeed(
            "hedge-reference",
            hedge_ws_url,
            hedge_market_id,
            self.hedge_writer,
        )
        self.feed_stop_timeout_sec = feed_stop_timeout_sec

    async def run(self, stop: asyncio.Event) -> None:
        feed_stop = asyncio.Event()
        feed_tasks = [
            asyncio.create_task(
                self.entropy_feed.run(feed_stop),
                name="reference-entropy",
            ),
            asyncio.create_task(
                self.hedge_feed.run(feed_stop),
                name="reference-hedge",
            ),
        ]
        try:
            await stop.wait()
        finally:
            self.entropy_writer.stop_accepting()
            self.hedge_writer.stop_accepting()
            feed_stop.set()
            _, pending = await asyncio.wait(
                feed_tasks,
                timeout=self.feed_stop_timeout_sec,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*feed_tasks, return_exceptions=True)
            self.entropy_writer.close()
            self.hedge_writer.close()
