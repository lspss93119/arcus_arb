"""zkLighter venue adapter (Lighter mainnet / Lighter Robinhood chain).

Market data and account state come from Lighter's public REST + websocket
APIs via plain aiohttp/websockets, so --record-only data collection works
without the SDK. Trading lazily imports the official `lighter` SDK
(https://github.com/elliottech/lighter-python) for transaction signing only.

Market orders carry mandatory avg-execution-price protection and settle
asynchronously on the authenticated account_orders websocket; send_taker()
hides that behind the same result shape the HL venue returns:
{status, filled_base, avg_px, err, unresolved}.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Optional

import aiohttp

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook
from .config import VenueConf
from .feeds import LighterBookFeed

log = logging.getLogger("lighter")

OPEN_STATUSES = {"in-progress", "pending", "open"}
AUTH_REFRESH_SEC = 8 * 60
REST_TIMEOUT = 10.0

# Lighter's protocol fee unit is the same ``FeeTick`` used by the official
# lighter-go reference implementation: 1,000,000 fee ticks represent a 1.0
# rate.  A basis point is 1/10,000 of a rate, hence 100 fee ticks = 1 bps.
LIGHTER_FEE_TICK_SCALE = 1_000_000
LIGHTER_ACCOUNT_TIERS = frozenset(("standard", "premium", "plus"))


@dataclass(frozen=True)
class LighterAccountLimits:
    """Authenticated, account-specific fee state returned by accountLimits."""

    user_tier: str
    user_tier_name: str
    current_maker_fee_tick: int
    current_taker_fee_tick: int
    maker_fee_bps: Decimal
    taker_fee_bps: Decimal
    source: str = "accountLimits"


def lighter_fee_tick_to_bps(fee_tick: int) -> Decimal:
    """Convert an official Lighter fee tick to basis points exactly."""
    if isinstance(fee_tick, bool) or not isinstance(fee_tick, int):
        raise ValueError("Lighter fee tick must be an integer")
    if not 0 <= fee_tick <= LIGHTER_FEE_TICK_SCALE:
        raise ValueError("Lighter fee tick is outside the valid range")
    return (Decimal(fee_tick) * Decimal("10000") /
            Decimal(LIGHTER_FEE_TICK_SCALE))


def _required_account_limit_int(
    payload: Mapping[str, Any], field: str
) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"accountLimits.{field} must be an integer")
    return value


def parse_lighter_account_limits(
    payload: Mapping[str, Any],
) -> LighterAccountLimits:
    """Validate and normalize an authenticated ``accountLimits`` response.

    The public ``orderBooks.taker_fee`` is intentionally not accepted here:
    only this account-scoped response can establish B0 fee verification.
    """
    if not isinstance(payload, Mapping):
        raise ValueError("accountLimits response must be an object")

    code = _required_account_limit_int(payload, "code")
    if code != 200:
        raise ValueError(f"accountLimits returned code={code}")

    raw_tier = payload.get("user_tier")
    raw_tier_name = payload.get("user_tier_name")
    if not isinstance(raw_tier, str) or not raw_tier.strip():
        raise ValueError("accountLimits.user_tier is missing")
    if not isinstance(raw_tier_name, str) or not raw_tier_name.strip():
        raise ValueError("accountLimits.user_tier_name is missing")

    user_tier = raw_tier.strip().lower()
    if user_tier not in LIGHTER_ACCOUNT_TIERS:
        raise ValueError(f"unknown Lighter account tier: {raw_tier!r}")

    maker_tick = _required_account_limit_int(
        payload, "current_maker_fee_tick"
    )
    taker_tick = _required_account_limit_int(
        payload, "current_taker_fee_tick"
    )
    return LighterAccountLimits(
        user_tier=user_tier,
        user_tier_name=raw_tier_name.strip(),
        current_maker_fee_tick=maker_tick,
        current_taker_fee_tick=taker_tick,
        maker_fee_bps=lighter_fee_tick_to_bps(maker_tick),
        taker_fee_bps=lighter_fee_tick_to_bps(taker_tick),
    )


class AccountOrdersFeed:
    """Authenticated stream of our own order updates (settlement channel)."""

    def __init__(self, name: str, ws_url: str, market_id: int,
                 account_index: int, signer) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.account_index = account_index
        self.signer = signer
        self.ready = asyncio.Event()
        self._pending: dict[int, asyncio.Future] = {}
        self._terminal: OrderedDict[int, dict] = OrderedDict()

    def watch(self, coi: int) -> asyncio.Future:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        if coi in self._terminal:
            fut.set_result(self._terminal[coi])
            return fut
        self._pending[coi] = fut
        return fut

    def unwatch(self, coi: int) -> None:
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.cancel()

    def _resolve(self, coi: int, info: dict) -> None:
        self._terminal[coi] = info
        while len(self._terminal) > 512:
            self._terminal.popitem(last=False)
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.set_result(info)

    def _handle_orders(self, msg: dict) -> None:
        receive_ts_ms = time.time_ns() // 1_000_000
        for lst in (msg.get("orders") or {}).values():
            for o in lst or []:
                status = str(o.get("status", ""))
                if status in OPEN_STATUSES:
                    continue
                try:
                    coi = int(o.get("client_order_index"))
                except (TypeError, ValueError):
                    continue
                fb = float(o.get("filled_base_amount") or 0.0)
                fq = float(o.get("filled_quote_amount") or 0.0)
                self._resolve(coi, {"status": status, "filled_base": fb,
                                    "filled_quote": fq,
                                    "avg_px": (fq / fb) if fb > 0 else None,
                                    "fill_receive_ts_ms": receive_ts_ms})

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                auth, err = self.signer.create_auth_token_with_expiry()
                if err is not None:
                    raise RuntimeError(f"auth token: {err}")
                connected_at = time.time()
                async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                      ping_interval=15, ping_timeout=15) as ws:
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        t = msg.get("type")
                        if t in ("subscribed/account_orders", "update/account_orders"):
                            if t.startswith("subscribed"):
                                log.info("[%s] account orders stream ready", self.name)
                            self.ready.set()
                            self._handle_orders(msg)
                        elif t == "connected":
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "channel": f"account_orders/{self.market_id}/"
                                           f"{self.account_index}",
                                "auth": auth}))
                        elif t == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        if stop.is_set():
                            break
                        if (time.time() - connected_at > AUTH_REFRESH_SEC
                                and not self._pending):
                            log.info("[%s] refreshing account ws auth", self.name)
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] account ws error: %s — retry in %.0fs",
                            self.name, e, backoff)
                self.ready.clear()
                if stop.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            self.ready.clear()


class LighterVenue:
    kind = "lighter"

    def __init__(self, conf: VenueConf, session: aiohttp.ClientSession,
                 settle_timeout_sec: float) -> None:
        assert conf.lighter_profile is not None
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.session = session
        self.settle_timeout = settle_timeout_sec
        self.profile = conf.lighter_profile
        self.book = OrderBook()
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0     # cumulative filled notional this session
        self.equity = None
        self.free = None
        self.start_equity = None
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.market_id = -1
        self.price_decimals = 2
        self.size_decimals = 4
        self.min_base = 0.0
        self.min_quote = 10.0
        self.signer = None
        # ``fee_bps`` is usable by B0 only when its account/venue source has
        # been explicitly verified from authenticated accountLimits.  In
        # particular, zero is a valid verified fee and must not be confused
        # with the configured default.
        self.fee_bps_verified = bool(getattr(conf, "fee_bps_verified", False))
        self.fee_source: str | None = None
        self.account_limits: LighterAccountLimits | None = None
        self.orders_feed: Optional[AccountOrdersFeed] = None
        self._coi = int(time.time() * 1000)

    # ------------------------------------------------------------------ REST

    async def _get(
        self,
        path: str,
        params: Optional[dict] = None,
        headers: Optional[dict[str, str]] = None,
    ):
        async with self.session.get(
                self.profile.api_url + path, params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
            r.raise_for_status()
            return await r.json()

    # ------------------------------------------------------------- lifecycle

    async def load_market(self) -> None:
        data = await self._get("/api/v1/orderBooks")
        for ob in data.get("order_books") or []:
            if ob.get("symbol") != self.conf.symbol:
                continue
            if ob.get("status") != "active":
                raise RuntimeError(f"[{self.name}] market status={ob.get('status')}")
            self.market_id = int(ob["market_id"])
            self.price_decimals = int(ob["supported_price_decimals"])
            self.size_decimals = int(ob["supported_size_decimals"])
            self.min_base = float(ob["min_base_amount"])
            self.min_quote = float(ob["min_quote_amount"])
            log.info("[%s] %s market_id=%d px_dec=%d sz_dec=%d min_base=%s "
                     "min_quote=%s public_taker_fee_unverified=%s",
                     self.name, ob["symbol"],
                     self.market_id, self.price_decimals, self.size_decimals,
                     ob["min_base_amount"], ob["min_quote_amount"],
                     ob.get("taker_fee"))
            return
        raise RuntimeError(f"[{self.name}] {self.conf.symbol} not found on "
                           f"{self.profile.name}")

    def init_signer(self) -> None:
        c = self.conf.lighter_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        try:
            from lighter import SignerClient
        except ImportError as e:
            raise RuntimeError(
                "live trading on Lighter needs the official SDK — "
                "pip install -r requirements-live.txt "
                "(git+https://github.com/elliottech/lighter-python.git)") from e
        signer = SignerClient(
            url=self.profile.api_url,
            account_index=c.account_index,
            api_private_keys={c.api_key_index: c.api_private_key},
            chain_id=self.profile.chain_id,
        )
        err = signer.check_client()
        if err is not None:
            raise RuntimeError(f"[{self.name}] API key check failed: {err}")
        self.signer = signer
        log.info("[%s] signer ready (account %d)", self.name, c.account_index)

    async def fetch_account_limits(self) -> LighterAccountLimits:
        """Fetch and verify this account's current Lighter fee state.

        ``accountLimits`` is authenticated with the official SDK auth token.
        The public orderBooks fee is deliberately not used as a fallback,
        because it is venue/market metadata rather than account state.
        """
        c = self.conf.lighter_creds
        if (
            c is None
            or c.account_index is None
            or c.api_key_index is None
        ):
            raise RuntimeError(
                f"[{self.name}] accountLimits requires Lighter credentials"
            )
        if self.signer is None:
            raise RuntimeError(
                f"[{self.name}] accountLimits authentication requires signer"
            )
        try:
            auth, err = self.signer.create_auth_token_with_expiry(
                api_key_index=c.api_key_index
            )
        except Exception:
            raise RuntimeError(
                f"[{self.name}] accountLimits authentication failed"
            ) from None
        if err is not None or not auth:
            raise RuntimeError(
                f"[{self.name}] accountLimits authentication failed"
            )
        try:
            payload = await self._get(
                "/api/v1/accountLimits",
                params={"account_index": c.account_index},
                headers={"Authorization": auth},
            )
            limits = parse_lighter_account_limits(payload)
        except ValueError:
            raise
        except Exception as exc:
            status = getattr(exc, "status", None)
            suffix = f" status={status}" if status is not None else ""
            raise RuntimeError(
                f"[{self.name}] accountLimits request failed{suffix}"
            ) from None
        self.account_limits = limits
        return limits

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        tasks = [asyncio.create_task(
            LighterBookFeed(self.name, self.profile.ws_url, self.market_id,
                            self.book, notify).run(stop),
            name=f"book-{self.key}")]
        if live:
            self.orders_feed = AccountOrdersFeed(
                self.name, self.profile.ws_url, self.market_id,
                self.conf.lighter_creds.account_index, self.signer)
            tasks.append(asyncio.create_task(self.orders_feed.run(stop),
                                             name=f"acct-{self.key}"))
        return tasks

    def ready_to_trade(self) -> bool:
        return self.orders_feed is not None and self.orders_feed.ready.is_set()

    async def warm_http(self) -> None:
        """Keep the order-path HTTPS connections warm (a cold TLS handshake
        adds 10-15ms to the first order after an idle spell)."""
        try:
            await self._get("/api/v1/status")
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)
        if self.signer is None:
            return
        try:
            sess = self.signer.api_client.rest_client.pool_manager
            async with sess.get(self.profile.api_url + "/api/v1/status",
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
                await r.read()
        except Exception as e:
            log.debug("[%s] signer keepalive failed: %r", self.name, e)

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        f = 10 ** self.price_decimals
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    # ------------------------------------------------------------- execution

    def _next_coi(self) -> int:
        self._coi += 1
        return self._coi

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False) -> dict:
        """Market order with avg-price protection; settle via account ws."""
        assert self.signer is not None
        from lighter import SignerClient
        order_send_ts_ms = time.time_ns() // 1_000_000
        coi = self._next_coi()
        fut = self.orders_feed.watch(coi) if self.orders_feed else None
        base_amount = int(round(qty * 10 ** self.size_decimals))
        price = int(round(limit_px * 10 ** self.price_decimals))
        try:
            _tx, resp, err = await self.signer.create_order(
                market_index=self.market_id,
                client_order_index=coi,
                base_amount=base_amount,
                price=price,
                is_ask=not is_buy,
                order_type=SignerClient.ORDER_TYPE_MARKET,
                time_in_force=SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only,
                order_expiry=SignerClient.DEFAULT_IOC_EXPIRY,
            )
        except Exception as e:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            msg = f"{type(e).__name__}: {e}"
            if getattr(e, "status", None) == 429 or "(429)" in str(e):
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False,
                    "order_send_ts_ms": order_send_ts_ms}
        ack_ts_ms = time.time_ns() // 1_000_000
        if err is not None or (getattr(resp, "code", 200) or 200) != 200:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            msg = str(err) if err is not None else \
                f"tx rejected code={resp.code} msg={getattr(resp, 'message', None)}"
            if "rate limit" in msg.lower():
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False,
                    "order_send_ts_ms": order_send_ts_ms,
                    "ack_ts_ms": ack_ts_ms}
        if fut is None:
            return {"status": "sent-unconfirmed", "filled_base": 0.0,
                    "avg_px": None, "err": None, "unresolved": True,
                    "order_send_ts_ms": order_send_ts_ms,
                    "ack_ts_ms": ack_ts_ms}
        try:
            info = await asyncio.wait_for(fut, timeout=self.settle_timeout)
            return {"status": info["status"], "filled_base": info["filled_base"],
                    "avg_px": info.get("avg_px"), "err": None, "unresolved": False,
                    "order_send_ts_ms": order_send_ts_ms,
                    "ack_ts_ms": ack_ts_ms,
                    "fill_receive_ts_ms": info.get("fill_receive_ts_ms")}
        except asyncio.TimeoutError:
            self.orders_feed.unwatch(coi)
            log.warning("[%s] no settle confirmation for coi %d in %.1fs",
                        self.name, coi, self.settle_timeout)
            return {"status": "timeout", "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True,
                    "order_send_ts_ms": order_send_ts_ms,
                    "ack_ts_ms": ack_ts_ms}

    # -------------------------------------------------------------- accounts

    async def _account(self) -> Optional[dict]:
        c = self.conf.lighter_creds
        if c is None or c.account_index is None:
            return None
        data = await self._get("/api/v1/account",
                               params={"by": "index",
                                       "value": str(c.account_index)})
        for acct in data.get("accounts") or []:
            return acct
        return None

    async def fetch_equity(self):
        acct = await self._account()
        if acct is None:
            return None
        return (float(acct.get("total_asset_value") or 0.0),
                float(acct.get("available_balance") or 0.0))

    async def fetch_position(self) -> float:
        acct = await self._account()
        if acct is None:
            raise RuntimeError(f"[{self.name}] account not found")
        for p in acct.get("positions") or []:
            if int(p.get("market_id", -1)) == self.market_id:
                return float(p.get("sign") or 1.0) * float(p.get("position") or 0.0)
        return 0.0

    async def close(self) -> None:
        if self.signer is not None:
            try:
                await self.signer.api_client.close()
            except Exception:
                pass
