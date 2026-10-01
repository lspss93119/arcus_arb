"""Two-venue market-data engine: Arcus vs Lighter-RH in Phase A.

The signal is a fixed band around the configured or effective center
(config.yaml):

    SELL Arcus / BUY hedge  when executable premium >= midline + upper (+fees)
    BUY Arcus / SELL hedge  when executable premium <= midline - lower (+fees)

Around the signal: per-direction persistence arming,
per-venue inventory ladder + position caps, per-venue order budgets and
reactive rate-limit exclusion, net-delta hedging, venue-outage pausing with
probing, and periodic on-chain reconciliation are retained for the mature
architecture.  Phase A itself is strictly record-only: public books and
market events are persisted to SQLite and no order path is enabled.
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import logging
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import aiohttp

from .arcus_auth import ArcusCredentials, ArcusSigner
from .arcus_execution import (
    ArcusAccountFeed,
    ArcusAccountRest,
    ArcusAccountState,
    ArcusMakerClient,
    resolve_arcus_account_fee_tier,
)
from .arcus_recorder import ArcusMarketRecorder
from .book import ArbPlan, floor_step, plan_arb
from .calibration import (
    RH_HEDGE_MIN_QTY,
    SessionLimits,
)
from .calibration_runtime import (
    CalibrationController,
    fetch_lighter_open_orders,
    resolve_verified_rh_fee_bps,
)
from .config import Config
from .entropy_quota import EntropyQuotaCoordinator
from .premium import calculate_premiums
from .recorder import MinuteRecorder
from .reference import ReferenceRecorder
from .storage import FLUSH_INTERVAL_SEC, MarketHistoryStore
from .strategy import RollingCenterUpdate, StrategyState, build_strategy
from .venue_arcus import ArcusVenue
from .venue_hl import HLVenue
from .venue_lighter import LighterVenue
from .volume_probe import (
    ProbeConfig,
    VolumeProbeRoundWriter,
    compute_probe_quantity,
)
from .volume_probe_runtime import VolumeProbeController

log = logging.getLogger("engine")

CSV_HEADER = [
    "ts",
    "direction",
    "buy_venue",
    "sell_venue",
    "qty",
    "buy_limit",
    "sell_limit",
    "buy_notional",
    "sell_notional",
    "exp_edge_usd",
    "gross_edge_usd",
    "marginal_premium_bps",
    "midline_bps",
    "inv_add_bps",
    "ok",
    "buy_fill",
    "sell_fill",
    "buy_status",
    "sell_status",
    "fill_edge_usd",
]
EXECUTION_TELEMETRY_HEADER = [
    "event_ts",
    "event_type",
    "timestamp",
    "execution_id",
    "direction",
    "expected_edge_usd",
    "fill_edge_usd",
    "actual_usd",
    "lifecycle_status",
    "buy_venue",
    "sell_venue",
    "requested_qty",
    "buy_filled_qty",
    "sell_filled_qty",
    "buy_avg_px",
    "sell_avg_px",
    "hedge_venue",
    "hedge_side",
    "hedge_filled_qty",
    "hedge_avg_px",
    "hedge_status",
]
BALANCE_POLL_SEC = 30.0
REFERENCE_HEDGE_KEYS = frozenset(("lighter", "lighter-rh"))


@dataclass
class _ExecutionContext:
    """In-memory accounting context for one execution and its hedge."""

    execution_id: str
    trade: dict
    residual_qty: float
    hedge_is_sell: bool
    realized_before_hedge: float | None
    hedge_filled_qty: float = 0.0
    hedge_priced_qty: float = 0.0
    hedge_result: float = 0.0
    hedge_notional: float = 0.0


class Engine:
    def __init__(
        self,
        cfg: Config,
        record_only: bool = False,
        *,
        tiny_live: bool = False,
        confirm_mainnet: bool = False,
        allow_first_order: bool = False,
        volume_probe: bool = False,
        probe_clip_usd: float | None = None,
        probe_side: str | None = None,
        probe_reprice_sec: float = 30.0,
        probe_max_runtime_sec: int = 1800,
        probe_max_loss_usd: float = 10.0,
    ) -> None:
        self.cfg = cfg
        self.strategy = build_strategy(cfg.strategy)
        self.entropy_quota = EntropyQuotaCoordinator()
        self.record_only = record_only
        self.tiny_live = tiny_live
        self.confirm_mainnet = confirm_mainnet
        self.allow_first_order = allow_first_order
        self.volume_probe = volume_probe
        self.probe_clip_usd = probe_clip_usd
        self.probe_side = probe_side
        self.probe_reprice_sec = probe_reprice_sec
        self.probe_max_runtime_sec = probe_max_runtime_sec
        self.probe_max_loss_usd = probe_max_loss_usd
        self.session: aiohttp.ClientSession | None = None
        # Venue and recorder implementations intentionally share a runtime
        # protocol but have different optional capabilities (Arcus recorders,
        # authenticated account feeds, and mature strategy venues).  Keep the
        # dynamic boundary explicit here; each adapter validates its own
        # external JSON/SDK inputs before exposing state to the engine.
        self.arcus: Any = None
        self.entropy: Any = None
        self.hedge: Any = None
        self.venues: dict[str, Any] = {}
        self.recorder: Any = None
        self.reference: Any = None
        self.market_history: Any = None
        self.markets_ready = False
        self.stop = asyncio.Event()
        self._update_evt = asyncio.Event()
        self._reconcile_evt = asyncio.Event()
        # per-venue locks: an execution holds both; a reconcile holds one, so
        # a chain read can never race an in-flight order on that venue
        self._venue_locks: dict[str, asyncio.Lock] = {}
        self._exec_tasks: set = set()
        self.halted = False
        self.consec_errors = 0
        self.last_trade_ts = 0.0
        self.trades = 0
        self.hedges = 0
        self.total_exp_edge = 0.0
        self.total_fill_edge = 0.0
        self.start_ts = time.time()
        self._last_skiplog = 0.0
        self._poke_due: float | None = None
        # per-direction persistence arming: direction key -> first-seen ts
        self._armed: dict[str, float | None] = {
            "sell_entropy": None,
            "buy_entropy": None,
        }
        self._step = 1e-4
        self._min_base = 0.0
        self._min_notional = 10.0
        self._mtm_baseline: float | None = None
        # proactive per-venue send budget: timestamps of recent order sends
        self._sends: dict[str, deque] = {}
        # reactive per-venue throttle: venue key -> excluded until
        self._venue_limited_until: dict[str, float] = {}
        # venue outage tracking: key -> down-since ts; a down venue pauses
        # trading and is probed every venue_probe_sec until it answers
        self._venue_down: dict[str, float] = {}
        self._venue_probe_at: dict[str, float] = {}
        self._venue_fetch_fails: dict[str, int] = {}
        # per-execution records for the dashboard (newest last)
        self.recent_trades: deque = deque(maxlen=50)
        self._execution_seq = 0
        self._execution_contexts: dict[str, _ExecutionContext] = {}
        # Optional override is useful for tests; live defaults to a separate
        # append-only file beside the configured engine log.
        self.execution_telemetry_csv: str | None = None
        self.calibration: CalibrationController | None = None
        self.volume_probe_controller: VolumeProbeController | None = None

    # ------------------------------------------------------------- utilities

    def _vlock(self, key: str) -> asyncio.Lock:
        lock = self._venue_locks.get(key)
        if lock is None:
            lock = self._venue_locks[key] = asyncio.Lock()
        return lock

    def _venue_rate_ok(self, v) -> bool:
        """True while the venue is under its max_orders_per_min (sliding 60s)."""
        dq = self._sends.setdefault(v.key, deque())
        now = time.time()
        while dq and now - dq[0] > 60.0:
            dq.popleft()
        return len(dq) < v.orders_per_min

    def _venue_limited(self, v) -> bool:
        return time.time() < self._venue_limited_until.get(v.key, 0.0)

    def _mark_limited(self, v) -> None:
        self._venue_limited_until[v.key] = time.time() + self.cfg.rate_limit_pause_sec
        log.warning(
            "[%s] rate limited — trading paused for %.0fs",
            v.name,
            self.cfg.rate_limit_pause_sec,
        )

    def _record_send(self, v) -> None:
        self._sends.setdefault(v.key, deque()).append(time.time())

    def request_stop(self) -> None:
        self.stop.set()
        self._update_evt.set()
        self._reconcile_evt.set()

    # ------------------------------------------------------------- lifecycle

    async def run(self) -> None:
        # Long keepalive so order-path connections survive quiet spells; the
        # keepalive loop pings inside this window to hold them open.
        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(keepalive_timeout=75.0, ttl_dns_cache=300)
        )
        self.session = session
        try:
            await self._run_inner()
        finally:
            await session.close()

    def _make_venue(self, vc):
        if self.session is None:
            raise RuntimeError("HTTP session is not initialized")
        session = self.session
        if getattr(vc, "key", None) == "arcus" or getattr(vc, "kind", None) == "arcus":
            return ArcusVenue(
                vc,
                session,
                rest_url=self.cfg.arcus_rest_url,
                ws_url=self.cfg.arcus_ws_url,
            )
        if vc.kind == "lighter":
            return LighterVenue(vc, session, self.cfg.settle_timeout_sec)
        return HLVenue(
            vc,
            self.cfg.hl_api_url,
            self.cfg.hl_ws_url,
            session,
            self.cfg.settle_timeout_sec,
            quota_coordinator=self.entropy_quota,
        )

    def _build_reference_recorder(self) -> ReferenceRecorder | None:
        if self.cfg.hedge_venue not in REFERENCE_HEDGE_KEYS:
            return None
        if not (self.record_only or self.cfg.recorder_enabled):
            return None
        if self.market_history is None:
            raise RuntimeError("market-history store must be initialized first")
        return ReferenceRecorder(
            symbol=self.cfg.symbol,
            hedge_key=self.cfg.hedge_venue,
            entropy_ws_url=self.entropy.ws_url,
            entropy_coin=self.entropy.coin,
            hedge_ws_url=self.hedge.profile.ws_url,
            hedge_market_id=self.hedge.market_id,
            store=self.market_history,
            quota_coordinator=self.entropy_quota,
        )

    async def _run_reference(self) -> None:
        if self.reference is None:
            return
        try:
            await self.reference.run(self.stop)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("reference recorder failed")

    async def _storage_flush_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=FLUSH_INTERVAL_SEC)
            except TimeoutError:
                pass
            if self.stop.is_set():
                break
            if self.market_history is not None:
                if self.calibration is not None:
                    await self.calibration.telemetry.flush()
                else:
                    await asyncio.to_thread(self.market_history.flush)

    async def _run_arcus_record_only(self, arcus: ArcusVenue) -> None:
        """Run Phase A using public Arcus and public Lighter market data only."""
        cfg = self.cfg
        self.arcus = arcus
        # The alias keeps mature venue-agnostic strategy and analysis helpers
        # reusable without presenting Entropy as a runtime venue.
        self.entropy = arcus
        self.hedge = self._make_venue(cfg.hedge)
        self.venues = {"arcus": self.arcus, "hedge": self.hedge}

        metadata, _ = await asyncio.gather(
            self.arcus.load_market(), self.hedge.load_market()
        )
        self.markets_ready = True
        self.market_history = MarketHistoryStore(cfg.recorder_database)
        self.recorder = ArcusMarketRecorder(
            self.market_history,
            symbol=cfg.symbol,
            arcus_book=self.arcus.book,
            rh_book=self.hedge.book,
            hedge=cfg.hedge_venue,
            is_fresh_seconds=cfg.staleness_sec,
        )
        self.recorder.record_metadata(metadata)
        self.arcus.set_market_data_sinks(
            self.recorder.record_trade,
            lambda attributes, receive_ms, monotonic_ns: (
                self.recorder.record_attributes(
                    attributes,
                    receive_ms,
                    monotonic_ns,
                    market_status=metadata.status,
                )
            ),
            self.recorder.record_l2_event,
        )
        await self._bootstrap_rolling_center()

        self._step = max(
            self.arcus.size_step,
            10 ** -min(self.arcus.size_decimals, self.hedge.size_decimals),
        )
        self._min_base = max(self.arcus.min_base, self.hedge.min_base, self._step)
        self._min_notional = max(
            cfg.min_order_notional, self.arcus.min_quote, self.hedge.min_quote
        )
        log.info(
            "pair ARCUS(%s)-%s(%s): %s fees=%.2f+%.2f step=%g min_ntl=$%g",
            self.arcus.exchange_symbol,
            self.hedge.name,
            self.hedge.conf.symbol,
            self._startup_strategy_desc(self.strategy.state()),
            self.arcus.fee_bps,
            self.hedge.fee_bps,
            self._step,
            self._min_notional,
        )
        log.info("No automatic strategy selection.")
        log.warning(
            "RECORD-ONLY — ARCUS trading disabled in Phase A; collecting "
            "public market data and sending no orders"
        )

        tasks: list[asyncio.Task] = [
            asyncio.create_task(self._storage_flush_loop(), name="storage-flush")
        ]
        tasks += self.arcus.start_tasks(self.stop, self._update_evt.set, False)
        tasks += self.hedge.start_tasks(self.stop, self._update_evt.set, False)
        tasks.append(asyncio.create_task(self.recorder.run(self.stop), name="recorder"))
        tasks.append(asyncio.create_task(self._status_loop(), name="status"))

        try:
            await self.stop.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for venue in self.venues.values():
                await venue.close()
            if self.market_history is not None:
                await asyncio.to_thread(self.market_history.flush)
                sample_count = self.market_history.count_rows("arcus_samples")
                trade_count = self.market_history.count_rows("arcus_trades")
                attribute_count = self.market_history.count_rows(
                    "arcus_market_attributes"
                )
                l2_stats = self.market_history.arcus_l2_stats()
                await asyncio.to_thread(self.market_history.close)
                log.info(
                    "shutdown — record-only samples=%d trades=%d attrs=%d "
                    "l2_events=%d l2_events_per_sec=%.2f db_bytes=%d wal_bytes=%d",
                    sample_count,
                    trade_count,
                    attribute_count,
                    l2_stats.rows,
                    l2_stats.events_per_sec,
                    l2_stats.database_bytes,
                    l2_stats.wal_bytes,
                )

    async def _wait_b0_market_state(self, timeout: float = 20.0) -> None:
        """Wait for both public books and explicit Arcus RTH state."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            attributes = getattr(self.arcus, "latest_attributes", None)
            if (
                self.arcus is not None
                and self.hedge is not None
                and self.arcus.book.ready
                and self.hedge.book.ready
                and attributes is not None
            ):
                return
            if self.stop.is_set():
                raise RuntimeError(
                    "B0 startup stopped before public BBO state was ready"
                )
            await asyncio.sleep(0.1)
        raise RuntimeError(
            "B0 startup timed out waiting for Arcus/RH BBO and market attributes"
        )

    async def _wait_b0_account_state(
        self, account_feed: ArcusAccountFeed, timeout: float = 15.0
    ) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if account_feed.ready.is_set() and account_feed.latest_fee_tier is not None:
                if not account_feed.required_channels_healthy:
                    errors = ", ".join(
                        f"{channel}: {error}"
                        for channel, error in account_feed.channel_errors.items()
                    )
                    raise RuntimeError(
                        "Arcus account channel health gate failed"
                        + (f": {errors}" if errors else "")
                    )
                return
            if self.stop.is_set():
                raise RuntimeError(
                    "B0 startup stopped before Arcus account subscriptions were ready"
                )
            await asyncio.sleep(0.1)
        raise RuntimeError(
            "Arcus accountAttributeUpdates did not expose the account fee tier"
        )

    async def _calibration_status_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.sleep(self.cfg.status_interval_sec)
            except asyncio.CancelledError:
                raise
            if self.calibration is None:
                continue
            self.halted = self.calibration.risk.halted
            stats = (
                self.market_history.arcus_l2_stats() if self.market_history else None
            )
            log.info(
                "[B0 status] premium=%s bps arcus=%s/%s RH=%s/%s "
                "state=%s residual=%s pnl=%s fills=%d notional=%s "
                "l2=%s events/s=%s%s",
                f"{self.premium_bps():+.2f}" if self.premium_bps() is not None else "—",
                self.arcus.book.best_bid() if self.arcus else "—",
                self.arcus.book.best_ask() if self.arcus else "—",
                self.hedge.book.best_bid() if self.hedge else "—",
                self.hedge.book.best_ask() if self.hedge else "—",
                self.calibration.lifecycle.state,
                self.calibration.accumulator.residual_exposure,
                self.calibration.pnl.actual_usd,
                self.calibration.risk.fill_events,
                self.calibration.risk.filled_notional_usd,
                stats.rows if stats else 0,
                f"{stats.events_per_sec:.2f}" if stats else "0.00",
                f" HALT={self.calibration.risk.halt_reason}"
                if self.calibration.risk.halted
                else "",
            )

    async def _run_arcus_volume_probe(self, arcus: ArcusVenue) -> None:
        """Run one explicitly approved Arcus maker/RH hedge/unwind round."""
        if not self.volume_probe or not self.confirm_mainnet:
            raise RuntimeError(
                "volume probe requires --volume-probe and --confirm-mainnet"
            )
        if self.session is None:
            raise RuntimeError("HTTP session is not initialized")
        if self.probe_clip_usd is None or self.probe_side is None:
            raise RuntimeError(
                "volume probe requires --probe-clip-usd and --probe-side"
            )

        cfg = self.cfg
        self.arcus = arcus
        self.entropy = arcus
        self.hedge = self._make_venue(cfg.hedge)
        self.venues = {"arcus": self.arcus, "hedge": self.hedge}
        tasks: list[asyncio.Task] = []
        executor: CalibrationController | None = None
        controller: VolumeProbeController | None = None
        account_feed: ArcusAccountFeed | None = None
        writer = VolumeProbeRoundWriter()
        credentials: ArcusCredentials | None = None
        maker: ArcusMakerClient | None = None
        account_rest: ArcusAccountRest | None = None
        startup_state: ArcusAccountState | None = None
        metadata: Any = None
        try:
            metadata, _ = await asyncio.gather(
                self.arcus.load_market(), self.hedge.load_market()
            )
            self.markets_ready = True
            if metadata.status != "ONLINE":
                raise RuntimeError(
                    f"Arcus {metadata.symbol} market status={metadata.status}; "
                    "aborting volume probe"
                )

            self.market_history = MarketHistoryStore(cfg.recorder_database)
            self.recorder = ArcusMarketRecorder(
                self.market_history,
                symbol=cfg.symbol,
                arcus_book=self.arcus.book,
                rh_book=self.hedge.book,
                hedge=cfg.hedge_venue,
                is_fresh_seconds=cfg.staleness_sec,
            )
            self.recorder.record_metadata(metadata)
            self.arcus.set_market_data_sinks(
                self.recorder.record_trade,
                lambda attributes, receive_ms, monotonic_ns: (
                    self.recorder.record_attributes(
                        attributes,
                        receive_ms,
                        monotonic_ns,
                        market_status=metadata.status,
                    )
                ),
                self.recorder.record_l2_event,
            )
            await self._bootstrap_rolling_center()

            credentials = ArcusCredentials.from_env()
            signer = ArcusSigner(credentials)
            if not cfg.creds_complete:
                raise RuntimeError(
                    "volume probe requires Lighter-RH credentials in .env: "
                    "LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX, and "
                    "LIGHTER_API_PRIVATE_KEY"
                )
            if not isinstance(self.hedge, LighterVenue):
                raise RuntimeError("volume probe requires Lighter-RH")
            lighter_hedge = self.hedge
            lighter_hedge.init_signer()
            rh_account_limits = await lighter_hedge.fetch_account_limits()
            rh_fee_bps = resolve_verified_rh_fee_bps(lighter_hedge, rh_account_limits)
            account_rest = ArcusAccountRest(self.session, rest_url=cfg.arcus_rest_url)
            fee_table = await account_rest.fee_tiers()
            startup_state = ArcusAccountState(
                startup_watermark_us=time.time_ns() // 1000
            )
            arcus_position = await account_rest.position(
                credentials.account_address,
                metadata.symbol,
                credentials.account_index,
            )
            rh_position = Decimal(str(await self.hedge.fetch_position()))
            rh_open_orders = await fetch_lighter_open_orders(self.hedge)
            if rh_open_orders:
                raise RuntimeError(
                    "RH open order exists; aborting volume probe without touching it"
                )
            ArcusAccountState.validate_starting_inventory(arcus_position, rh_position)
            arcus_open_orders = await account_rest.open_orders(
                credentials.account_address,
                metadata.symbol,
                credentials.account_index,
            )
            stale_probe = startup_state.validate_startup_orders(
                arcus_open_orders, calibration_prefix="vp-"
            )

            account_feed = ArcusAccountFeed(
                credentials.account_address,
                metadata.symbol,
                ws_url=cfg.arcus_ws_url,
                account_index=credentials.account_index,
                startup_state=startup_state,
                api_key=credentials.api_key,
                client_prefix="vp-",
                fixed_quantity=None,
            )
            maker = ArcusMakerClient(
                credentials=credentials,
                signer=signer,
                rpc=account_feed,
                ws_url=cfg.arcus_ws_url,
                client_prefix="vp-",
                fixed_quantity=None,
            )
            tasks += self.arcus.start_tasks(self.stop, self._update_evt.set, False)
            tasks += self.hedge.start_tasks(self.stop, self._update_evt.set, True)
            tasks.append(
                asyncio.create_task(account_feed.run(self.stop), name="acct-arcus-vp")
            )
            tasks.append(
                asyncio.create_task(self._storage_flush_loop(), name="storage-flush")
            )
            tasks.append(
                asyncio.create_task(self.recorder.run(self.stop), name="recorder")
            )

            await self._wait_b0_account_state(account_feed)
            await self._wait_b0_market_state()
            account_fee = account_feed.latest_fee_tier
            fee_tier = resolve_arcus_account_fee_tier(fee_table, account_fee)
            if fee_tier is None:
                raise RuntimeError(
                    "Arcus account fee tier could not be resolved; aborting volume probe"
                )
            bid = self.arcus.book.best_bid()
            ask = self.arcus.book.best_ask()
            if bid is None or ask is None:
                raise RuntimeError(
                    "fresh Arcus BBO is unavailable; aborting volume probe"
                )
            arcus_bid = Decimal(str(bid))
            arcus_ask = Decimal(str(ask))
            rh_step = Decimal(1).scaleb(-int(self.hedge.size_decimals))
            rh_min = max(
                RH_HEDGE_MIN_QTY,
                rh_step,
                Decimal(str(self.hedge.min_base)),
            )
            quantity = compute_probe_quantity(
                clip_usd=Decimal(str(self.probe_clip_usd)),
                arcus_bid=arcus_bid,
                arcus_ask=arcus_ask,
                probe_side=self.probe_side,
                arcus_step=Decimal(metadata.step_size),
                arcus_min_size=(
                    Decimal(metadata.min_order_size)
                    if metadata.min_order_size is not None
                    else None
                ),
                arcus_max_size=(
                    Decimal(metadata.max_order_size)
                    if metadata.max_order_size is not None
                    else None
                ),
                arcus_min_notional=(
                    Decimal(metadata.min_order_notional)
                    if metadata.min_order_notional is not None
                    else None
                ),
                rh_step=rh_step,
                rh_min_size=rh_min,
            )
            limits = SessionLimits(
                max_filled_notional_usd=max(
                    Decimal("500"),
                    quantity * ((arcus_bid + arcus_ask) / Decimal("2")) * 2
                    + Decimal("1"),
                ),
                max_loss_usd=Decimal(str(self.probe_max_loss_usd)),
                max_runtime_seconds=self.probe_max_runtime_sec,
                max_order_qty=quantity,
            )
            executor = CalibrationController(
                arcus=self.arcus,
                hedge=self.hedge,
                maker=maker,
                account_feed=account_feed,
                account_rest=account_rest,
                account_state=startup_state,
                metadata=metadata,
                fee_tier=fee_tier,
                strategy=self.strategy,
                store=self.market_history,
                allow_first_order=True,
                staleness_sec=cfg.staleness_sec,
                rh_fee_bps=rh_fee_bps,
                session_limits=limits,
                client_prefix="vp-",
            )
            controller = VolumeProbeController(
                executor=executor,
                config=ProbeConfig(
                    clip_usd=Decimal(str(self.probe_clip_usd)),
                    probe_side=self.probe_side,
                    reprice_sec=self.probe_reprice_sec,
                    max_runtime_sec=self.probe_max_runtime_sec,
                    max_loss_usd=Decimal(str(self.probe_max_loss_usd)),
                ),
                symbol=metadata.symbol,
                writer=writer,
                tolerance=min(Decimal(metadata.step_size), rh_step),
            )
            self.calibration = executor
            self.volume_probe_controller = controller
            account_feed.on_fill = controller.on_fill
            account_feed.on_order = controller.on_order
            account_feed.on_disconnect = controller.on_disconnect
            account_feed.on_connect = controller.on_connect

            for stale_order in stale_probe:
                if stale_order.client_id is None:
                    raise RuntimeError("known vp- Arcus order has no clientId")
                await maker.cancel_calibration_order(
                    market_id=metadata.market_id,
                    client_id=stale_order.client_id,
                )
                log.warning(
                    "[volume-probe] canceled stale Arcus order clientId=%s orderId=%s",
                    stale_order.client_id,
                    stale_order.order_id,
                )
            if stale_probe:
                cancel_deadline = time.monotonic() + 15.0
                while time.monotonic() < cancel_deadline:
                    remaining = await account_rest.open_orders(
                        credentials.account_address,
                        metadata.symbol,
                        credentials.account_index,
                    )
                    if not any(
                        (
                            stale.order_id is not None
                            and stale.order_id == order.order_id
                        )
                        or (
                            stale.client_id is not None
                            and stale.client_id == order.client_id
                        )
                        for stale in stale_probe
                        for order in remaining
                    ):
                        break
                    await asyncio.sleep(1.0)
                else:
                    raise RuntimeError(
                        "stale vp- Arcus order did not reach terminal state"
                    )
                stale_fills = await account_rest.fills(
                    credentials.account_address,
                    metadata.symbol,
                    credentials.account_index,
                )
                stale_order_ids = {
                    order.order_id
                    for order in stale_probe
                    if order.order_id is not None
                }
                stale_client_ids = {
                    order.client_id
                    for order in stale_probe
                    if order.client_id is not None
                }
                if any(
                    fill.created_at_us is not None
                    and fill.created_at_us > startup_state.startup_watermark_us
                    and (
                        fill.order_id in stale_order_ids
                        or fill.client_id in stale_client_ids
                    )
                    for fill in stale_fills
                ):
                    raise RuntimeError(
                        "stale vp- Arcus order filled during startup cancellation"
                    )

            fresh_arcus_open_orders = await account_rest.open_orders(
                credentials.account_address,
                metadata.symbol,
                credentials.account_index,
            )
            if fresh_arcus_open_orders:
                startup_state.validate_startup_orders(
                    fresh_arcus_open_orders, calibration_prefix="vp-"
                )
                raise RuntimeError(
                    "Arcus vp- order remains open after cancellation; aborting"
                )
            arcus_position = await account_rest.position(
                credentials.account_address,
                metadata.symbol,
                credentials.account_index,
            )
            rh_position = Decimal(str(await self.hedge.fetch_position()))
            rh_open_orders = await fetch_lighter_open_orders(self.hedge)
            if rh_open_orders:
                raise RuntimeError(
                    "RH open order appeared during volume-probe preflight; aborting"
                )
            ArcusAccountState.validate_starting_inventory(arcus_position, rh_position)

            controller.begin_build(quantity)
            proposed = controller.candidate(
                best_bid=arcus_bid,
                best_ask=arcus_ask,
            )
            log.info(
                "[volume-probe pre-order] side=%s price=%s qty=%s notional=%s "
                "hedge_side=%s TIF=ALO reprice=%ss max_runtime=%ss max_loss=$%s",
                proposed.arcus_side,
                proposed.price,
                proposed.quantity,
                proposed.notional_usd,
                proposed.hedge_side,
                self.probe_reprice_sec,
                self.probe_max_runtime_sec,
                self.probe_max_loss_usd,
            )
            if not self.allow_first_order:
                controller.metrics.status = "PREORDER_ONLY"
                controller.metrics.final_arcus_position = arcus_position
                controller.metrics.final_rh_position = rh_position
                controller.metrics.finished_at = datetime.now(UTC).isoformat()
                writer.append(controller.metrics)
                log.warning(
                    "[volume-probe] pre-order STOP: no Arcus order submitted; "
                    "use --approve-first-order only after human review"
                )
                return

            async def final_state_reader():
                assert account_rest is not None and credentials is not None
                final_arcus = await account_rest.position(
                    credentials.account_address,
                    metadata.symbol,
                    credentials.account_index,
                )
                final_rh = Decimal(str(await self.hedge.fetch_position()))
                final_arcus_orders = await account_rest.open_orders(
                    credentials.account_address,
                    metadata.symbol,
                    credentials.account_index,
                )
                final_rh_orders = await fetch_lighter_open_orders(self.hedge)
                return final_arcus, final_rh, final_arcus_orders, final_rh_orders

            tasks.append(
                asyncio.create_task(
                    controller.run(
                        self.stop,
                        quantity=quantity,
                        final_state_reader=final_state_reader,
                    ),
                    name="arcus-volume-probe",
                )
            )
            await self.stop.wait()
        finally:
            self.stop.set()
            probe_task = next(
                (task for task in tasks if task.get_name() == "arcus-volume-probe"),
                None,
            )
            if probe_task is not None and not probe_task.done():
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(probe_task, timeout=20.0)
            if executor is not None:
                with contextlib.suppress(Exception):
                    await executor.shutdown()
            for task in tasks:
                if task is not probe_task:
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for venue in self.venues.values():
                with contextlib.suppress(Exception):
                    await venue.close()
            if self.market_history is not None:
                if self.calibration is not None:
                    with contextlib.suppress(Exception):
                        await self.calibration.telemetry.flush()
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(self.market_history.flush)
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(self.market_history.close)

    async def _run_arcus_tiny_live(self, arcus: ArcusVenue) -> None:
        """Run B0 preflight and, only after the separate approval flag, B0.

        The normal CLI reaches this method only with ``--tiny-live`` and
        ``--confirm-mainnet``.  ``allow_first_order`` is a third deliberate
        gate: without it the method performs public/account preflight, prints
        the proposed order, and stops before calling ``placeOrder``.
        """
        from .calibration import SessionLimits

        if not self.tiny_live or not self.confirm_mainnet:
            raise RuntimeError(
                "B0 live execution requires --tiny-live and --confirm-mainnet"
            )
        if self.session is None:
            raise RuntimeError("HTTP session is not initialized")

        cfg = self.cfg
        self.arcus = arcus
        self.entropy = arcus  # compatibility alias for generic premium helpers
        self.hedge = self._make_venue(cfg.hedge)
        self.venues = {"arcus": self.arcus, "hedge": self.hedge}
        tasks: list[asyncio.Task] = []
        controller: CalibrationController | None = None
        account_feed: ArcusAccountFeed | None = None
        try:
            metadata, _ = await asyncio.gather(
                self.arcus.load_market(), self.hedge.load_market()
            )
            self.markets_ready = True
            if metadata.status != "ONLINE":
                raise RuntimeError(
                    f"Arcus SNDK market status={metadata.status}; aborting B0"
                )
            calibration_qty = Decimal("0.01")
            if metadata.min_order_size is None:
                raise RuntimeError(
                    "Arcus SNDK metadata omitted minOrderSize; refusing to "
                    "assume a live calibration quantity"
                )
            if Decimal(metadata.min_order_size) > calibration_qty:
                raise RuntimeError(
                    f"Arcus SNDK minOrderSize={metadata.min_order_size} "
                    f"exceeds fixed B0 quantity={calibration_qty}"
                )
            if (
                metadata.max_order_size is not None
                and Decimal(metadata.max_order_size) < calibration_qty
            ):
                raise RuntimeError(
                    f"Arcus SNDK maxOrderSize={metadata.max_order_size} "
                    f"is below fixed B0 quantity={calibration_qty}"
                )
            if calibration_qty % Decimal(metadata.step_size) != 0:
                raise RuntimeError(
                    f"fixed B0 quantity={calibration_qty} is not aligned to "
                    f"Arcus stepSize={metadata.step_size}"
                )

            self.market_history = MarketHistoryStore(cfg.recorder_database)
            self.recorder = ArcusMarketRecorder(
                self.market_history,
                symbol=cfg.symbol,
                arcus_book=self.arcus.book,
                rh_book=self.hedge.book,
                hedge=cfg.hedge_venue,
                is_fresh_seconds=cfg.staleness_sec,
            )
            self.recorder.record_metadata(metadata)
            self.arcus.set_market_data_sinks(
                self.recorder.record_trade,
                lambda attributes, receive_ms, monotonic_ns: (
                    self.recorder.record_attributes(
                        attributes,
                        receive_ms,
                        monotonic_ns,
                        market_status=metadata.status,
                    )
                ),
                self.recorder.record_l2_event,
            )
            await self._bootstrap_rolling_center()

            # Arcus credentials are loaded only inside this explicitly gated
            # path.  No wallet generation or registration is attempted.
            credentials = ArcusCredentials.from_env()
            signer = ArcusSigner(credentials)
            if not cfg.creds_complete:
                raise RuntimeError(
                    "B0 requires Lighter-RH credentials in .env: "
                    "LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX, and "
                    "LIGHTER_API_PRIVATE_KEY"
                )
            if not isinstance(self.hedge, LighterVenue):
                raise RuntimeError(
                    "B0 accountLimits fee verification requires Lighter-RH"
                )
            lighter_hedge = self.hedge
            lighter_hedge.init_signer()
            rh_account_limits = await lighter_hedge.fetch_account_limits()
            rh_fee_bps = resolve_verified_rh_fee_bps(lighter_hedge, rh_account_limits)
            account_rest = ArcusAccountRest(self.session, rest_url=cfg.arcus_rest_url)
            fee_table = await account_rest.fee_tiers()
            startup_state = ArcusAccountState(
                startup_watermark_us=time.time_ns() // 1000
            )
            arcus_position = await account_rest.position(
                credentials.account_address,
                metadata.symbol,
                credentials.account_index,
            )
            rh_position = Decimal(str(await self.hedge.fetch_position()))
            rh_open_orders = await fetch_lighter_open_orders(self.hedge)
            if rh_open_orders:
                raise RuntimeError(
                    "RH SNDK open order exists; aborting B0 without touching it"
                )
            ArcusAccountState.validate_starting_inventory(arcus_position, rh_position)
            arcus_open_orders = await account_rest.open_orders(
                credentials.account_address,
                metadata.symbol,
                credentials.account_index,
            )
            calibration_prefix = "b0-"
            stale_calibration = startup_state.validate_startup_orders(
                arcus_open_orders, calibration_prefix=calibration_prefix
            )

            account_feed = ArcusAccountFeed(
                credentials.account_address,
                metadata.symbol,
                ws_url=cfg.arcus_ws_url,
                account_index=credentials.account_index,
                startup_state=startup_state,
                api_key=credentials.api_key,
            )
            maker = ArcusMakerClient(
                credentials=credentials,
                signer=signer,
                rpc=account_feed,
                ws_url=cfg.arcus_ws_url,
            )

            # The public market feeds and account state stream are started
            # before any order decision.  The account stream is public for
            # reads; the API key is used only by the signed post/cancel RPC.
            tasks += self.arcus.start_tasks(self.stop, self._update_evt.set, False)
            tasks += self.hedge.start_tasks(self.stop, self._update_evt.set, True)
            tasks.append(
                asyncio.create_task(account_feed.run(self.stop), name="acct-arcus-b0")
            )
            tasks.append(
                asyncio.create_task(self._storage_flush_loop(), name="storage-flush")
            )
            tasks.append(
                asyncio.create_task(self.recorder.run(self.stop), name="recorder")
            )

            await self._wait_b0_account_state(account_feed)
            await self._wait_b0_market_state()
            account_fee = account_feed.latest_fee_tier
            fee_tier = resolve_arcus_account_fee_tier(fee_table, account_fee)
            if fee_tier is None:
                raise RuntimeError(
                    "Arcus account fee tier could not be resolved from "
                    "accountAttributeUpdates and /v1/feetiers; aborting B0"
                )

            controller = CalibrationController(
                arcus=self.arcus,
                hedge=self.hedge,
                maker=maker,
                account_feed=account_feed,
                account_rest=account_rest,
                account_state=startup_state,
                metadata=metadata,
                fee_tier=fee_tier,
                strategy=self.strategy,
                store=self.market_history,
                allow_first_order=self.allow_first_order,
                staleness_sec=cfg.staleness_sec,
                rh_fee_bps=rh_fee_bps,
                session_limits=SessionLimits(),
            )
            self.calibration = controller
            account_feed.on_fill = controller.on_fill
            account_feed.on_order = controller.on_order
            account_feed.on_disconnect = controller.on_disconnect
            account_feed.on_connect = controller.on_connect

            # Existing calibration orders are explicitly safe to identify and
            # cancel; unknown user orders were rejected above.
            for stale_order in stale_calibration:
                if stale_order.client_id is None:
                    raise RuntimeError("known Arcus calibration order has no clientId")
                await maker.cancel_calibration_order(
                    market_id=metadata.market_id,
                    client_id=stale_order.client_id,
                )
                log.warning(
                    "[B0] canceled stale calibration order clientId=%s orderId=%s",
                    stale_order.client_id,
                    stale_order.order_id,
                )
            if stale_calibration:
                cancel_deadline = time.monotonic() + 15.0
                while time.monotonic() < cancel_deadline:
                    remaining = await account_rest.open_orders(
                        credentials.account_address,
                        metadata.symbol,
                        credentials.account_index,
                    )

                    def matches_stale(order) -> bool:
                        return any(
                            (
                                stale.order_id is not None
                                and stale.order_id == order.order_id
                            )
                            or (
                                stale.client_id is not None
                                and stale.client_id == order.client_id
                            )
                            for stale in stale_calibration
                        )

                    if not any(matches_stale(order) for order in remaining):
                        break
                    await asyncio.sleep(1.0)
                else:
                    raise RuntimeError(
                        "stale Arcus calibration order did not reach terminal "
                        "state after cancel; aborting B0"
                    )

                # A cancel request can race a fill.  Do not adopt or hedge a
                # previous session's fill during startup; instead fail closed
                # if the stale order produced any post-watermark fill while
                # it was being canceled.
                stale_fills = await account_rest.fills(
                    credentials.account_address,
                    metadata.symbol,
                    credentials.account_index,
                )
                stale_order_ids = {
                    order.order_id
                    for order in stale_calibration
                    if order.order_id is not None
                }
                stale_client_ids = {
                    order.client_id
                    for order in stale_calibration
                    if order.client_id is not None
                }
                if any(
                    fill.created_at_us is not None
                    and fill.created_at_us > startup_state.startup_watermark_us
                    and (
                        fill.order_id in stale_order_ids
                        or fill.client_id in stale_client_ids
                    )
                    for fill in stale_fills
                ):
                    raise RuntimeError(
                        "stale Arcus calibration order filled during startup "
                        "cancellation; aborting without adopting its inventory"
                    )

            fresh_arcus_open_orders = await account_rest.open_orders(
                credentials.account_address,
                metadata.symbol,
                credentials.account_index,
            )
            # The initial startup gate rejected unknown orders.  Re-run it
            # after stale-order cancellation so a newly observed or lingering
            # order cannot be mistaken for an empty calibration book.
            if fresh_arcus_open_orders:
                startup_state.validate_startup_orders(
                    fresh_arcus_open_orders, calibration_prefix=calibration_prefix
                )
                raise RuntimeError(
                    "Arcus calibration order remains open after cancellation; "
                    "aborting before any replacement"
                )

            # Re-read all startup gates after stale-order cleanup.  The first
            # read is not enough because a cancel/fill race can change both
            # inventory and the account order set while preflight is running.
            arcus_position = await account_rest.position(
                credentials.account_address,
                metadata.symbol,
                credentials.account_index,
            )
            rh_position = Decimal(str(await self.hedge.fetch_position()))
            rh_open_orders = await fetch_lighter_open_orders(self.hedge)
            if rh_open_orders:
                raise RuntimeError(
                    "RH SNDK open order appeared during B0 preflight; aborting"
                )
            ArcusAccountState.validate_starting_inventory(arcus_position, rh_position)

            preflight = controller.pre_order_state(
                arcus_position=arcus_position,
                rh_position=rh_position,
                arcus_open_orders=fresh_arcus_open_orders,
                rh_open_orders=rh_open_orders,
            )
            self._log_b0_pre_order_state(preflight, metadata)
            if not self.allow_first_order:
                log.warning(
                    "[B0] pre-order STOP: no Arcus order will be submitted; "
                    "obtain separate human approval before using "
                    "--approve-first-order"
                )
                return

            tasks.append(
                asyncio.create_task(
                    controller.run(self.stop), name="arcus-b0-calibration"
                )
            )
            tasks.append(
                asyncio.create_task(self._calibration_status_loop(), name="b0-status")
            )
            await self.stop.wait()
        finally:
            self.stop.set()
            calibration_task = next(
                (task for task in tasks if task.get_name() == "arcus-b0-calibration"),
                None,
            )
            if calibration_task is not None and not calibration_task.done():
                # Let the controller execute its cancel/reconcile barrier
                # while both account and public sockets are still alive.
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(calibration_task, timeout=20.0)
            if controller is not None:
                with contextlib.suppress(Exception):
                    await controller.shutdown()
            for task in tasks:
                if task is not calibration_task:
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for venue in self.venues.values():
                with contextlib.suppress(Exception):
                    await venue.close()
            if self.market_history is not None:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(self.market_history.flush)
                with contextlib.suppress(Exception):
                    log.info(
                        "[B0] shutdown calibration_events=%d samples=%d trades=%d",
                        self.market_history.count_rows("arcus_calibration_events"),
                        self.market_history.count_rows("arcus_samples"),
                        self.market_history.count_rows("arcus_trades"),
                    )
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(self.market_history.close)

    def _log_b0_pre_order_state(self, state, metadata) -> None:
        candidate = state.proposed_quote
        log.info(
            "[B0 pre-order] ARCUS fee tier=%s maker=%.4fbps taker=%.4fbps "
            "market id=%s symbol=%s tick=%s step=%s minOrderSize=%s "
            "status=%s",
            state.fee_tier,
            state.maker_fee_bps,
            state.taker_fee_bps,
            metadata.market_id,
            metadata.symbol,
            metadata.tick_size,
            metadata.step_size,
            metadata.min_order_size,
            metadata.status,
        )
        log.info(
            "[B0 pre-order] RH account tier=%s name=%s "
            "current_maker_fee_tick=%s current_taker_fee_tick=%s "
            "verified_maker=%.4fbps verified_taker=%.4fbps source=%s",
            state.rh_account_tier,
            state.rh_account_tier_name,
            state.rh_current_maker_fee_tick,
            state.rh_current_taker_fee_tick,
            state.rh_verified_maker_fee_bps,
            state.rh_verified_taker_fee_bps,
            state.rh_fee_source,
        )
        log.info(
            "[B0 pre-order] positions ARCUS=%s RH=%s open_orders ARCUS=%s RH=%s",
            state.arcus_position,
            state.rh_position,
            list(state.arcus_open_orders),
            list(state.rh_open_orders),
        )
        log.info(
            "[B0 pre-order] BBO ARCUS=%s/%s RH=%s/%s premium=%s center=%s "
            "center_source=%s isOutsideRth=%s sequence=%s",
            state.arcus_bid,
            state.arcus_ask,
            state.rh_bid,
            state.rh_ask,
            state.premium_bps,
            state.center_bps,
            state.center_source,
            state.outside_rth,
            getattr(self.arcus.book, "sequence_health", "UNKNOWN"),
        )
        if candidate is None:
            log.info("[B0 pre-order] proposed maker quote=NONE; no >=4.0bps side")
        else:
            log.info(
                "[B0 pre-order] proposed maker side=%s price=%s qty=%s "
                "expected_edge=%s bps expected_usd=%s TIF=ALO",
                candidate.side,
                candidate.price,
                candidate.quantity,
                candidate.expected_edge_bps,
                candidate.expected_usd,
            )
        log.info(
            "[B0 pre-order] limits qty=%s max_fill_events=%s "
            "max_notional=$%s max_loss=$%s runtime=%ss "
            "RH_slippage_allowance=%sbps RH_hard_cap=%sbps",
            state.limits.max_order_qty,
            state.limits.max_fill_events,
            state.limits.max_filled_notional_usd,
            state.limits.max_loss_usd,
            state.limits.max_runtime_seconds,
            "2.0",
            "20.0",
        )

    async def _run_inner(self) -> None:
        cfg = self.cfg
        if self.volume_probe and (self.record_only or self.tiny_live):
            raise RuntimeError(
                "volume probe is mutually exclusive with record-only and tiny-live"
            )
        selected = self._make_venue(getattr(cfg, "arcus", cfg.entropy))
        if self.volume_probe and not isinstance(selected, ArcusVenue):
            raise RuntimeError("volume probe requires an Arcus primary venue")
        if isinstance(selected, ArcusVenue):
            if self.volume_probe:
                await self._run_arcus_volume_probe(selected)
                return
            if self.tiny_live:
                await self._run_arcus_tiny_live(selected)
                return
            if not self.record_only:
                raise RuntimeError(
                    "Phase A is record-only; pass --record-only. "
                    "Arcus trading is not implemented in Phase A"
                )
            await self._run_arcus_record_only(selected)
            return

        # Compatibility path for the mature strategy/execution unit tests and
        # historical helpers.  It is unreachable from the Arcus config.
        self.entropy = selected
        self.hedge = self._make_venue(cfg.hedge)
        self.venues = {"entropy": self.entropy, "hedge": self.hedge}
        await asyncio.gather(self.entropy.load_market(), self.hedge.load_market())
        self.markets_ready = True
        if cfg.recorder_enabled or self.record_only:
            self.market_history = MarketHistoryStore(cfg.recorder_database)
        await self._bootstrap_rolling_center()

        live = not self.record_only
        if live:
            log.info(
                "execution config: leg_slippage=%.2fbps "
                "hedge_slippage=%.2fbps persistence=%.2fs cooldown=%.2fs "
                "max_order=$%g inventory_scale=%.2fbps",
                cfg.leg_slippage_bps,
                cfg.hedge_slippage_bps,
                cfg.premium_persist_sec,
                cfg.cooldown_sec,
                cfg.max_order_notional,
                cfg.inventory_scale_bps,
            )
        if live:
            if not cfg.creds_complete:
                raise RuntimeError(
                    "live trading needs credentials for both venues in .env "
                    "(see .env.example); use --record-only to run without "
                    "them / 实盘需要在 .env 中配置两个交易所的密钥，仅采集数据"
                    "请用 --record-only"
                )
            self.entropy.init_signer()
            self.hedge.init_signer()
            if self.hedge.kind == "hl":
                self.entropy.share_nonces_with(self.hedge)
        if (
            self.hedge.kind == "hl"
            and self.entropy._query_address()
            and self.entropy._query_address() == self.hedge._query_address()
        ):
            self.hedge.include_core_equity = False  # shared account: count once

        self._step = 10 ** -min(self.entropy.size_decimals, self.hedge.size_decimals)
        self._min_base = max(self.entropy.min_base, self.hedge.min_base, self._step)
        self._min_notional = max(
            cfg.min_order_notional, self.entropy.min_quote, self.hedge.min_quote
        )
        strategy_desc = self._startup_strategy_desc(self.strategy.state())
        log.info(
            "pair ENTROPY(%s)-%s(%s): %s fees=%.2f+%.2f step=%g min_ntl=$%g",
            self.entropy.conf.symbol,
            self.hedge.name,
            self.hedge.conf.symbol,
            strategy_desc,
            self.entropy.fee_bps,
            self.hedge.fee_bps,
            self._step,
            self._min_notional,
        )
        log.info("No automatic strategy selection.")

        if self.record_only:
            log.warning("RECORD-ONLY — collecting minute data, no strategy, no orders")
        else:
            log.warning(
                "LIVE — real orders will be sent (use --record-only "
                "for credential-less data collection)"
            )
            await self._reconcile_positions(hedge=False, strict=True)
            log.info(
                "starting positions: %s (net %+.6g)",
                " ".join(f"{v.name}={v.position:+.6g}" for v in self.venues.values()),
                sum(v.position for v in self.venues.values()),
            )

        reference_task = None
        self.reference = self._build_reference_recorder()
        if self.reference is not None:
            reference_task = asyncio.create_task(
                self._run_reference(),
                name="reference",
            )

        tasks: list[asyncio.Task] = []
        if self.market_history is not None:
            tasks.append(
                asyncio.create_task(self._storage_flush_loop(), name="storage-flush")
            )
        for v in self.venues.values():
            tasks += v.start_tasks(self.stop, self._update_evt.set, live)
        if cfg.recorder_enabled or self.record_only:
            if self.market_history is None:
                raise RuntimeError(
                    "recorder requires an initialized market-history store"
                )
            self.recorder = MinuteRecorder(
                self.market_history,
                self.entropy.book,
                self.hedge.book,
                cfg.staleness_sec,
                symbol=cfg.symbol,
                hedge=cfg.hedge_venue,
            )
            tasks.append(
                asyncio.create_task(self.recorder.run(self.stop), name="recorder")
            )
        if not self.record_only:
            if self.strategy.requires_observations:
                tasks.append(
                    asyncio.create_task(
                        self._strategy_observation_loop(),
                        name="strategy-observer",
                    )
                )
            tasks.append(asyncio.create_task(self._strategy_loop(), name="strategy"))
            tasks.append(asyncio.create_task(self._balance_loop(), name="balances"))
            tasks.append(
                asyncio.create_task(self._http_keepalive_loop(), name="keepalive")
            )
        tasks.append(asyncio.create_task(self._status_loop(), name="status"))
        if live:
            tasks.append(asyncio.create_task(self._reconcile_loop(), name="reconcile"))

        await self.stop.wait()
        if self._exec_tasks:  # let in-flight executions settle, never cancel
            log.info(
                "waiting for %d in-flight execution(s) to settle", len(self._exec_tasks)
            )
            await asyncio.wait(self._exec_tasks, timeout=cfg.settle_timeout_sec + 2.0)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if reference_task is not None:
            await asyncio.gather(reference_task, return_exceptions=True)
        for v in self.venues.values():
            await v.close()
        if self.market_history is not None:
            await asyncio.to_thread(self.market_history.close)
        log.info(
            "shutdown — %d trades, %d hedges, exp edge $%.4f, fill edge $%.4f",
            self.trades,
            self.hedges,
            self.total_exp_edge,
            self.total_fill_edge,
        )

    # --------------------------------------------------------------- signals

    def _inv_add_bps(self, buy, sell) -> float:
        """Inventory ladder: a surcharge that grows once a venue's position
        passes floor_frac of its cap in the direction the trade would add to
        (buying adds when that venue is >= flat long; selling adds when the
        venue is <= flat short). Max of the two venues' ramps."""
        scale = self.cfg.inventory_scale_bps
        if scale <= 0:
            return 0.0
        floor = min(max(self.cfg.inventory_floor_frac, 0.0), 0.99)

        def ramp(v, adding: bool) -> float:
            if not adding:
                return 0.0
            ref = v.book.mid()
            if ref is None:
                return 0.0
            u = min(abs(v.position) * ref / v.cap_usd, 1.0)
            if u <= floor:
                return 0.0
            return scale * (u - floor) / (1.0 - floor)

        return max(ramp(buy, buy.position >= 0), ramp(sell, sell.position <= 0))

    def _eff_threshold(self, buy, sell, state: StrategyState) -> float:
        """Net hurdle (bps, on top of fees) for the direction buy->sell.

        selling entropy: executable premium must clear midline + upper;
        buying entropy: the reverse premium must clear lower - midline."""
        if not state.ready or state.center_bps is None:
            raise RuntimeError("strategy state is not ready")
        if sell.key == "entropy":
            base = state.center_bps + state.upper_bps
        else:
            base = state.lower_bps - state.center_bps
        return base + self._inv_add_bps(buy, sell)

    def _headroom(self, buy, sell, ref_px: float) -> float:
        hb = buy.cap_usd - buy.position * ref_px
        hs = sell.cap_usd + sell.position * ref_px
        return min(hb, hs)

    def _plan(self, buy, sell, cap_notional: float, state: StrategyState):
        return plan_arb(
            buy.book,
            sell.book,
            threshold_bps=self._eff_threshold(buy, sell, state),
            buy_fee_bps=buy.fee_bps,
            sell_fee_bps=sell.fee_bps,
            take_fraction=self.cfg.take_fraction,
            cap_notional=cap_notional,
            min_base=self._min_base,
            min_notional=self._min_notional,
            size_step=self._step,
        )

    # -------------------------------------------------------------- strategy

    def _log_rolling_center_update(
        self, update: RollingCenterUpdate, *, initialized: bool
    ) -> None:
        label = "initialized" if initialized else "updated"
        log.info(
            "rolling center %s: old=%+.2fbps new=%+.2fbps window=%gh "
            "samples=%d range=[%+.2f,%+.2f]",
            label,
            update.old_center_bps,
            update.new_center_bps,
            update.window_sec / 3600.0,
            update.samples,
            update.range_min_bps,
            update.range_max_bps,
        )

    async def _bootstrap_rolling_center(self, now: float | None = None) -> None:
        """Load only the recent premium window for an optional rolling center."""
        if getattr(self.strategy, "center_mode", "fixed") != "rolling":
            return
        now = time.time() if now is None else now
        fallback = self.strategy.fixed_center_bps
        window_hours = self.strategy.center_window_hours
        update_minutes = self.strategy.center_update_minutes
        log.info(
            "center config: mode=rolling fallback=%+.2fbps window=%gh update=%gm",
            fallback,
            window_hours,
            update_minutes,
        )

        observations = []
        if self.market_history is not None:
            start_ms = int((now - self.strategy.window_sec - 2 * 1.0) * 1000)
            end_ms = int(now * 1000)
            try:
                observations = await asyncio.to_thread(
                    self.market_history.recent_premium_observations,
                    self.cfg.symbol,
                    self.cfg.hedge_venue,
                    start_ms,
                    end_ms,
                )
            except Exception:
                # History is an optional bootstrap aid; a storage/read error
                # must not take down live execution or change the fallback.
                log.exception("rolling center history bootstrap failed")

        try:
            update = self.strategy.bootstrap(observations, now=now)
        except Exception:
            log.exception("rolling center bootstrap failed; using fallback")
            update = None
        if update is not None:
            self._log_rolling_center_update(update, initialized=True)
            return

        samples, span_sec = self.strategy.rolling_history_summary(now=now)
        log.info(
            "rolling center unavailable: history=%.1fh required=%gh "
            "using fallback=%+.2fbps",
            span_sec / 3600.0,
            window_hours,
            fallback,
        )

    async def _strategy_loop(self) -> None:
        while not self.stop.is_set():
            await self._update_evt.wait()
            self._update_evt.clear()
            if self.stop.is_set():
                break
            try:
                await self._evaluate()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("evaluate failed")

    def _sample_strategy_observation(self, now: float | None = None) -> bool:
        if not self.strategy.requires_observations:
            return False
        now = time.time() if now is None else now
        cfg = self.cfg
        if not (
            self.entropy.book.is_fresh(cfg.staleness_sec)
            and self.hedge.book.is_fresh(cfg.staleness_sec)
        ):
            return False
        e_bid = self.entropy.book.best_bid()
        e_ask = self.entropy.book.best_ask()
        h_bid = self.hedge.book.best_bid()
        h_ask = self.hedge.book.best_ask()
        if None in (e_bid, e_ask, h_bid, h_ask):
            return False
        before = self.strategy.state()
        values = calculate_premiums(e_bid, e_ask, h_bid, h_ask)
        update = self.strategy.update(now, values.premium_bps)
        if isinstance(update, RollingCenterUpdate):
            self._log_rolling_center_update(update, initialized=False)
        after = self.strategy.state()
        if after != before:
            self._update_evt.set()
        return True

    async def _strategy_observation_loop(self) -> None:
        while not self.stop.is_set():
            try:
                self._sample_strategy_observation()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("strategy observation failed")
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=1.0)
            except TimeoutError:
                pass

    def _schedule_poke(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        due = loop.time() + max(delay, 0.01)
        if self._poke_due is not None and self._poke_due <= due + 0.02:
            return

        def _fire() -> None:
            self._poke_due = None
            self._update_evt.set()

        self._poke_due = due
        loop.call_at(due, _fire)

    def _skiplog(self, fmt: str, *args) -> None:
        now = time.time()
        if now - self._last_skiplog >= 2.0:
            self._last_skiplog = now
            log.info(fmt, *args)

    async def _evaluate(self) -> None:
        cfg = self.cfg
        if self.halted:
            return
        now = time.time()
        if now - self.last_trade_ts < cfg.cooldown_sec:
            self._schedule_poke(cfg.cooldown_sec - (now - self.last_trade_ts))
            return
        best = self._scan(now)
        if best is None:
            return
        buy, sell, plan, state = best
        # _scan verified both locks free and nothing ran since (no awaits),
        # so these acquires take the no-suspension fast path
        await self._vlock(buy.key).acquire()
        await self._vlock(sell.key).acquire()
        # run as a task so a shutdown cancels the strategy loop's await, never
        # the in-flight execution itself (both legs must settle)
        t = asyncio.create_task(self._execute_locked(buy, sell, plan, state))
        self._exec_tasks.add(t)
        t.add_done_callback(self._exec_tasks.discard)
        await asyncio.shield(t)

    async def _execute_locked(
        self, buy, sell, plan: ArbPlan, state: StrategyState
    ) -> None:
        """Run one execution while holding both venue locks (acquired by the
        caller), then release them and settle the aftermath: unresolved
        outcomes escalate to reconcile, everything else gets a net-delta
        check."""
        execution_id = self._new_execution_id()
        unresolved = False
        try:
            unresolved = await self._execute(
                buy, sell, plan, state, execution_id=execution_id
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("execute failed")
        finally:
            self._vlock(buy.key).release()
            self._vlock(sell.key).release()
        if unresolved:
            self._reconcile_evt.set()
        else:
            await self._maybe_hedge(execution_id)
        self._update_evt.set()  # freed venues may have a queued opportunity

    def _scan(self, now: float):
        """Evaluate both directions; returns the best executable
        (buy, sell, plan), or None."""
        cfg = self.cfg
        state = self.strategy.state()
        if not state.ready or state.center_bps is None:
            self._armed["sell_entropy"] = None
            self._armed["buy_entropy"] = None
            return None
        best = None
        for buy, sell, dkey in (
            (self.hedge, self.entropy, "sell_entropy"),
            (self.entropy, self.hedge, "buy_entropy"),
        ):
            if not (
                buy.book.is_fresh(cfg.staleness_sec)
                and sell.book.is_fresh(cfg.staleness_sec)
            ):
                continue
            if not (buy.ready_to_trade() and sell.ready_to_trade()):
                continue
            if self._venue_down:
                continue  # a venue in outage pauses the (only) pair
            if self._vlock(buy.key).locked() or self._vlock(sell.key).locked():
                continue  # mid-execution or mid-reconcile
            if self._venue_limited(buy) or self._venue_limited(sell):
                continue  # reactive 429 exclusion
            if not (self._venue_rate_ok(buy) and self._venue_rate_ok(sell)):
                self._skiplog("%s deferred: venue order budget exhausted", dkey)
                continue
            # never refire into books that predate the venue's own last trade
            if (
                buy.book.last_update_ts <= buy.last_traded_ts
                or sell.book.last_update_ts <= sell.last_traded_ts
            ):
                continue
            plan, reason = self._plan(buy, sell, cfg.max_order_notional, state)
            edge_present = reason not in ("no_edge", "empty_book")
            if not edge_present:
                self._armed[dkey] = None
                continue
            armed = self._armed.get(dkey)
            if armed is None:
                # premium persistence: only fire if the edge survives
                # premium_persist_sec (filters one-tick phantoms)
                self._armed[dkey] = now
                self._schedule_poke(cfg.premium_persist_sec)
                continue
            if now - armed < cfg.premium_persist_sec:
                self._schedule_poke(cfg.premium_persist_sec - (now - armed))
                continue
            if plan is None:
                continue
            headroom = self._headroom(buy, sell, plan.buy_limit)
            if headroom < plan.buy_notional:
                plan, _ = self._plan(
                    buy,
                    sell,
                    min(cfg.max_order_notional, headroom),
                    state,
                )
                if plan is None:
                    self._skiplog(
                        "%s blocked by position caps (headroom $%.0f)",
                        dkey,
                        max(headroom, 0.0),
                    )
                    continue
            if best is None or plan.exp_edge_usd > best[2].exp_edge_usd:
                best = (buy, sell, plan, state)
        return best

    # ------------------------------------------------------------- execution

    async def _execute(
        self,
        buy,
        sell,
        plan: ArbPlan,
        state: StrategyState,
        execution_id: str | None = None,
    ) -> bool:
        """Send both legs and settle the fills. Both venue locks are held by
        the caller. Returns True when an outcome is unresolved and the caller
        must escalate to reconcile."""
        if self.halted:
            return False
        execution_id = execution_id or self._new_execution_id()
        cfg = self.cfg
        inv_bps = self._inv_add_bps(buy, sell)
        direction = "sell_entropy" if sell.key == "entropy" else "buy_entropy"
        self.last_trade_ts = time.time()
        log.info(
            "[ARB] %s: BUY %s %.6g @<=%.6g | SELL %s @>=%.6g | "
            "take $%.0f of $%.0f | prem %.2fbps | exp $%.4f",
            direction,
            buy.name,
            plan.qty,
            plan.buy_limit,
            sell.name,
            plan.sell_limit,
            plan.buy_notional,
            plan.q_max_notional,
            plan.marginal_premium_bps,
            plan.exp_edge_usd,
        )
        slip = cfg.leg_slippage_bps / 1e4
        buy_bound = buy.px_round(plan.buy_limit * (1 + slip), round_up=False)
        sell_bound = sell.px_round(plan.sell_limit * (1 - slip), round_up=True)
        self._record_send(buy)
        self._record_send(sell)
        res = await asyncio.gather(
            buy.send_taker(is_buy=True, qty=plan.qty, limit_px=buy_bound),
            sell.send_taker(is_buy=False, qty=plan.qty, limit_px=sell_bound),
            return_exceptions=True,
        )

        def normalize_result(result: Any) -> dict[str, Any]:
            if isinstance(result, dict):
                return cast(dict[str, Any], result)
            return {
                "status": "send-failed",
                "filled_base": 0.0,
                "avg_px": None,
                "err": repr(result),
                "unresolved": False,
            }

        binfo, sinfo = [normalize_result(result) for result in res]
        for v, info, side in ((buy, binfo, "buy"), (sell, sinfo, "sell")):
            if info.get("err"):
                log.error("[%s] %s leg: %s", v.name, side, info["err"])
        bfill = binfo["filled_base"]
        sfill = sinfo["filled_base"]
        buy.position += bfill
        sell.position -= sfill
        if bfill:
            bpx = binfo.get("avg_px") or plan.buy_limit
            buy.cash -= bfill * bpx * (1 + plan.buy_fee)
            buy.volume_usd += bfill * bpx
        if sfill:
            spx = sinfo.get("avg_px") or plan.sell_limit
            sell.cash += sfill * spx * (1 - plan.sell_fee)
            sell.volume_usd += sfill * spx

        matched = min(bfill, sfill)
        fill_edge = 0.0
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            fill_edge = matched * (
                sinfo["avg_px"] * (1 - plan.sell_fee)
                - binfo["avg_px"] * (1 + plan.buy_fee)
            )
            self.total_fill_edge += fill_edge
        log.info(
            "[SETTLED] %s: buy %s %s %.6g/%.6g | sell %s %s %.6g/%.6g | "
            "matched %.6g | fill edge $%.4f",
            direction,
            buy.name,
            binfo["status"],
            bfill,
            plan.qty,
            sell.name,
            sinfo["status"],
            sfill,
            plan.qty,
            matched,
            fill_edge,
        )
        buy.last_traded_ts = sell.last_traded_ts = time.time()

        unresolved = binfo.get("unresolved") or sinfo.get("unresolved")
        hard_err = binfo.get("err") is not None or sinfo.get("err") is not None
        rate_limited = False
        for v, info in ((buy, binfo), (sell, sinfo)):
            if str(info.get("err", "")).startswith("RATE_LIMITED"):
                rate_limited = True
                self._mark_limited(v)
            elif "margin" in str(info.get("status", "")).lower():
                log.warning(
                    "[%s] margin rejection — collateral exhausted, pausing venue",
                    v.name,
                )
                self._mark_limited(v)
        sent_ok = not hard_err and not unresolved
        if sent_ok:
            self.consec_errors = 0
        elif not rate_limited:
            self.consec_errors += 1
            if self.consec_errors >= cfg.max_consecutive_errors:
                self.halted = True
                log.critical(
                    "HALTED after %d consecutive execution problems "
                    "— flatten manually and restart / 连续执行异常，"
                    "引擎已停止，请手动平仓后重启",
                    self.consec_errors,
                )
        if sent_ok:
            self.trades += 1
            self.total_exp_edge += plan.exp_edge_usd
        initial_status = f"{binfo['status']}/{sinfo['status']}"
        realized_before_hedge = self._realized_leg_result(
            bfill,
            sfill,
            binfo.get("avg_px"),
            sinfo.get("avg_px"),
            plan.buy_fee,
            plan.sell_fee,
        )
        residual_qty = abs(bfill - sfill)
        has_fills = bfill > 0.0 or sfill > 0.0
        actual: float | None
        if unresolved:
            actual = None
            lifecycle_status = f"{initial_status} → pending"
        elif residual_qty > 1e-12:
            actual = None
            lifecycle_status = f"{initial_status} → hedging"
        elif realized_before_hedge is None and has_fills:
            actual = None
            lifecycle_status = f"{initial_status} → pending"
        else:
            actual = realized_before_hedge or 0.0
            lifecycle_status = initial_status
        trade = self._record_trade(
            direction,
            plan,
            None if unresolved else fill_edge,
            lifecycle_status,
            sent_ok,
            execution_id=execution_id,
            actual=actual,
        )
        needs_followup = unresolved or residual_qty > 1e-12 or actual is None
        trade.update(
            {
                "buy_venue": buy.name,
                "sell_venue": sell.name,
                "requested_qty": plan.qty,
                "buy_filled_qty": bfill,
                "sell_filled_qty": sfill,
                "buy_avg_px": binfo.get("avg_px"),
                "sell_avg_px": sinfo.get("avg_px"),
                "hedge_status": "pending" if needs_followup else "not_required",
            }
        )
        self._persist_execution_event(
            trade,
            "execution_opened" if needs_followup else "execution_finalized",
        )
        if unresolved or residual_qty > 1e-12:
            self._execution_contexts[execution_id] = _ExecutionContext(
                execution_id=execution_id,
                trade=trade,
                residual_qty=residual_qty,
                hedge_is_sell=bfill > sfill,
                realized_before_hedge=realized_before_hedge,
            )
        self._log_csv(
            direction,
            buy,
            sell,
            plan,
            sent_ok,
            bfill,
            sfill,
            binfo["status"],
            sinfo["status"],
            fill_edge,
            inv_bps,
            state,
        )
        self.last_trade_ts = time.time()
        return bool(unresolved)

    def _new_execution_id(self) -> str:
        self._execution_seq += 1
        return f"exec-{self._execution_seq}"

    @staticmethod
    def _realized_leg_result(
        buy_fill: float,
        sell_fill: float,
        buy_price: float | None,
        sell_price: float | None,
        buy_fee: float,
        sell_fee: float,
    ) -> float | None:
        if buy_fill > 0.0 and buy_price is None:
            return None
        if sell_fill > 0.0 and sell_price is None:
            return None
        return sell_fill * (sell_price or 0.0) * (1.0 - sell_fee) - buy_fill * (
            buy_price or 0.0
        ) * (1.0 + buy_fee)

    def _record_trade(
        self,
        direction: str,
        plan: ArbPlan,
        fill_edge,
        status: str,
        ok: bool,
        *,
        execution_id: str,
        actual: float | None,
    ) -> dict:
        trade = {
            "ts": time.time(),
            "direction": direction,
            "qty": plan.qty,
            "notional": plan.buy_notional,
            "prem_bps": plan.marginal_premium_bps,
            "exp": plan.exp_edge_usd,
            "fill": fill_edge,
            "actual": actual,
            "status": status,
            "ok": ok,
            "execution_id": execution_id,
            "hedge_venue": None,
            "hedge_side": None,
            "hedge_filled_qty": 0.0,
            "hedge_avg_px": None,
        }
        self.recent_trades.append(trade)
        return trade

    def _execution_telemetry_path(self) -> str:
        if self.execution_telemetry_csv is not None:
            return self.execution_telemetry_csv
        log_dir = os.path.dirname(self.cfg.log_file) or "logs"
        return os.path.join(
            log_dir,
            f"executions-{self.cfg.symbol}-{self.cfg.hedge_venue}.csv",
        )

    def _persist_execution_event(self, trade: dict, event_type: str) -> None:
        """Append a complete lifecycle snapshot without affecting trading.

        Rows are append-only; offline consumers recover the current/final
        state by taking the last row for each execution_id.
        """
        try:
            path = self._execution_telemetry_path()
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            new = not os.path.exists(path) or os.path.getsize(path) == 0
            with open(path, "a", newline="") as fh:
                writer = csv.writer(fh)
                if new:
                    writer.writerow(EXECUTION_TELEMETRY_HEADER)
                writer.writerow(
                    [
                        time.time(),
                        event_type,
                        trade.get("ts"),
                        trade.get("execution_id"),
                        trade.get("direction"),
                        trade.get("exp"),
                        trade.get("fill"),
                        trade.get("actual"),
                        trade.get("status"),
                        trade.get("buy_venue"),
                        trade.get("sell_venue"),
                        trade.get("requested_qty"),
                        trade.get("buy_filled_qty"),
                        trade.get("sell_filled_qty"),
                        trade.get("buy_avg_px"),
                        trade.get("sell_avg_px"),
                        trade.get("hedge_venue"),
                        trade.get("hedge_side"),
                        trade.get("hedge_filled_qty"),
                        trade.get("hedge_avg_px"),
                        trade.get("hedge_status"),
                    ]
                )
        except Exception:
            # Telemetry must never interrupt order settlement or hedging.
            log.exception("execution telemetry write failed")

    def _mark_hedge_started(self, execution_id: str | None) -> None:
        if execution_id is None:
            return
        context = self._execution_contexts.get(execution_id)
        if context is None:
            return
        trade = context.trade
        if trade.get("hedge_status") == "hedging":
            return
        trade["hedge_status"] = "hedging"
        self._persist_execution_event(trade, "hedge_started")

    async def _maybe_hedge(self, execution_id: str | None = None) -> None:
        net = sum(v.position for v in self.venues.values())
        if abs(net) > self.cfg.net_tolerance_base:
            execution_id = self._select_hedge_context(execution_id, net)
            await self._hedge(net, execution_id=execution_id)

    def _select_hedge_context(self, preferred_id: str | None, net: float) -> str | None:
        """Choose the pending execution whose residual this hedge can settle.

        The normal path supplies the current execution id.  Reconciliation can
        hedge an older residual, though, so fall back to the oldest pending
        context with the same inventory direction rather than losing the
        execution-to-hedge correlation.
        """
        hedge_is_sell = net > 0.0
        if preferred_id is not None:
            preferred = self._execution_contexts.get(preferred_id)
            if (
                preferred is not None
                and preferred.hedge_is_sell == hedge_is_sell
                and preferred.hedge_filled_qty < preferred.residual_qty
            ):
                return preferred_id
        for execution_id, context in self._execution_contexts.items():
            if (
                context.hedge_is_sell == hedge_is_sell
                and context.hedge_filled_qty < context.residual_qty
            ):
                return execution_id
        return None

    def _note_hedge_result(
        self, execution_id: str | None, v, is_sell: bool, info: dict
    ) -> None:
        if execution_id is None:
            return
        context = self._execution_contexts.get(execution_id)
        if context is None or context.hedge_is_sell != is_sell:
            return
        trade = context.trade
        trade["hedge_venue"] = v.name
        trade["hedge_side"] = "SELL" if is_sell else "BUY"
        if info.get("err") is not None or info.get("unresolved"):
            trade["hedge_status"] = "unresolved"
            trade["status"] = "hedge-unresolved"
            self._persist_execution_event(trade, "execution_finalized")
            return
        try:
            filled = max(float(info.get("filled_base") or 0.0), 0.0)
        except (TypeError, ValueError):
            filled = 0.0
        if filled <= 0.0:
            trade["hedge_status"] = "unresolved"
            trade["status"] = "hedge-unresolved"
            self._persist_execution_event(trade, "execution_finalized")
            return

        remaining = max(context.residual_qty - context.hedge_filled_qty, 0.0)
        applied = min(filled, remaining)
        context.hedge_filled_qty += applied
        trade["hedge_filled_qty"] = context.hedge_filled_qty
        try:
            avg_px = float(info["avg_px"]) if info.get("avg_px") is not None else None
        except (TypeError, ValueError):
            avg_px = None
        if applied > 0.0 and avg_px is not None:
            context.hedge_priced_qty += applied
            context.hedge_notional += applied * avg_px
            fee = v.fee_bps / 1e4
            context.hedge_result += (
                applied * avg_px * (1.0 - fee)
                if is_sell
                else -applied * avg_px * (1.0 + fee)
            )
            trade["hedge_avg_px"] = context.hedge_notional / context.hedge_priced_qty
        if context.hedge_filled_qty + 1e-12 < context.residual_qty:
            trade["hedge_status"] = "partial"
            trade["status"] = "hedging"
            self._persist_execution_event(trade, "hedge_settled")
            return
        if (
            context.realized_before_hedge is None
            or context.hedge_priced_qty + 1e-12 < context.hedge_filled_qty
        ):
            trade["hedge_status"] = "filled"
            trade["status"] = "hedged (actual unavailable)"
            self._persist_execution_event(trade, "execution_finalized")
            self._execution_contexts.pop(execution_id, None)
            return
        trade["actual"] = context.realized_before_hedge + context.hedge_result
        trade["hedge_status"] = "filled"
        trade["status"] = "hedged"
        self._persist_execution_event(trade, "execution_finalized")
        self._execution_contexts.pop(execution_id, None)

    async def _hedge(self, net: float, execution_id: str | None = None) -> None:
        """Reduce the venue that carries the imbalance back toward net zero
        (reduce-only taker with hedge_slippage_bps price protection)."""
        cfg = self.cfg
        is_sell = net > 0
        sgn = 1.0 if net > 0 else -1.0
        slip = cfg.hedge_slippage_bps / 1e4
        for v in sorted(
            self.venues.values(),
            key=lambda x: (self._venue_limited(x), -x.position * sgn),
        ):
            if v.position * sgn <= 0:
                continue
            if v.key in self._venue_down or not v.book.is_fresh(cfg.staleness_sec):
                continue  # unreachable or blind: cannot hedge here
            lk = self._vlock(v.key)
            if lk.locked():
                continue
            qty = floor_step(min(abs(net), abs(v.position)), self._step)
            if qty < v.min_base:
                continue
            ref = v.book.best_bid() if is_sell else v.book.best_ask()
            if ref is None:
                continue
            limit = (
                v.px_round(ref * (1 - slip), False)
                if is_sell
                else v.px_round(ref * (1 + slip), True)
            )
            if qty * limit < max(cfg.min_order_notional, v.min_quote):
                continue
            self._mark_hedge_started(execution_id)
            await lk.acquire()  # verified free, no awaits since: fast path
            try:
                log.warning(
                    "[HEDGE] net %+.6g — %s %.6g on %s @%.6g",
                    net,
                    "SELL" if is_sell else "BUY",
                    qty,
                    v.name,
                    limit,
                )
                self.hedges += 1
                self._record_send(v)  # counts toward the budget, never blocked
                info = await v.send_taker(
                    is_buy=not is_sell, qty=qty, limit_px=limit, reduce_only=True
                )
                if info.get("err") or info.get("unresolved"):
                    log.error("[HEDGE] %s: %s", v.name, info.get("err") or "unresolved")
                    if str(info.get("err", "")).startswith("RATE_LIMITED"):
                        self._mark_limited(v)
                    self._note_hedge_result(execution_id, v, is_sell, info)
                    self._reconcile_evt.set()
                else:
                    fill = info["filled_base"]
                    v.position += -fill if is_sell else fill
                    if fill:
                        px = info.get("avg_px") or limit
                        fee = v.fee_bps / 1e4
                        v.cash += (
                            fill * px * (1 - fee) if is_sell else -fill * px * (1 + fee)
                        )
                        v.volume_usd += fill * px
                    log.info(
                        "[HEDGE SETTLED] %s %s %.6g/%.6g",
                        v.name,
                        info["status"],
                        fill,
                        qty,
                    )
                    self._note_hedge_result(execution_id, v, is_sell, info)
                v.last_traded_ts = time.time()
            finally:
                lk.release()
            return
        log.warning(
            "[HEDGE] net %+.6g below hedgeable minimum — carrying "
            "(next reconcile retries)",
            net,
        )

    # --------------------------------------------------- reconcile / status

    # Lighter's REST account state lags its ws settlements; overwriting a
    # venue that traded seconds ago "restores" stale positions and triggers
    # phantom hedge oscillations. Grace-guard + venue lock prevent that.
    RECONCILE_GRACE_SEC = 5.0

    async def _reconcile_positions(self, hedge: bool, strict: bool = False) -> None:
        now = time.time()
        vs = []
        for v in self.venues.values():
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                continue  # just traded: chain read would be stale
            if v.key in self._venue_down and now < self._venue_probe_at.get(v.key, 0.0):
                continue  # down venue: probe only every venue_probe_sec
            vs.append(v)
        if not vs:
            return
        got = await asyncio.gather(
            *(self._reconcile_venue(v, strict) for v in vs), return_exceptions=True
        )
        for r in got:
            if isinstance(r, BaseException):
                raise r  # strict startup: fail loudly
        if hedge:
            await self._maybe_hedge()

    async def _reconcile_venue(self, v, strict: bool) -> None:
        async with self._vlock(v.key):
            now = time.time()
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                return  # traded while waiting for the lock
            try:
                r = await v.fetch_position()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if strict:
                    raise RuntimeError(
                        f"[{v.name}] cannot fetch starting position: {e!r}"
                    )
                # exchange unreachable (e.g. scheduled maintenance): pause
                # trading and keep probing until it answers again
                n = self._venue_fetch_fails.get(v.key, 0) + 1
                self._venue_fetch_fails[v.key] = n
                self._venue_probe_at[v.key] = now + self.cfg.venue_probe_sec
                if n >= 3 and v.key not in self._venue_down:
                    self._venue_down[v.key] = now
                    log.critical(
                        "[%s] API unreachable (%d attempts) — "
                        "trading PAUSED; probing every %.0fs until "
                        "it recovers",
                        v.name,
                        n,
                        self.cfg.venue_probe_sec,
                    )
                elif v.key not in self._venue_down:
                    log.warning("[%s] position fetch failed (%d): %r", v.name, n, e)
                return
            if v.key in self._venue_down:
                log.warning(
                    "[%s] API recovered after %.0fs outage — trading RESUMED",
                    v.name,
                    now - self._venue_down.pop(v.key),
                )
                self._update_evt.set()
            self._venue_fetch_fails[v.key] = 0
            delta = r - v.position
            if abs(delta) > 1e-12:
                if abs(delta) > self.cfg.net_tolerance_base:
                    log.warning(
                        "[%s] reconcile: chain %+.6g vs local %+.6g — adopting chain",
                        v.name,
                        r,
                        v.position,
                    )
                mid = v.book.mid()
                if mid is not None:
                    v.cash -= delta * mid
                v.position = r

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(
                    self._reconcile_evt.wait(), timeout=self.cfg.reconcile_sec
                )
                self._reconcile_evt.clear()
                await asyncio.sleep(1.0)
            except TimeoutError:
                pass
            if self.stop.is_set():
                break
            try:
                await self._reconcile_positions(hedge=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconcile failed")

    async def _balance_loop(self) -> None:
        while not self.stop.is_set():
            for v in self.venues.values():
                try:
                    got = await v.fetch_equity()
                    if got is not None:
                        v.equity, v.free = got
                        if v.start_equity is None:
                            v.start_equity = v.equity
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.debug("[%s] equity poll failed: %r", v.name, e)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=BALANCE_POLL_SEC)
            except TimeoutError:
                pass

    async def _http_keepalive_loop(self) -> None:
        if self.cfg.http_keepalive_sec <= 0:
            return
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(
                    self.stop.wait(), timeout=self.cfg.http_keepalive_sec
                )
                return
            except TimeoutError:
                pass
            await asyncio.gather(
                *(v.warm_http() for v in self.venues.values()), return_exceptions=True
            )

    def account_delta(self) -> float | None:
        """Change in real account equity since start (both venues)."""
        total = 0.0
        for v in self.venues.values():
            if v.equity is None or v.start_equity is None:
                return None
            total += v.equity - v.start_equity
        return total

    def session_pnl(self) -> float | None:
        total = 0.0
        for v in self.venues.values():
            m = v.book.mid()
            if m is None:
                return None
            total += v.cash + v.position * m
        if self._mtm_baseline is None:
            self._mtm_baseline = total
        return total - self._mtm_baseline

    def premium_bps(self) -> float | None:
        e_bid = self.entropy.book.best_bid()
        e_ask = self.entropy.book.best_ask()
        h_bid = self.hedge.book.best_bid()
        h_ask = self.hedge.book.best_ask()
        if None in (e_bid, e_ask, h_bid, h_ask):
            return None
        return calculate_premiums(e_bid, e_ask, h_bid, h_ask).premium_bps

    def _strategy_abs_band(self, state: StrategyState) -> tuple[float, float]:
        if state.center_bps is None:
            raise RuntimeError("strategy state has no center")
        return state.center_bps - state.lower_bps, state.center_bps + state.upper_bps

    def _startup_strategy_desc(self, state: StrategyState) -> str:
        if state.ready and state.center_bps is not None:
            low, high = self._strategy_abs_band(state)
            return (
                f"strategy={self.cfg.strategy.name} "
                f"center={state.center_bps:+.2f}bps "
                f"band=[{low:+.2f},{high:+.2f}]"
            )
        return (
            f"strategy={self.cfg.strategy.name} "
            f"window={state.window_minutes}m "
            f"center=WARMING_UP "
            f"band-offset=[{-state.lower_bps:+.2f},{state.upper_bps:+.2f}]"
        )

    def _status_strategy_desc(self, state: StrategyState) -> str:
        if state.ready and state.center_bps is not None:
            low, high = self._strategy_abs_band(state)
            return (
                f"strategy={self.cfg.strategy.name} "
                f"center={state.center_bps:+.2f} "
                f"band={low:+.2f}..{high:+.2f}"
            )
        span_min = (state.warmup_span_sec or 0.0) / 60.0
        coverage = 100.0 * (state.coverage_ratio or 0.0)
        return (
            f"strategy={self.cfg.strategy.name} "
            f"WARMING_UP window={state.window_minutes}m "
            f"span={span_min:.1f}m valid={coverage:.1f}%"
        )

    async def _status_loop(self) -> None:
        cfg = self.cfg
        while not self.stop.is_set():
            try:
                await asyncio.sleep(cfg.status_interval_sec)
            except asyncio.CancelledError:
                raise
            books = " | ".join(
                f"{v.name} {v.book.best_bid() or '—'}/{v.book.best_ask() or '—'}"
                + ("" if v.book.is_fresh(cfg.staleness_sec) else " STALE")
                + (" RATE-LTD" if self._venue_limited(v) else "")
                + (" DOWN" if v.key in self._venue_down else "")
                for v in self.venues.values()
            )
            prem = self.premium_bps()
            prem_s = f"{prem:+.2f}" if prem is not None else "—"
            pos = " ".join(f"{v.name} {v.position:+.6g}" for v in self.venues.values())
            net = sum(v.position for v in self.venues.values())
            pnl = self.session_pnl()
            rec = f" | rec {self.recorder.rows_written} rows" if self.recorder else ""
            strategy_desc = self._status_strategy_desc(self.strategy.state())
            log.info(
                "[status] %s | prem %s bps | %s | pos %s "
                "net %+.6g | trades %d hedges %d | MTM %s expEdge $%.4f "
                "fillEdge $%.4f%s%s",
                books,
                prem_s,
                strategy_desc,
                pos,
                net,
                self.trades,
                self.hedges,
                f"${pnl:+.4f}" if pnl is not None else "—",
                self.total_exp_edge,
                self.total_fill_edge,
                rec,
                " *** HALTED ***" if self.halted else "",
            )

    def _log_csv(
        self,
        direction,
        buy,
        sell,
        plan: ArbPlan,
        ok: bool,
        bfill,
        sfill,
        bstatus,
        sstatus,
        fill_edge,
        inv_bps,
        strategy_state: StrategyState,
    ) -> None:
        try:
            path = self.cfg.trades_csv
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
            if os.path.exists(path):
                with open(path) as fh0:
                    if fh0.readline().strip() != ",".join(CSV_HEADER):
                        os.replace(path, path + ".old")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(CSV_HEADER)
                w.writerow(
                    [
                        f"{time.time():.3f}",
                        direction,
                        buy.name,
                        sell.name,
                        f"{plan.qty:.8g}",
                        plan.buy_limit,
                        plan.sell_limit,
                        f"{plan.buy_notional:.2f}",
                        f"{plan.sell_notional:.2f}",
                        f"{plan.exp_edge_usd:.4f}",
                        f"{plan.gross_edge_usd:.4f}",
                        f"{plan.marginal_premium_bps:.3f}",
                        f"{strategy_state.center_bps:.3f}",
                        f"{inv_bps:.3f}",
                        int(ok),
                        f"{bfill:.8g}",
                        f"{sfill:.8g}",
                        bstatus,
                        sstatus,
                        f"{fill_edge:.4f}",
                    ]
                )
        except Exception:
            log.exception("csv write failed")
