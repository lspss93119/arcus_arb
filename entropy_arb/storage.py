"""SQLite persistence for market history observations."""

from __future__ import annotations

import logging
import math
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

SCHEMA_VERSION = "1"
FLUSH_INTERVAL_SEC = 10.0
MAX_PENDING_ROWS_PER_DATASET = 100_000
DEFAULT_BUSY_TIMEOUT_MS = 10_000

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SampleRow:
    timestamp_ms: int
    symbol: str
    hedge: str
    premium_bps: float
    sell_edge_bps: float
    buy_edge_bps: float
    entropy_bid: float
    entropy_ask: float
    hedge_bid: float
    hedge_ask: float
    entropy_book_update_ms: int
    hedge_book_update_ms: int


@dataclass(frozen=True)
class MinuteRow:
    minute_ts: int
    symbol: str
    hedge: str
    entropy_bid: float
    entropy_ask: float
    hedge_bid: float
    hedge_ask: float
    premium_open_bps: float
    premium_high_bps: float
    premium_low_bps: float
    premium_close_bps: float
    premium_mean_bps: float
    premium_std_bps: float
    sell_edge_mean_bps: float
    sell_edge_max_bps: float
    buy_edge_mean_bps: float
    buy_edge_max_bps: float
    samples: int


@dataclass(frozen=True)
class EntropyReferenceRow:
    symbol: str
    hedge: str
    recv_ms: int
    oracle_px: float
    mark_px: float


@dataclass(frozen=True)
class HedgeReferenceRow:
    symbol: str
    hedge: str
    recv_ms: int
    server_ms: int
    index_px: float
    mark_px: float


@dataclass(frozen=True)
class ArcusSampleRow:
    timestamp_ms: int
    symbol: str
    arcus_bid: float
    arcus_ask: float
    arcus_bid_size: float
    arcus_ask_size: float
    arcus_mid: float
    rh_bid: float
    rh_ask: float
    rh_bid_size: float
    rh_ask_size: float
    rh_mid: float
    premium_bps: float
    arcus_book_sequence_id: int | None
    arcus_global_sequence_id: int | None
    arcus_exchange_timestamp_us: int | None
    arcus_local_receive_ts_ms: int
    arcus_local_receive_monotonic_ns: int | None
    rh_local_receive_ts_ms: int
    is_outside_rth: bool | None
    current_settlement_price: str | None
    upper_trading_bound: str | None
    lower_trading_bound: str | None
    next_upper_trading_bound: str | None
    next_lower_trading_bound: str | None
    hedge: str = "lighter-rh"


@dataclass(frozen=True)
class ArcusMinuteRow:
    minute_ts: int
    symbol: str
    hedge: str
    arcus_bid: float
    arcus_ask: float
    rh_bid: float
    rh_ask: float
    premium_open_bps: float
    premium_high_bps: float
    premium_low_bps: float
    premium_close_bps: float
    premium_mean_bps: float
    premium_std_bps: float
    samples: int


@dataclass(frozen=True)
class ArcusTradeRow:
    symbol: str
    market_id: int
    market_display_name: str
    trade_id: str
    exchange_timestamp_us: int
    local_receive_ts_ms: int
    local_receive_monotonic_ns: int | None
    price: str
    quantity: str
    aggressor_side: str | None
    sequence_number: int


@dataclass(frozen=True)
class ArcusMarketAttributesRow:
    symbol: str
    market_id: int
    market_display_name: str
    market_status: str
    local_receive_ts_ms: int
    local_receive_monotonic_ns: int | None
    event_timestamp_us: int | None
    market_sequence_num: int | None
    is_outside_rth: bool | None
    current_settlement_price: str | None
    upper_trading_bound: str | None
    lower_trading_bound: str | None
    next_upper_trading_bound: str | None
    next_lower_trading_bound: str | None


@dataclass(frozen=True)
class ArcusMarketMetadataRow:
    discovered_at_ms: int
    symbol: str
    market_id: int
    market_display_name: str
    status: str
    tick_size: str
    step_size: str
    min_order_size: str | None
    min_order_notional: str | None
    max_order_size: str | None
    is_outside_rth: bool | None
    current_settlement_price: str | None
    upper_trading_bound: str | None
    lower_trading_bound: str | None
    next_upper_trading_bound: str | None
    next_lower_trading_bound: str | None
    regular_trading_hours: str | None


@dataclass(frozen=True)
class InsertCounts:
    inserted: int = 0
    duplicates: int = 0
    conflicts: int = 0


@dataclass(frozen=True)
class FlushReport:
    ok: bool
    datasets: dict[str, InsertCounts]


_SPECS = {
    "samples": (SampleRow, ("symbol", "hedge", "timestamp_ms"), (
        "timestamp_ms", "symbol", "hedge", "premium_bps", "sell_edge_bps", "buy_edge_bps",
        "entropy_bid", "entropy_ask", "hedge_bid", "hedge_ask", "entropy_book_update_ms", "hedge_book_update_ms")),
    "minutes": (MinuteRow, ("symbol", "hedge", "minute_ts"), (
        "minute_ts", "symbol", "hedge", "entropy_bid", "entropy_ask", "hedge_bid", "hedge_ask",
        "premium_open_bps", "premium_high_bps", "premium_low_bps", "premium_close_bps", "premium_mean_bps",
        "premium_std_bps", "sell_edge_mean_bps", "sell_edge_max_bps", "buy_edge_mean_bps", "buy_edge_max_bps", "samples")),
    "entropy_reference": (EntropyReferenceRow, ("symbol", "hedge", "recv_ms", "oracle_px", "mark_px"),
                           ("symbol", "hedge", "recv_ms", "oracle_px", "mark_px")),
    "hedge_reference": (HedgeReferenceRow, ("symbol", "hedge", "recv_ms", "server_ms", "index_px", "mark_px"),
                         ("symbol", "hedge", "recv_ms", "server_ms", "index_px", "mark_px")),
    "arcus_samples": (ArcusSampleRow, ("symbol", "hedge", "timestamp_ms"), (
        "timestamp_ms", "symbol", "hedge", "arcus_bid", "arcus_ask",
        "arcus_bid_size", "arcus_ask_size", "arcus_mid", "rh_bid", "rh_ask",
        "rh_bid_size", "rh_ask_size", "rh_mid", "premium_bps",
        "arcus_book_sequence_id", "arcus_global_sequence_id",
        "arcus_exchange_timestamp_us", "arcus_local_receive_ts_ms",
        "arcus_local_receive_monotonic_ns", "rh_local_receive_ts_ms",
        "is_outside_rth", "current_settlement_price", "upper_trading_bound",
        "lower_trading_bound", "next_upper_trading_bound",
        "next_lower_trading_bound")),
    "arcus_minutes": (ArcusMinuteRow, ("symbol", "hedge", "minute_ts"), (
        "minute_ts", "symbol", "hedge", "arcus_bid", "arcus_ask", "rh_bid",
        "rh_ask", "premium_open_bps", "premium_high_bps", "premium_low_bps",
        "premium_close_bps", "premium_mean_bps", "premium_std_bps", "samples")),
    "arcus_trades": (ArcusTradeRow, (
        "symbol", "market_id", "trade_id", "exchange_timestamp_us", "price",
        "quantity"), (
        "symbol", "market_id", "market_display_name", "trade_id",
        "exchange_timestamp_us", "local_receive_ts_ms",
        "local_receive_monotonic_ns", "price", "quantity", "aggressor_side",
        "sequence_number")),
    "arcus_market_attributes": (ArcusMarketAttributesRow, (
        "symbol", "market_id", "local_receive_ts_ms", "local_receive_monotonic_ns"), (
        "symbol", "market_id", "market_display_name", "market_status",
        "local_receive_ts_ms", "local_receive_monotonic_ns", "event_timestamp_us",
        "market_sequence_num", "is_outside_rth", "current_settlement_price",
        "upper_trading_bound", "lower_trading_bound", "next_upper_trading_bound",
        "next_lower_trading_bound")),
    "arcus_market_metadata": (ArcusMarketMetadataRow, (
        "market_id", "discovered_at_ms"), (
        "discovered_at_ms", "symbol", "market_id", "market_display_name",
        "status", "tick_size", "step_size", "min_order_size",
        "min_order_notional", "max_order_size", "is_outside_rth",
        "current_settlement_price", "upper_trading_bound", "lower_trading_bound",
        "next_upper_trading_bound", "next_lower_trading_bound",
        "regular_trading_hours")),
}

_CREATE = {
    "meta": "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "samples": """CREATE TABLE IF NOT EXISTS samples (
        timestamp_ms INTEGER NOT NULL, symbol TEXT NOT NULL, hedge TEXT NOT NULL,
        premium_bps REAL NOT NULL, sell_edge_bps REAL NOT NULL, buy_edge_bps REAL NOT NULL,
        entropy_bid REAL NOT NULL, entropy_ask REAL NOT NULL, hedge_bid REAL NOT NULL, hedge_ask REAL NOT NULL,
        entropy_book_update_ms INTEGER NOT NULL, hedge_book_update_ms INTEGER NOT NULL,
        PRIMARY KEY (symbol, hedge, timestamp_ms))""",
    "minutes": """CREATE TABLE IF NOT EXISTS minutes (
        minute_ts INTEGER NOT NULL, symbol TEXT NOT NULL, hedge TEXT NOT NULL,
        entropy_bid REAL NOT NULL, entropy_ask REAL NOT NULL, hedge_bid REAL NOT NULL, hedge_ask REAL NOT NULL,
        premium_open_bps REAL NOT NULL, premium_high_bps REAL NOT NULL, premium_low_bps REAL NOT NULL,
        premium_close_bps REAL NOT NULL, premium_mean_bps REAL NOT NULL, premium_std_bps REAL NOT NULL,
        sell_edge_mean_bps REAL NOT NULL, sell_edge_max_bps REAL NOT NULL, buy_edge_mean_bps REAL NOT NULL,
        buy_edge_max_bps REAL NOT NULL, samples INTEGER NOT NULL,
        PRIMARY KEY (symbol, hedge, minute_ts))""",
    "entropy_reference": "CREATE TABLE IF NOT EXISTS entropy_reference (symbol TEXT NOT NULL, hedge TEXT NOT NULL, recv_ms INTEGER NOT NULL, oracle_px REAL NOT NULL, mark_px REAL NOT NULL, PRIMARY KEY (symbol, hedge, recv_ms, oracle_px, mark_px))",
    "hedge_reference": "CREATE TABLE IF NOT EXISTS hedge_reference (symbol TEXT NOT NULL, hedge TEXT NOT NULL, recv_ms INTEGER NOT NULL, server_ms INTEGER NOT NULL, index_px REAL NOT NULL, mark_px REAL NOT NULL, PRIMARY KEY (symbol, hedge, recv_ms, server_ms, index_px, mark_px))",
    "arcus_samples": """CREATE TABLE IF NOT EXISTS arcus_samples (
        timestamp_ms INTEGER NOT NULL, symbol TEXT NOT NULL, hedge TEXT NOT NULL,
        arcus_bid REAL NOT NULL, arcus_ask REAL NOT NULL,
        arcus_bid_size REAL NOT NULL, arcus_ask_size REAL NOT NULL,
        arcus_mid REAL NOT NULL, rh_bid REAL NOT NULL, rh_ask REAL NOT NULL,
        rh_bid_size REAL NOT NULL, rh_ask_size REAL NOT NULL, rh_mid REAL NOT NULL,
        premium_bps REAL NOT NULL, arcus_book_sequence_id INTEGER,
        arcus_global_sequence_id INTEGER, arcus_exchange_timestamp_us INTEGER,
        arcus_local_receive_ts_ms INTEGER NOT NULL,
        arcus_local_receive_monotonic_ns INTEGER, rh_local_receive_ts_ms INTEGER NOT NULL,
        is_outside_rth INTEGER, current_settlement_price TEXT,
        upper_trading_bound TEXT, lower_trading_bound TEXT,
        next_upper_trading_bound TEXT, next_lower_trading_bound TEXT,
        PRIMARY KEY (symbol, hedge, timestamp_ms))""",
    "arcus_minutes": """CREATE TABLE IF NOT EXISTS arcus_minutes (
        minute_ts INTEGER NOT NULL, symbol TEXT NOT NULL, hedge TEXT NOT NULL,
        arcus_bid REAL NOT NULL, arcus_ask REAL NOT NULL, rh_bid REAL NOT NULL,
        rh_ask REAL NOT NULL, premium_open_bps REAL NOT NULL,
        premium_high_bps REAL NOT NULL, premium_low_bps REAL NOT NULL,
        premium_close_bps REAL NOT NULL, premium_mean_bps REAL NOT NULL,
        premium_std_bps REAL NOT NULL, samples INTEGER NOT NULL,
        PRIMARY KEY (symbol, hedge, minute_ts))""",
    "arcus_trades": """CREATE TABLE IF NOT EXISTS arcus_trades (
        symbol TEXT NOT NULL, market_id INTEGER NOT NULL,
        market_display_name TEXT NOT NULL, trade_id TEXT NOT NULL,
        exchange_timestamp_us INTEGER NOT NULL, local_receive_ts_ms INTEGER NOT NULL,
        local_receive_monotonic_ns INTEGER, price TEXT NOT NULL, quantity TEXT NOT NULL,
        aggressor_side TEXT, sequence_number INTEGER NOT NULL,
        PRIMARY KEY (symbol, market_id, trade_id, exchange_timestamp_us, price, quantity))""",
    "arcus_market_attributes": """CREATE TABLE IF NOT EXISTS arcus_market_attributes (
        symbol TEXT NOT NULL, market_id INTEGER NOT NULL,
        market_display_name TEXT NOT NULL, market_status TEXT NOT NULL,
        local_receive_ts_ms INTEGER NOT NULL, local_receive_monotonic_ns INTEGER,
        event_timestamp_us INTEGER, market_sequence_num INTEGER,
        is_outside_rth INTEGER, current_settlement_price TEXT,
        upper_trading_bound TEXT, lower_trading_bound TEXT,
        next_upper_trading_bound TEXT, next_lower_trading_bound TEXT,
        PRIMARY KEY (symbol, market_id, local_receive_ts_ms, local_receive_monotonic_ns))""",
    "arcus_market_metadata": """CREATE TABLE IF NOT EXISTS arcus_market_metadata (
        discovered_at_ms INTEGER NOT NULL, symbol TEXT NOT NULL, market_id INTEGER NOT NULL,
        market_display_name TEXT NOT NULL, status TEXT NOT NULL, tick_size TEXT NOT NULL,
        step_size TEXT NOT NULL, min_order_size TEXT, min_order_notional TEXT,
        max_order_size TEXT, is_outside_rth INTEGER, current_settlement_price TEXT,
        upper_trading_bound TEXT, lower_trading_bound TEXT,
        next_upper_trading_bound TEXT, next_lower_trading_bound TEXT,
        regular_trading_hours TEXT,
        PRIMARY KEY (market_id, discovered_at_ms))""",
}


def _is_lock_error(exc: sqlite3.OperationalError) -> bool:
    """Return whether *exc* is one of SQLite's retryable lock errors."""
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int):
        primary_code = code & 0xFF
        return primary_code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
    message = str(exc).lower()
    return "busy" in message or "locked" in message


class MarketHistoryStore:
    def _configure_wal(self, busy_timeout_ms: int) -> None:
        """Enable WAL, retrying only transient first-open lock failures.

        SQLite's WAL mode transition is a database-wide operation.  On a
        brand-new path several processes can reach it at the same time before
        any schema transaction exists.  The explicit retry loop keeps that
        initialization race bounded by the existing ten-second policy while
        leaving the normal connection busy timeout in place after WAL is set.
        """
        timeout_ms = max(0, int(busy_timeout_ms))
        retry_budget_s = min(timeout_ms, DEFAULT_BUSY_TIMEOUT_MS) / 1000.0
        deadline = time.monotonic() + retry_budget_s
        # PRAGMA journal_mode=WAL may otherwise use a separate SQLite busy
        # handler whose wait is not accounted for by our explicit deadline.
        self._conn.execute("PRAGMA busy_timeout=0")
        delay_s = 0.01
        while True:
            try:
                self._conn.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as exc:
                if not _is_lock_error(exc):
                    raise
                remaining_s = deadline - time.monotonic()
                if remaining_s <= 0:
                    raise
                time.sleep(min(delay_s, remaining_s))
                delay_s = min(delay_s * 2.0, 0.25)
        self._conn.execute(f"PRAGMA busy_timeout={timeout_ms}")

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
                 max_pending_rows_per_dataset: int = MAX_PENDING_ROWS_PER_DATASET):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._max_pending = max_pending_rows_per_dataset
        self._db_lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._buffer_lock = threading.Lock()
        self._buffers = {name: [] for name in _SPECS}
        self._dropped_rows = {name: 0 for name in _SPECS}
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        try:
            self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
            self._configure_wal(busy_timeout_ms)
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            with self._db_lock:
                # Reserve the write slot before bootstrapping any schema so
                # concurrent first-open processes serialize cleanly.
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    for sql in _CREATE.values():
                        self._conn.execute(sql)
                    current = self._conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
                    if current and current[0] != SCHEMA_VERSION:
                        raise RuntimeError(f"unsupported market-history schema version: {current[0]}")
                    if current is None:
                        now = datetime.now(timezone.utc).isoformat()
                        self._conn.execute("INSERT INTO meta(key,value) VALUES('schema_version',?)", (SCHEMA_VERSION,))
                        self._conn.execute("INSERT INTO meta(key,value) VALUES('created_at_utc',?)", (now,))
                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise
        except Exception:
            self._conn.close()
            raise

    @property
    def pending_rows(self) -> dict[str, int]:
        with self._buffer_lock:
            return {name: len(rows) for name, rows in self._buffers.items()}

    @property
    def dropped_rows(self) -> dict[str, int]:
        with self._buffer_lock:
            return dict(self._dropped_rows)

    def _append(self, dataset: str, row: object) -> None:
        with self._buffer_lock:
            buf = self._buffers[dataset]
            if len(buf) >= self._max_pending:
                self._dropped_rows[dataset] += 1
                dropped = self._dropped_rows[dataset]
                if dropped == 1 or dropped % 1000 == 0:
                    logger.critical("dropped %d pending %s row(s) at buffer cap", dropped, dataset)
                return
            buf.append(row)

    def append_sample(self, row: SampleRow) -> None: self._append("samples", row)
    def append_minute(self, row: MinuteRow) -> None: self._append("minutes", row)
    def append_entropy_reference(self, row: EntropyReferenceRow) -> None: self._append("entropy_reference", row)
    def append_hedge_reference(self, row: HedgeReferenceRow) -> None: self._append("hedge_reference", row)
    def append_arcus_sample(self, row: ArcusSampleRow) -> None: self._append("arcus_samples", row)
    def append_arcus_minute(self, row: ArcusMinuteRow) -> None: self._append("arcus_minutes", row)
    def append_arcus_trade(self, row: ArcusTradeRow) -> None: self._append("arcus_trades", row)
    def append_arcus_market_attributes(self, row: ArcusMarketAttributesRow) -> None:
        self._append("arcus_market_attributes", row)
    def append_arcus_market_metadata(self, row: ArcusMarketMetadataRow) -> None:
        self._append("arcus_market_metadata", row)

    def recent_premium_observations(
        self,
        symbol: str,
        hedge: str,
        start_ms: int,
        end_ms: int,
    ) -> list[tuple[float, float]]:
        """Read a bounded, causal premium window for center bootstrap.

        Samples are the primary source because they contain the exact
        midpoint-to-midpoint premium used by the live strategy.  Minute means
        are a compatibility fallback for databases that have minute history
        but no sample rows in the requested window.  ``end_ms`` is exclusive.
        """
        if end_ms <= start_ms:
            return []
        with self._db_lock:
            rows = self._conn.execute(
                "SELECT timestamp_ms, premium_bps FROM arcus_samples "
                "WHERE symbol=? AND hedge=? AND timestamp_ms>=? "
                "AND timestamp_ms<? ORDER BY timestamp_ms",
                (symbol, hedge, int(start_ms), int(end_ms)),
            ).fetchall()
            if not rows:
                rows = self._conn.execute(
                    "SELECT timestamp_ms, premium_bps FROM samples "
                "WHERE symbol=? AND hedge=? AND timestamp_ms>=? "
                "AND timestamp_ms<? ORDER BY timestamp_ms",
                (symbol, hedge, int(start_ms), int(end_ms)),
                ).fetchall()
            if not rows:
                rows = self._conn.execute(
                    "SELECT minute_ts, premium_mean_bps FROM arcus_minutes "
                    "WHERE symbol=? AND hedge=? AND minute_ts*1000>=? "
                    "AND minute_ts*1000<? ORDER BY minute_ts",
                    (symbol, hedge, int(start_ms), int(end_ms)),
                ).fetchall()
            if not rows:
                rows = self._conn.execute(
                    "SELECT minute_ts, premium_mean_bps FROM minutes "
                    "WHERE symbol=? AND hedge=? AND minute_ts*1000>=? "
                    "AND minute_ts*1000<? ORDER BY minute_ts",
                    (symbol, hedge, int(start_ms), int(end_ms)),
                ).fetchall()
        return [
            (float(timestamp) / 1000.0, float(value))
            for timestamp, value in rows
            if value is not None and math.isfinite(float(value))
        ]

    def count_rows(self, dataset: str) -> int:
        """Return a committed row count for smoke tests and the dashboard."""
        if dataset not in _SPECS:
            raise ValueError(f"unknown dataset: {dataset}")
        with self._db_lock:
            return int(self._conn.execute(
                f"SELECT COUNT(*) FROM {dataset}"
            ).fetchone()[0])

    def _write(self, dataset: str, rows: Sequence[object]) -> InsertCounts:
        _, keys, fields = _SPECS[dataset]
        quoted = ",".join(fields)
        placeholders = ",".join("?" for _ in fields)
        sql = f"INSERT INTO {dataset} ({quoted}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        counts = [0, 0, 0]
        for row in rows:
            values = tuple(getattr(row, field) for field in fields)
            cur = self._conn.execute(sql, values)
            if cur.rowcount:
                counts[0] += 1
                continue
            where = " AND ".join(f"{key}=?" for key in keys)
            existing = self._conn.execute(f"SELECT {quoted} FROM {dataset} WHERE {where}", tuple(getattr(row, key) for key in keys)).fetchone()
            if existing == values:
                counts[1] += 1
            else:
                counts[2] += 1
                logger.error("conflicting %s row for key %s", dataset, tuple(getattr(row, key) for key in keys))
        return InsertCounts(*counts)

    def flush(self) -> FlushReport:
        # Serialize flushes, but never hold the buffer lock over SQLite I/O.
        with self._flush_lock:
            with self._buffer_lock:
                snapshot = {name: tuple(rows) for name, rows in self._buffers.items() if rows}
            if not snapshot:
                return FlushReport(True, {})
            try:
                with self._db_lock:
                    self._conn.execute("BEGIN")
                    try:
                        results = {name: self._write(name, rows) for name, rows in snapshot.items()}
                        self._conn.commit()
                    except Exception:
                        self._conn.rollback()
                        raise
                with self._buffer_lock:
                    for name, rows in snapshot.items():
                        current = self._buffers[name]
                        if tuple(current[:len(rows)]) != rows:
                            raise RuntimeError(f"pending {name} prefix changed during flush")
                    for name, rows in snapshot.items():
                        del self._buffers[name][:len(rows)]
                return FlushReport(True, results)
            except sqlite3.Error:
                logger.exception("market-history flush failed")
                return FlushReport(False, {})
            except Exception:
                logger.exception("market-history flush committed but buffer removal failed")
                return FlushReport(False, {})

    def import_rows(self, dataset: str, rows: Sequence[object]) -> InsertCounts:
        if dataset not in _SPECS:
            raise ValueError(f"unknown dataset: {dataset}")
        with self._db_lock:
            self._conn.execute("BEGIN")
            try:
                result = self._write(dataset, rows)
                self._conn.commit()
                return result
            except Exception:
                self._conn.rollback()
                raise

    def set_meta(self, key: str, value: str) -> None:
        with self._db_lock:
            self._conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
            self._conn.commit()

    def close(self) -> None:
        report = self.flush()
        if not report.ok:
            logger.error("market-history final flush failed")
        with self._db_lock:
            self._conn.close()
