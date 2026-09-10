import multiprocessing as mp
import sqlite3
import threading
from pathlib import Path

import pytest

from entropy_arb.storage import (
    EntropyReferenceRow,
    HedgeReferenceRow,
    InsertCounts,
    MarketHistoryStore,
    MinuteRow,
    SampleRow,
)


def sample(ts: int = 1_700_000_000_000, premium: float = 10.0) -> SampleRow:
    return SampleRow(
        timestamp_ms=ts,
        symbol="SNDK",
        hedge="lighter-rh",
        premium_bps=premium,
        sell_edge_bps=8.0,
        buy_edge_bps=-12.0,
        entropy_bid=100.09,
        entropy_ask=100.11,
        hedge_bid=99.99,
        hedge_ask=100.01,
        entropy_book_update_ms=ts - 500,
        hedge_book_update_ms=ts - 300,
    )


def minute(ts: int = 1_699_999_980) -> MinuteRow:
    return MinuteRow(
        minute_ts=ts,
        symbol="SNDK",
        hedge="lighter-rh",
        entropy_bid=100.09,
        entropy_ask=100.11,
        hedge_bid=99.99,
        hedge_ask=100.01,
        premium_open_bps=10.0,
        premium_high_bps=20.0,
        premium_low_bps=10.0,
        premium_close_bps=20.0,
        premium_mean_bps=15.0,
        premium_std_bps=5.0,
        sell_edge_mean_bps=13.0,
        sell_edge_max_bps=18.0,
        buy_edge_mean_bps=-17.0,
        buy_edge_max_bps=-12.0,
        samples=2,
    )


def test_fresh_database_creates_schema_and_meta(tmp_path: Path):
    db = tmp_path / "market-history.sqlite"
    store = MarketHistoryStore(db)
    store.close()

    with sqlite3.connect(db) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {
            "meta",
            "samples",
            "minutes",
            "entropy_reference",
            "hedge_reference",
        } <= tables
        assert conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone() == ("1",)
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2


def test_second_open_is_idempotent(tmp_path: Path):
    db = tmp_path / "market-history.sqlite"
    MarketHistoryStore(db).close()
    MarketHistoryStore(db).close()
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone() == ("ok",)


def test_recent_premium_observations_are_bounded_and_ordered(tmp_path: Path):
    db = tmp_path / "market-history.sqlite"
    store = MarketHistoryStore(db)
    store.append_sample(sample(ts=10_000, premium=1.0))
    store.append_sample(sample(ts=20_000, premium=2.0))
    store.append_sample(sample(ts=30_000, premium=3.0))
    assert store.flush().ok

    observations = store.recent_premium_observations(
        "SNDK", "lighter-rh", start_ms=15_000, end_ms=30_000
    )

    assert observations == [(20.0, 2.0)]
    store.close()


def test_recent_premium_observations_fall_back_to_minute_means(tmp_path: Path):
    db = tmp_path / "market-history.sqlite"
    store = MarketHistoryStore(db)
    store.append_minute(minute(ts=20))
    assert store.flush().ok

    observations = store.recent_premium_observations(
        "SNDK", "lighter-rh", start_ms=19_000, end_ms=21_000
    )

    assert observations == [(0.02, 15.0)]
    store.close()


def test_exact_duplicate_is_noop_and_conflicting_payload_does_not_replace(
    tmp_path: Path,
):
    db = tmp_path / "market-history.sqlite"
    store = MarketHistoryStore(db)
    store.append_sample(sample())
    first = store.flush()
    assert first.ok
    assert first.datasets["samples"].inserted == 1

    store.append_sample(sample())
    duplicate = store.flush()
    assert duplicate.datasets["samples"].duplicates == 1

    store.append_sample(sample(premium=999.0))
    conflict = store.flush()
    assert conflict.datasets["samples"].conflicts == 1
    store.close()

    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT premium_bps FROM samples WHERE symbol=? AND hedge=? AND timestamp_ms=?",
            ("SNDK", "lighter-rh", 1_700_000_000_000),
        ).fetchone() == (10.0,)


def test_flush_transaction_rolls_back_all_datasets_and_keeps_buffers(tmp_path: Path):
    db = tmp_path / "market-history.sqlite"
    store = MarketHistoryStore(db)
    store._conn.execute(
        "CREATE TRIGGER fail_minutes BEFORE INSERT ON minutes "
        "BEGIN SELECT RAISE(ABORT, 'forced failure'); END"
    )
    store.append_sample(sample())
    store.append_minute(minute())

    report = store.flush()
    assert not report.ok
    assert store.pending_rows["samples"] == 1
    assert store.pending_rows["minutes"] == 1

    store._conn.execute("DROP TRIGGER fail_minutes")
    assert store.flush().ok
    store.close()

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM samples").fetchone() == (1,)
        assert conn.execute("SELECT COUNT(*) FROM minutes").fetchone() == (1,)


def test_reference_rows_write_with_full_payload_primary_keys(tmp_path: Path):
    store = MarketHistoryStore(tmp_path / "history.sqlite")
    entropy = EntropyReferenceRow("SNDK", "lighter-rh", 10, 100.0, 100.1)
    hedge = HedgeReferenceRow("SNDK", "lighter-rh", 10, 11, 99.9, 100.0)
    assert store.import_rows("entropy_reference", [entropy, entropy]) == InsertCounts(
        inserted=1, duplicates=1, conflicts=0
    )
    assert store.import_rows("hedge_reference", [hedge]).inserted == 1
    store.close()
    with sqlite3.connect(tmp_path / "history.sqlite") as conn:
        assert conn.execute("PRAGMA table_info(entropy_reference)").fetchall()
        assert conn.execute("SELECT COUNT(*) FROM hedge_reference").fetchone() == (1,)


def test_import_meta_cap_pragmas_and_unknown_dataset(tmp_path: Path, caplog):
    store = MarketHistoryStore(
        tmp_path / "history.sqlite", max_pending_rows_per_dataset=1
    )
    assert store._conn.execute("PRAGMA busy_timeout").fetchone() == (10000,)
    assert store._conn.execute("PRAGMA foreign_keys").fetchone() == (1,)
    store.set_meta("owner", "research")
    store.set_meta("owner", "updated")
    assert store._conn.execute(
        "SELECT value FROM meta WHERE key='owner'"
    ).fetchone() == ("updated",)
    store.append_sample(sample())
    with caplog.at_level("CRITICAL"):
        store.append_sample(sample(ts=2))
    assert store.pending_rows["samples"] == 1
    assert store.dropped_rows["samples"] == 1
    assert "dropped 1" in caplog.text
    with pytest.raises(ValueError):
        store.import_rows("unknown", [])
    store._conn.execute(
        "CREATE TRIGGER fail_import BEFORE INSERT ON samples "
        "BEGIN SELECT RAISE(ABORT, 'forced import failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError):
        store.import_rows("samples", [sample(ts=3)])
    store._conn.execute("DROP TRIGGER fail_import")
    store.close()


def test_flush_serializes_append_until_prefix_removal(tmp_path: Path):
    store = MarketHistoryStore(tmp_path / "history.sqlite")
    store.append_sample(sample())
    entered = threading.Event()
    release = threading.Event()
    original = store._write

    def blocked(dataset, rows):
        entered.set()
        release.wait(timeout=2)
        return original(dataset, rows)

    store._write = blocked  # type: ignore[method-assign]
    thread = threading.Thread(target=store.flush)
    thread.start()
    assert entered.wait(timeout=2)
    append_thread = threading.Thread(target=lambda: store.append_sample(sample(ts=2)))
    append_thread.start()
    append_thread.join(timeout=1)
    assert not append_thread.is_alive()
    release.set()
    thread.join(timeout=2)
    append_thread.join(timeout=2)
    assert store.pending_rows["samples"] == 1
    assert store.flush().datasets["samples"].inserted == 1
    store.close()


def _write_process(db_path: str, symbol: str, start_ms: int, count: int) -> None:
    store = MarketHistoryStore(db_path)
    for i in range(count):
        row = sample(ts=start_ms + i)
        row = SampleRow(**{**row.__dict__, "symbol": symbol})
        store.append_sample(row)
    report = store.flush()
    if not report.ok:
        raise RuntimeError("child flush failed")
    store.close()


def _fresh_database_process(
    db_path: str,
    barrier,
    result_queue,
    worker_id: int,
) -> None:
    """Race first-open schema initialization in an independent process."""
    try:
        barrier.wait(timeout=20)
        store = MarketHistoryStore(db_path)
        store.append_sample(
            SampleRow(
                **{
                    **sample(ts=1_700_100_000_000 + worker_id).__dict__,
                    "symbol": f"W{worker_id}",
                }
            )
        )
        report = store.flush()
        if not report.ok:
            raise RuntimeError("fresh-database flush failed")
        store.close()
    except BaseException as exc:
        result_queue.put((worker_id, type(exc).__name__, str(exc)))
        raise
    else:
        result_queue.put((worker_id, "ok", ""))


def test_fresh_database_concurrent_initialization_is_serialized(tmp_path: Path):
    """Independent processes can initialize and write a brand-new database."""
    db = tmp_path / "fresh-market-history.sqlite"
    ctx = mp.get_context("spawn")
    workers = 8
    barrier = ctx.Barrier(workers)
    result_queue = ctx.Queue()
    processes = [
        ctx.Process(
            target=_fresh_database_process,
            args=(str(db), barrier, result_queue, worker_id),
        )
        for worker_id in range(workers)
    ]
    for process in processes:
        process.start()
    results = []
    try:
        for process in processes:
            process.join(30)
            if process.is_alive():
                process.terminate()
                process.join(5)
        for _ in range(workers):
            results.append(result_queue.get(timeout=5))
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(5)

    assert all(process.exitcode == 0 for process in processes), results
    assert len(results) == workers, results
    assert all(result[1] == "ok" for result in results), results
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone() == ("ok",)
        assert conn.execute("SELECT COUNT(*) FROM samples").fetchone() == (workers,)


def test_two_real_processes_write_same_wal_database(tmp_path: Path):
    db = tmp_path / "market-history.sqlite"
    MarketHistoryStore(db).close()
    ctx = mp.get_context("spawn")
    p1 = ctx.Process(
        target=_write_process, args=(str(db), "SNDK", 1_700_000_000_000, 200)
    )
    p2 = ctx.Process(
        target=_write_process, args=(str(db), "ANTH", 1_700_001_000_000, 200)
    )
    p1.start()
    p2.start()
    p1.join(20)
    p2.join(20)
    assert p1.exitcode == 0
    assert p2.exitcode == 0

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM samples").fetchone() == (400,)
        assert conn.execute("PRAGMA quick_check").fetchone() == ("ok",)


def test_busy_flush_keeps_pending_rows_for_retry(tmp_path: Path):
    db = tmp_path / "market-history.sqlite"
    store = MarketHistoryStore(db, busy_timeout_ms=50)
    blocker = sqlite3.connect(db, timeout=0.05)
    blocker.execute("PRAGMA journal_mode=WAL")
    blocker.execute("BEGIN IMMEDIATE")
    store.append_sample(sample())

    report = store.flush()
    assert not report.ok
    assert store.pending_rows["samples"] == 1

    blocker.rollback()
    blocker.close()
    retry = store.flush()
    assert retry.ok
    assert retry.datasets["samples"].inserted == 1
    store.close()


def test_pending_buffer_cap_counts_drops_without_evicting_old_rows(tmp_path: Path):
    db = tmp_path / "market-history.sqlite"
    store = MarketHistoryStore(db, max_pending_rows_per_dataset=2)
    store.append_sample(sample(ts=1))
    store.append_sample(sample(ts=2))
    store.append_sample(sample(ts=3))
    assert store.pending_rows["samples"] == 2
    assert store.dropped_rows["samples"] == 1
    assert store.flush().ok
    store.close()
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT timestamp_ms FROM samples ORDER BY timestamp_ms"
        ).fetchall() == [(1,), (2,)]
