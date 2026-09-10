"""Minute recorder: aggregation and SQLite persistence.

Run:  python3 -m pytest tests/  (or  python3 tests/test_recorder.py)
"""

import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.recorder import MinuteRecorder  # noqa: E402
from entropy_arb.storage import MarketHistoryStore  # noqa: E402


def set_book(book, bid, ask):
    book.apply_hl([[{"px": str(bid), "sz": "10"}], [{"px": str(ask), "sz": "10"}]])


def rows(database, table):
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()


def test_minute_aggregation_and_rollover():
    e_book, h_book = OrderBook(), OrderBook()
    database = os.path.join(tempfile.mkdtemp(), "history.sqlite")
    store = MarketHistoryStore(database)
    rec = MinuteRecorder(
        store, e_book, h_book, staleness_sec=1e9, symbol="SNDK", hedge="lighter-rh"
    )

    t0 = 1_700_000_000.0  # 20s into a minute (boundary at ...020)
    # minute 1: entropy 10 bps rich, then 20 bps rich
    set_book(e_book, 100.09, 100.11)  # mid 100.10
    set_book(h_book, 99.99, 100.01)  # mid 100.00
    rec.sample(t0)
    set_book(e_book, 100.19, 100.21)  # mid 100.20
    rec.sample(t0 + 10)
    # next minute: back to 10 bps rich -> flushes minute 1
    set_book(e_book, 100.09, 100.11)
    rec.sample(t0 + 45)
    rec.close()  # flushes the partial minute 2

    store.flush()
    persisted = rows(database, "minutes")
    assert len(persisted) == 2
    m1, m2 = persisted
    assert (m1["symbol"], m1["hedge"]) == ("SNDK", "lighter-rh")
    assert (m2["symbol"], m2["hedge"]) == ("SNDK", "lighter-rh")
    assert m1["samples"] == 2 and m2["samples"] == 1
    assert abs(m1["premium_open_bps"] - 10.0) < 0.2
    assert abs(m1["premium_high_bps"] - 20.0) < 0.2
    assert abs(m1["premium_close_bps"] - 20.0) < 0.2
    assert abs(m1["premium_mean_bps"] - 15.0) < 0.2
    # executable edges: sell = bid_e/ask_h - 1, buy = bid_h/ask_e - 1
    assert abs(m2["sell_edge_max_bps"] - ((100.09 / 100.01 - 1) * 1e4)) < 0.05
    assert abs(m2["buy_edge_max_bps"] - ((99.99 / 100.11 - 1) * 1e4)) < 0.05
    # closes carry the last books
    assert m2["entropy_bid"] == 100.09
    assert m2["hedge_ask"] == 100.01
    store.close()


def test_sample_rows_match_minute_bbo_math_and_timestamp():
    e_book, h_book = OrderBook(), OrderBook()
    directory = tempfile.mkdtemp()
    database = os.path.join(directory, "history.sqlite")
    store = MarketHistoryStore(database)
    rec = MinuteRecorder(
        store, e_book, h_book, staleness_sec=1e9, symbol="SNDK", hedge="lighter-rh"
    )

    t0 = 1_700_000_020.123
    set_book(e_book, 100.09, 100.11)
    set_book(h_book, 99.99, 100.01)
    e_book.last_update_ts = 1_700_000_019.456
    h_book.last_update_ts = 1_700_000_019.789
    rec.sample(t0)
    set_book(e_book, 100.19, 100.21)
    e_book.last_update_ts = 1_700_000_020.987
    rec.sample(t0 + 1.0)
    rec.close()
    store.flush()
    sample_rows = rows(database, "samples")
    minute_rows = rows(database, "minutes")
    assert len(sample_rows) == 2
    assert minute_rows[0]["samples"] == len(sample_rows)
    assert [r["timestamp_ms"] for r in sample_rows] == [
        int(t0 * 1000),
        int((t0 + 1.0) * 1000),
    ]
    assert (sample_rows[0]["timestamp_ms"] // 60_000) * 60 == minute_rows[0][
        "minute_ts"
    ]

    first = sample_rows[0]
    assert first["premium_bps"] == (
        (((100.09 + 100.11) / 2) / ((99.99 + 100.01) / 2) - 1) * 1e4
    )
    assert first["sell_edge_bps"] == ((100.09 / 100.01) - 1) * 1e4
    assert first["buy_edge_bps"] == ((99.99 / 100.11) - 1) * 1e4
    assert first["entropy_bid"] == 100.09
    assert first["entropy_ask"] == 100.11
    assert first["hedge_bid"] == 99.99
    assert first["hedge_ask"] == 100.01
    assert first["entropy_book_update_ms"] == 1_700_000_019_456
    assert first["hedge_book_update_ms"] == 1_700_000_019_789
    store.close()


def test_samples_share_one_store_across_hedges():
    e_book, h_book = OrderBook(), OrderBook()
    directory = tempfile.mkdtemp()
    store = MarketHistoryStore(os.path.join(directory, "history.sqlite"))
    rec = MinuteRecorder(
        store, e_book, h_book, staleness_sec=1e9, symbol="SNDK", hedge="lighter"
    )
    set_book(e_book, 100.0, 100.02)
    set_book(h_book, 100.0, 100.02)
    rec.sample(1_700_000_000.0)
    rec.close()

    store.flush()
    assert len(rows(store.path, "samples")) == 1
    store.close()


def test_no_csv_is_created_when_sampling():
    e_book, h_book = OrderBook(), OrderBook()
    directory = tempfile.mkdtemp()
    legacy_path = os.path.join(directory, "samples-SNDK-lighter.csv")
    legacy_content = b"legacy-header\nlegacy-row\n"
    with open(legacy_path, "wb") as fh:
        fh.write(legacy_content)
    legacy_before = os.stat(legacy_path)

    store = MarketHistoryStore(os.path.join(directory, "history.sqlite"))
    rec = MinuteRecorder(
        store, e_book, h_book, staleness_sec=1e9, symbol="SNDK", hedge="lighter"
    )
    set_book(e_book, 100.0, 100.02)
    set_book(h_book, 100.0, 100.02)
    rec.sample(1_700_000_000.0)
    rec.close()

    legacy_after = os.stat(legacy_path)
    with open(legacy_path, "rb") as fh:
        assert fh.read() == legacy_content
    assert legacy_after.st_size == legacy_before.st_size
    assert legacy_after.st_mtime_ns == legacy_before.st_mtime_ns
    assert not os.path.exists(os.path.join(directory, "minutes-SNDK-lighter.csv"))
    store.close()


def test_samples_remain_buffered_until_store_flush():
    e_book, h_book = OrderBook(), OrderBook()
    directory = tempfile.mkdtemp()
    database = os.path.join(directory, "history.sqlite")
    store = MarketHistoryStore(database)
    rec = MinuteRecorder(
        store, e_book, h_book, staleness_sec=1e9, symbol="SNDK", hedge="lighter"
    )
    set_book(e_book, 100.0, 100.02)
    set_book(h_book, 100.0, 100.02)

    for offset in range(12):
        rec.sample(1_700_000_000.0 + offset)
    assert store.pending_rows["samples"] == 12
    store.flush()
    assert len(rows(database, "samples")) == 12
    rec.close()
    store.close()


def test_stale_books_are_skipped():
    e_book, h_book = OrderBook(), OrderBook()
    database = os.path.join(tempfile.mkdtemp(), "history.sqlite")
    store = MarketHistoryStore(database)
    rec = MinuteRecorder(
        store, e_book, h_book, staleness_sec=1e9, symbol="SNDK", hedge="lighter-rh"
    )
    rec.sample(1_700_000_000.0)  # both books empty -> nothing recorded
    set_book(e_book, 100.0, 100.02)  # only one side fresh
    rec.sample(1_700_000_001.0)
    rec.close()
    assert rec.rows_written == 0
    store.flush()
    assert rows(database, "samples") == []
    assert rows(database, "minutes") == []
    store.close()


def test_two_recorders_append_to_one_store():
    e_book, h_book = OrderBook(), OrderBook()
    database = os.path.join(tempfile.mkdtemp(), "history.sqlite")
    store = MarketHistoryStore(database)
    set_book(e_book, 100.0, 100.02)
    set_book(h_book, 100.0, 100.02)
    for start in (1_700_000_000.0, 1_700_000_060.0):
        rec = MinuteRecorder(
            store, e_book, h_book, staleness_sec=1e9, symbol="SNDK", hedge="lighter-rh"
        )
        rec.sample(start)
        rec.close()
    store.flush()
    assert len(rows(database, "minutes")) == 2
    store.close()


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
