import asyncio
import copy
import csv
import inspect
import json
import logging
import sqlite3

import pytest

from entropy_arb.reference import (
    ENTROPY_REFERENCE_HEADER,
    LIGHTER_REFERENCE_HEADER,
    EntropyReferenceStoreWriter,
    HedgeReferenceStoreWriter,
    HLReferenceFeed,
    LighterReferenceFeed,
    ReferenceParseError,
    ReferenceRecorder,
    parse_hl_reference,
    parse_lighter_reference,
    reference_paths,
)
from entropy_arb.storage import MarketHistoryStore


class StubReferenceWriter:
    def __init__(self):
        self.enabled = True
        self.rows = []

    def write(self, row):
        if self.enabled:
            self.rows.append(row)


class FakeWebSocket:
    def __init__(self, frames, stop):
        self.frames = list(frames)
        self.stop = stop
        self.sent = []
        self.decode_sent = json.loads

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.frames:
            return self.frames.pop(0)
        self.stop.set()
        raise StopAsyncIteration

    async def send(self, raw):
        self.sent.append(self.decode_sent(raw))


class FakeConnect:
    def __init__(self, websocket):
        self.websocket = websocket
        self.calls = []

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.websocket


HL_REFERENCE_FRAME = {
    "channel": "activeAssetCtx",
    "data": {
        "coin": "io:SNDK",
        "ctx": {
            "oraclePx": "1485.0",
            "markPx": "1485.0",
            "midPx": "1483.3",
            "funding": "0.0000015625",
        },
    },
}

LIGHTER_REFERENCE_FRAME = {
    "channel": "market_stats:139",
    "market_stats": {
        "market_id": 139,
        "symbol": "SNDK",
        "index_price": "1488.07",
        "mark_price": "1483.77",
        "mid_price": "1483.83",
        "best_bid_price": "1483.74",
        "best_ask_price": "1483.92",
        "funding_rate": "0.0004",
    },
    "timestamp": 1_787_993_704_054,
    "type": "subscribed/market_stats",
}


def test_hl_feed_subscribes_and_persists_identical_frames():
    async def scenario():
        stop = asyncio.Event()
        frames = [
            json.dumps(HL_REFERENCE_FRAME),
            json.dumps(HL_REFERENCE_FRAME),
        ]
        ws = FakeWebSocket(frames, stop)
        writer = StubReferenceWriter()
        feed = HLReferenceFeed(
            "ENTROPY",
            "wss://api.hyperliquid.xyz/ws",
            "io:SNDK",
            writer,
            connect=FakeConnect(ws),
            clock_ns=lambda: 1_700_000_000_123_456_789,
        )
        await feed.run(stop)
        assert ws.sent[0] == {
            "method": "subscribe",
            "subscription": {
                "type": "activeAssetCtx",
                "coin": "io:SNDK",
            },
        }
        assert writer.rows == [
            (1_700_000_000_123, 1485.0, 1485.0),
            (1_700_000_000_123, 1485.0, 1485.0),
        ]

    asyncio.run(scenario())


def test_lighter_feed_subscribes_after_connected_frame():
    async def scenario():
        stop = asyncio.Event()
        ws = FakeWebSocket(
            [
                json.dumps({"type": "connected"}),
                json.dumps(LIGHTER_REFERENCE_FRAME),
            ],
            stop,
        )
        writer = StubReferenceWriter()
        feed = LighterReferenceFeed(
            "LIGHTER",
            "wss://mainnet.zklighter.elliot.ai/stream",
            139,
            writer,
            connect=FakeConnect(ws),
            clock_ns=lambda: 1_700_000_000_999_000_000,
        )
        await feed.run(stop)
        assert ws.sent[0] == {
            "type": "subscribe",
            "channel": "market_stats/139",
        }
        assert writer.rows == [
            (
                1_700_000_000_999,
                1_787_993_704_054,
                1488.07,
                1483.77,
            ),
        ]

    asyncio.run(scenario())


def test_recv_ms_is_captured_before_json_and_parser(monkeypatch):
    async def scenario():
        from entropy_arb import reference

        stop = asyncio.Event()
        ws = FakeWebSocket([json.dumps(HL_REFERENCE_FRAME)], stop)
        writer = StubReferenceWriter()
        calls = []
        real_loads = json.loads
        real_parser = reference.parse_hl_reference

        def clock_ns():
            calls.append("clock")
            return 1_700_000_000_123_000_000

        def tracked_loads(raw):
            calls.append("json")
            return real_loads(raw)

        def tracked_parser(msg, *, coin):
            calls.append("parser")
            return real_parser(msg, coin=coin)

        monkeypatch.setattr(reference.json, "loads", tracked_loads)
        monkeypatch.setattr(reference, "parse_hl_reference", tracked_parser)
        original_write = writer.write

        def tracked_write(row):
            calls.append("write")
            original_write(row)

        writer.write = tracked_write
        feed = HLReferenceFeed(
            "ENTROPY",
            "wss://example.invalid/ws",
            "io:SNDK",
            writer,
            connect=FakeConnect(ws),
            clock_ns=clock_ns,
        )
        await feed.run(stop)
        assert calls[:4] == ["clock", "json", "parser", "write"]

    asyncio.run(scenario())


def test_reference_feeds_have_no_trading_notify_dependency():
    assert "notify" not in inspect.signature(HLReferenceFeed).parameters
    assert "notify" not in inspect.signature(LighterReferenceFeed).parameters
    assert "book" not in inspect.signature(HLReferenceFeed).parameters
    assert "book" not in inspect.signature(LighterReferenceFeed).parameters


def test_malformed_relevant_frame_is_skipped_without_ending_loop(caplog):
    async def scenario():
        stop = asyncio.Event()
        invalid = copy.deepcopy(HL_REFERENCE_FRAME)
        invalid["data"]["ctx"]["oraclePx"] = None
        ws = FakeWebSocket(
            [json.dumps(invalid), json.dumps(HL_REFERENCE_FRAME)],
            stop,
        )
        writer = StubReferenceWriter()
        feed = HLReferenceFeed(
            "ENTROPY",
            "wss://example.invalid/ws",
            "io:SNDK",
            writer,
            connect=FakeConnect(ws),
        )
        await feed.run(stop)
        assert len(writer.rows) == 1

    with caplog.at_level(logging.WARNING, logger="reference"):
        asyncio.run(scenario())
    assert "malformed relevant reference frame" in caplog.text


def test_parser_failure_is_skipped_without_reconnecting(monkeypatch, caplog):
    async def scenario():
        from entropy_arb import reference

        stop = asyncio.Event()
        ws = FakeWebSocket(
            [json.dumps(HL_REFERENCE_FRAME), json.dumps(HL_REFERENCE_FRAME)],
            stop,
        )
        connector = FakeConnect(ws)
        writer = StubReferenceWriter()
        real_parser = reference.parse_hl_reference
        calls = 0

        def flaky_parser(msg, *, coin):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("parser unavailable")
            return real_parser(msg, coin=coin)

        monkeypatch.setattr(reference, "parse_hl_reference", flaky_parser)
        feed = HLReferenceFeed(
            "ENTROPY",
            "wss://example.invalid/ws",
            "io:SNDK",
            writer,
            connect=connector,
            sleep=lambda _: asyncio.sleep(0),
        )
        await feed.run(stop)
        assert connector.calls == [
            (
                "wss://example.invalid/ws",
                {
                    "max_size": 2**23,
                    "open_timeout": 10,
                    "ping_interval": 15,
                    "ping_timeout": 15,
                },
            )
        ]
        assert len(writer.rows) == 1

    with caplog.at_level(logging.WARNING, logger="reference"):
        asyncio.run(scenario())
    assert "malformed relevant reference frame" in caplog.text


@pytest.mark.parametrize("raw", [json.dumps([]), json.dumps("noise")])
def test_hl_non_object_frames_are_silent_and_connection_continues(raw, caplog):
    async def scenario():
        stop = asyncio.Event()
        ws = FakeWebSocket([raw, json.dumps(HL_REFERENCE_FRAME)], stop)
        connector = FakeConnect(ws)
        writer = StubReferenceWriter()
        feed = HLReferenceFeed(
            "ENTROPY",
            "wss://example.invalid/ws",
            "io:SNDK",
            writer,
            connect=connector,
            sleep=lambda _: asyncio.sleep(0),
        )
        await feed.run(stop)
        assert connector.calls == [
            (
                "wss://example.invalid/ws",
                {
                    "max_size": 2**23,
                    "open_timeout": 10,
                    "ping_interval": 15,
                    "ping_timeout": 15,
                },
            )
        ]
        assert len(writer.rows) == 1

    with caplog.at_level(logging.WARNING, logger="reference"):
        asyncio.run(scenario())
    assert caplog.records == []


@pytest.mark.parametrize("raw", [json.dumps([]), json.dumps("noise")])
def test_lighter_non_object_frames_are_silent_and_connection_continues(raw, caplog):
    async def scenario():
        stop = asyncio.Event()
        ws = FakeWebSocket(
            [
                raw,
                json.dumps({"type": "connected"}),
                json.dumps(LIGHTER_REFERENCE_FRAME),
            ],
            stop,
        )
        connector = FakeConnect(ws)
        writer = StubReferenceWriter()
        feed = LighterReferenceFeed(
            "LIGHTER",
            "wss://example.invalid/ws",
            139,
            writer,
            connect=connector,
        )
        await feed.run(stop)
        assert len(connector.calls) == 1
        assert len(writer.rows) == 1

    with caplog.at_level(logging.WARNING, logger="reference"):
        asyncio.run(scenario())
    assert caplog.records == []


def test_irrelevant_frames_do_not_write(caplog):
    async def scenario():
        stop = asyncio.Event()
        ws = FakeWebSocket(
            [
                json.dumps({"channel": "pong"}),
                json.dumps(
                    {
                        **LIGHTER_REFERENCE_FRAME,
                        "channel": "market_stats:32",
                        "market_stats": {
                            **LIGHTER_REFERENCE_FRAME["market_stats"],
                            "market_id": 32,
                        },
                    }
                ),
            ],
            stop,
        )
        writer = StubReferenceWriter()
        feed = LighterReferenceFeed(
            "LIGHTER",
            "wss://example.invalid/ws",
            139,
            writer,
            connect=FakeConnect(ws),
        )
        await feed.run(stop)
        assert writer.rows == []

    with caplog.at_level(logging.WARNING, logger="reference"):
        asyncio.run(scenario())
    assert caplog.records == []


def test_reference_feed_reconnect_backoff_caps_at_thirty_seconds():
    class FailedConnection:
        async def __aenter__(self):
            raise OSError("disconnected")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class AlwaysFailConnect:
        def __call__(self, url, **kwargs):
            return FailedConnection()

    async def scenario():
        stop = asyncio.Event()
        delays = []

        async def fake_sleep(delay):
            delays.append(delay)
            if len(delays) == 7:
                stop.set()

        feed = LighterReferenceFeed(
            "LIGHTER",
            "wss://example.invalid/ws",
            139,
            StubReferenceWriter(),
            connect=AlwaysFailConnect(),
            sleep=fake_sleep,
        )
        await feed.run(stop)
        assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]

    asyncio.run(scenario())


def test_reference_feed_reconnect_backoff_resets_after_successful_connection():
    class FailedConnection:
        async def __aenter__(self):
            raise OSError("disconnected")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class OneFrameThenDrop:
        def __init__(self):
            self.sent = []
            self.yielded = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.yielded:
                self.yielded = True
                return json.dumps({"type": "connected"})
            raise OSError("dropped after connect")

        async def send(self, raw):
            self.sent.append(json.loads(raw))

    class ConnectSequence:
        def __init__(self):
            self.attempt = 0

        def __call__(self, url, **kwargs):
            self.attempt += 1
            if self.attempt == 1:
                return FailedConnection()
            return OneFrameThenDrop()

    async def scenario():
        stop = asyncio.Event()
        delays = []

        async def fake_sleep(delay):
            delays.append(delay)
            if len(delays) == 2:
                stop.set()

        feed = LighterReferenceFeed(
            "LIGHTER",
            "wss://example.invalid/ws",
            139,
            StubReferenceWriter(),
            connect=ConnectSequence(),
            sleep=fake_sleep,
        )
        await feed.run(stop)
        assert delays == [1.0, 1.0]

    asyncio.run(scenario())


def test_parse_hl_reference_from_probe_shape():
    assert parse_hl_reference(HL_REFERENCE_FRAME, coin="io:SNDK") == (1485.0, 1485.0)


@pytest.mark.parametrize(
    "message_type",
    ["subscribed/market_stats", "update/market_stats"],
)
@pytest.mark.parametrize("market_id", [139, 32])
def test_parse_lighter_reference_for_mainnet_and_rh(message_type, market_id):
    msg = {
        **LIGHTER_REFERENCE_FRAME,
        "channel": f"market_stats:{market_id}",
        "market_stats": {
            **LIGHTER_REFERENCE_FRAME["market_stats"],
            "market_id": market_id,
        },
        "type": message_type,
    }
    assert parse_lighter_reference(msg, market_id=market_id) == (
        1_787_993_704_054,
        1488.07,
        1483.77,
    )


def test_lighter_positive_integer_timestamp_has_no_wall_clock_gate():
    msg = {**LIGHTER_REFERENCE_FRAME, "timestamp": 1}
    assert parse_lighter_reference(msg, market_id=139)[0] == 1


def test_hl_wrong_coin_and_irrelevant_channel_return_none():
    wrong_coin = copy.deepcopy(HL_REFERENCE_FRAME)
    wrong_coin["data"]["coin"] = "io:BTC"
    assert parse_hl_reference(wrong_coin, coin="io:SNDK") is None
    assert parse_hl_reference({"channel": "pong"}, coin="io:SNDK") is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("oraclePx", None),
        ("oraclePx", 0),
        ("oraclePx", -1),
        ("oraclePx", "NaN"),
        ("markPx", "Infinity"),
    ],
)
def test_hl_relevant_invalid_price_raises(field, value):
    msg = copy.deepcopy(HL_REFERENCE_FRAME)
    msg["data"]["ctx"][field] = value
    with pytest.raises(ReferenceParseError):
        parse_hl_reference(msg, coin="io:SNDK")


def test_lighter_wrong_market_returns_none():
    assert parse_lighter_reference(LIGHTER_REFERENCE_FRAME, market_id=32) is None


def test_lighter_control_ack_without_market_payload_is_irrelevant():
    assert (
        parse_lighter_reference({"type": "subscribed/market_stats"}, market_id=139)
        is None
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timestamp", None),
        ("timestamp", 0),
        ("timestamp", -1),
        ("timestamp", 1.5),
        ("index_price", None),
        ("index_price", 0),
        ("index_price", -1),
        ("index_price", "NaN"),
        ("mark_price", "Infinity"),
    ],
)
def test_lighter_relevant_invalid_required_field_raises(field, value):
    msg = copy.deepcopy(LIGHTER_REFERENCE_FRAME)
    if field == "timestamp":
        msg[field] = value
    else:
        msg["market_stats"][field] = value
    with pytest.raises(ReferenceParseError):
        parse_lighter_reference(msg, market_id=139)


@pytest.mark.parametrize(
    ("parser", "kwargs", "path"),
    [
        (
            parse_hl_reference,
            {"coin": "io:SNDK"},
            ("data", "ctx", "oraclePx"),
        ),
        (
            parse_lighter_reference,
            {"market_id": 139},
            ("market_stats", "mark_price"),
        ),
    ],
)
def test_missing_required_field_raises(parser, kwargs, path):
    source = (
        HL_REFERENCE_FRAME if parser is parse_hl_reference else LIGHTER_REFERENCE_FRAME
    )
    msg = copy.deepcopy(source)
    parent = msg
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    with pytest.raises(ReferenceParseError):
        parser(msg, **kwargs)


def test_reference_headers_are_exact():
    assert ENTROPY_REFERENCE_HEADER == (
        "recv_ms",
        "oracle_px",
        "mark_px",
    )
    assert LIGHTER_REFERENCE_HEADER == (
        "recv_ms",
        "server_ms",
        "index_px",
        "mark_px",
    )


@pytest.mark.parametrize(
    ("symbol", "hedge_key", "expected"),
    [
        (
            "SNDK",
            "lighter",
            (
                "logs/reference-SNDK-lighter-entropy.csv",
                "logs/reference-SNDK-lighter.csv",
            ),
        ),
        (
            "SNDK",
            "lighter-rh",
            (
                "logs/reference-SNDK-lighter-rh-entropy.csv",
                "logs/reference-SNDK-lighter-rh.csv",
            ),
        ),
        (
            "ETH",
            "future-lighter-profile",
            (
                "logs/reference-ETH-future-lighter-profile-entropy.csv",
                "logs/reference-ETH-future-lighter-profile.csv",
            ),
        ),
    ],
)
def test_reference_paths_are_namespaced(symbol, hedge_key, expected):
    assert reference_paths(symbol, hedge_key) == expected


def read_rows(path):
    with open(path, newline="") as fh:
        return list(csv.reader(fh))


def test_entropy_store_writer_buffers_canonical_rows(tmp_path):
    store = MarketHistoryStore(tmp_path / "history.sqlite")
    writer = EntropyReferenceStoreWriter(store, "SNDK", "lighter")
    row = (1_787_993_704_054, 1485.0, 1485.0)
    writer.write(row)
    writer.write(row)
    writer.close()
    store.flush()
    with sqlite3.connect(store.path) as conn:
        assert conn.execute(
            "SELECT symbol, hedge, recv_ms, oracle_px, mark_px FROM entropy_reference"
        ).fetchall() == [("SNDK", "lighter", *row)]
    store.close()


def test_hedge_store_writer_buffers_canonical_rows(tmp_path):
    store = MarketHistoryStore(tmp_path / "history.sqlite")
    writer = HedgeReferenceStoreWriter(store, "SNDK", "lighter-rh")
    writer.write((1, 2, 100.0, 101.0))
    store.flush()
    with sqlite3.connect(store.path) as conn:
        assert conn.execute(
            "SELECT symbol, hedge, recv_ms, server_ms, index_px, mark_px FROM hedge_reference"
        ).fetchall() == [("SNDK", "lighter-rh", 1, 2, 100.0, 101.0)]
    store.close()


def test_stop_accepting_rejects_late_rows_but_close_flushes_prior_rows(tmp_path):
    store = MarketHistoryStore(tmp_path / "history.sqlite")
    writer = EntropyReferenceStoreWriter(store, "SNDK", "lighter")
    writer.write((1, 100.0, 101.0))
    writer.stop_accepting()
    writer.write((2, 102.0, 103.0))
    writer.close()
    store.flush()
    with sqlite3.connect(store.path) as conn:
        assert conn.execute("SELECT recv_ms FROM entropy_reference").fetchall() == [
            (1,)
        ]
    store.close()


def test_reference_recorder_shutdown_flushes_both_final_buffers(tmp_path, monkeypatch):
    async def scenario():
        from entropy_arb import reference

        feeds_started = 0
        both_started = asyncio.Event()

        class EntropyFeed:
            def __init__(self, name, ws_url, coin, writer, **kwargs):
                self.writer = writer

            async def run(self, stop):
                nonlocal feeds_started
                self.writer.write((1, 100.0, 101.0))
                feeds_started += 1
                if feeds_started == 2:
                    both_started.set()
                await stop.wait()

        class HedgeFeed:
            def __init__(self, name, ws_url, market_id, writer, **kwargs):
                self.writer = writer

            async def run(self, stop):
                nonlocal feeds_started
                self.writer.write((1, 2, 100.0, 101.0))
                feeds_started += 1
                if feeds_started == 2:
                    both_started.set()
                await stop.wait()

        monkeypatch.setattr(reference, "HLReferenceFeed", EntropyFeed)
        monkeypatch.setattr(reference, "LighterReferenceFeed", HedgeFeed)
        store = MarketHistoryStore(tmp_path / "history.sqlite")
        recorder = ReferenceRecorder(
            symbol="SNDK",
            hedge_key="lighter",
            entropy_ws_url="wss://hl.invalid/ws",
            entropy_coin="io:SNDK",
            hedge_ws_url="wss://lighter.invalid/ws",
            hedge_market_id=139,
            store=store,
        )
        stop = asyncio.Event()
        task = asyncio.create_task(recorder.run(stop))
        await both_started.wait()
        stop.set()
        await task
        store.flush()
        with sqlite3.connect(store.path) as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM entropy_reference"
            ).fetchone() == (1,)
            assert conn.execute("SELECT COUNT(*) FROM hedge_reference").fetchone() == (
                1,
            )
        store.close()

    asyncio.run(scenario())


def test_shutdown_stops_feeds_before_idempotent_writer_close(monkeypatch):
    async def scenario():
        from entropy_arb import reference

        events = []
        feed_count = 0
        feeds_started = asyncio.Event()

        class OrderedWriter:
            enabled = True

            def __init__(self, store, symbol, hedge):
                self.path = f"{symbol}-{hedge}"
                self.closed = False

            def write(self, row):
                assert not self.closed

            def stop_accepting(self):
                events.append(f"stop-accepting:{self.path}")

            def close(self):
                if not self.closed:
                    assert events.count("feed-stopped") == 2
                    events.append(f"close:{self.path}")
                    self.closed = True

        class OrderedFeed:
            def __init__(self, name, ws_url, identity, writer, **kwargs):
                self.writer = writer

            async def run(self, stop):
                nonlocal feed_count
                feed_count += 1
                if feed_count == 2:
                    feeds_started.set()
                await stop.wait()
                events.append("feed-stopped")

        monkeypatch.setattr(reference, "EntropyReferenceStoreWriter", OrderedWriter)
        monkeypatch.setattr(reference, "HedgeReferenceStoreWriter", OrderedWriter)
        monkeypatch.setattr(reference, "HLReferenceFeed", OrderedFeed)
        monkeypatch.setattr(reference, "LighterReferenceFeed", OrderedFeed)
        recorder = ReferenceRecorder(
            symbol="SNDK",
            hedge_key="lighter-rh",
            entropy_ws_url="wss://hl.invalid/ws",
            entropy_coin="io:SNDK",
            hedge_ws_url="wss://rh.invalid/ws",
            hedge_market_id=32,
            store=MarketHistoryStore(":memory:"),
        )
        stop = asyncio.Event()
        task = asyncio.create_task(recorder.run(stop))
        await feeds_started.wait()
        stop.set()
        await task
        first_close = min(
            index for index, event in enumerate(events) if event.startswith("close:")
        )
        last_feed_stop = max(
            index for index, event in enumerate(events) if event == "feed-stopped"
        )
        assert last_feed_stop < first_close
        recorder.entropy_writer.close()
        recorder.hedge_writer.close()

    asyncio.run(scenario())


def test_stuck_feed_is_cancelled_and_awaited_before_close(monkeypatch):
    async def scenario():
        from entropy_arb import reference

        events = []
        started_count = 0
        both_started = asyncio.Event()

        class OrderedWriter:
            def __init__(self, store, symbol, hedge):
                self.path = f"{symbol}-{hedge}"
                self.enabled = True
                self.closed = False

            def write(self, row):
                if self.closed:
                    raise AssertionError("write after close")

            def stop_accepting(self):
                self.enabled = False

            def close(self):
                if not self.closed:
                    self.closed = True
                    events.append(f"close:{self.path}")

        class StuckFeed:
            def __init__(self, name, ws_url, identity, writer, **kwargs):
                self.name = name

            async def run(self, stop):
                nonlocal started_count
                started_count += 1
                if started_count == 2:
                    both_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    events.append(f"feed-cancelled:{self.name}")
                    raise

        monkeypatch.setattr(reference, "EntropyReferenceStoreWriter", OrderedWriter)
        monkeypatch.setattr(reference, "HedgeReferenceStoreWriter", OrderedWriter)
        monkeypatch.setattr(reference, "HLReferenceFeed", StuckFeed)
        monkeypatch.setattr(reference, "LighterReferenceFeed", StuckFeed)
        recorder = ReferenceRecorder(
            symbol="SNDK",
            hedge_key="lighter",
            entropy_ws_url="wss://hl.invalid/ws",
            entropy_coin="io:SNDK",
            hedge_ws_url="wss://lighter.invalid/ws",
            hedge_market_id=139,
            store=MarketHistoryStore(":memory:"),
            feed_stop_timeout_sec=0.01,
        )
        stop = asyncio.Event()
        task = asyncio.create_task(recorder.run(stop))
        await both_started.wait()
        stop.set()
        await task

        cancel_positions = [
            index
            for index, event in enumerate(events)
            if event.startswith("feed-cancelled:")
        ]
        close_positions = [
            index for index, event in enumerate(events) if event.startswith("close:")
        ]
        assert len(cancel_positions) == 2
        assert len(close_positions) == 2
        assert max(cancel_positions) < min(close_positions)

    asyncio.run(scenario())


def test_feed_failure_preserves_sibling_writer(tmp_path, monkeypatch):
    async def scenario():
        from entropy_arb import reference

        sibling_started = asyncio.Event()

        class FailedFeed:
            def __init__(self, name, ws_url, coin, writer, **kwargs):
                pass

            async def run(self, stop):
                raise RuntimeError("feed failed")

        class SiblingFeed:
            def __init__(self, name, ws_url, market_id, writer, **kwargs):
                self.writer = writer

            async def run(self, stop):
                self.writer.write((1, 2, 100.0, 101.0))
                sibling_started.set()
                await stop.wait()

        monkeypatch.setattr(reference, "HLReferenceFeed", FailedFeed)
        monkeypatch.setattr(reference, "LighterReferenceFeed", SiblingFeed)
        recorder = ReferenceRecorder(
            symbol="SNDK",
            hedge_key="lighter",
            entropy_ws_url="wss://hl.invalid/ws",
            entropy_coin="io:SNDK",
            hedge_ws_url="wss://lighter.invalid/ws",
            hedge_market_id=139,
            store=MarketHistoryStore(tmp_path / "history.sqlite"),
        )
        stop = asyncio.Event()
        task = asyncio.create_task(recorder.run(stop))
        await sibling_started.wait()
        stop.set()
        await task
        recorder.hedge_writer.store.flush()
        with sqlite3.connect(recorder.hedge_writer.store.path) as conn:
            assert conn.execute("SELECT COUNT(*) FROM hedge_reference").fetchone() == (
                1,
            )
        recorder.hedge_writer.store.close()

    asyncio.run(scenario())


def test_bad_header_disables_only_one_sibling_writer(tmp_path, monkeypatch):
    async def scenario():
        from entropy_arb import reference

        entropy_path, hedge_path = reference_paths("SNDK", "lighter", str(tmp_path))
        original = b"wrong,header\n1,2\n"
        with open(entropy_path, "wb") as fh:
            fh.write(original)
        both_started = asyncio.Event()
        started_count = 0

        class Feed:
            def __init__(self, name, ws_url, identity, writer, **kwargs):
                self.writer = writer

            async def run(self, stop):
                nonlocal started_count
                self.writer.write((1, 100.0, 101.0))
                started_count += 1
                if started_count == 2:
                    both_started.set()
                await stop.wait()

        class HedgeFeed:
            def __init__(self, name, ws_url, market_id, writer, **kwargs):
                self.writer = writer

            async def run(self, stop):
                self.writer.write((1, 2, 100.0, 101.0))
                nonlocal started_count
                started_count += 1
                if started_count == 2:
                    both_started.set()
                await stop.wait()

        monkeypatch.setattr(reference, "HLReferenceFeed", Feed)
        monkeypatch.setattr(reference, "LighterReferenceFeed", HedgeFeed)
        recorder = ReferenceRecorder(
            symbol="SNDK",
            hedge_key="lighter",
            entropy_ws_url="wss://hl.invalid/ws",
            entropy_coin="io:SNDK",
            hedge_ws_url="wss://lighter.invalid/ws",
            hedge_market_id=139,
            store=MarketHistoryStore(tmp_path / "history.sqlite"),
        )
        stop = asyncio.Event()
        task = asyncio.create_task(recorder.run(stop))
        await both_started.wait()
        stop.set()
        await task
        assert open(entropy_path, "rb").read() == original
        recorder.hedge_writer.store.flush()
        with sqlite3.connect(recorder.hedge_writer.store.path) as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM entropy_reference"
            ).fetchone() == (1,)
            assert conn.execute("SELECT COUNT(*) FROM hedge_reference").fetchone() == (
                1,
            )
        recorder.hedge_writer.store.close()

    asyncio.run(scenario())


def test_sibling_isolation_prevents_write_after_stop_accepting(tmp_path, monkeypatch):
    async def scenario():
        from entropy_arb import reference

        events = []
        both_started = asyncio.Event()
        started_count = 0

        class TrackingWriter:
            def __init__(self, store, symbol, hedge):
                self.path = f"{symbol}-{hedge}"
                self.accepting = True
                self.closed = False

            @property
            def enabled(self):
                return self.accepting and not self.closed

            def write(self, row):
                assert not self.closed
                if self.accepting:
                    events.append(f"write:{self.path}")

            def stop_accepting(self):
                self.accepting = False
                events.append(f"stop-accepting:{self.path}")

            def close(self):
                if not self.closed:
                    self.closed = True
                    events.append(f"close:{self.path}")

        class Feed:
            def __init__(self, name, ws_url, identity, writer, **kwargs):
                self.writer = writer

            async def run(self, stop):
                nonlocal started_count
                self.writer.write((1, 100.0, 101.0))
                started_count += 1
                if started_count == 2:
                    both_started.set()
                await stop.wait()

        monkeypatch.setattr(reference, "EntropyReferenceStoreWriter", TrackingWriter)
        monkeypatch.setattr(reference, "HedgeReferenceStoreWriter", TrackingWriter)
        monkeypatch.setattr(reference, "HLReferenceFeed", Feed)
        monkeypatch.setattr(reference, "LighterReferenceFeed", Feed)
        recorder = ReferenceRecorder(
            symbol="SNDK",
            hedge_key="lighter",
            entropy_ws_url="wss://hl.invalid/ws",
            entropy_coin="io:SNDK",
            hedge_ws_url="wss://lighter.invalid/ws",
            hedge_market_id=139,
            store=MarketHistoryStore(tmp_path / "history.sqlite"),
        )
        stop = asyncio.Event()
        task = asyncio.create_task(recorder.run(stop))
        await both_started.wait()
        stop.set()
        await task
        first_close = min(
            index for index, event in enumerate(events) if event.startswith("close:")
        )
        assert all(
            not event.startswith("write:") for event in events[first_close + 1 :]
        )
        assert all(
            events.index(event) < first_close
            for event in events
            if event.startswith("stop-accepting:")
        )

    asyncio.run(scenario())
