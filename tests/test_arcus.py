"""Focused Phase A tests for Arcus public market data."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from entropy_arb.arcus import (
    ARCUS_PUBLIC_SUBSCRIPTIONS,
    ArcusMarketAttributes,
    ArcusMarketMetadata,
    ArcusTrade,
    parse_arcus_book_snapshot,
    parse_arcus_book_update,
    parse_arcus_market,
    parse_arcus_market_attributes,
    parse_arcus_trades,
    resolve_arcus_market,
)
from entropy_arb.arcus_book import ArcusOrderBook
from entropy_arb.arcus_feed import ArcusBookFeed
from entropy_arb.arcus_recorder import ArcusMarketRecorder
from entropy_arb.premium import calculate_premiums
from entropy_arb.storage import (
    ArcusMarketAttributesRow,
    ArcusMarketMetadataRow,
    ArcusSampleRow,
    ArcusTradeRow,
    MarketHistoryStore,
)
from entropy_arb.venue_arcus import ArcusVenue


MARKET = {
    "marketDisplayName": "SNDK-USD",
    "fullAssetName": "SanDisk",
    "marketId": 33,
    "status": "ONLINE",
    "baseAsset": "SNDK",
    "quoteAsset": "USD",
    "tickSize": "0.01",
    "stepSize": "0.0000001",
    "tickTiers": [{"upToPrice": "5000", "tick": "0.01"}],
    "minOrderNotional": "5",
    "minOrderSize": "0.01",
    "maxOrderSize": "100000",
    "isOutsideRth": False,
    "currentSettlementPrice": None,
    "upperTradingBound": None,
    "lowerTradingBound": None,
    "nextUpperTradingBound": None,
    "nextLowerTradingBound": None,
    "regularTradingHours": {
        "startSecondsOfDay": 14400,
        "endSecondsOfDay": 72000,
        "timezone": "America/New_York",
        "isOvernight": False,
    },
}

SNAPSHOT = {
    "type": "subscribed",
    "channel": "l2OrderbookUpdates",
    "id": "SNDK-USD",
    "contents": {
        "bids": [["1540.35", "0.3182"], ["1540.16", "0.3246408"]],
        "asks": [["1540.67", "0.3307"], ["1540.86", "0.2811"]],
        "lastSequenceId": 91051778,
        "globalSequenceId": 1789133352,
        "timestamp": 1788425525850028,
    },
}

UPDATE = {
    "type": "channel_data",
    "channel": "l2OrderbookUpdates",
    "id": "SNDK-USD",
    "contents": {
        "bids": [["1540.35", "0"]],
        "asks": [["1540.67", "0.4412"]],
        "lastSequenceId": 91051779,
        "globalSequenceId": 1789133353,
    },
}

TRADE_MESSAGE = {
    "type": "channel_data",
    "channel": "trades",
    "id": "SNDK-USD",
    "contents": [
        {
            "takerOrderId": "taker-1",
            "makerOrderId": "maker-2",
            "takerAddress": "0xabc",
            "makerAddress": "0xdef",
            "price": "1540.50",
            "size": "0.25",
            "tradeId": "trade-3",
            "timestamp": 1788425525851000,
            "sequenceNumber": 12,
        }
    ],
}

ATTRIBUTES_MESSAGE = {
    "type": "subscribed",
    "channel": "marketAttributes",
    "contents": {
        "isSnapshot": True,
        "entries": [
            {
                "marketId": 33,
                "marketDisplayName": "SNDK-USD",
                "offHoursInitialMarginFraction": "0.15",
                "isOutsideRth": False,
                "currentSettlementPrice": None,
                "upperTradingBound": "1600.00",
                "lowerTradingBound": "1500.00",
                "nextUpperTradingBound": "1610.00",
                "nextLowerTradingBound": "1490.00",
                "isUpperInExpansionZone": False,
                "isLowerInExpansionZone": False,
                "upperZoneEnteredAt": None,
                "upperExpectedExpansionAt": None,
                "lowerZoneEnteredAt": None,
                "lowerExpectedExpansionAt": None,
                "boundSide": None,
                "timestamp": 1788425525852000,
                "marketSequenceNum": 44,
            }
        ],
    },
}


def test_arcus_market_metadata_parsing_and_symbol_resolution() -> None:
    metadata = parse_arcus_market(MARKET)

    assert isinstance(metadata, ArcusMarketMetadata)
    assert metadata.market_id == 33
    assert metadata.symbol == "SNDK-USD"
    assert metadata.base_asset == "SNDK"
    assert metadata.tick_size == "0.01"
    assert metadata.step_size == "0.0000001"
    assert metadata.min_order_size == "0.01"
    assert metadata.min_order_notional == "5"
    assert metadata.status == "ONLINE"
    assert resolve_arcus_market([MARKET], "SNDK").market_id == 33


def test_arcus_bbo_snapshot_parsing() -> None:
    snapshot = parse_arcus_book_snapshot(SNAPSHOT)

    assert snapshot.last_sequence_id == 91051778
    assert snapshot.global_sequence_id == 1789133352
    assert snapshot.bids[0] == ("1540.35", "0.3182")
    assert snapshot.asks[0] == ("1540.67", "0.3307")


def test_arcus_incremental_book_update_parsing_and_quantities() -> None:
    update = parse_arcus_book_update(UPDATE)

    assert update.last_sequence_id == 91051779
    assert update.bids == (("1540.35", "0"),)
    assert update.asks == (("1540.67", "0.4412"),)


def test_arcus_sequence_gap_invalidates_book_until_fresh_snapshot() -> None:
    book = ArcusOrderBook()
    book.apply_snapshot(parse_arcus_book_snapshot(SNAPSHOT), 1000, 10)
    assert book.ready
    assert book.health == "OK"
    assert book.best_bid_size == 0.3182

    assert book.apply_update(parse_arcus_book_update(UPDATE), 1001, 11)
    assert book.best_bid() == 1540.16
    assert book.best_ask_size == 0.4412

    gap = dict(UPDATE)
    gap["contents"] = dict(UPDATE["contents"], lastSequenceId=91051781)
    assert not book.apply_update(parse_arcus_book_update(gap), 1002, 12)
    assert not book.ready
    assert book.health == "RESYNC"
    assert book.sequence_gap_count == 1
    assert book.best_ask() is None

    blocked = dict(UPDATE)
    blocked["contents"] = dict(UPDATE["contents"], lastSequenceId=91051782)
    assert not book.apply_update(parse_arcus_book_update(blocked), 1003, 13)
    assert not book.ready

    fresh = dict(SNAPSHOT)
    fresh["contents"] = dict(SNAPSHOT["contents"], lastSequenceId=91051790)
    book.apply_snapshot(parse_arcus_book_snapshot(fresh), 1004, 14)
    assert book.ready
    assert book.health == "OK"
    assert book.sequence_id == 91051790


def test_public_trade_parsing_does_not_infer_aggressor_side() -> None:
    trades = parse_arcus_trades(TRADE_MESSAGE)

    assert len(trades) == 1
    trade = trades[0]
    assert isinstance(trade, ArcusTrade)
    assert trade.trade_id == "trade-3"
    assert trade.exchange_timestamp_us == 1788425525851000
    assert trade.price == "1540.50"
    assert trade.quantity == "0.25"
    assert trade.aggressor_side is None


def test_market_attributes_parsing_preserves_nullable_fields() -> None:
    attributes = parse_arcus_market_attributes(ATTRIBUTES_MESSAGE, market_id=33)

    assert isinstance(attributes, ArcusMarketAttributes)
    assert attributes.is_outside_rth is False
    assert attributes.current_settlement_price is None
    assert attributes.upper_trading_bound == "1600.00"
    assert attributes.lower_trading_bound == "1500.00"
    assert attributes.next_upper_trading_bound == "1610.00"
    assert attributes.next_lower_trading_bound == "1490.00"
    assert attributes.market_sequence_num == 44


def test_only_three_arcus_public_subscriptions_are_used() -> None:
    assert ARCUS_PUBLIC_SUBSCRIPTIONS == (
        "l2OrderbookUpdates",
        "trades",
        "marketAttributes",
    )
    assert "bbo" not in ARCUS_PUBLIC_SUBSCRIPTIONS


def test_midpoint_premium_uses_arcus_over_rh_midpoint() -> None:
    result = calculate_premiums(1540.35, 1540.67, 1539.0, 1540.0)
    arcus_mid = (1540.35 + 1540.67) / 2
    rh_mid = (1539.0 + 1540.0) / 2

    assert result.premium_bps == pytest.approx((arcus_mid / rh_mid - 1) * 10000)


def _sample_row() -> ArcusSampleRow:
    return ArcusSampleRow(
        timestamp_ms=1788425526000,
        symbol="SNDK",
        arcus_bid=1540.35,
        arcus_ask=1540.67,
        arcus_bid_size=0.3182,
        arcus_ask_size=0.3307,
        arcus_mid=1540.51,
        rh_bid=1539.0,
        rh_ask=1540.0,
        rh_bid_size=1.2,
        rh_ask_size=2.3,
        rh_mid=1539.5,
        premium_bps=6.56,
        arcus_book_sequence_id=91051778,
        arcus_global_sequence_id=1789133352,
        arcus_exchange_timestamp_us=1788425525850028,
        arcus_local_receive_ts_ms=1788425525850030,
        arcus_local_receive_monotonic_ns=123,
        rh_local_receive_ts_ms=1788425525850031,
        is_outside_rth=False,
        current_settlement_price=None,
        upper_trading_bound="1600.00",
        lower_trading_bound="1500.00",
        next_upper_trading_bound="1610.00",
        next_lower_trading_bound="1490.00",
    )


def _trade_row() -> ArcusTradeRow:
    return ArcusTradeRow(
        symbol="SNDK",
        market_id=33,
        market_display_name="SNDK-USD",
        trade_id="trade-3",
        exchange_timestamp_us=1788425525851000,
        local_receive_ts_ms=1788425525851001,
        local_receive_monotonic_ns=456,
        price="1540.50",
        quantity="0.25",
        aggressor_side=None,
        sequence_number=12,
    )


def test_sqlite_arcus_sample_and_public_trade_persistence_is_restart_safe(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "market-history.sqlite"
    store = MarketHistoryStore(db_path)
    sample = _sample_row()
    trade = _trade_row()
    store.append_arcus_sample(sample)
    store.append_arcus_trade(trade)
    store.flush()
    store.close()

    reopened = MarketHistoryStore(db_path)
    assert reopened.count_rows("arcus_samples") == 1
    assert reopened.count_rows("arcus_trades") == 1
    assert reopened.recent_premium_observations(
        "SNDK", "lighter-rh", 0, 1788425527000
    ) == [(pytest.approx(1788425526.0), pytest.approx(6.56))]
    reopened.close()


def test_sqlite_market_attributes_persistence(tmp_path: Path) -> None:
    store = MarketHistoryStore(tmp_path / "market-history.sqlite")
    store.append_arcus_market_attributes(
        ArcusMarketAttributesRow(
            symbol="SNDK",
            market_id=33,
            market_display_name="SNDK-USD",
            market_status="ONLINE",
            local_receive_ts_ms=1000,
            local_receive_monotonic_ns=99,
            event_timestamp_us=1001,
            market_sequence_num=44,
            is_outside_rth=False,
            current_settlement_price=None,
            upper_trading_bound="1600.00",
            lower_trading_bound="1500.00",
            next_upper_trading_bound="1610.00",
            next_lower_trading_bound="1490.00",
        )
    )
    store.flush()
    assert store.count_rows("arcus_market_attributes") == 1
    store.close()


def test_sqlite_market_metadata_persistence(tmp_path: Path) -> None:
    store = MarketHistoryStore(tmp_path / "market-history.sqlite")
    store.append_arcus_market_metadata(
        ArcusMarketMetadataRow(
            discovered_at_ms=1000,
            symbol="SNDK",
            market_id=33,
            market_display_name="SNDK-USD",
            status="ONLINE",
            tick_size="0.01",
            step_size="0.0000001",
            min_order_size="0.01",
            min_order_notional="5",
            max_order_size="100000",
            is_outside_rth=False,
            current_settlement_price=None,
            upper_trading_bound=None,
            lower_trading_bound=None,
            next_upper_trading_bound=None,
            next_lower_trading_bound=None,
            regular_trading_hours='{"timezone":"America/New_York"}',
        )
    )
    store.flush()
    assert store.count_rows("arcus_market_metadata") == 1
    store.close()


def test_arcus_book_staleness_is_fail_closed() -> None:
    book = ArcusOrderBook()
    book.apply_snapshot(parse_arcus_book_snapshot(SNAPSHOT), 1000, 10)
    book.alive_ts = time.time() - 100

    assert not book.is_fresh(5)
    book.mark_stale()
    assert book.health == "STALE"
    assert not book.ready


def test_arcus_order_attempt_fails_locally() -> None:
    venue = ArcusVenue()

    with pytest.raises(RuntimeError, match="Arcus trading is not implemented in Phase A"):
        venue.send_taker("buy", 1, 1)
    with pytest.raises(RuntimeError, match="Arcus trading is not implemented in Phase A"):
        venue.init_signer()


def test_arcus_venue_requires_no_private_credentials() -> None:
    venue = ArcusVenue()

    assert venue.supports_trading is False
    assert not hasattr(venue, "private_key")
    assert not hasattr(venue, "signer")


class _FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)


def test_arcus_feed_subscribes_to_exactly_three_public_channels() -> None:
    feed = ArcusBookFeed("SNDK-USD", ArcusOrderBook())
    websocket = _FakeWebSocket()

    asyncio.run(feed.subscribe_public(websocket))
    subscriptions = [json.loads(message) for message in websocket.sent]

    assert len(subscriptions) == 3
    assert [item["channel"] for item in subscriptions] == list(
        ARCUS_PUBLIC_SUBSCRIPTIONS
    )
    assert subscriptions[0]["id"] == "SNDK-USD"
    assert subscriptions[1]["id"] == "SNDK-USD"
    assert "id" not in subscriptions[2]


def test_arcus_feed_gap_requests_book_resync_without_extra_channels() -> None:
    feed = ArcusBookFeed("SNDK-USD", ArcusOrderBook())
    websocket = _FakeWebSocket()

    async def exercise() -> None:
        await feed.handle_message(websocket, SNAPSHOT, 1000, 10)
        await feed.handle_message(websocket, {**UPDATE, "contents": {
            **UPDATE["contents"],
            "lastSequenceId": 91051781,
        }}, 1001, 11)

    asyncio.run(exercise())
    requests = [json.loads(message) for message in websocket.sent]

    assert [item["channel"] for item in requests] == [
        "l2OrderbookUpdates",
        "l2OrderbookUpdates",
    ]
    assert feed.book.health == "RESYNC"


def test_rolling_center_reads_arcus_rh_midpoint_samples(tmp_path: Path) -> None:
    store = MarketHistoryStore(tmp_path / "market-history.sqlite")
    store.append_arcus_sample(_sample_row())
    store.flush()

    assert store.recent_premium_observations(
        "SNDK", "lighter-rh", 0, 1788425527000
    ) == [(pytest.approx(1788425526.0), pytest.approx(6.56))]
    store.close()


def test_rolling_center_consumes_arcus_rh_midpoint_premium(tmp_path: Path) -> None:
    from entropy_arb.config import load_config
    from entropy_arb.engine import Engine

    base = _sample_row()
    second = replace(
        base,
        timestamp_ms=base.timestamp_ms + 43_199_000,
        premium_bps=7.56,
    )
    store = MarketHistoryStore(tmp_path / "market-history.sqlite")
    store.append_arcus_sample(base)
    store.append_arcus_sample(second)
    store.flush()
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "strategy:\n"
        "  name: stable_basis\n"
        "  params:\n"
        "    center_mode: rolling\n"
        "    center_bps: 0\n"
        "    center_window_hours: 12\n"
        "    center_update_minutes: 60\n"
        "    upper_bps: 4\n"
        "    lower_bps: 4\n"
    )
    cfg = load_config(
        str(config_path), "/tmp/arcus-arb-no-such.env",
        symbol="SNDK", hedge_venue="lighter-rh",
    )
    engine = Engine(cfg, record_only=True)
    engine.market_history = store
    now = (base.timestamp_ms + 43_200_000) / 1000.0

    asyncio.run(engine._bootstrap_rolling_center(now=now))

    assert engine.strategy.state().center_bps == pytest.approx(7.06)
    store.close()


def test_arcus_recorder_records_bbo_and_attributes(tmp_path: Path) -> None:
    arcus_book = ArcusOrderBook()
    arcus_book.apply_snapshot(parse_arcus_book_snapshot(SNAPSHOT), 1000, 10)
    arcus_book.alive_ts = time.time()
    rh_book = SimpleNamespace(
        best_bid=lambda: 1539.0,
        best_ask=lambda: 1540.0,
        best_bid_size=1.2,
        best_ask_size=2.3,
        mid=1539.5,
        is_fresh=lambda max_age: True,
        alive_ts=1000.0,
    )
    store = MarketHistoryStore(tmp_path / "market-history.sqlite")
    recorder = ArcusMarketRecorder(
        store,
        symbol="SNDK",
        arcus_book=arcus_book,
        rh_book=rh_book,
        is_fresh_seconds=5,
    )

    recorder.record_attributes(
        parse_arcus_market_attributes(ATTRIBUTES_MESSAGE, market_id=33),
        local_receive_ts_ms=1002,
        local_receive_monotonic_ns=12,
        market_status="ONLINE",
    )
    assert recorder.record_sample(timestamp_ms=1003, monotonic_ns=13)
    store.flush()

    assert store.count_rows("arcus_samples") == 1
    assert store.count_rows("arcus_market_attributes") == 1
    store.close()


def test_record_only_startup_uses_mocked_public_arcus_and_rh_feeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from entropy_arb.config import load_config
    from entropy_arb.engine import Engine

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "strategy:\n"
        "  name: stable_basis\n"
        "  params:\n"
        "    center_bps: 0\n"
        "    upper_bps: 4\n"
        "    lower_bps: 4\n"
        "recorder:\n"
        "  database: " + str(tmp_path / "market-history.sqlite") + "\n"
    )
    cfg = load_config(
        str(config_path), "/tmp/arcus-arb-no-such.env",
        symbol="SNDK", hedge_venue="lighter-rh",
    )
    metadata = parse_arcus_market(MARKET)
    arcus = ArcusVenue(cfg.arcus)
    arcus.market = metadata
    arcus.exchange_symbol = metadata.symbol
    arcus.size_step = 0.0000001
    arcus.size_decimals = 7
    arcus.min_quote = 5.0

    class FakeRh:
        kind = "lighter"
        key = "hedge"
        name = "RH"
        fee_bps = 0.0
        cap_usd = 1000.0
        orders_per_min = 30
        size_decimals = 4
        min_base = 0.0
        min_quote = 0.0
        position = 0.0
        book = SimpleNamespace(
            best_bid=lambda: None,
            best_ask=lambda: None,
            is_fresh=lambda max_age: False,
        )
        conf = SimpleNamespace(symbol="SNDK")

        async def load_market(self):
            self.loaded = True

        def start_tasks(self, stop, notify, live):
            self.started_live = live
            return []

        async def close(self):
            self.closed = True

    rh = FakeRh()
    calls = {"arcus_load": 0, "arcus_start": 0}

    async def fake_arcus_load():
        calls["arcus_load"] += 1
        return metadata

    def fake_arcus_start(stop, notify, live):
        calls["arcus_start"] += 1
        arcus.started_live = live
        return []

    arcus.load_market = fake_arcus_load  # type: ignore[method-assign]
    arcus.start_tasks = fake_arcus_start  # type: ignore[method-assign]
    monkeypatch.setattr(
        Engine,
        "_make_venue",
        lambda self, conf: arcus if conf.key == "arcus" else rh,
    )

    async def scenario() -> None:
        engine = Engine(cfg, record_only=True)
        task = asyncio.create_task(engine._run_inner())
        await asyncio.sleep(0)
        engine.request_stop()
        await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(scenario())
    assert calls == {"arcus_load": 1, "arcus_start": 1}
    assert arcus.started_live is False
    assert rh.started_live is False
    assert rh.loaded is True


def test_arcus_non_record_only_startup_fails_before_market_network_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from entropy_arb.config import load_config
    from entropy_arb.engine import Engine

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "strategy:\n"
        "  name: stable_basis\n"
        "  params:\n"
        "    center_bps: 0\n"
        "    upper_bps: 4\n"
        "    lower_bps: 4\n"
    )
    cfg = load_config(
        str(config_path), "/tmp/arcus-arb-no-such.env",
        symbol="SNDK", hedge_venue="lighter-rh",
    )
    arcus = ArcusVenue(cfg.arcus)
    loaded = False

    async def unexpected_load():
        nonlocal loaded
        loaded = True
        raise AssertionError("market discovery must not run in non-record-only mode")

    arcus.load_market = unexpected_load  # type: ignore[method-assign]
    monkeypatch.setattr(
        Engine,
        "_make_venue",
        lambda self, conf: arcus,
    )

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="Phase A is record-only"):
            await Engine(cfg, record_only=False)._run_inner()

    asyncio.run(scenario())
    assert loaded is False


def test_record_only_dashboard_mentions_arcus_and_rh() -> None:
    from entropy_arb.dashboard import Dashboard

    dashboard = Dashboard.__new__(Dashboard)
    dashboard.lang = "en"
    dashboard.eng = SimpleNamespace(
        record_only=True,
        cfg=SimpleNamespace(symbol="SNDK", staleness_sec=5.0),
        arcus=SimpleNamespace(
            book=SimpleNamespace(
                best_bid=1540.35,
                best_ask=1540.67,
                is_fresh=lambda max_age: True,
                health="OK",
                alive_ts=time.time(),
            )
        ),
        hedge=SimpleNamespace(
            label="RH",
            book=SimpleNamespace(
                best_bid=1539.0,
                best_ask=1540.0,
                is_fresh=lambda max_age: True,
                alive_ts=time.time(),
            )
        ),
        rolling_center_bps=None,
        latest_sample_ts=1000,
        recorder=SimpleNamespace(rows_written=7),
    )

    rendered = dashboard._record_only_panel()
    rendered_text = rendered.renderable.plain

    assert "ARCUS" in rendered_text
    assert "RH" in rendered_text
    assert "RECORD-ONLY" in rendered_text
    assert "Arcus trading disabled" in rendered_text
    assert "OK" in rendered_text
