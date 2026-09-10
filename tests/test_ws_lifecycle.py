import asyncio
import json
import logging
from types import SimpleNamespace

from entropy_arb.book import OrderBook
from entropy_arb.feeds import HLBookFeed
from entropy_arb.reference import HLReferenceFeed, ReferenceRecorder
from entropy_arb.venue_hl import HLVenue
from entropy_arb.ws_lifecycle import active_entropy_ws_count, reset_entropy_ws_state

BOOK_FRAME = json.dumps(
    {
        "channel": "l2Book",
        "data": {
            "coin": "io:SNDK",
            "levels": [
                [{"px": "100", "sz": "1"}],
                [{"px": "101", "sz": "1"}],
            ],
        },
    }
)


class StubWriter:
    enabled = True

    def write(self, row):
        pass


class TrackingWebSocket:
    def __init__(self, frames, *, active, max_active, stop=None, error=None):
        self.frames = list(frames)
        self.active = active
        self.max_active = max_active
        self.stop = stop
        self.error = error
        self.entered = False
        self.exited = False
        self.sent = []

    async def __aenter__(self):
        self.entered = True
        self.active[0] += 1
        self.max_active[0] = max(self.max_active[0], self.active[0])
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True
        self.active[0] -= 1
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.frames:
            return self.frames.pop(0)
        if self.error is not None:
            error, self.error = self.error, None
            raise error
        if self.stop is not None:
            self.stop.set()
        raise StopAsyncIteration

    async def send(self, raw):
        self.sent.append(json.loads(raw))

    async def close(self):
        return None


class ConnectSequence:
    def __init__(self, sockets):
        self.sockets = list(sockets)
        self.calls = 0

    def __call__(self, url, **kwargs):
        self.calls += 1
        return self.sockets.pop(0)


def test_market_feed_reconnect_has_no_overlapping_entropy_instances(caplog):
    async def scenario():
        reset_entropy_ws_state()
        stop = asyncio.Event()
        active = [0]
        max_active = [0]
        first = TrackingWebSocket(
            [BOOK_FRAME],
            active=active,
            max_active=max_active,
            error=OSError("dropped"),
        )
        second = TrackingWebSocket([], active=active, max_active=max_active, stop=stop)
        connector = ConnectSequence([first, second])
        delays = []

        async def fake_sleep(delay):
            delays.append(delay)

        feed = HLBookFeed(
            "ENTROPY",
            "wss://example.invalid/ws",
            "io:SNDK",
            OrderBook(),
            lambda: None,
            ping_sec=999.0,
        )
        # The production feed uses the module-level connector; patching it
        # keeps this test entirely offline.
        from entropy_arb import feeds

        original_connect = feeds.ws_connect
        original_sleep = feeds.asyncio.sleep
        feeds.ws_connect = connector
        feeds.asyncio.sleep = fake_sleep
        try:
            await feed.run(stop)
        finally:
            feeds.ws_connect = original_connect
            feeds.asyncio.sleep = original_sleep
        assert connector.calls == 2
        assert max_active[0] == 1
        assert active_entropy_ws_count() == 0
        assert first.exited and second.exited
        assert delays == [1.0]

    with caplog.at_level(logging.DEBUG, logger="feeds"):
        asyncio.run(scenario())
    assert "[entropy-ws] OPEN" in caplog.text
    assert "[entropy-ws] CONNECTED" in caplog.text
    assert "[entropy-ws] ERROR" in caplog.text
    assert "[entropy-ws] CLOSING" in caplog.text
    assert "[entropy-ws] CLOSED" in caplog.text
    assert any(
        "[entropy-ws] ERROR" in record.getMessage()
        and "active=0" in record.getMessage()
        for record in caplog.records
    )


def test_repeated_market_connect_failures_never_accumulate_active_instances():
    async def scenario():
        reset_entropy_ws_state()
        stop = asyncio.Event()
        active = [0]
        max_active = [0]
        sockets = [
            TrackingWebSocket(
                [],
                active=active,
                max_active=max_active,
                error=OSError("connect failed"),
            )
            for _ in range(4)
        ]
        connector = ConnectSequence(sockets)
        delays = []

        async def fake_sleep(delay):
            delays.append(delay)
            if len(delays) == 4:
                stop.set()

        feed = HLBookFeed(
            "ENTROPY",
            "wss://example.invalid/ws",
            "io:SNDK",
            OrderBook(),
            lambda: None,
        )
        from entropy_arb import feeds

        original_connect = feeds.ws_connect
        original_sleep = feeds.asyncio.sleep
        feeds.ws_connect = connector
        feeds.asyncio.sleep = fake_sleep
        try:
            await feed.run(stop)
        finally:
            feeds.ws_connect = original_connect
            feeds.asyncio.sleep = original_sleep
        assert connector.calls == 4
        assert max_active[0] == 1
        assert active_entropy_ws_count() == 0
        assert delays == [1.0, 2.0, 4.0, 8.0]

    asyncio.run(scenario())


def test_market_feed_shutdown_closes_the_current_entropy_instance():
    async def scenario():
        reset_entropy_ws_state()
        stop = asyncio.Event()
        entered = asyncio.Event()
        active = [0]
        max_active = [0]

        class BlockingWebSocket(TrackingWebSocket):
            async def __aenter__(self):
                result = await super().__aenter__()
                entered.set()
                return result

            async def __anext__(self):
                await asyncio.Future()
                raise AssertionError("unreachable")

        socket = BlockingWebSocket([], active=active, max_active=max_active)
        connector = ConnectSequence([socket])
        feed = HLBookFeed(
            "ENTROPY",
            "wss://example.invalid/ws",
            "io:SNDK",
            OrderBook(),
            lambda: None,
        )
        from entropy_arb import feeds

        original_connect = feeds.ws_connect
        feeds.ws_connect = connector
        task = asyncio.create_task(feed.run(stop))
        try:
            await asyncio.wait_for(entered.wait(), timeout=0.5)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        finally:
            feeds.ws_connect = original_connect
        assert socket.exited
        assert active_entropy_ws_count() == 0

    asyncio.run(scenario())


def test_entropy_reference_is_one_loop_with_bounded_active_count():
    async def scenario():
        reset_entropy_ws_state()
        stop = asyncio.Event()
        active = [0]
        max_active = [0]
        socket = TrackingWebSocket([], active=active, max_active=max_active, stop=stop)
        connector = ConnectSequence([socket])
        feed = HLReferenceFeed(
            "entropy-reference",
            "wss://example.invalid/ws",
            "io:SNDK",
            StubWriter(),
            connect=connector,
            sleep=lambda _: asyncio.sleep(0),
        )
        # Reference feed currently accepts one run task; a normal run may
        # reconnect, but must not create a second local loop.
        await feed.run(stop)
        assert connector.calls == 1
        assert max_active[0] == 1
        assert active_entropy_ws_count() == 0

    asyncio.run(scenario())


def test_normal_entropy_startup_has_one_market_and_one_reference_loop():
    async def scenario():
        stop = asyncio.Event()
        venue = object.__new__(HLVenue)
        venue.conf = SimpleNamespace(hl_dex="io")
        venue.name = "ENTROPY"
        venue.key = "entropy"
        venue.ws_url = "wss://example.invalid/ws"
        venue.coin = "io:SNDK"
        venue.book = OrderBook()
        market_tasks = venue.start_tasks(stop, lambda: None, live=False)
        assert len(market_tasks) == 1
        assert market_tasks[0].get_name() == "book-entropy"
        for task in market_tasks:
            task.cancel()
        await asyncio.gather(*market_tasks, return_exceptions=True)

        recorder = ReferenceRecorder(
            symbol="SNDK",
            hedge_key="lighter-rh",
            entropy_ws_url="wss://example.invalid/ws",
            entropy_coin="io:SNDK",
            hedge_ws_url="wss://example.invalid/hedge",
            hedge_market_id=32,
            store=object(),
        )
        calls = {"entropy": 0, "hedge": 0}

        class SpyFeed:
            def __init__(self, key):
                self.key = key

            async def run(self, feed_stop):
                calls[self.key] += 1
                await feed_stop.wait()

        recorder.entropy_feed = SpyFeed("entropy")
        recorder.hedge_feed = SpyFeed("hedge")
        recorder_task = asyncio.create_task(recorder.run(stop))
        for _ in range(3):
            await asyncio.sleep(0)
        assert calls == {"entropy": 1, "hedge": 1}
        stop.set()
        await asyncio.wait_for(recorder_task, timeout=0.5)

    asyncio.run(scenario())
