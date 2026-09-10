import asyncio
import logging

from entropy_arb.book import OrderBook
from entropy_arb.entropy_quota import (
    EntropyQuotaCoordinator,
    is_entropy_quota_error,
)
from entropy_arb.feeds import HLBookFeed
from entropy_arb.reference import HLReferenceFeed
from entropy_arb.ws_lifecycle import (
    active_entropy_ws_count,
    reset_entropy_ws_state,
)


class QuotaError(Exception):
    code = 1008


class OtherPolicyError(Exception):
    code = 1008


class FailedConnection:
    def __init__(self, error):
        self.error = error

    async def __aenter__(self):
        raise self.error

    async def __aexit__(self, exc_type, exc, tb):
        return False


class ConnectSequence:
    def __init__(self, connections):
        self.connections = list(connections)
        self.calls = 0

    def __call__(self, url, **kwargs):
        self.calls += 1
        return self.connections.pop(0)


class StubWriter:
    enabled = True

    def write(self, row):
        pass


def test_only_exact_entropy_connection_quota_error_is_classified():
    assert is_entropy_quota_error(
        QuotaError("received 1008 Cannot open more than 15 connections")
    )
    assert not is_entropy_quota_error(
        OtherPolicyError("received 1008 policy violation")
    )
    assert not is_entropy_quota_error(
        RuntimeError("Cannot open more than 15 connections")
    )


def test_quota_coordinator_tracks_pressure_and_repeated_errors_extend_cooldown():
    now = [100.0]
    coordinator = EntropyQuotaCoordinator(clock=lambda: now[0])
    coordinator.mark_main_connected(now=now[0])
    coordinator.note_quota_error("reference", now=now[0])
    assert coordinator.quota_pressure(now=100.0)
    assert coordinator.last_quota_error_at == 100.0
    assert coordinator.cooldown_until == 160.0
    assert not coordinator.reference_recovery_allowed(now=100.0)

    now[0] = 130.0
    coordinator.note_quota_error("main", now=now[0])
    assert coordinator.cooldown_until == 190.0
    assert coordinator.last_quota_error_at == 130.0


def test_reference_recovery_requires_main_connected_and_ten_healthy_seconds():
    coordinator = EntropyQuotaCoordinator()
    assert not coordinator.reference_recovery_allowed(now=0.0)
    coordinator.mark_main_connected(now=0.0)
    assert not coordinator.reference_recovery_allowed(now=9.999)
    assert coordinator.reference_recovery_allowed(now=10.0)
    coordinator.mark_main_disconnected(now=10.0)
    assert not coordinator.reference_recovery_allowed(now=100.0)


def test_main_quota_backoff_is_15_30_60_120_capped():
    coordinator = EntropyQuotaCoordinator()
    assert [coordinator.quota_reconnect_delay(n) for n in range(1, 7)] == [
        15.0,
        30.0,
        60.0,
        120.0,
        120.0,
        120.0,
    ]


def test_main_quota_errors_use_dedicated_backoff_and_remain_bounded(caplog):
    async def scenario():
        reset_entropy_ws_state()
        stop = asyncio.Event()
        coordinator = EntropyQuotaCoordinator()
        connector = ConnectSequence(
            [
                FailedConnection(
                    QuotaError(
                        "received 1008 (policy violation) "
                        "Cannot open more than 15 connections"
                    )
                )
                for _ in range(4)
            ]
        )
        delays = []

        async def wait_or_stop(feed_stop, delay):
            delays.append(delay)
            if len(delays) == 4:
                stop.set()
            return not feed_stop.is_set()

        setattr(coordinator, "wait_or_stop", wait_or_stop)
        feed = HLBookFeed(
            "ENTROPY",
            "wss://example.invalid/ws",
            "io:SNDK",
            OrderBook(),
            lambda: None,
            quota_coordinator=coordinator,
        )
        from entropy_arb import feeds

        original_connect = feeds.ws_connect
        setattr(feeds, "ws_connect", connector)
        try:
            await feed.run(stop)
        finally:
            setattr(feeds, "ws_connect", original_connect)
        assert delays == [15.0, 30.0, 60.0, 120.0]
        assert connector.calls == 4
        assert active_entropy_ws_count() == 0

    with caplog.at_level(logging.DEBUG, logger="entropy-quota"):
        asyncio.run(scenario())
    assert "main quota reconnect attempt=4 delay=120s" in caplog.text


def test_reference_quota_error_is_suppressed_without_opening_another_socket():
    async def scenario():
        coordinator = EntropyQuotaCoordinator()
        coordinator.note_quota_error("reference")
        stop = asyncio.Event()
        connector = ConnectSequence([])
        feed = HLReferenceFeed(
            "entropy-reference",
            "wss://example.invalid/ws",
            "io:SNDK",
            StubWriter(),
            connect=connector,
            quota_coordinator=coordinator,
        )
        task = asyncio.create_task(feed.run(stop))
        await asyncio.sleep(0)
        assert connector.calls == 0
        stop.set()
        await asyncio.wait_for(task, timeout=0.2)

    asyncio.run(scenario())


def test_main_quota_error_suppresses_reference_recovery_too():
    coordinator = EntropyQuotaCoordinator()
    coordinator.note_quota_error("main", now=100.0)
    coordinator.mark_main_connected(now=100.0)
    assert not coordinator.reference_recovery_allowed(now=110.0)
    assert coordinator.cooldown_until == 160.0


def test_reference_wait_is_interruptible_by_shutdown():
    async def scenario():
        coordinator = EntropyQuotaCoordinator()
        coordinator.note_quota_error("reference")
        stop = asyncio.Event()
        wait_task = asyncio.create_task(coordinator.wait_reference_recovery(stop))
        await asyncio.sleep(0)
        stop.set()
        assert await asyncio.wait_for(wait_task, timeout=0.2) is False

    asyncio.run(scenario())


def test_reference_recovery_opens_only_after_main_health_and_cooldown():
    async def scenario():
        coordinator = EntropyQuotaCoordinator(
            cooldown_sec=0.03,
            main_healthy_required_sec=0.03,
        )
        coordinator.mark_main_connected()
        coordinator.note_quota_error("reference")
        stop = asyncio.Event()
        opened = asyncio.Event()

        class OneFrameSocket:
            async def __aenter__(self):
                opened.set()
                return self

            async def __aexit__(self, exc_type, exc, tb):
                stop.set()
                return False

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            async def send(self, raw):
                return None

        connector = ConnectSequence([OneFrameSocket()])
        feed = HLReferenceFeed(
            "entropy-reference",
            "wss://example.invalid/ws",
            "io:SNDK",
            StubWriter(),
            connect=connector,
            quota_coordinator=coordinator,
        )
        task = asyncio.create_task(feed.run(stop))
        await asyncio.wait_for(opened.wait(), timeout=0.2)
        await asyncio.wait_for(task, timeout=0.2)
        assert connector.calls == 1

    asyncio.run(scenario())


def test_quota_coordinator_exposes_main_health_state():
    coordinator = EntropyQuotaCoordinator()
    coordinator.mark_main_connected(now=1.0)
    snapshot = coordinator.snapshot(now=4.0)
    assert snapshot.main_connected
    assert snapshot.main_healthy_for_s == 3.0
    coordinator.mark_main_disconnected(now=5.0)
    assert not coordinator.snapshot(now=5.0).main_connected
