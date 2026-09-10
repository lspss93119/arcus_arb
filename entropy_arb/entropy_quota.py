"""Local coordination for Entropy websocket connection-quota pressure.

The Entropy service limits concurrent websocket sessions.  This module does
not change what the feeds subscribe to or provide a second supervisor socket;
it only lets the trading-critical market feed take precedence over the
secondary reference feed after a specific server quota rejection.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass

QUOTA_COOLDOWN_SEC = 60.0
MAIN_HEALTHY_REQUIRED_SEC = 10.0
MAIN_QUOTA_BACKOFF_SEC: tuple[float, ...] = (15.0, 30.0, 60.0, 120.0)
_QUOTA_MESSAGE = "Cannot open more than 15 connections"


def is_entropy_quota_error(exc: BaseException) -> bool:
    """Return true only for Entropy's known concurrent-session rejection.

    A close code of 1008 is also used for unrelated policy violations, so the
    message must contain the exact quota wording before the error is shared
    with the other Entropy feed.
    """
    code = getattr(exc, "code", None)
    if code is None:
        received = getattr(exc, "rcvd", None)
        code = getattr(received, "code", None)
    if str(code) != "1008":
        return False
    return _QUOTA_MESSAGE in str(exc)


@dataclass(frozen=True, slots=True)
class EntropyQuotaSnapshot:
    """Read-only view of the coordinator's local websocket state."""

    quota_pressure: bool
    last_quota_error_at: float | None
    cooldown_until: float
    main_connected: bool
    main_healthy_for_s: float


class EntropyQuotaCoordinator:
    """Coordinate recovery of Entropy's market and reference sockets.

    The coordinator is deliberately in-memory and process-local.  Feed tasks
    signal state transitions synchronously; async waits are interruptible by
    either a state change or the engine's shutdown event.
    """

    def __init__(
        self,
        *,
        cooldown_sec: float = QUOTA_COOLDOWN_SEC,
        main_healthy_required_sec: float = MAIN_HEALTHY_REQUIRED_SEC,
        clock: Callable[[], float] = time.monotonic,
        logger: logging.Logger | None = None,
    ) -> None:
        if not math.isfinite(cooldown_sec) or cooldown_sec < 0:
            raise ValueError("cooldown_sec must be finite and non-negative")
        if (
            not math.isfinite(main_healthy_required_sec)
            or main_healthy_required_sec < 0
        ):
            raise ValueError(
                "main_healthy_required_sec must be finite and non-negative"
            )
        self.cooldown_sec = float(cooldown_sec)
        self.main_healthy_required_sec = float(main_healthy_required_sec)
        self._clock = clock
        self._log = logger or logging.getLogger("entropy-quota")
        self._last_quota_error_at: float | None = None
        self._cooldown_until = 0.0
        self._main_connected_since: float | None = None
        self._wake = asyncio.Event()
        self._last_suppression_log_at: float | None = None
        self._last_recovery_log_at: float | None = None

    @property
    def last_quota_error_at(self) -> float | None:
        return self._last_quota_error_at

    @property
    def cooldown_until(self) -> float:
        return self._cooldown_until

    @property
    def main_connected(self) -> bool:
        return self._main_connected_since is not None

    def _now(self, now: float | None) -> float:
        return self._clock() if now is None else float(now)

    def main_healthy_for_s(self, now: float | None = None) -> float:
        if self._main_connected_since is None:
            return 0.0
        return max(0.0, self._now(now) - self._main_connected_since)

    def quota_pressure(self, now: float | None = None) -> bool:
        return self._now(now) < self._cooldown_until

    def snapshot(self, now: float | None = None) -> EntropyQuotaSnapshot:
        resolved_now = self._now(now)
        return EntropyQuotaSnapshot(
            quota_pressure=resolved_now < self._cooldown_until,
            last_quota_error_at=self._last_quota_error_at,
            cooldown_until=self._cooldown_until,
            main_connected=self.main_connected,
            main_healthy_for_s=self.main_healthy_for_s(resolved_now),
        )

    def mark_main_connected(self, now: float | None = None) -> None:
        """Start (or preserve) the main feed's continuous healthy streak."""
        if self._main_connected_since is not None:
            return
        resolved_now = self._now(now)
        self._main_connected_since = resolved_now
        self._last_recovery_log_at = None
        self._log.info(
            "[entropy-quota] main connected; waiting %.0fs before reference recovery",
            self.main_healthy_required_sec,
        )
        self._wake.set()

    def mark_main_disconnected(self, now: float | None = None) -> None:
        """End the continuous main-feed streak and wake reference waiters."""
        del now  # The transition is stateful; no timestamp is needed.
        if self._main_connected_since is None:
            return
        self._main_connected_since = None
        self._last_recovery_log_at = None
        self._wake.set()

    def note_quota_error(self, source: str, now: float | None = None) -> None:
        """Activate/reset the shared quota cooldown after a specific error."""
        resolved_now = self._now(now)
        self._last_quota_error_at = resolved_now
        self._cooldown_until = max(
            self._cooldown_until,
            resolved_now + self.cooldown_sec,
        )
        self._last_suppression_log_at = None
        self._log.warning(
            "[entropy-quota] quota pressure detected source=%s cooldown=%.0fs",
            source,
            max(0.0, self._cooldown_until - resolved_now),
        )
        self._wake.set()

    def reference_recovery_allowed(self, now: float | None = None) -> bool:
        """Whether the secondary feed may open its next websocket."""
        resolved_now = self._now(now)
        return (
            self._main_connected_since is not None
            and self._main_healthy_for_at(resolved_now)
            >= self.main_healthy_required_sec
            and resolved_now >= self._cooldown_until
        )

    def _main_healthy_for_at(self, now: float) -> float:
        if self._main_connected_since is None:
            return 0.0
        return max(0.0, now - self._main_connected_since)

    def quota_reconnect_delay(self, attempt: int) -> float:
        """Return the dedicated main-feed quota backoff, capped at 120s."""
        if attempt <= 0:
            raise ValueError("attempt must be positive")
        index = min(attempt, len(MAIN_QUOTA_BACKOFF_SEC)) - 1
        return MAIN_QUOTA_BACKOFF_SEC[index]

    async def wait_or_stop(self, stop: asyncio.Event, delay: float) -> bool:
        """Wait for ``delay`` while making shutdown immediate."""
        if stop.is_set():
            return False
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(0.0, delay))
        except TimeoutError:
            return not stop.is_set()
        return False

    def _log_reference_wait(self, now: float, remaining: float) -> None:
        """Rate-limit repetitive suppression diagnostics."""
        if (
            self._last_suppression_log_at is not None
            and now - self._last_suppression_log_at < 10.0
        ):
            return
        self._last_suppression_log_at = now
        self._log.info(
            "[entropy-quota] reference suppressed remaining=%.0fs",
            max(0.0, remaining),
        )

    async def _wait_for_wake_or_stop(
        self,
        stop: asyncio.Event,
        timeout: float | None,
    ) -> bool:
        """Return true on stop, false on coordinator wake/timeout."""
        stop_task = asyncio.create_task(stop.wait())
        wake_task = asyncio.create_task(self._wake.wait())
        tasks = {stop_task, wake_task}
        try:
            done, _ = await asyncio.wait(
                tasks,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            return stop_task in done and stop_task.result()
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def wait_reference_recovery(self, stop: asyncio.Event) -> bool:
        """Wait until cooldown and the main healthy-streak gate both pass."""
        while not stop.is_set():
            now = self._now(None)
            if self.reference_recovery_allowed(now):
                healthy_for = self._main_healthy_for_at(now)
                if self._last_recovery_log_at != self._main_connected_since:
                    self._last_recovery_log_at = self._main_connected_since
                    self._log.info(
                        "[entropy-quota] main healthy %.1fs; reference "
                        "recovery allowed",
                        healthy_for,
                    )
                return True

            self._wake.clear()
            now = self._now(None)
            deadline: float | None = None
            if self.quota_pressure(now):
                remaining = max(0.0, self._cooldown_until - now)
                self._log_reference_wait(now, remaining)
                deadline = self._cooldown_until
            if self._main_connected_since is not None:
                healthy_deadline = (
                    self._main_connected_since + self.main_healthy_required_sec
                )
                deadline = (
                    healthy_deadline
                    if deadline is None
                    else max(deadline, healthy_deadline)
                )
            timeout = None if deadline is None else max(0.0, deadline - now)
            if await self._wait_for_wake_or_stop(stop, timeout):
                return False
        return False


__all__ = [
    "EntropyQuotaCoordinator",
    "EntropyQuotaSnapshot",
    "MAIN_HEALTHY_REQUIRED_SEC",
    "MAIN_QUOTA_BACKOFF_SEC",
    "QUOTA_COOLDOWN_SEC",
    "is_entropy_quota_error",
]
