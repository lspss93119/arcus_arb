"""Local lifecycle telemetry for Entropy websocket connections.

The active counter tracks websocket instances that entered their async context,
not connection attempts.  Feed implementations still own reconnect policy and
shutdown; this module only records those lifecycle transitions.
"""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Final

_ACTIVE_IDS: set[str] = set()
_ACTIVE_LOCK: Final = threading.Lock()


def active_entropy_ws_count() -> int:
    """Return the number of currently open Entropy websocket instances."""
    with _ACTIVE_LOCK:
        return len(_ACTIVE_IDS)


def reset_entropy_ws_state() -> None:
    """Reset local lifecycle state for isolated tests.

    Production code never needs to reset the counter; every tracked instance
    removes itself in ``closed``.  The helper keeps tests independent when a
    test intentionally exercises a cancelled task.
    """
    with _ACTIVE_LOCK:
        _ACTIVE_IDS.clear()


class EntropyWebSocketLifecycle:
    """Record one websocket attempt and its local open/close transitions."""

    def __init__(
        self,
        purpose: str,
        *,
        attempt: int,
        logger: logging.Logger,
        count_active: bool = True,
    ) -> None:
        self.connection_id = uuid.uuid4().hex[:12]
        self.purpose = purpose
        self.attempt = attempt
        self.logger = logger
        self.count_active = count_active
        self._opened = False
        self._closing = False
        self._closed = False

    def attempt_started(self) -> None:
        self.logger.debug(
            "[entropy-ws] ATTEMPT id=%s purpose=%s attempt=%d active=%d",
            self.connection_id,
            self.purpose,
            self.attempt,
            active_entropy_ws_count(),
        )

    def opened(self) -> None:
        if self._opened:
            return
        self._opened = True
        if self.count_active:
            with _ACTIVE_LOCK:
                _ACTIVE_IDS.add(self.connection_id)
        self.logger.info(
            "[entropy-ws] OPEN id=%s purpose=%s active=%d",
            self.connection_id,
            self.purpose,
            active_entropy_ws_count(),
        )

    def connected(self) -> None:
        self.logger.info(
            "[entropy-ws] CONNECTED id=%s purpose=%s attempt=%d active=%d",
            self.connection_id,
            self.purpose,
            self.attempt,
            active_entropy_ws_count(),
        )

    def error(self, exc: BaseException, *, reconnect_delay: float) -> None:
        code = getattr(exc, "code", None)
        self.logger.warning(
            "[entropy-ws] ERROR id=%s purpose=%s attempt=%d code=%s "
            "active=%d reconnect_delay=%.1fs: %s",
            self.connection_id,
            self.purpose,
            self.attempt,
            code,
            active_entropy_ws_count(),
            reconnect_delay,
            exc,
        )

    def closing(self) -> None:
        if self._closing:
            return
        self._closing = True
        self.logger.info(
            "[entropy-ws] CLOSING id=%s purpose=%s active=%d",
            self.connection_id,
            self.purpose,
            active_entropy_ws_count(),
        )

    def closed(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.count_active and self._opened:
            with _ACTIVE_LOCK:
                _ACTIVE_IDS.discard(self.connection_id)
        self.logger.info(
            "[entropy-ws] CLOSED id=%s purpose=%s active=%d",
            self.connection_id,
            self.purpose,
            active_entropy_ws_count(),
        )
