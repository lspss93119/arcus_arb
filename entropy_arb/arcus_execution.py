"""Arcus maker-only order and account-stream primitives for Phase B0.

There is intentionally no Arcus taker method here.  The only mutating method
is a signed LIMIT+ALO placement, plus explicit cancellation for the same
calibration order.  Account subscriptions remain public according to the
current Arcus API and are used as the source of truth for asynchronous order
and fill lifecycle events.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # pragma: no cover - compatibility with websockets < 14
    from websockets import connect as ws_connect

from .arcus import ArcusMarketAttributes
from .arcus_auth import (
    ArcusApiKeyRegistration,
    ArcusCredentials,
    ArcusSigner,
    build_cancel_ordersign_payload,
    build_ordersign_payload,
    validate_registered_api_key,
)
from .calibration import ARCUS_CALIBRATION_QTY

log = logging.getLogger("arcus-execution")

ARCUS_ACCOUNT_SUBSCRIPTIONS = (
    "userFills",
    "orders",
    "positions",
    "accountAttributeUpdates",
)
ARCUS_TERMINAL_ORDER_STATUSES = frozenset(
    (
        "FILLED",
        "CANCELED",
        "MARGIN_CANCELED",
        "REJECTED",
        "TPSL_CANCELED",
        "TPSL_TRIGGERED",
    )
)
REST_TIMEOUT = 10.0
GOOD_TIL_TIME_DAYS = 32


class ArcusOrderError(RuntimeError):
    """Raised when a maker-only Arcus operation cannot be submitted safely."""


_REJECTION_DETAIL_LIMIT = 256
_SENSITIVE_REJECTION_DETAIL = re.compile(
    r"(?i)(api[\s_-]*key|private[\s_-]*key|signature|secret|passphrase|"
    r"signed[\s_-]*(?:request|payload))[\"']?\s*(?:=|:|\s)\s*[\"']?"
    r"[^,\s;\}\]\"']+[\"']?"
)
_UNSANITIZED_SENSITIVE_REJECTION_DETAIL = re.compile(
    r"(?i)(api[\s_-]*key|private[\s_-]*key|signature|secret|passphrase|"
    r"signed[\s_-]*(?:request|payload))[\"']?\s*(?:=|:|\s)\s*[\"']?"
    r"(?!<redacted>)[^,\s;\}\]\"']+"
)


def _sanitize_rejection_detail(value: Any) -> str | None:
    if value is None or not isinstance(value, (str, int, float, bool, Decimal)):
        return None
    detail = str(value).strip()
    if not detail:
        return None
    detail = _SENSITIVE_REJECTION_DETAIL.sub(
        lambda match: f"{match.group(1)}=<redacted>", detail
    )
    if _UNSANITIZED_SENSITIVE_REJECTION_DETAIL.search(detail):
        detail = "<redacted sensitive server detail>"
    return detail[:_REJECTION_DETAIL_LIMIT]


def _order_rejection_details(
    response: Mapping[str, Any],
) -> tuple[str | None, str | None]:
    sources: list[Mapping[str, Any]] = [response]
    scalar_error = response.get("error")
    for nested_key in ("error", "result", "data"):
        nested = response.get(nested_key)
        if isinstance(nested, Mapping):
            sources.append(nested)

    code = None
    message = _sanitize_rejection_detail(scalar_error)
    for source in sources:
        if code is None:
            for key in ("code", "errorCode", "error_code", "errorCodeString"):
                code = _sanitize_rejection_detail(source.get(key))
                if code is not None:
                    break
        if message is None:
            for key in (
                "message",
                "errorMessage",
                "error_message",
                "reason",
                "detail",
            ):
                message = _sanitize_rejection_detail(source.get(key))
                if message is not None:
                    break
        if code is not None and message is not None:
            break
    return code, message


class ArcusOrderRejected(ArcusOrderError):
    """Raised when Arcus explicitly rejects a placeOrder request."""

    def __init__(
        self,
        *,
        status: int,
        code: str | None = None,
        message: str | None = None,
    ) -> None:
        self.status = status
        self.code = _sanitize_rejection_detail(code)
        self.message = _sanitize_rejection_detail(message)
        details = [f"status={status}"]
        if self.code is not None:
            details.append(f"code={self.code}")
        if self.message is not None:
            details.append(f"message={self.message}")
        super().__init__(f"Arcus placeOrder rejected: {' '.join(details)}")


class ArcusAloWouldCross(ArcusOrderError):
    """Raised locally when an ALO quote would remove liquidity."""


class ArcusRateLimited(RuntimeError):
    """Raised when an Arcus REST request is temporarily rate limited."""

    def __init__(self, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__("Arcus REST request rate limited (HTTP 429)")


def _decimal(value: Any, field: str, *, allow_none: bool = False) -> Decimal | None:
    if value is None and allow_none:
        return None
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal")
    return result


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _retry_after_seconds(value: Any) -> float | None:
    if value is None:
        return None
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            delay = parsedate_to_datetime(str(value)).timestamp() - time.time()
        except (IndexError, TypeError, ValueError, OverflowError):
            return None
    if not math.isfinite(delay) or delay < 0:
        return None
    return delay


def _integer(
    value: Any,
    field: str,
    *,
    allow_none: bool = True,
    allow_negative: bool = False,
) -> int | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if result < 0 and not allow_negative:
        raise ValueError(f"{field} must be >= 0")
    return result


@dataclass(frozen=True)
class ArcusUserFill:
    trade_id: str
    order_id: str
    client_id: str | None
    market_id: int | None
    market_display_name: str | None
    side: str
    price: Decimal
    quantity: Decimal
    fee: Decimal | None
    # The current public userFills example omits store-only fields such as
    # createdAt/fee, even though REST /v1/fills provides them.  Keep these
    # optional so a live fill can still trigger the immediate hedge; the B0
    # controller reconciles missing fee data from the public REST history.
    created_at_us: int | None
    sequence_number: int | None
    is_snapshot: bool
    local_receive_ts_ms: int | None = None
    local_receive_monotonic_ns: int | None = None


def _fill_from_content(
    content: Mapping[str, Any],
    *,
    message: Mapping[str, Any],
    is_snapshot: bool,
) -> ArcusUserFill:
    # Arcus account snapshots wrap the rows in ``contents.fills``.  The row
    # schema itself uses fillPrice/fillSize; keep the older aliases only for
    # recorded fixtures and REST compatibility.  An explicitly present null
    # canonical value must remain invalid rather than silently falling back.
    price_value = (
        content["fillPrice"] if "fillPrice" in content else content.get("price")
    )
    quantity_value = (
        content["fillSize"]
        if "fillSize" in content
        else content.get("size", content.get("quantity"))
    )
    price = _decimal(price_value, "fill.price")
    quantity = _decimal(quantity_value, "fill.size")
    assert price is not None and quantity is not None
    if price <= 0 or quantity <= 0:
        raise ValueError("fill price and size must be > 0")
    side = str(content.get("side", "")).upper()
    if side not in ("BUY", "SELL"):
        raise ValueError("fill.side must be BUY or SELL")
    created_at = _integer(
        content.get("createdAt", content.get("timestamp")),
        "fill.createdAt",
    )
    market_id = _integer(
        content.get("marketId", message.get("marketId")), "fill.marketId"
    )
    return ArcusUserFill(
        trade_id=_optional_text(content.get("tradeId")) or "",
        order_id=_optional_text(content.get("orderId")) or "",
        client_id=_optional_text(content.get("clientId")),
        market_id=market_id,
        market_display_name=_optional_text(
            content.get(
                "market", content.get("marketDisplayName", message.get("market"))
            )
        ),
        side=side,
        price=price,
        quantity=quantity,
        fee=_decimal(content.get("fee"), "fill.fee", allow_none=True),
        created_at_us=created_at,
        sequence_number=_integer(content.get("sequenceNumber"), "fill.sequenceNumber"),
        is_snapshot=is_snapshot,
    )


def parse_arcus_user_fill(message: Mapping[str, Any]) -> ArcusUserFill | None:
    """Parse one official Arcus userFills frame.

    A snapshot may contain a list in deployments that expose the snapshot as
    an array.  Use :func:`parse_arcus_user_fills` for that form; this helper
    returns the single object form and ``None`` for an empty snapshot.
    """
    fills = parse_arcus_user_fills(message)
    return fills[0] if fills else None


def _user_fill_rows(
    message: Mapping[str, Any],
) -> tuple[list[Any], bool]:
    if message.get("channel") != "userFills":
        raise ValueError("message is not userFills")
    contents = message.get("contents")
    if contents is None:
        return [], bool(message.get("type") == "subscribed")
    if isinstance(contents, Mapping):
        # Current Arcus subscribe snapshots use this wrapper.  Live
        # channel_data frames continue to carry one fill object directly.
        if "fills" in contents:
            rows = contents["fills"]
            if not isinstance(rows, list):
                raise ValueError("userFills.contents.fills must be an array")
            return rows, bool(
                message.get("type") == "subscribed"
                or contents.get("isSnapshot") is True
            )
        return [contents], bool(
            message.get("type") == "subscribed" or contents.get("isSnapshot") is True
        )
    if isinstance(contents, list):
        return contents, bool(message.get("type") == "subscribed")
    raise ValueError("userFills.contents must be an object or array")


def _parse_arcus_user_fills(
    message: Mapping[str, Any],
    *,
    tolerate_snapshot_errors: bool,
) -> tuple[list[ArcusUserFill], int]:
    rows, is_snapshot = _user_fill_rows(message)
    result = []
    skipped = 0
    for index, item in enumerate(rows):
        try:
            if not isinstance(item, Mapping):
                raise ValueError("userFills.contents entries must be objects")
            result.append(
                _fill_from_content(item, message=message, is_snapshot=is_snapshot)
            )
        except ValueError as exc:
            if not (is_snapshot and tolerate_snapshot_errors):
                raise
            skipped += 1
            fields = (
                ",".join(sorted(str(key) for key in item))
                if isinstance(item, Mapping)
                else "<non-object>"
            )
            log.warning(
                "[ARCUS] skipped malformed historical userFills row "
                "index=%d fields=%s error=%s",
                index,
                fields,
                exc,
            )
    return result, skipped


def parse_arcus_user_fills(
    message: Mapping[str, Any], *, tolerate_snapshot_errors: bool = False
) -> list[ArcusUserFill]:
    fills, _ = _parse_arcus_user_fills(
        message, tolerate_snapshot_errors=tolerate_snapshot_errors
    )
    return fills


@dataclass(frozen=True)
class ArcusOrderUpdate:
    order_id: str
    client_id: str | None
    market_id: int | None
    market_display_name: str | None
    side: str | None
    status: str
    state: str | None
    price: Decimal | None
    original_size: Decimal | None
    remaining_size: Decimal | None
    avg_fill_price: Decimal | None
    created_at_us: int | None
    updated_at_us: int | None
    sequence_number: int | None
    is_snapshot: bool
    last_sequence_id: int | None = None
    filled_size: Decimal | None = None
    rejection_reason: str | None = None


def parse_arcus_order_update(
    message: Mapping[str, Any], content: Mapping[str, Any] | None = None
) -> ArcusOrderUpdate:
    if message.get("channel") != "orders":
        raise ValueError("message is not orders")
    raw = message.get("contents") if content is None else content
    if not isinstance(raw, Mapping):
        raise ValueError("orders.contents must be an object")
    status = str(raw.get("status", raw.get("state", ""))).upper()
    if not status:
        raise ValueError("orders.contents.status is required")
    state = raw.get("state")
    return ArcusOrderUpdate(
        order_id=str(raw.get("orderId", "")),
        client_id=_optional_text(raw.get("clientId")),
        market_id=_integer(raw.get("marketId"), "order.marketId"),
        market_display_name=_optional_text(
            raw.get("marketDisplayName", raw.get("market"))
        ),
        side=(str(raw["side"]).upper() if raw.get("side") is not None else None),
        status=status,
        state=str(state).upper() if state is not None else None,
        price=_decimal(raw.get("price"), "order.price", allow_none=True),
        original_size=_decimal(
            raw.get("originalSize"), "order.originalSize", allow_none=True
        ),
        remaining_size=_decimal(
            raw.get("remainingSize"), "order.remainingSize", allow_none=True
        ),
        avg_fill_price=_decimal(
            raw.get("avgFillPrice"), "order.avgFillPrice", allow_none=True
        ),
        created_at_us=_integer(raw.get("createdAt"), "order.createdAt"),
        updated_at_us=_integer(raw.get("updatedAt"), "order.updatedAt"),
        sequence_number=_integer(raw.get("sequenceNumber"), "order.sequenceNumber"),
        is_snapshot=bool(
            message.get("type") == "subscribed" or raw.get("isSnapshot") is True
        ),
        last_sequence_id=_integer(
            raw.get("lastSequenceId", message.get("lastSequenceId")),
            "order.lastSequenceId",
        ),
        filled_size=_decimal(
            raw.get("filledSize"), "order.filledSize", allow_none=True
        ),
        rejection_reason=_optional_text(raw.get("rejectionReason")),
    )


def is_retryable_post_only_reject(update: ArcusOrderUpdate) -> bool:
    """Return whether an order rejection is proven to have filled nothing."""

    return (
        update.status == "REJECTED"
        and update.rejection_reason == "POST_ONLY_WOULD_CROSS"
        and update.filled_size is not None
        and update.filled_size == Decimal("0")
        and update.original_size is not None
        and update.remaining_size is not None
        and update.remaining_size == update.original_size
    )


def parse_arcus_order_snapshot(
    message: Mapping[str, Any],
) -> tuple[list[ArcusOrderUpdate], list[ArcusOrderUpdate]]:
    if message.get("channel") != "orders" or message.get("type") != "subscribed":
        raise ValueError("message is not an orders snapshot")
    contents = message.get("contents")
    if not isinstance(contents, Mapping):
        raise ValueError("orders snapshot contents must be an object")
    open_orders = contents.get("openOrders") or []
    recent_closed = contents.get("recentClosedOrders") or []
    if not isinstance(open_orders, list) or not isinstance(recent_closed, list):
        raise ValueError("orders snapshot arrays are invalid")
    snapshot_message = dict(message)
    if snapshot_message.get("lastSequenceId") is None:
        snapshot_message["lastSequenceId"] = contents.get("lastSequenceId")
    return (
        [parse_arcus_order_update(snapshot_message, item) for item in open_orders],
        [parse_arcus_order_update(snapshot_message, item) for item in recent_closed],
    )


@dataclass(frozen=True)
class ArcusFeeTier:
    level: int
    name: str
    maker_fee_ppm: int
    taker_fee_ppm: int

    @property
    def maker_fee_bps(self) -> Decimal:
        return Decimal(self.maker_fee_ppm) / Decimal("100")

    @property
    def taker_fee_bps(self) -> Decimal:
        return Decimal(self.taker_fee_ppm) / Decimal("100")


def _fee_value(raw: Mapping[str, Any], camel: str, snake: str) -> Any:
    return raw.get(camel, raw.get(snake))


def parse_arcus_fee_tiers(payload: Mapping[str, Any]) -> tuple[ArcusFeeTier, ...]:
    rows = payload.get("tiers")
    if not isinstance(rows, list) or not rows:
        raise ValueError("Arcus fee tier response has no tiers")
    result = []
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise ValueError(f"fee tier {index} is not an object")
        level = _integer(
            _fee_value(raw, "level", "level"),
            f"fee tier {index}.level",
            allow_none=False,
        )
        maker = _integer(
            _fee_value(raw, "makerFeePpm", "maker_fee_ppm"),
            f"fee tier {index}.makerFeePpm",
            allow_none=False,
            allow_negative=True,
        )
        taker = _integer(
            _fee_value(raw, "takerFeePpm", "taker_fee_ppm"),
            f"fee tier {index}.takerFeePpm",
            allow_none=False,
        )
        assert level is not None and maker is not None and taker is not None
        result.append(ArcusFeeTier(level, str(raw.get("name", level)), maker, taker))
    return tuple(result)


def parse_arcus_account_fee_tier(message: Mapping[str, Any]) -> ArcusFeeTier | None:
    """Extract the account ``feeTier`` entry from account attributes."""
    contents = message.get("contents", message)
    if not isinstance(contents, Mapping):
        return None
    entries = contents.get("entries")
    if not isinstance(entries, list):
        return None
    for raw in entries:
        if not isinstance(raw, Mapping) or str(raw.get("type", "")) != "feeTier":
            continue
        # The current accountAttributeUpdates schema calls this field
        # ``feeTierLevel``.  Keep the older aliases only for recorded fixtures
        # from the initial public API rollout.
        level_value = _fee_value(raw, "feeTierLevel", "level")
        if level_value is None:
            level_value = raw.get("tier")
        maker_value = _fee_value(raw, "makerFeePpm", "maker_fee_ppm")
        taker_value = _fee_value(raw, "takerFeePpm", "taker_fee_ppm")
        if level_value is None or maker_value is None or taker_value is None:
            return None
        level = _integer(level_value, "account fee tier.level", allow_none=False)
        maker = _integer(
            maker_value,
            "account fee tier.makerFeePpm",
            allow_none=False,
            allow_negative=True,
        )
        taker = _integer(taker_value, "account fee tier.takerFeePpm", allow_none=False)
        assert level is not None and maker is not None and taker is not None
        return ArcusFeeTier(level, str(raw.get("name", level)), maker, taker)
    return None


def resolve_arcus_account_fee_tier(
    table: tuple[ArcusFeeTier, ...], account_tier: ArcusFeeTier | None
) -> ArcusFeeTier | None:
    if account_tier is None:
        return None
    for row in table:
        if row.level == account_tier.level:
            # Prefer the current exchange table rates over any stale account
            # copy, while retaining the account's resolved level.
            return row
    return None


@dataclass
class ArcusAccountState:
    startup_watermark_us: int
    calibration_client_ids: set[str] = field(default_factory=set)
    seen_trade_ids: set[str] = field(default_factory=set)
    account_sequence_id: int | None = None

    def __post_init__(self) -> None:
        if self.calibration_client_ids is None:
            self.calibration_client_ids = set()
        if self.seen_trade_ids is None:
            self.seen_trade_ids = set()

    def is_pre_start_fill(self, fill: ArcusUserFill) -> bool:
        return (
            fill.created_at_us is not None
            and fill.created_at_us <= self.startup_watermark_us
        )

    def should_hedge_fill(self, fill: ArcusUserFill) -> bool:
        if fill.trade_id and fill.trade_id in self.seen_trade_ids:
            return False
        # The streaming userFills snapshot can omit createdAt.  Without an
        # exchange timestamp there is no safe way to distinguish an old fill
        # from a new fill after a reconnect, so never trigger a non-idempotent
        # hedge from that snapshot alone.
        if fill.is_snapshot and fill.created_at_us is None:
            return False
        if self.is_pre_start_fill(fill):
            return False
        if not (
            fill.client_id in self.calibration_client_ids
            or fill.order_id in self.calibration_client_ids
        ):
            return False
        # The initial subscription snapshot is filtered by the startup
        # watermark/known calibration IDs.  On a runtime reconnect, however,
        # a snapshot may contain a fill that occurred after this session's
        # order was placed; that fill remains actionable and must not be lost.
        if fill.trade_id:
            self.seen_trade_ids.add(fill.trade_id)
        if fill.sequence_number is not None:
            self.account_sequence_id = fill.sequence_number
        return True

    def validate_startup_orders(
        self, orders: list[ArcusOrderUpdate], *, calibration_prefix: str
    ) -> list[ArcusOrderUpdate]:
        known = []
        for order in orders:
            # /v1/openOrders is expected to contain open rows, but accept all
            # non-terminal statuses so values such as UNTRIGGERED cannot be
            # silently ignored at the live safety gate.
            is_terminal = (
                order.status.upper() in ARCUS_TERMINAL_ORDER_STATUSES
                or (getattr(order, "state", None) or "").upper()
                in ARCUS_TERMINAL_ORDER_STATUSES
            )
            is_open = not is_terminal
            if not is_open:
                continue
            if order.client_id and order.client_id.startswith(calibration_prefix):
                known.append(order)
            else:
                raise RuntimeError(
                    "unknown Arcus SNDK open order; aborting without cancel"
                )
        return known

    @staticmethod
    def validate_starting_inventory(
        arcus_position: Decimal,
        rh_position: Decimal,
        *,
        tolerance: Decimal = Decimal("0"),
    ) -> None:
        if (
            abs(Decimal(arcus_position)) > tolerance
            or abs(Decimal(rh_position)) > tolerance
        ):
            raise RuntimeError("non-zero starting inventory; aborting calibration")


@dataclass(frozen=True)
class ArcusOrderAck:
    order_id: str | None
    client_id: str
    status: int


class ArcusMakerClient:
    """Signed Arcus LIMIT+ALO client; no taker or modify operation exists."""

    supports_trading = True
    supports_taker = False

    def __init__(
        self,
        *,
        credentials: ArcusCredentials,
        signer: ArcusSigner,
        rpc: Any = None,
        ws_url: str = "wss://api.arcus.xyz/v1/ws",
        client_prefix: str = "b0-",
        fixed_quantity: Decimal | None = ARCUS_CALIBRATION_QTY,
    ) -> None:
        if not client_prefix:
            raise ValueError("Arcus client prefix must not be empty")
        self.credentials = credentials
        self.signer = signer
        self.rpc = rpc
        self.ws_url = ws_url
        self.client_prefix = client_prefix
        self.fixed_quantity = (
            Decimal(fixed_quantity) if fixed_quantity is not None else None
        )
        self.orders_sent = 0
        self._known_order_ids: set[str] = set()

    @staticmethod
    def validate_calibration_order_type(order_type: str, time_in_force: str) -> None:
        if order_type.upper() != "LIMIT" or time_in_force.upper() != "ALO":
            raise ValueError("Phase B0 Arcus calibration orders must be LIMIT+ALO")

    @staticmethod
    def would_cross(
        side: str,
        price: Decimal,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
    ) -> bool:
        side = side.upper()
        if side == "BUY":
            return best_ask is not None and price >= best_ask
        if side == "SELL":
            return best_bid is not None and price <= best_bid
        raise ValueError("side must be BUY or SELL")

    def _require_rpc(self):
        if self.rpc is None:
            raise ArcusOrderError("Arcus account websocket is not connected")
        return self.rpc

    async def place_alo(
        self,
        *,
        market_id: int,
        side: str,
        price: Decimal,
        quantity: Decimal,
        tick_size: Decimal,
        step_size: Decimal,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
        client_id: str,
        reduce_only: bool = False,
    ) -> ArcusOrderAck:
        self.validate_calibration_order_type("LIMIT", "ALO")
        side = side.upper()
        if not client_id.startswith(self.client_prefix):
            raise ArcusOrderError(
                f"Arcus clientId must use the {self.client_prefix} prefix"
            )
        if self.fixed_quantity is not None and Decimal(quantity) != self.fixed_quantity:
            if self.client_prefix == "b0-":
                raise ArcusOrderError(
                    f"B0 Arcus quantity is fixed at {ARCUS_CALIBRATION_QTY} SNDK"
                )
            raise ArcusOrderError(f"Arcus quantity is fixed at {self.fixed_quantity}")
        if self.would_cross(side, price, best_bid, best_ask):
            raise ArcusAloWouldCross(
                "Arcus ALO quote would cross current BBO; no taker fallback"
            )
        timestamp_ns = time.time_ns()
        good_til_time_us = (timestamp_ns // 1000) + GOOD_TIL_TIME_DAYS * 86_400_000_000
        signed = build_ordersign_payload(
            self.credentials,
            market_id=market_id,
            side=side,
            price=Decimal(price),
            quantity=Decimal(quantity),
            tick_size=Decimal(tick_size),
            step_size=Decimal(step_size),
            timestamp_ns=timestamp_ns,
            good_til_time_us=good_til_time_us,
            client_id=client_id,
            reduce_only=reduce_only,
        )
        body = {
            "address": self.credentials.account_address,
            "accountIndex": self.credentials.account_index,
            "marketId": market_id,
            "orderSide": side,
            "orderType": "LIMIT",
            "quantity": str(quantity),
            "price": str(price),
            "timeInForce": "ALO",
            "goodTilTime": str(good_til_time_us),
            "timestamp": timestamp_ns,
            "reduceOnly": bool(reduce_only),
            "clientId": client_id,
        }
        response = await self._require_rpc().post(
            "placeOrder", body, self.signer.sign_typed(signed), timestamp_ns
        )
        status = int(response.get("status", 0))
        if 400 <= status < 500:
            code, message = _order_rejection_details(response)
            raise ArcusOrderRejected(status=status, code=code, message=message)
        if status not in (200, 202):
            raise ArcusOrderError(f"Arcus placeOrder rejected with status={status}")
        self.orders_sent += 1
        result = response.get("result") or {}
        ack = ArcusOrderAck(
            order_id=_optional_text(result.get("orderId")),
            client_id=_optional_text(result.get("clientId")) or client_id,
            status=status,
        )
        if ack.order_id:
            self._known_order_ids.add(ack.order_id)
        return ack

    async def cancel_calibration_order(
        self,
        *,
        market_id: int | None = None,
        order_id: str | None = None,
        client_id: str | None = None,
    ) -> Mapping[str, Any]:
        if (order_id is None) == (client_id is None):
            raise ValueError("cancel requires exactly one Arcus order or client id")
        if market_id is None:
            raise ValueError("market_id is required for an Arcus cancel")
        if client_id is not None and not client_id.startswith(self.client_prefix):
            raise ArcusOrderError(
                f"Arcus refuses to cancel a non-{self.client_prefix} clientId"
            )
        if order_id is not None and order_id not in self._known_order_ids:
            raise ArcusOrderError("Phase B0 refuses to cancel an unknown Arcus orderId")
        timestamp_ns = time.time_ns()
        signed = build_cancel_ordersign_payload(
            self.credentials,
            market_id=market_id,
            timestamp_ns=timestamp_ns,
            order_id=order_id,
            client_id=client_id,
        )
        body: dict[str, Any] = {
            "address": self.credentials.account_address,
            "accountIndex": self.credentials.account_index,
            "marketId": market_id,
            "kind": "orderId" if order_id is not None else "clientId",
            "timestamp": timestamp_ns,
        }
        if order_id is not None:
            body["orderId"] = order_id
        else:
            body["clientId"] = client_id
        return await self._require_rpc().post(
            "cancelOrder", body, self.signer.sign_typed(signed), timestamp_ns
        )


Callback = Callable[..., Any]


class ArcusAccountFeed:
    """Public account stream plus the signed RPC transport used by B0."""

    def __init__(
        self,
        account_address: str,
        market_display_name: str,
        *,
        ws_url: str = "wss://api.arcus.xyz/v1/ws",
        account_index: int = 0,
        on_fill: Callback | None = None,
        on_order: Callback | None = None,
        on_attribute: Callback | None = None,
        on_disconnect: Callback | None = None,
        on_connect: Callback | None = None,
        startup_state: ArcusAccountState | None = None,
        api_key: str | None = None,
        client_prefix: str = "b0-",
        fixed_quantity: Decimal | None = ARCUS_CALIBRATION_QTY,
    ) -> None:
        if not client_prefix:
            raise ValueError("Arcus client prefix must not be empty")
        self.account_address = account_address
        self.market_display_name = market_display_name
        self.ws_url = ws_url
        self.account_index = account_index
        self.on_fill = on_fill
        self.on_order = on_order
        self.on_attribute = on_attribute
        self.on_disconnect = on_disconnect
        self.on_connect = on_connect
        self.startup_state = startup_state
        self.api_key = api_key
        self.client_prefix = client_prefix
        self.fixed_quantity = (
            Decimal(fixed_quantity) if fixed_quantity is not None else None
        )
        self.ready = asyncio.Event()
        self.healthy = False
        self.websocket: Any = None
        self._request_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self.latest_fee_tier: ArcusFeeTier | None = None
        self.latest_attributes: ArcusMarketAttributes | None = None
        # Retain both open and recent-closed updates so a cancel/fill race can
        # be reconciled after callbacks are attached or after a reconnect.
        self.latest_orders: dict[str, ArcusOrderUpdate] = {}
        self._subscribed_channels: set[str] = set()
        # A malformed frame must invalidate only its own channel.  The
        # aggregate property below remains fail-closed for B0 quoting while
        # allowing independent account channels to continue processing.
        self.channel_health: dict[str, bool] = {
            channel: False for channel in ARCUS_ACCOUNT_SUBSCRIPTIONS
        }
        self.channel_errors: dict[str, str] = {}
        self.channel_error_counts: dict[str, int] = {}

    @property
    def required_channels_healthy(self) -> bool:
        return self.ready.is_set() and all(
            self.channel_health.get(channel, False)
            for channel in ARCUS_ACCOUNT_SUBSCRIPTIONS
        )

    def _mark_channel_healthy(self, channel: str) -> None:
        self.channel_health[channel] = True
        self.channel_errors.pop(channel, None)

    def _mark_channel_error(self, channel: str, error: Exception | str) -> None:
        self.channel_health[channel] = False
        self.channel_errors[channel] = str(error)
        self.channel_error_counts[channel] = (
            self.channel_error_counts.get(channel, 0) + 1
        )
        log.warning(
            "[ARCUS] account channel error channel=%s error=%s",
            channel,
            error,
        )

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _callback(self, callback: Callback | None, *args: Any) -> None:
        if callback is None:
            return
        result = callback(*args)
        if inspect.isawaitable(result):
            await result

    async def subscribe(self, websocket) -> None:
        messages = [
            {
                "type": "subscribe",
                "channel": "userFills",
                "id": self.account_address,
                "accountIndex": self.account_index,
                "market": self.market_display_name,
                "nFills": 500,
            },
            {
                "type": "subscribe",
                "channel": "orders",
                "id": self.account_address,
                "accountIndex": self.account_index,
                "market": self.market_display_name,
                "nRecentClosed": 100,
            },
            {
                "type": "subscribe",
                "channel": "positions",
                "id": self.account_address,
                "accountIndex": self.account_index,
                "market": self.market_display_name,
            },
            {
                "type": "subscribe",
                "channel": "accountAttributeUpdates",
                "id": self.account_address,
                "accountIndex": self.account_index,
            },
        ]
        for message in messages:
            await websocket.send(json.dumps(message, separators=(",", ":")))

    async def post(
        self, method: str, payload: Mapping[str, Any], signature: str, timestamp: int
    ) -> Mapping[str, Any]:
        if method not in {"placeOrder", "cancelOrder"}:
            raise ArcusOrderError(
                "Phase B0 Arcus RPC allows only LIMIT+ALO placeOrder and "
                "same-order cancelOrder"
            )
        if method == "placeOrder":
            if (
                str(payload.get("orderType", "")).upper() != "LIMIT"
                or str(payload.get("timeInForce", "")).upper() != "ALO"
            ):
                raise ArcusOrderError("Phase B0 Arcus placeOrder requires LIMIT+ALO")
            client_id = payload.get("clientId")
            if not isinstance(client_id, str) or not client_id.startswith(
                self.client_prefix
            ):
                raise ArcusOrderError(
                    f"Arcus placeOrder requires a {self.client_prefix} clientId"
                )
            if self.fixed_quantity is not None:
                try:
                    quantity = Decimal(str(payload.get("quantity")))
                except Exception as exc:
                    raise ArcusOrderError(
                        "B0 Arcus quantity must be the fixed calibration size"
                    ) from exc
                if quantity != self.fixed_quantity:
                    raise ArcusOrderError(
                        f"B0 Arcus quantity is fixed at {ARCUS_CALIBRATION_QTY} SNDK"
                    )
        elif payload.get("clientId") is not None and not str(
            payload["clientId"]
        ).startswith(self.client_prefix):
            raise ArcusOrderError(
                f"Arcus cancel refuses a non-{self.client_prefix} clientId"
            )
        websocket = self.websocket
        if websocket is None or not self.healthy:
            raise ArcusOrderError("Arcus account websocket is not connected")
        request_id = self._next_request_id()
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        message = {
            "type": "post",
            "id": request_id,
            "request": {
                "type": method,
                "payload": dict(payload),
                "apiKey": self.api_key or "",
                "timestamp": str(timestamp),
                "signature": signature,
            },
        }
        if not self.api_key:
            self._pending.pop(request_id, None)
            raise ArcusOrderError("Arcus account feed has no API key for signed RPC")
        try:
            await websocket.send(json.dumps(message, separators=(",", ":")))
            return await asyncio.wait_for(future, timeout=REST_TIMEOUT)
        finally:
            self._pending.pop(request_id, None)

    async def _handle_message(self, message: Mapping[str, Any]) -> None:
        if message.get("type") == "degraded":
            # Arcus documents this as a connection-health signal.  Treat it
            # like a disconnect for B0: no signed mutation may proceed while
            # the account channel is degraded, and the normal reconnect path
            # must re-establish the account snapshots before reconciliation.
            self.healthy = False
            self.ready.clear()
            await self._callback(self.on_disconnect)
            websocket = self.websocket
            if websocket is not None:
                close = getattr(websocket, "close", None)
                if close is not None:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
            return
        if message.get("type") == "subscribed":
            channel = message.get("channel")
            if isinstance(channel, str):
                self._subscribed_channels.add(channel)
                if set(ARCUS_ACCOUNT_SUBSCRIPTIONS) <= self._subscribed_channels:
                    self.ready.set()
        request_id = message.get("id")
        if (
            isinstance(request_id, int)
            and request_id in self._pending
            and ("status" in message or "result" in message or "error" in message)
        ):
            future = self._pending[request_id]
            if not future.done():
                future.set_result(message)
            return

        channel = message.get("channel")
        if not isinstance(channel, str) or channel not in ARCUS_ACCOUNT_SUBSCRIPTIONS:
            return
        try:
            message_type = message.get("type")
            if message_type not in ("subscribed", "channel_data"):
                raise ValueError(f"unsupported account frame type={message_type!s}")
            if channel == "userFills":
                receive_ms = int(time.time() * 1000)
                receive_ns = time.monotonic_ns()
                is_snapshot = bool(message.get("type") == "subscribed")
                fills, skipped = _parse_arcus_user_fills(
                    message,
                    # Historical snapshot rows can be malformed without
                    # being actionable.  New fills remain strict and fail
                    # closed so no unsafe hedge can be triggered.
                    tolerate_snapshot_errors=is_snapshot,
                )
                for fill in fills:
                    fill = replace(
                        fill,
                        local_receive_ts_ms=receive_ms,
                        local_receive_monotonic_ns=receive_ns,
                    )
                    if (
                        self.startup_state is not None
                        and fill.sequence_number is not None
                    ):
                        self.startup_state.account_sequence_id = fill.sequence_number
                    await self._callback(self.on_fill, fill)
                if skipped:
                    self._mark_channel_error(
                        channel,
                        f"skipped {skipped} malformed historical snapshot row(s)",
                    )
                else:
                    self._mark_channel_healthy(channel)
                return
            if channel == "orders":
                if message.get("type") == "subscribed":
                    open_orders, closed_orders = parse_arcus_order_snapshot(message)
                    for order in [*open_orders, *closed_orders]:
                        self._remember_order(order)
                        await self._callback(self.on_order, order)
                elif message.get("type") == "channel_data":
                    order = parse_arcus_order_update(message)
                    self._remember_order(order)
                    await self._callback(self.on_order, order)
                self._mark_channel_healthy(channel)
                return
            if channel == "positions":
                # Positions are consumed by the authenticated REST startup
                # gate; the account stream still participates in the health
                # gate and must remain independently alive.
                self._mark_channel_healthy(channel)
                return
            if channel == "accountAttributeUpdates":
                tier = parse_arcus_account_fee_tier(message)
                if tier is not None:
                    self.latest_fee_tier = tier
                await self._callback(self.on_attribute, message)
                self._mark_channel_healthy(channel)
                return
        except Exception as exc:
            self._mark_channel_error(channel, exc)

    def _remember_order(self, order: ArcusOrderUpdate) -> None:
        for value in (order.order_id, order.client_id):
            if value:
                self.latest_orders[str(value)] = order

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with ws_connect(
                    self.ws_url,
                    max_size=2**23,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    self.websocket = websocket
                    self.healthy = True
                    self.ready.clear()
                    self._subscribed_channels.clear()
                    self.channel_health = {
                        channel: False for channel in ARCUS_ACCOUNT_SUBSCRIPTIONS
                    }
                    self.channel_errors.clear()
                    await self.subscribe(websocket)
                    log.info("[ARCUS] account websocket connected")
                    await self._callback(self.on_connect)
                    async for raw in websocket:
                        backoff = 1.0
                        await self._handle_message(json.loads(raw))
                        if stop.is_set():
                            break
                    self.healthy = False
                    self.ready.clear()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.healthy = False
                self.ready.clear()
                log.warning("[ARCUS] account websocket error: %s", exc)
            finally:
                self.websocket = None
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(
                            ArcusOrderError("Arcus account websocket disconnected")
                        )
                self._pending.clear()
                if self.on_disconnect is not None:
                    await self._callback(self.on_disconnect)
            if stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)


class ArcusAccountRest:
    """Public account-state reads used by the startup safety gate."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        rest_url: str = "https://api.arcus.xyz",
    ) -> None:
        self.session = session
        self.rest_url = rest_url.rstrip("/")

    async def get(self, path: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        async with self.session.get(
            self.rest_url + path,
            params=dict(params),
            timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT),
        ) as response:
            if response.status == 429:
                raise ArcusRateLimited(
                    _retry_after_seconds(response.headers.get("Retry-After"))
                )
            response.raise_for_status()
            payload = await response.json()
        if not isinstance(payload, Mapping):
            raise RuntimeError(f"Arcus {path} returned a non-object response")
        return payload

    async def validate_api_key_registration(
        self, credentials: ArcusCredentials, signer: ArcusSigner
    ) -> ArcusApiKeyRegistration:
        """Verify the existing signer is active for this wallet/subaccount."""

        payload = await self.get(
            "/v1/apiKeys",
            {"address": credentials.account_address},
        )
        return validate_registered_api_key(
            payload,
            derived_public_key=signer.public_key_hex,
            runtime_account_index=credentials.account_index,
        )

    async def fee_tiers(self) -> tuple[ArcusFeeTier, ...]:
        return parse_arcus_fee_tiers(await self.get("/v1/feetiers", {}))

    async def open_orders(
        self, address: str, market: str, account_index: int
    ) -> list[ArcusOrderUpdate]:
        payload = await self.get(
            "/v1/openOrders",
            {"address": address, "market": market, "accountIndex": account_index},
        )
        rows = payload.get("openOrders", payload.get("orders", []))
        if not isinstance(rows, list):
            raise RuntimeError("Arcus open orders response has no array")
        message = {
            "channel": "orders",
            "type": "subscribed",
            "lastSequenceId": payload.get(
                "lastSequenceId",
                (payload.get("contents") or {}).get("lastSequenceId")
                if isinstance(payload.get("contents"), Mapping)
                else None,
            ),
        }
        return [parse_arcus_order_update(message, row) for row in rows]

    async def fills(
        self,
        address: str,
        market: str,
        account_index: int,
        *,
        from_us: int | None = None,
    ) -> list[ArcusUserFill]:
        """Read recent fills for reconnect/cancel-race reconciliation.

        Arcus returns newest-first.  The controller sorts the parsed rows
        before dispatching them so a replay observes fills in causal order.
        This is a read-only endpoint; it is never used to place or cancel an
        order.
        """
        params: dict[str, Any] = {
            "address": address,
            "market": market,
            "accountIndex": account_index,
            "limit": 1000,
        }
        if from_us is not None:
            params["from"] = int(from_us)
        payload = await self.get("/v1/fills", params)
        rows = payload.get("fills", [])
        if not isinstance(rows, list):
            raise RuntimeError("Arcus fills response has no array")
        result: list[ArcusUserFill] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise RuntimeError("Arcus fills response contains a non-object row")
            fill = _fill_from_content(
                row,
                message={"channel": "userFills", "market": market},
                is_snapshot=False,
            )
            result.append(
                replace(
                    fill,
                    local_receive_ts_ms=time.time_ns() // 1_000_000,
                    local_receive_monotonic_ns=time.monotonic_ns(),
                )
            )
        return result

    async def position(self, address: str, market: str, account_index: int) -> Decimal:
        payload = await self.get(
            "/v1/positions",
            {"address": address, "market": market, "accountIndex": account_index},
        )
        rows = payload.get("positions", {})
        if isinstance(rows, Mapping):
            position_rows = list(rows.values())
        elif isinstance(rows, list):  # compatibility with an older response
            position_rows = rows
        else:
            raise RuntimeError("Arcus positions response has no object")
        for row in position_rows:
            if not isinstance(row, Mapping):
                continue
            row_market = row.get("market") or row.get("marketDisplayName")
            if row_market is not None and str(row_market).upper() != market.upper():
                continue
            # The request is market-scoped.  Numeric-only rows are therefore
            # still safe to use when the server omits its display ticker.
            size = _decimal(row.get("size", "0"), "position.size")
            assert size is not None
            if row.get("side") == "SHORT" and size > 0:
                size = -size
            return size
        return Decimal("0")

    async def account(self, address: str, account_index: int) -> Mapping[str, Any]:
        return await self.get(
            "/v1/account", {"address": address, "accountIndex": account_index}
        )
