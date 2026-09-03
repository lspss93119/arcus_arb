"""Typed parsers for the Arcus public market-data API.

Phase A deliberately contains only public market discovery and streaming
market-data models.  No credential, signing, or order-routing model belongs in
this module.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional

ARCUS_REST_URL = "https://api.arcus.xyz"
ARCUS_WS_URL = "wss://api.arcus.xyz/v1/ws"
ARCUS_PUBLIC_SUBSCRIPTIONS = (
    "l2OrderbookUpdates",
    "trades",
    "marketAttributes",
)
ARCUS_BOOK_LEVELS = 20


class ArcusMarketDataError(ValueError):
    """Raised when an Arcus public payload cannot be used safely."""


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ArcusMarketDataError(f"{path} must be an object")
    return value


def _required(mapping: Mapping[str, Any], key: str, path: str) -> Any:
    if key not in mapping:
        raise ArcusMarketDataError(f"{path}.{key} is required")
    return mapping[key]


def _string(mapping: Mapping[str, Any], key: str, path: str) -> str:
    value = _required(mapping, key, path)
    if not isinstance(value, str) or not value.strip():
        raise ArcusMarketDataError(f"{path}.{key} must be a non-empty string")
    return value


def _optional_string(mapping: Mapping[str, Any], key: str) -> Optional[str]:
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ArcusMarketDataError(f"{key} must be a string or null")
    return value


def _decimal_text(value: Any, path: str, *, positive: bool = False,
                  nonnegative: bool = False) -> str:
    if isinstance(value, bool) or value is None:
        raise ArcusMarketDataError(f"{path} must be a decimal string")
    if not isinstance(value, (str, int, float)):
        raise ArcusMarketDataError(f"{path} must be a decimal string")
    text = str(value)
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError):
        raise ArcusMarketDataError(f"{path} is not a valid decimal: {value!r}")
    if not number.is_finite():
        raise ArcusMarketDataError(f"{path} must be finite")
    if positive and number <= 0:
        raise ArcusMarketDataError(f"{path} must be > 0")
    if nonnegative and number < 0:
        raise ArcusMarketDataError(f"{path} must be >= 0")
    return text


def _optional_decimal(mapping: Mapping[str, Any], key: str) -> Optional[str]:
    value = mapping.get(key)
    if value is None:
        return None
    return _decimal_text(value, key)


def _integer(value: Any, path: str, *, optional: bool = False) -> Optional[int]:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ArcusMarketDataError(f"{path} must be an integer")
    if value < 0:
        raise ArcusMarketDataError(f"{path} must be >= 0")
    return value


def _optional_bool(mapping: Mapping[str, Any], key: str) -> Optional[bool]:
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ArcusMarketDataError(f"{key} must be boolean or null")
    return value


@dataclass(frozen=True)
class ArcusMarketMetadata:
    market_id: int
    symbol: str
    base_asset: str
    quote_asset: str
    tick_size: str
    step_size: str
    status: str
    min_order_notional: Optional[str]
    min_order_size: Optional[str]
    max_order_size: Optional[str]
    tick_tiers: tuple[Mapping[str, Any], ...]
    is_outside_rth: Optional[bool]
    current_settlement_price: Optional[str]
    upper_trading_bound: Optional[str]
    lower_trading_bound: Optional[str]
    next_upper_trading_bound: Optional[str]
    next_lower_trading_bound: Optional[str]
    regular_trading_hours: Optional[Mapping[str, Any]]
    raw: Mapping[str, Any]


def parse_arcus_market(raw: Mapping[str, Any]) -> ArcusMarketMetadata:
    """Parse one item from the public ``GET /v1/markets`` response."""
    item = _mapping(raw, "market")
    market_id = _integer(_required(item, "marketId", "market"), "market.marketId")
    assert market_id is not None
    tiers = item.get("tickTiers") or []
    if not isinstance(tiers, list):
        raise ArcusMarketDataError("market.tickTiers must be an array")
    copied_tiers: list[Mapping[str, Any]] = []
    for index, tier in enumerate(tiers):
        copied_tiers.append(dict(_mapping(tier, f"market.tickTiers[{index}]")))
    rth = item.get("regularTradingHours")
    if rth is not None:
        rth = dict(_mapping(rth, "market.regularTradingHours"))
    return ArcusMarketMetadata(
        market_id=market_id,
        symbol=_string(item, "marketDisplayName", "market"),
        base_asset=_string(item, "baseAsset", "market"),
        quote_asset=_string(item, "quoteAsset", "market"),
        tick_size=_decimal_text(
            _required(item, "tickSize", "market"), "market.tickSize", positive=True
        ),
        step_size=_decimal_text(
            _required(item, "stepSize", "market"), "market.stepSize", positive=True
        ),
        status=_string(item, "status", "market").upper(),
        min_order_notional=_optional_decimal(item, "minOrderNotional"),
        min_order_size=_optional_decimal(item, "minOrderSize"),
        max_order_size=_optional_decimal(item, "maxOrderSize"),
        tick_tiers=tuple(copied_tiers),
        is_outside_rth=_optional_bool(item, "isOutsideRth"),
        current_settlement_price=_optional_decimal(item, "currentSettlementPrice"),
        upper_trading_bound=_optional_decimal(item, "upperTradingBound"),
        lower_trading_bound=_optional_decimal(item, "lowerTradingBound"),
        next_upper_trading_bound=_optional_decimal(item, "nextUpperTradingBound"),
        next_lower_trading_bound=_optional_decimal(item, "nextLowerTradingBound"),
        regular_trading_hours=rth,
        raw=dict(item),
    )


def resolve_arcus_market(
    markets: Iterable[Mapping[str, Any]], requested_symbol: str
) -> ArcusMarketMetadata:
    """Resolve a CLI symbol against Arcus metadata without precision constants."""
    requested = requested_symbol.strip().upper()
    if not requested:
        raise ArcusMarketDataError("Arcus symbol must not be empty")
    parsed = [parse_arcus_market(item) for item in markets]
    exact = [m for m in parsed if m.symbol.upper() == requested]
    if not exact:
        exact = [m for m in parsed if m.base_asset.upper() == requested]
    if not exact:
        raise ArcusMarketDataError(
            f"Arcus market {requested_symbol!r} was not found in public metadata"
        )
    if len(exact) > 1:
        names = ", ".join(m.symbol for m in exact)
        raise ArcusMarketDataError(
            f"Arcus symbol {requested_symbol!r} is ambiguous: {names}"
        )
    market = exact[0]
    if market.status != "ONLINE":
        raise ArcusMarketDataError(
            f"Arcus market {market.symbol} status={market.status}; refusing feed"
        )
    return market


@dataclass(frozen=True)
class ArcusBookSnapshot:
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]
    last_sequence_id: int
    global_sequence_id: Optional[int]
    exchange_timestamp_us: Optional[int]


@dataclass(frozen=True)
class ArcusBookUpdate:
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]
    last_sequence_id: int
    global_sequence_id: int
    exchange_timestamp_us: Optional[int]


def _levels(value: Any, path: str) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        raise ArcusMarketDataError(f"{path} must be an array")
    result: list[tuple[str, str]] = []
    for index, level in enumerate(value):
        if not isinstance(level, (list, tuple)) or len(level) != 2:
            raise ArcusMarketDataError(f"{path}[{index}] must be [price, size]")
        price = _decimal_text(level[0], f"{path}[{index}][0]", positive=True)
        size = _decimal_text(
            level[1], f"{path}[{index}][1]", nonnegative=True
        )
        result.append((price, size))
    return tuple(result)


def _book_contents(message: Mapping[str, Any], expected_type: str) -> Mapping[str, Any]:
    if message.get("type") != expected_type:
        raise ArcusMarketDataError(
            f"l2OrderbookUpdates message type must be {expected_type!r}"
        )
    if message.get("channel") != "l2OrderbookUpdates":
        raise ArcusMarketDataError("message is not l2OrderbookUpdates")
    return _mapping(_required(message, "contents", "message"), "message.contents")


def parse_arcus_book_snapshot(message: Mapping[str, Any]) -> ArcusBookSnapshot:
    contents = _book_contents(message, "subscribed")
    sequence = _integer(
        _required(contents, "lastSequenceId", "message.contents"),
        "message.contents.lastSequenceId",
    )
    assert sequence is not None
    global_sequence = _integer(
        contents.get("globalSequenceId"),
        "message.contents.globalSequenceId",
        optional=True,
    )
    timestamp = _integer(
        contents.get("timestamp"), "message.contents.timestamp", optional=True
    )
    return ArcusBookSnapshot(
        bids=_levels(_required(contents, "bids", "message.contents"), "bids"),
        asks=_levels(_required(contents, "asks", "message.contents"), "asks"),
        last_sequence_id=sequence,
        global_sequence_id=global_sequence,
        exchange_timestamp_us=timestamp,
    )


def parse_arcus_book_update(message: Mapping[str, Any]) -> ArcusBookUpdate:
    contents = _book_contents(message, "channel_data")
    sequence = _integer(
        _required(contents, "lastSequenceId", "message.contents"),
        "message.contents.lastSequenceId",
    )
    global_sequence = _integer(
        _required(contents, "globalSequenceId", "message.contents"),
        "message.contents.globalSequenceId",
    )
    assert sequence is not None and global_sequence is not None
    timestamp = _integer(
        contents.get("timestamp"), "message.contents.timestamp", optional=True
    )
    return ArcusBookUpdate(
        bids=_levels(_required(contents, "bids", "message.contents"), "bids"),
        asks=_levels(_required(contents, "asks", "message.contents"), "asks"),
        last_sequence_id=sequence,
        global_sequence_id=global_sequence,
        exchange_timestamp_us=timestamp,
    )


@dataclass(frozen=True)
class ArcusTrade:
    market_id: Optional[int]
    market_display_name: Optional[str]
    taker_order_id: Optional[str]
    maker_order_id: Optional[str]
    taker_address: Optional[str]
    maker_address: Optional[str]
    trade_id: Optional[str]
    exchange_timestamp_us: int
    price: str
    quantity: str
    aggressor_side: Optional[str]
    sequence_number: int


def _optional_text(item: Mapping[str, Any], key: str) -> Optional[str]:
    value = item.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ArcusMarketDataError(f"trade.{key} must be a string or null")
    return value


def parse_arcus_trades(message: Mapping[str, Any]) -> list[ArcusTrade]:
    """Parse a public trades frame; missing aggressor side remains ``None``."""
    if message.get("channel") != "trades":
        raise ArcusMarketDataError("message is not trades")
    if message.get("type") == "subscribed":
        return []
    if message.get("type") != "channel_data":
        raise ArcusMarketDataError("trades message must be channel_data")
    contents = _required(message, "contents", "message")
    if not isinstance(contents, list):
        raise ArcusMarketDataError("message.contents must be an array")
    market_id = _integer(message.get("marketId"), "message.marketId", optional=True)
    market_name = message.get("marketDisplayName")
    if market_name is not None and not isinstance(market_name, str):
        raise ArcusMarketDataError("message.marketDisplayName must be a string")
    result: list[ArcusTrade] = []
    for index, raw in enumerate(contents):
        item = _mapping(raw, f"message.contents[{index}]")
        timestamp = _integer(
            _required(item, "timestamp", f"message.contents[{index}]"),
            f"message.contents[{index}].timestamp",
        )
        sequence = _integer(
            _required(item, "sequenceNumber", f"message.contents[{index}]"),
            f"message.contents[{index}].sequenceNumber",
        )
        assert timestamp is not None and sequence is not None
        result.append(
            ArcusTrade(
                market_id=market_id,
                market_display_name=market_name,
                taker_order_id=_optional_text(item, "takerOrderId"),
                maker_order_id=_optional_text(item, "makerOrderId"),
                taker_address=_optional_text(item, "takerAddress"),
                maker_address=_optional_text(item, "makerAddress"),
                trade_id=_optional_text(item, "tradeId"),
                exchange_timestamp_us=timestamp,
                price=_decimal_text(item.get("price"), "trade.price", positive=True),
                quantity=_decimal_text(item.get("size"), "trade.size", positive=True),
                aggressor_side=(
                    _optional_text(item, "aggressorSide")
                    or _optional_text(item, "side")
                ),
                sequence_number=sequence,
            )
        )
    return result


@dataclass(frozen=True)
class ArcusMarketAttributes:
    is_snapshot: bool
    market_id: int
    market_display_name: str
    off_hours_initial_margin_fraction: Optional[str]
    is_outside_rth: Optional[bool]
    current_settlement_price: Optional[str]
    upper_trading_bound: Optional[str]
    lower_trading_bound: Optional[str]
    next_upper_trading_bound: Optional[str]
    next_lower_trading_bound: Optional[str]
    is_upper_in_expansion_zone: Optional[bool]
    is_lower_in_expansion_zone: Optional[bool]
    upper_zone_entered_at: Optional[int]
    upper_expected_expansion_at: Optional[int]
    lower_zone_entered_at: Optional[int]
    lower_expected_expansion_at: Optional[int]
    bound_event: Optional[str]
    bound_side: Optional[str]
    event_timestamp_us: Optional[int]
    market_sequence_num: Optional[int]


def _attribute_entry(
    raw: Mapping[str, Any], is_snapshot: bool
) -> ArcusMarketAttributes:
    item = _mapping(raw, "marketAttributes.entry")
    market_id = _integer(_required(item, "marketId", "entry"), "entry.marketId")
    assert market_id is not None
    def optional_time(key: str) -> Optional[int]:
        return _integer(item.get(key), f"entry.{key}", optional=True)

    return ArcusMarketAttributes(
        is_snapshot=is_snapshot,
        market_id=market_id,
        market_display_name=_string(item, "marketDisplayName", "entry"),
        off_hours_initial_margin_fraction=_optional_decimal(
            item, "offHoursInitialMarginFraction"
        ),
        is_outside_rth=_optional_bool(item, "isOutsideRth"),
        current_settlement_price=_optional_decimal(item, "currentSettlementPrice"),
        upper_trading_bound=_optional_decimal(item, "upperTradingBound"),
        lower_trading_bound=_optional_decimal(item, "lowerTradingBound"),
        next_upper_trading_bound=_optional_decimal(item, "nextUpperTradingBound"),
        next_lower_trading_bound=_optional_decimal(item, "nextLowerTradingBound"),
        is_upper_in_expansion_zone=_optional_bool(item, "isUpperInExpansionZone"),
        is_lower_in_expansion_zone=_optional_bool(item, "isLowerInExpansionZone"),
        upper_zone_entered_at=optional_time("upperZoneEnteredAt"),
        upper_expected_expansion_at=optional_time("upperExpectedExpansionAt"),
        lower_zone_entered_at=optional_time("lowerZoneEnteredAt"),
        lower_expected_expansion_at=optional_time("lowerExpectedExpansionAt"),
        bound_event=_optional_text(item, "boundEvent"),
        bound_side=_optional_text(item, "boundSide"),
        event_timestamp_us=optional_time("timestamp"),
        market_sequence_num=optional_time("marketSequenceNum"),
    )


def parse_arcus_market_attributes(
    message: Mapping[str, Any], market_id: int
) -> Optional[ArcusMarketAttributes]:
    """Parse the target entry from the global market-attributes channel."""
    if message.get("channel") != "marketAttributes":
        raise ArcusMarketDataError("message is not marketAttributes")
    if message.get("type") not in ("subscribed", "channel_data"):
        raise ArcusMarketDataError("marketAttributes message type is invalid")
    contents = _mapping(_required(message, "contents", "message"), "message.contents")
    is_snapshot = contents.get("isSnapshot")
    if not isinstance(is_snapshot, bool):
        raise ArcusMarketDataError("marketAttributes.contents.isSnapshot must be boolean")
    entries = contents.get("entries")
    if not isinstance(entries, list):
        raise ArcusMarketDataError("marketAttributes.contents.entries must be an array")
    for entry in entries:
        parsed = _attribute_entry(_mapping(entry, "entry"), is_snapshot)
        if parsed.market_id == market_id:
            return parsed
    return None
