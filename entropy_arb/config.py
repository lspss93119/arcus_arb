"""Configuration for the Arcus × Lighter-RH runtime.

The split is deliberate: config.yaml contains analysis/recording settings and
is safe to share/commit as an example; record-only Phase A needs no environment
secrets; B0 loads its live credentials separately and only behind explicit CLI
gates. Which market pair to observe is stated explicitly on every start
(--symbol, --hedge). Every YAML key is validated against the schema below, so a
typo is an error rather than a setting that silently does nothing.

Strategy model:

    premium_bps = (arcus_price / hedge_price - 1) * 10_000

    stable_basis:
        SELL Arcus / BUY hedge fires when executable premium
            >= center_bps + upper_bps
        BUY Arcus / SELL hedge fires when executable premium
            <= center_bps - lower_bps

    Both hurdles are net of both venues' taker fees, so a full round trip
    nets >= (upper_bps + lower_bps) after fees by construction.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

HL_API_URL = "https://api.hyperliquid.xyz"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"   # official ws — the only HL feed used
ARCUS_REST_URL = "https://api.arcus.xyz"
ARCUS_WS_URL = "wss://api.arcus.xyz/v1/ws"

HEDGE_VENUES = ("lighter", "lighter-rh", "tradexyz")
DEFAULT_RECORDER_DATABASE = "data/market-history.sqlite"


@dataclass(frozen=True)
class LighterProfile:
    name: str
    api_url: str
    ws_url: str
    chain_id: int


# Endpoint profiles for the two supported zkLighter deployments (these match
# lighter-python's lighter.endpoint_profiles, duplicated here so --record-only
# data collection works without the SDK installed).
LIGHTER_PROFILES: Dict[str, LighterProfile] = {
    "lighter": LighterProfile(
        "mainnet", "https://mainnet.zklighter.elliot.ai",
        "wss://mainnet.zklighter.elliot.ai/stream", 304),
    "lighter-rh": LighterProfile(
        "robinhood", "https://api.rh.lighter.xyz",
        "wss://api.rh.lighter.xyz/stream", 466324),
}

# Lighter's mainnet market uses ANTHROPIC for the canonical ANTH CLI symbol.
# Keep the alias venue-scoped so the CLI/config symbol remains canonical and
# the Robinhood profile is not changed implicitly.
LIGHTER_SYMBOL_ALIASES: Dict[str, Dict[str, str]] = {
    "lighter": {
        "ANTH": "ANTHROPIC",
    },
    "lighter-rh": {
        "ANTH": "ANTHROPIC",
    },
}


@dataclass
class LighterCreds:
    account_index: Optional[int]
    api_key_index: Optional[int]
    api_private_key: Optional[str]

    @property
    def complete(self) -> bool:
        return (self.account_index is not None and self.api_key_index is not None
                and bool(self.api_private_key))


@dataclass
class HLCreds:
    private_key: Optional[str]
    account_address: Optional[str]

    @property
    def complete(self) -> bool:
        return bool(self.private_key)


@dataclass
class VenueConf:
    key: str                  # "arcus" | "hedge"
    kind: str                 # "arcus" | "hl" | "lighter"
    label: str                # human name for logs, e.g. "ARCUS", "RH"
    symbol: str
    fee_bps: float
    cap_usd: float
    orders_per_min: int
    # hl
    hl_dex: str = ""
    hl_creds: Optional[HLCreds] = None
    # lighter
    lighter_profile: Optional[LighterProfile] = None
    lighter_creds: Optional[LighterCreds] = None
    # B0 must distinguish an explicitly verified zero fee from the default
    # modeling value.  This flag never changes the configured fee itself.
    fee_bps_verified: bool = False


@dataclass(frozen=True)
class StrategyConf:
    name: str
    upper_bps: float
    lower_bps: float
    center_bps: Optional[float] = None
    window_minutes: Optional[int] = None
    # stable_basis may optionally source its center from a causal rolling
    # median.  The fallback remains the explicitly configured center_bps.
    center_mode: str = "fixed"
    center_window_hours: float = 12.0
    center_update_minutes: int = 60


@dataclass
class Config:
    symbol: str
    hedge_venue: str
    arcus: VenueConf
    hedge: VenueConf
    strategy: StrategyConf
    # sizing
    take_fraction: float
    max_order_notional: float
    min_order_notional: float
    # inventory ladder
    inventory_scale_bps: float
    inventory_floor_frac: float
    # execution
    premium_persist_sec: float
    cooldown_sec: float
    settle_timeout_sec: float
    leg_slippage_bps: float
    hedge_slippage_bps: float
    net_tolerance_base: float
    max_consecutive_errors: int
    rate_limit_pause_sec: float
    staleness_sec: float
    reconcile_sec: float
    venue_probe_sec: float
    http_keepalive_sec: float
    # recorder
    recorder_enabled: bool
    recorder_database: str
    # logging
    log_level: str
    status_interval_sec: float
    trades_csv: str
    dashboard: bool
    log_file: str
    # runtime
    hl_api_url: str = HL_API_URL
    hl_ws_url: str = HL_WS_URL
    arcus_rest_url: str = ARCUS_REST_URL
    arcus_ws_url: str = ARCUS_WS_URL

    @property
    def entropy(self) -> VenueConf:
        """Compatibility alias for mature generic strategy/test helpers.

        The runtime and configuration use ``arcus``; this alias avoids an
        unnecessary rewrite of venue-agnostic historical code in the fork.
        """
        return self.arcus

    @property
    def creds_complete(self) -> bool:
        for v in (self.arcus, self.hedge):
            if v.kind == "arcus":
                continue
            if v.kind == "hl" and not (v.hl_creds and v.hl_creds.complete):
                return False
            if v.kind == "lighter" and not (v.lighter_creds
                                            and v.lighter_creds.complete):
                return False
        return True

# ----------------------------------------------------------------- YAML layer

# Schema: nested dict of key -> type (or nested dict). Unknown keys are errors.
_SCHEMA: Dict[str, Any] = {
    "strategy": {
        "name": str,
        "params": dict,
    },
    "arcus": {
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "hedge": {
        "taker_fee_bps": float,
        "taker_fee_bps_verified": bool,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "sizing": {
        "take_fraction": float,
        "max_order_notional_usd": float,
        "min_order_notional_usd": float,
    },
    "inventory": {
        "scale_bps": float,
        "floor_frac": float,
    },
    "execution": {
        "premium_persist_sec": float,
        "cooldown_sec": float,
        "settle_timeout_sec": float,
        "leg_slippage_bps": float,
        "hedge_slippage_bps": float,
        "net_tolerance_base": float,
        "max_consecutive_errors": int,
        "rate_limit_pause_sec": float,
        "staleness_sec": float,
        "reconcile_sec": float,
        "venue_probe_sec": float,
        "http_keepalive_sec": float,
    },
    "recorder": {
        "enabled": bool,
        "database": str,
    },
    "logging": {
        "level": str,
        "status_interval_sec": float,
        "trades_csv": str,
        "dashboard": bool,
        "file": str,
    },
}


class ConfigError(ValueError):
    pass


def _validate(node: Any, schema: Dict[str, Any], path: str = "") -> None:
    if not isinstance(node, dict):
        raise ConfigError(f"'{path or '<root>'}' must be a mapping")
    for key, val in node.items():
        here = f"{path}.{key}" if path else str(key)
        if key not in schema:
            raise ConfigError(f"unknown config key '{here}' "
                              f"(valid: {', '.join(sorted(schema))})")
        want = schema[key]
        if isinstance(want, dict):
            _validate(val, want, here)
        elif want is float:
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be a number, got {val!r}")
        elif want is int:
            if not isinstance(val, int) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be an integer, got {val!r}")
        elif want is bool:
            if not isinstance(val, bool):
                raise ConfigError(f"'{here}' must be true/false, got {val!r}")
        elif want is str:
            if not isinstance(val, str):
                raise ConfigError(f"'{here}' must be a string, got {val!r}")
        elif want is dict:
            if not isinstance(val, dict):
                raise ConfigError(f"'{here}' must be a mapping")


def _get(d: dict, section: str, key: str, default):
    return (d.get(section) or {}).get(key, default)


def _finite_number(params: dict, key: str, path: str) -> float:
    if key not in params:
        raise ConfigError(f"'{path}.{key}' is required")
    value = params[key]
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigError(f"'{path}.{key}' must be a number")
    value = float(value)
    if not math.isfinite(value):
        raise ConfigError(f"'{path}.{key}' must be finite")
    return value


def _optional_finite_number(params: dict, key: str, default: float,
                            path: str) -> float:
    if key not in params:
        return default
    value = params[key]
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigError(f"'{path}.{key}' must be a number")
    value = float(value)
    if not math.isfinite(value):
        raise ConfigError(f"'{path}.{key}' must be finite")
    if value <= 0:
        raise ConfigError(f"'{path}.{key}' must be > 0")
    return value


def _optional_positive_int(params: dict, key: str, default: int,
                           path: str) -> int:
    if key not in params:
        return default
    value = params[key]
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigError(f"'{path}.{key}' must be a positive integer")
    return value


def _parse_strategy(raw: dict) -> StrategyConf:
    node = raw.get("strategy")
    if not isinstance(node, dict):
        raise ConfigError("'strategy' is required and must be a mapping")

    name = node.get("name")
    params = node.get("params")
    if name not in ("stable_basis", "drifting_basis"):
        raise ConfigError(f"unknown strategy {name!r}")
    if not isinstance(params, dict):
        raise ConfigError("'strategy.params' must be a mapping")

    if name == "stable_basis":
        allowed = {
            "center_mode", "center_bps", "center_window_hours",
            "center_update_minutes", "upper_bps", "lower_bps",
        }
    else:
        allowed = {"window_minutes", "upper_bps", "lower_bps"}

    unknown = set(params) - allowed
    if unknown:
        key = sorted(unknown)[0]
        raise ConfigError(
            f"unknown/forbidden strategy parameter 'strategy.params.{key}'"
        )

    upper = _finite_number(params, "upper_bps", "strategy.params")
    lower = _finite_number(params, "lower_bps", "strategy.params")
    if upper <= 0 or lower <= 0:
        raise ConfigError("strategy upper_bps and lower_bps must be > 0")

    if name == "stable_basis":
        center = _finite_number(params, "center_bps", "strategy.params")
        center_mode = params.get("center_mode", "fixed")
        if not isinstance(center_mode, str):
            raise ConfigError("'strategy.params.center_mode' must be a string")
        if center_mode not in ("fixed", "rolling"):
            raise ConfigError(
                "strategy.params.center_mode must be 'fixed' or 'rolling'"
            )
        center_window_hours = _optional_finite_number(
            params, "center_window_hours", 12.0, "strategy.params"
        )
        center_update_minutes = _optional_positive_int(
            params, "center_update_minutes", 60, "strategy.params"
        )
        return StrategyConf(
            name=name,
            center_bps=center,
            upper_bps=upper,
            lower_bps=lower,
            center_mode=center_mode,
            center_window_hours=center_window_hours,
            center_update_minutes=center_update_minutes,
        )

    if "window_minutes" not in params:
        raise ConfigError("'strategy.params.window_minutes' is required")
    window = params["window_minutes"]
    if not isinstance(window, int) or isinstance(window, bool) or window <= 0:
        raise ConfigError("strategy.params.window_minutes must be a positive integer")
    return StrategyConf(
        name=name,
        window_minutes=window,
        upper_bps=upper,
        lower_bps=lower,
    )


# ------------------------------------------------------------------ env layer

def _env_s(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if v not in (None, "") else None


def _env_i(name: str) -> Optional[int]:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else None


# -------------------------------------------------------------------- loading

def load_config(config_file: str = "config.yaml", env_file: str = ".env", *,
                symbol: str, hedge_venue: str) -> Config:
    load_dotenv(env_file)
    try:
        with open(config_file) as fh:
            raw = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        raise ConfigError(
            f"config file '{config_file}' not found — copy config.example.yaml "
            f"to config.yaml and edit it / 未找到配置文件，请先复制 "
            f"config.example.yaml 为 config.yaml 并修改")
    if "thresholds" in raw:
        raise ConfigError(
            "legacy 'thresholds:' config is no longer supported; use:\n"
            "strategy:\n"
            "  name: stable_basis\n"
            "  params:\n"
            "    center_bps: <old midline_bps>\n"
            "    upper_bps: <old upper_bps>\n"
            "    lower_bps: <old lower_bps>"
        )
    recorder = raw.get("recorder")
    if isinstance(recorder, dict) and "csv" in recorder:
        raise ConfigError(
            "legacy 'recorder.csv' is no longer supported; use\n"
            "recorder.database: data/market-history.sqlite"
        )
    _validate(raw, _SCHEMA)

    symbol = (symbol or "").strip()
    if not symbol:
        raise ConfigError("--symbol is required, e.g. --symbol SNDK / "
                          "必须用 --symbol 指定交易品种")
    if hedge_venue not in HEDGE_VENUES:
        raise ConfigError(
            f"--hedge must be one of {list(HEDGE_VENUES)}, got "
            f"{hedge_venue!r} / --hedge 必须是 {list(HEDGE_VENUES)} 之一")
    strategy_conf = _parse_strategy(raw)

    take_fraction = float(_get(raw, "sizing", "take_fraction", 0.5))
    if not 0.0 < take_fraction <= 1.0:
        raise ConfigError("sizing.take_fraction must be in (0, 1] — taking "
                          "more than the profitable depth loses money on the "
                          "tail / 必须在 (0, 1] 之间")

    arcus = VenueConf(
        key="arcus", kind="arcus", label="ARCUS",
        symbol=symbol,
        fee_bps=float(_get(raw, "arcus", "taker_fee_bps", 0.0)),
        cap_usd=float(_get(raw, "arcus", "max_position_usd", 1000.0)),
        orders_per_min=int(_get(raw, "arcus", "max_orders_per_min", 0)),
    )

    if hedge_venue == "tradexyz":
        hedge = VenueConf(
            key="hedge", kind="hl", label="XYZ",
            symbol=symbol,
            fee_bps=float(_get(raw, "hedge", "taker_fee_bps", 1.0)),
            cap_usd=float(_get(raw, "hedge", "max_position_usd", 1000.0)),
            orders_per_min=int(_get(raw, "hedge", "max_orders_per_min", 120)),
            fee_bps_verified=bool(
                _get(raw, "hedge", "taker_fee_bps_verified", False)
            ),
            hl_dex="xyz",
            hl_creds=HLCreds(
                _env_s("HL_PRIVATE_KEY_XYZ") or _env_s("HL_PRIVATE_KEY"),
                _env_s("HL_ACCOUNT_ADDRESS_XYZ") or _env_s("HL_ACCOUNT_ADDRESS")),
        )
    else:
        hedge_symbol = LIGHTER_SYMBOL_ALIASES.get(hedge_venue, {}).get(
            symbol, symbol)
        hedge = VenueConf(
            key="hedge", kind="lighter",
            label="LIGHTER" if hedge_venue == "lighter" else "RH",
            symbol=hedge_symbol,
            fee_bps=float(_get(raw, "hedge", "taker_fee_bps", 0.0)),
            cap_usd=float(_get(raw, "hedge", "max_position_usd", 1000.0)),
            orders_per_min=int(_get(raw, "hedge", "max_orders_per_min", 30)),
            fee_bps_verified=bool(
                _get(raw, "hedge", "taker_fee_bps_verified", False)
            ),
            lighter_profile=LIGHTER_PROFILES[hedge_venue],
            lighter_creds=LighterCreds(_env_i("LIGHTER_ACCOUNT_INDEX"),
                                       _env_i("LIGHTER_API_KEY_INDEX"),
                                       _env_s("LIGHTER_API_PRIVATE_KEY")),
        )

    return Config(
        symbol=symbol,
        hedge_venue=hedge_venue,
        arcus=arcus,
        hedge=hedge,
        strategy=strategy_conf,
        take_fraction=take_fraction,
        max_order_notional=float(_get(raw, "sizing", "max_order_notional_usd", 500.0)),
        min_order_notional=float(_get(raw, "sizing", "min_order_notional_usd", 10.0)),
        inventory_scale_bps=float(_get(raw, "inventory", "scale_bps", 10.0)),
        inventory_floor_frac=float(_get(raw, "inventory", "floor_frac", 0.5)),
        premium_persist_sec=float(_get(raw, "execution", "premium_persist_sec", 0.3)),
        cooldown_sec=float(_get(raw, "execution", "cooldown_sec", 0.0)),
        settle_timeout_sec=float(_get(raw, "execution", "settle_timeout_sec", 5.0)),
        leg_slippage_bps=float(_get(raw, "execution", "leg_slippage_bps", 50.0)),
        hedge_slippage_bps=float(_get(raw, "execution", "hedge_slippage_bps", 20.0)),
        net_tolerance_base=float(_get(raw, "execution", "net_tolerance_base", 0.001)),
        max_consecutive_errors=int(_get(raw, "execution", "max_consecutive_errors", 3)),
        rate_limit_pause_sec=float(_get(raw, "execution", "rate_limit_pause_sec", 10.0)),
        staleness_sec=float(_get(raw, "execution", "staleness_sec", 10.0)),
        reconcile_sec=float(_get(raw, "execution", "reconcile_sec", 15.0)),
        venue_probe_sec=float(_get(raw, "execution", "venue_probe_sec", 30.0)),
        http_keepalive_sec=float(_get(raw, "execution", "http_keepalive_sec", 10.0)),
        recorder_enabled=bool(_get(raw, "recorder", "enabled", True)),
        recorder_database=str(_get(
            raw, "recorder", "database", DEFAULT_RECORDER_DATABASE)),
        log_level=str(_get(raw, "logging", "level", "INFO")).upper(),
        status_interval_sec=float(_get(raw, "logging", "status_interval_sec", 30.0)),
        trades_csv=_get(raw, "logging", "trades_csv", "logs/trades.csv"),
        dashboard=bool(_get(raw, "logging", "dashboard", True)),
        log_file=_get(raw, "logging", "file", "logs/engine.log"),
        arcus_rest_url=ARCUS_REST_URL,
        arcus_ws_url=ARCUS_WS_URL,
    )
