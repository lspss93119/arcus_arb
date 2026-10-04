"""Arcus Ed25519 request authentication for the Phase B0 maker path.

This module deliberately does not generate wallets, register API keys, or
expose an order-capable client to the record-only venue.  Credentials are
loaded only when the explicitly gated Phase B0 runtime asks for them.
"""

from __future__ import annotations

import base64
import json
import math
import os
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


class ArcusCredentialError(RuntimeError):
    """Raised when a complete, internally consistent Arcus identity is absent."""


@dataclass(frozen=True)
class ArcusApiKeyRegistration:
    """Safe summary of the runtime key's read-only Arcus registration."""

    fingerprint: str
    account_index: int
    status: str
    valid_until: str | None


def _normalized_public_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip().lower().removeprefix("0x")
    if not re.fullmatch(r"[0-9a-f]{64}", candidate):
        return None
    return candidate


def _public_key_fingerprint(public_key_hex: str) -> str:
    return f"{public_key_hex[:8]}…{public_key_hex[-8:]}"


def _api_key_rows(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: Any = payload.get("apiKeys")
    if rows is None:
        rows = payload.get("keys")
    if rows is None:
        data = payload.get("data")
        if isinstance(data, Mapping):
            rows = data.get("apiKeys", data.get("keys"))
        elif isinstance(data, list):
            rows = data
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise ArcusCredentialError(
            "Arcus credential preflight failed: "
            "API key registration response has no valid key list"
        )
    return list(rows)


def _registration_key(row: Mapping[str, Any]) -> str | None:
    for field_name in ("apiKey", "publicKey", "key"):
        key = _normalized_public_key(row.get(field_name))
        if key is not None:
            return key
    return None


_SAFE_STATUS_VALUES = frozenset(
    {
        "ACTIVE",
        "DELETED",
        "DISABLED",
        "EXPIRED",
        "INACTIVE",
        "PENDING",
        "REVOKED",
        "SUSPENDED",
    }
)


def _status_text(value: Any) -> tuple[str, str]:
    raw = str(value or "UNKNOWN").strip().upper()
    return raw, raw if raw in _SAFE_STATUS_VALUES else "UNKNOWN"


def _account_index(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return (
        result if str(result) == str(value).strip() or isinstance(value, int) else None
    )


def _valid_until_epoch(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            timestamp = float(value)
        except (OverflowError, ValueError):
            return None
        if not math.isfinite(timestamp):
            return None
        if timestamp > 10**14:
            timestamp /= 1_000_000
        elif timestamp > 10**11:
            timestamp /= 1_000
        return timestamp
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        timestamp = float(Decimal(text))
    except (InvalidOperation, ValueError):
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.timestamp()
    if not math.isfinite(timestamp):
        return None
    if timestamp > 10**14:
        timestamp /= 1_000_000
    elif timestamp > 10**11:
        timestamp /= 1_000
    return timestamp


def validate_registered_api_key(
    payload: Mapping[str, Any],
    *,
    derived_public_key: str,
    runtime_account_index: int,
) -> ArcusApiKeyRegistration:
    """Validate the exact runtime key without returning any secret material."""

    derived = _normalized_public_key(derived_public_key)
    if derived is None:
        raise ArcusCredentialError(
            "Arcus credential preflight failed: derived API key is invalid"
        )
    rows = _api_key_rows(payload)
    matches = [row for row in rows if _registration_key(row) == derived]
    if not matches:
        raise ArcusCredentialError(
            "Arcus credential preflight failed: "
            "API key is not registered for this wallet"
        )
    if len(matches) != 1:
        raise ArcusCredentialError(
            "Arcus credential preflight failed: API key registration is ambiguous"
        )
    row = matches[0]
    raw_status, status = _status_text(row.get("status"))
    if raw_status != "ACTIVE":
        raise ArcusCredentialError(
            f"Arcus credential preflight failed: API key status={status}"
        )
    registered_account_index = _account_index(row.get("accountIndex"))
    if registered_account_index is None:
        raise ArcusCredentialError(
            "Arcus credential preflight failed: API key accountIndex is invalid"
        )
    if registered_account_index != runtime_account_index:
        raise ArcusCredentialError(
            "Arcus credential preflight failed: "
            f"API key belongs to accountIndex={registered_account_index}, "
            f"runtime={runtime_account_index}"
        )
    valid_until_value = row.get("validUntil")
    valid_until = None if valid_until_value is None else str(valid_until_value)
    if valid_until_value is not None:
        expiry = _valid_until_epoch(valid_until_value)
        if expiry is None:
            raise ArcusCredentialError(
                "Arcus credential preflight failed: API key validUntil is invalid"
            )
        if expiry <= time.time():
            raise ArcusCredentialError(
                "Arcus credential preflight failed: API key expired"
            )
    return ArcusApiKeyRegistration(
        fingerprint=_public_key_fingerprint(derived),
        account_index=registered_account_index,
        status=status,
        valid_until=valid_until,
    )


def format_credential_status(
    env: Mapping[str, str] | None = None,
) -> str:
    """Return non-secret Arcus credential presence diagnostics.

    The private-key line reports only whether the canonical direct-value
    variable is populated.  It never includes key material or file contents.
    Legacy sources remain accepted by :meth:`ArcusCredentials.from_env`, but
    are deliberately not echoed by this diagnostic.
    """
    values = os.environ if env is None else env

    def present(name: str) -> str:
        value = values.get(name)
        return "PRESENT" if isinstance(value, str) and value.strip() else "MISSING"

    names = (
        "ARCUS_ACCOUNT_ADDRESS",
        "ARCUS_ACCOUNT_INDEX",
        "ARCUS_API_KEY",
        "ARCUS_PRIVATE_KEY",
    )
    return "\n".join(f"{name}: {present(name)}" for name in names)


def canonical_json(value: Mapping[str, Any]) -> bytes:
    """Return Arcus's compact, key-sorted JSON signing bytes."""
    return json.dumps(
        value, separators=(",", ":"), sort_keys=True, ensure_ascii=False
    ).encode("utf-8")


@dataclass(frozen=True)
class ArcusCredentials:
    """Existing, user-provided Arcus API identity.

    ``private_key_text`` is intentionally excluded from repr/equality output.
    It may be a PEM document or a 32-byte hexadecimal Ed25519 seed loaded from
    the canonical direct-value environment variable or its legacy fallbacks.
    The corresponding public key is verified against ``api_key`` by
    :class:`ArcusSigner` before a request can be signed.
    """

    account_address: str
    api_key: str = field(repr=False)
    private_key_text: str = field(repr=False, compare=False)
    account_index: int = 0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ArcusCredentials:
        values = os.environ if env is None else env

        def read(name: str) -> str | None:
            value = values.get(name)
            if value is None:
                return None
            value = value.strip()
            return value or None

        address = read("ARCUS_ACCOUNT_ADDRESS")
        api_key = read("ARCUS_API_KEY")
        private_text = read("ARCUS_PRIVATE_KEY")
        if private_text is None:
            private_text = read("ARCUS_ED25519_PRIVATE_KEY")
        if private_text is None:
            private_file = read("ARCUS_ED25519_PRIVATE_KEY_FILE")
            if private_file is not None:
                try:
                    private_text = Path(private_file).read_text()
                except OSError as exc:
                    raise ArcusCredentialError(
                        "cannot read ARCUS_ED25519_PRIVATE_KEY_FILE"
                    ) from exc
                private_text = private_text.strip()

        missing = []
        if address is None:
            missing.append("ARCUS_ACCOUNT_ADDRESS")
        if api_key is None:
            missing.append("ARCUS_API_KEY")
        if private_text is None:
            missing.append(
                "ARCUS_PRIVATE_KEY, ARCUS_ED25519_PRIVATE_KEY, or "
                "ARCUS_ED25519_PRIVATE_KEY_FILE"
            )
        if missing:
            raise ArcusCredentialError(
                "missing Arcus live credentials: " + ", ".join(missing)
            )
        assert address is not None and api_key is not None and private_text is not None

        if address[:2].lower() == "0x":
            address = "0x" + address[2:]

        account_index_text = read("ARCUS_ACCOUNT_INDEX") or "0"
        try:
            account_index = int(account_index_text)
        except ValueError as exc:
            raise ArcusCredentialError(
                "ARCUS_ACCOUNT_INDEX must be an integer"
            ) from exc
        if not 0 <= account_index <= 9:
            raise ArcusCredentialError("ARCUS_ACCOUNT_INDEX must be between 0 and 9")

        return cls(
            account_address=address,
            api_key=api_key.lower().removeprefix("0x"),
            private_key_text=private_text,
            account_index=account_index,
        )


def _integer_units(value: Decimal, unit: Decimal, name: str) -> int:
    if value <= 0 or unit <= 0:
        raise ValueError(f"{name} must be > 0")
    quotient = value / unit
    if quotient != quotient.to_integral_value():
        raise ValueError(f"{name} must be an exact multiple of {unit}")
    return int(quotient)


def build_ordersign_payload(
    credentials: ArcusCredentials,
    *,
    market_id: int,
    side: str,
    price: Decimal,
    quantity: Decimal,
    tick_size: Decimal,
    step_size: Decimal,
    timestamp_ns: int,
    good_til_time_us: int,
    client_id: str,
    reduce_only: bool = False,
) -> dict[str, Any]:
    """Build the official Scheme 1 plain ``placeOrder`` payload.

    The values are the engine-native integer ticks/quantums.  The returned
    insertion order is not relied on for signing; Arcus canonicalizes sorted
    keys, and :func:`canonical_json` mirrors that behavior.
    """
    if not isinstance(market_id, int) or market_id < 0:
        raise ValueError("market_id must be a non-negative integer")
    if side.upper() not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")
    if not isinstance(timestamp_ns, int) or timestamp_ns <= 0:
        raise ValueError("timestamp_ns must be a positive integer")
    if not isinstance(good_til_time_us, int) or good_til_time_us <= 0:
        raise ValueError("good_til_time_us must be a positive integer")
    if not client_id:
        raise ValueError("client_id is required for a calibration order")
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", credentials.account_address):
        raise ArcusCredentialError("ARCUS_ACCOUNT_ADDRESS must be a 20-byte 0x address")

    return {
        "ad": credentials.account_address.lower(),
        "ai": credentials.account_index,
        "c": client_id.lower(),
        "ct": timestamp_ns,
        "g": good_til_time_us * 1000,
        "m": market_id,
        "op": 1,
        "p": _integer_units(price, tick_size, "price"),
        "q": _integer_units(quantity, step_size, "quantity"),
        "r": 1 if reduce_only else 0,
        "s": 0 if side.upper() == "BUY" else 1,
        "t": 3,
        "v": 1,
    }


def build_cancel_ordersign_payload(
    credentials: ArcusCredentials,
    *,
    market_id: int,
    timestamp_ns: int,
    order_id: str | None = None,
    client_id: str | None = None,
) -> dict[str, Any]:
    """Build the official Scheme 1 ``cancelOrder`` payload."""
    if (order_id is None) == (client_id is None):
        raise ValueError("cancel requires exactly one of order_id or client_id")
    payload: dict[str, Any] = {
        "ad": credentials.account_address.lower(),
        "ai": credentials.account_index,
        "ct": timestamp_ns,
        "m": market_id,
        "op": 2,
        "v": 1,
    }
    if order_id is not None:
        payload["id"] = order_id
    else:
        assert client_id is not None
        payload["c"] = client_id.lower()
    return payload


class ArcusSigner:
    """Small Ed25519 signer with no registration or wallet functionality."""

    def __init__(self, credentials: ArcusCredentials) -> None:
        self.credentials = credentials
        self._private_key = self._load_private_key(credentials.private_key_text)
        public_key = self._private_key.public_key().public_bytes_raw().hex()
        self._public_key_hex = public_key
        expected = credentials.api_key.lower().removeprefix("0x")
        if not re.fullmatch(r"[0-9a-f]{64}", expected) or public_key != expected:
            raise ArcusCredentialError(
                "ARCUS_API_KEY does not match the supplied Ed25519 private key"
            )
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", credentials.account_address):
            raise ArcusCredentialError(
                "ARCUS_ACCOUNT_ADDRESS must be a 20-byte 0x address"
            )

    @property
    def public_key_hex(self) -> str:
        """Return the derived public key for internal registration checks."""

        return self._public_key_hex

    @staticmethod
    def _load_private_key(value: str):
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import ed25519
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise ArcusCredentialError(
                "Arcus live signing needs cryptography; install requirements-live.txt"
            ) from exc

        raw = value.strip()
        try:
            if "BEGIN" in raw:
                key = serialization.load_pem_private_key(
                    raw.encode("utf-8"), password=None
                )
                if not isinstance(key, ed25519.Ed25519PrivateKey):
                    raise ArcusCredentialError("Arcus private key is not Ed25519")
                return key
            compact = raw.removeprefix("0x")
            if re.fullmatch(r"[0-9a-fA-F]{64}", compact):
                return ed25519.Ed25519PrivateKey.from_private_bytes(
                    bytes.fromhex(compact)
                )
            try:
                decoded = base64.b64decode(raw, validate=True)
            except Exception:
                decoded = b""
            if len(decoded) == 32:
                return ed25519.Ed25519PrivateKey.from_private_bytes(decoded)
        except (ValueError, TypeError, InvalidOperation) as exc:
            raise ArcusCredentialError("invalid Arcus Ed25519 private key") from exc
        raise ArcusCredentialError(
            "Arcus private key must be an Ed25519 PEM or 32-byte hex seed"
        )

    def sign_typed(self, payload: Mapping[str, Any]) -> str:
        return self._private_key.sign(canonical_json(payload)).hex()

    def __repr__(self) -> str:
        return (
            "ArcusSigner("
            f"api_key={_public_key_fingerprint(self.public_key_hex)!r}, "
            f"account_index={self.credentials.account_index})"
        )
