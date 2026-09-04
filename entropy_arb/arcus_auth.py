"""Arcus Ed25519 request authentication for the Phase B0 maker path.

This module deliberately does not generate wallets, register API keys, or
expose an order-capable client to the record-only venue.  Credentials are
loaded only when the explicitly gated Phase B0 runtime asks for them.
"""
from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping


class ArcusCredentialError(RuntimeError):
    """Raised when a complete, internally consistent Arcus identity is absent."""


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
    api_key: str
    private_key_text: str = field(repr=False, compare=False)
    account_index: int = 0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ArcusCredentials":
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
            raise ArcusCredentialError("ARCUS_ACCOUNT_INDEX must be an integer") from exc
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
        "c": client_id,
        "ct": timestamp_ns,
        "g": good_til_time_us * 1000,
        "m": market_id,
        "op": 1,
        "p": _integer_units(price, tick_size, "price"),
        "q": _integer_units(quantity, step_size, "quantity"),
        "r": 0,
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
        payload["c"] = client_id
    return payload


class ArcusSigner:
    """Small Ed25519 signer with no registration or wallet functionality."""

    def __init__(self, credentials: ArcusCredentials) -> None:
        self.credentials = credentials
        self._private_key = self._load_private_key(credentials.private_key_text)
        public_key = self._private_key.public_key().public_bytes_raw().hex()
        expected = credentials.api_key.lower().removeprefix("0x")
        if not re.fullmatch(r"[0-9a-f]{64}", expected) or public_key != expected:
            raise ArcusCredentialError(
                "ARCUS_API_KEY does not match the supplied Ed25519 private key"
            )
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", credentials.account_address):
            raise ArcusCredentialError("ARCUS_ACCOUNT_ADDRESS must be a 20-byte 0x address")

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
        return f"ArcusSigner(api_key={self.credentials.api_key!r}, account_index={self.credentials.account_index})"
