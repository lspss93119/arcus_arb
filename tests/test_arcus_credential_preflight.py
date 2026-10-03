"""Read-only Arcus API-key registration preflight tests."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import pytest

from entropy_arb.arcus_auth import ArcusCredentialError, ArcusCredentials, ArcusSigner
from entropy_arb.arcus_execution import ArcusAccountRest
from entropy_arb.engine import _arcus_credential_preflight

ADDRESS = "0x" + "11" * 20


def _seed(byte: int) -> str:
    return (bytes([byte]) * 32).hex()


def _api_key(seed_hex: str) -> str:
    from cryptography.hazmat.primitives.asymmetric import ed25519

    key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed_hex))
    return key.public_key().public_bytes_raw().hex()


def _credentials(seed: int = 1) -> tuple[ArcusCredentials, ArcusSigner]:
    seed_hex = _seed(seed)
    credentials = ArcusCredentials.from_env(
        {
            "ARCUS_ACCOUNT_ADDRESS": ADDRESS,
            "ARCUS_ACCOUNT_INDEX": "0",
            "ARCUS_API_KEY": _api_key(seed_hex),
            "ARCUS_PRIVATE_KEY": seed_hex,
        }
    )
    return credentials, ArcusSigner(credentials)


class _Response:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def json(self) -> dict[str, Any]:
        return self.payload

    def raise_for_status(self) -> None:
        return None


class _Session:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, *, params: dict[str, Any], timeout: Any) -> _Response:
        del timeout
        self.calls.append((url, params))
        return _Response(self.payload)


def _check(payload: dict[str, Any], *, seed: int = 1):
    credentials, signer = _credentials(seed)
    session = _Session(payload)
    rest = ArcusAccountRest(cast(Any, session), rest_url="https://arcus.test")
    result = asyncio.run(rest.validate_api_key_registration(credentials, signer))
    return result, session


def _row(
    *,
    seed: int = 1,
    status: str = "ACTIVE",
    account_index: int = 0,
    valid_until: str | None = "2099-01-01T00:00:00Z",
) -> dict[str, Any]:
    return {
        "apiKey": _api_key(_seed(seed)),
        "status": status,
        "accountIndex": account_index,
        "validUntil": valid_until,
    }


def test_registered_active_matching_key_passes_and_uses_read_only_endpoint() -> None:
    result, session = _check({"apiKeys": [_row()]})

    assert result.fingerprint.startswith(_api_key(_seed(1))[:8])
    assert result.fingerprint.endswith(_api_key(_seed(1))[-8:])
    assert result.account_index == 0
    assert result.status == "ACTIVE"
    assert result.valid_until == "2099-01-01T00:00:00Z"
    assert session.calls == [
        (
            "https://arcus.test/v1/apiKeys",
            {"address": ADDRESS},
        )
    ]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            {"apiKeys": []},
            "API key is not registered for this wallet",
        ),
        (
            {"apiKeys": [_row(status="REVOKED")]},
            "API key status=REVOKED",
        ),
        (
            {"apiKeys": [_row(status="INACTIVE")]},
            "API key status=INACTIVE",
        ),
        (
            {"apiKeys": [_row(account_index=1)]},
            "API key belongs to accountIndex=1, runtime=0",
        ),
        (
            {"apiKeys": [_row(valid_until="2000-01-01T00:00:00Z")]},
            "API key expired",
        ),
    ],
)
def test_registration_preflight_fails_closed_for_unsafe_registration(
    payload: dict[str, Any], message: str
) -> None:
    credentials, signer = _credentials()
    rest = ArcusAccountRest(cast(Any, _Session(payload)), rest_url="https://arcus.test")

    with pytest.raises(ArcusCredentialError, match=message):
        asyncio.run(rest.validate_api_key_registration(credentials, signer))


def test_registration_error_does_not_expose_key_material() -> None:
    private_key = _seed(2)
    derived_key = _api_key(private_key)
    credentials, signer = _credentials(2)
    session = _Session({"apiKeys": [_row(seed=1)]})
    rest = ArcusAccountRest(cast(Any, session), rest_url="https://arcus.test")

    with pytest.raises(ArcusCredentialError) as caught:
        asyncio.run(rest.validate_api_key_registration(credentials, signer))

    message = str(caught.value)
    assert private_key not in message
    assert derived_key not in message
    assert ADDRESS not in message
    assert derived_key not in repr(credentials)
    assert derived_key not in repr(signer)


def test_public_private_equality_validation_remains_fail_closed() -> None:
    private_key = _seed(2)
    env = {
        "ARCUS_ACCOUNT_ADDRESS": ADDRESS,
        "ARCUS_ACCOUNT_INDEX": "0",
        "ARCUS_API_KEY": _api_key(_seed(1)),
        "ARCUS_PRIVATE_KEY": private_key,
    }

    with pytest.raises(
        ArcusCredentialError,
        match="ARCUS_API_KEY does not match the supplied Ed25519 private key",
    ):
        ArcusSigner(ArcusCredentials.from_env(env))


def test_successful_preflight_logs_only_safe_registration_summary(caplog) -> None:
    credentials, signer = _credentials()
    rest = ArcusAccountRest(
        cast(Any, _Session({"apiKeys": [_row()]})),
        rest_url="https://arcus.test",
    )
    caplog.set_level(logging.INFO, logger="engine")

    asyncio.run(_arcus_credential_preflight(rest, credentials, signer))

    assert "[ARCUS] credential preflight" in caplog.text
    assert "account_index=0" in caplog.text
    assert "status=ACTIVE" in caplog.text
    assert "valid_until=2099-01-01T00:00:00Z" in caplog.text
    assert _api_key(_seed(1)) not in caplog.text
    assert _seed(1) not in caplog.text
