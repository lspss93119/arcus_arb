"""Focused Arcus credential-source tests.

All key material in this module is deterministic test data.  No test loads
the repository's .env and no test submits an order.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from entropy_arb import arcus_auth
from entropy_arb.arcus_auth import (
    ArcusCredentialError,
    ArcusCredentials,
    ArcusSigner,
)

ADDRESS = "0x" + "11" * 20


def _seed(byte: int) -> str:
    return (bytes([byte]) * 32).hex()


def _api_key(seed_hex: str) -> str:
    from cryptography.hazmat.primitives.asymmetric import ed25519

    key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed_hex))
    return key.public_key().public_bytes_raw().hex()


def _env(seed_hex: str | None = None) -> dict[str, str]:
    values = {
        "ARCUS_ACCOUNT_ADDRESS": ADDRESS,
        "ARCUS_ACCOUNT_INDEX": "0",
        "ARCUS_API_KEY": _api_key(seed_hex or _seed(1)),
    }
    if seed_hex is not None:
        values["ARCUS_PRIVATE_KEY"] = seed_hex
    return values


def test_canonical_private_key_value_loads_successfully() -> None:
    seed = _seed(1)

    credentials = ArcusCredentials.from_env(_env(seed))

    assert credentials.private_key_text == seed
    ArcusSigner(credentials)


def test_canonical_private_key_is_a_value_not_a_file_path() -> None:
    seed = _seed(2)
    assert not Path(seed).exists()

    credentials = ArcusCredentials.from_env(_env(seed))

    assert credentials.private_key_text == seed


def test_canonical_private_key_takes_precedence_over_legacy_direct_value() -> None:
    canonical = _seed(3)
    legacy = _seed(4)
    env = _env(canonical)
    env["ARCUS_ED25519_PRIVATE_KEY"] = legacy

    credentials = ArcusCredentials.from_env(env)

    assert credentials.private_key_text == canonical
    ArcusSigner(credentials)


def test_legacy_direct_private_key_still_loads_without_canonical_value() -> None:
    legacy = _seed(5)
    env = _env(legacy)
    env.pop("ARCUS_PRIVATE_KEY")
    env["ARCUS_ED25519_PRIVATE_KEY"] = legacy
    env["ARCUS_API_KEY"] = _api_key(legacy)

    credentials = ArcusCredentials.from_env(env)

    assert credentials.private_key_text == legacy


def test_legacy_direct_private_key_precedes_legacy_file(tmp_path: Path) -> None:
    direct = _seed(5)
    from_file = _seed(6)
    key_file = tmp_path / "arcus-ed25519.key"
    key_file.write_text(from_file)
    env = _env(direct)
    env.pop("ARCUS_PRIVATE_KEY")
    env["ARCUS_ED25519_PRIVATE_KEY"] = direct
    env["ARCUS_ED25519_PRIVATE_KEY_FILE"] = str(key_file)
    env["ARCUS_API_KEY"] = _api_key(direct)

    credentials = ArcusCredentials.from_env(env)

    assert credentials.private_key_text == direct


def test_legacy_private_key_file_still_loads_as_last_fallback(
    tmp_path: Path,
) -> None:
    legacy = _seed(6)
    key_file = tmp_path / "arcus-ed25519.key"
    key_file.write_text(legacy + "\n")
    env = _env(legacy)
    env.pop("ARCUS_PRIVATE_KEY")
    env["ARCUS_ED25519_PRIVATE_KEY_FILE"] = str(key_file)
    env["ARCUS_API_KEY"] = _api_key(legacy)

    credentials = ArcusCredentials.from_env(env)

    assert credentials.private_key_text == legacy


def test_missing_all_private_key_sources_fails_closed() -> None:
    with pytest.raises(ArcusCredentialError, match="ARCUS_PRIVATE_KEY"):
        ArcusCredentials.from_env(_env())


def test_credential_status_contains_only_presence_markers() -> None:
    secret = _seed(7)
    env = _env(secret)
    env.pop("ARCUS_ACCOUNT_INDEX")

    assert hasattr(arcus_auth, "format_credential_status")
    output = arcus_auth.format_credential_status(env)

    assert "ARCUS_PRIVATE_KEY: PRESENT" in output
    assert "PRESENT" in output
    assert "MISSING" in output
    assert secret not in output
    assert "0x" + "11" * 20 not in output
    assert _api_key(secret) not in output
    assert all(
        line.rsplit(": ", 1)[-1] in {"PRESENT", "MISSING"}
        for line in output.splitlines()
    )


def test_missing_credential_status_does_not_expose_secret_material() -> None:
    secret = _seed(8)
    assert hasattr(arcus_auth, "format_credential_status")
    output = arcus_auth.format_credential_status({})

    assert "ARCUS_PRIVATE_KEY: MISSING" in output
    assert secret not in output
    assert all(
        line.rsplit(": ", 1)[-1] in {"PRESENT", "MISSING"}
        for line in output.splitlines()
    )


def test_invalid_private_key_error_does_not_expose_secret_material() -> None:
    secret = "not-a-private-key-secret"
    env = _env(_seed(9))
    env["ARCUS_PRIVATE_KEY"] = secret

    credentials = ArcusCredentials.from_env(env)
    with pytest.raises(ArcusCredentialError) as error:
        ArcusSigner(credentials)

    assert secret not in str(error.value)
