import dataclasses

import pytest

from queue_service.config import Settings


def test_from_env_parses_every_supported_type(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QS_BLOCK_MS", "100")  # int
    monkeypatch.setenv("QS_LEASE_S", "2.5")  # float
    monkeypatch.setenv("QS_STREAM", "other")  # str
    monkeypatch.setenv("QS_LOG_JSON", "false")  # bool
    s = Settings.from_env()
    assert (s.block_ms, s.lease_s, s.stream, s.log_json) == (100, 2.5, "other", False)
    assert isinstance(s.block_ms, int)  # a str here is what broke XREADGROUP once


def test_from_env_defaults_when_unset() -> None:
    assert Settings.from_env() == Settings()


def test_every_field_has_a_supported_type() -> None:
    # Guards from_env: adding a field of a new type must fail here, not at runtime.
    assert {f.type for f in dataclasses.fields(Settings)} <= {int, float, str, bool}
