"""Configuration and HTTP client contracts for BI-050 slice 1."""

import importlib
from collections.abc import Mapping
from typing import Any

import httpx
import pytest

from batch_ingestion.config import AppConfig, ConfigurationError, HttpConfig

ENVIRONMENT = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "batch_lab",
    "POSTGRES_USER": "batch_lab",
    "POSTGRES_PASSWORD": "postgres-secret",
    "S3_ENDPOINT_URL": "http://localhost:3900",
    "S3_REGION": "garage",
    "S3_BUCKET": "batch-lab",
    "S3_ACCESS_KEY": "access-secret",
    "S3_SECRET_KEY": "storage-secret",
    "S3_ADDRESSING_STYLE": "path",
    "S3_TOTAL_MAX_ATTEMPTS": "3",
    "S3_CONNECT_TIMEOUT_SECONDS": "2",
    "S3_READ_TIMEOUT_SECONDS": "5",
    "MOCK_API_BASE_URL": "http://localhost:8080",
    "HTTP_CONNECT_TIMEOUT_SECONDS": "2",
    "HTTP_READ_TIMEOUT_SECONDS": "10",
    "HTTP_MAX_ATTEMPTS": "4",
    "HTTP_BACKOFF_BASE_SECONDS": "0.25",
    "HTTP_BACKOFF_MAX_SECONDS": "4",
    "HTTP_MAX_ELAPSED_SECONDS": "20",
    "HTTP_PAGE_SIZE": "25",
    "HTTP_MAX_PAGES": "100",
    "HTTP_MAX_RESPONSE_BYTES": "1048576",
    "HTTP_MAX_BACKFILL_DAYS": "31",
    "FILE_MAX_BYTES": "8388608",
    "FILE_SPOOL_THRESHOLD_BYTES": "1048576",
    "FILE_MAX_RECORDS": "100000",
    "FILE_PARQUET_BATCH_ROWS": "1024",
    "FILE_MAX_DIAGNOSTIC_DETAILS": "100",
    "SNAPSHOT_PAGE_SIZE": "100",
    "LOG_LEVEL": "INFO",
}


def _load(overrides: Mapping[str, str] | None = None) -> AppConfig:
    environment = dict(ENVIRONMENT)
    environment.update(overrides or {})
    return AppConfig.from_env(environment)


def test_all_http_settings_are_loaded_and_secrets_remain_redacted() -> None:
    config = _load()

    assert config.http == HttpConfig(
        base_url="http://localhost:8080",
        connect_timeout_seconds=2.0,
        read_timeout_seconds=10.0,
        max_attempts=4,
        backoff_base_seconds=0.25,
        backoff_max_seconds=4.0,
        max_elapsed_seconds=20.0,
        page_size=25,
        max_pages=100,
        max_response_bytes=1_048_576,
        max_backfill_days=31,
    )
    rendered = repr(config)
    assert "postgres-secret" not in rendered
    assert "access-secret" not in rendered
    assert "storage-secret" not in rendered


@pytest.mark.parametrize(
    "name",
    [
        "HTTP_BACKOFF_MAX_SECONDS",
        "HTTP_MAX_ELAPSED_SECONDS",
        "HTTP_PAGE_SIZE",
        "HTTP_MAX_PAGES",
        "HTTP_MAX_RESPONSE_BYTES",
        "HTTP_MAX_BACKFILL_DAYS",
    ],
)
def test_new_http_settings_must_be_positive(name: str) -> None:
    with pytest.raises(ConfigurationError, match=name):
        _load({name: "0"})


@pytest.mark.parametrize(
    ("name", "value", "maximum"),
    [
        ("HTTP_PAGE_SIZE", "101", 100),
        ("HTTP_MAX_PAGES", "10000", 9_999),
    ],
)
def test_protocol_limited_http_settings_enforce_upper_bounds(
    name: str,
    value: str,
    maximum: int,
) -> None:
    with pytest.raises(ConfigurationError, match=rf"{name} must not exceed {maximum}"):
        _load({name: value})


@pytest.mark.parametrize(
    "name",
    ["HTTP_BACKOFF_MAX_SECONDS", "HTTP_MAX_ELAPSED_SECONDS"],
)
def test_new_http_float_settings_reject_non_finite_values(name: str) -> None:
    with pytest.raises(ConfigurationError, match=name):
        _load({name: "inf"})


def test_http_client_uses_explicit_four_phase_timeout_and_context_management(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from batch_ingestion.api.client import open_http_client

    calls: list[dict[str, Any]] = []
    lifecycle: list[str] = []

    class FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            calls.append(kwargs)

        def __enter__(self) -> "FakeClient":
            lifecycle.append("enter")
            return self

        def __exit__(self, *args: object) -> None:
            lifecycle.append("exit")

    monkeypatch.setattr(httpx, "Client", FakeClient)

    with open_http_client(_load().http) as client:
        assert isinstance(client, FakeClient)
        assert lifecycle == ["enter"]

    assert lifecycle == ["enter", "exit"]
    assert len(calls) == 1
    assert calls[0]["base_url"] == "http://localhost:8080"
    assert calls[0]["trust_env"] is False
    assert "transport" not in calls[0]
    timeout = calls[0]["timeout"]
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.connect == 2.0
    assert timeout.read == 10.0
    assert timeout.write == 10.0
    assert timeout.pool == 2.0


def test_remaining_deadline_clips_every_timeout_phase() -> None:
    from batch_ingestion.api.client import timeout_for_remaining

    timeout = timeout_for_remaining(_load().http, remaining_seconds=0.75)

    assert timeout.connect == 0.75
    assert timeout.read == 0.75
    assert timeout.write == 0.75
    assert timeout.pool == 0.75

    with pytest.raises(ValueError, match="remaining_seconds must be positive"):
        timeout_for_remaining(_load().http, remaining_seconds=0.0)


def test_importing_api_client_does_not_construct_a_client_or_read_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import batch_ingestion.api.client as client_module

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("import performed runtime I/O or configuration loading")

    monkeypatch.setattr(httpx, "Client", unexpected)
    monkeypatch.setattr(AppConfig, "from_env", unexpected)

    importlib.reload(client_module)
