import pytest

from batch_ingestion.config import AppConfig, ConfigurationError

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
    "SNAPSHOT_PAGE_SIZE": "100",
    "LOG_LEVEL": "INFO",
}


def test_configuration_is_loaded_only_from_explicit_mapping() -> None:
    config = AppConfig.from_env(ENVIRONMENT)

    assert config.postgres.port == 5432
    assert config.storage.addressing_style == "path"
    assert config.storage.total_max_attempts == 3
    assert config.storage.connect_timeout_seconds == 2.0
    assert config.storage.read_timeout_seconds == 5.0
    assert config.http.max_attempts == 4
    assert config.snapshot_page_size == 100


def test_configuration_repr_redacts_secrets() -> None:
    config = AppConfig.from_env(ENVIRONMENT)

    rendered = repr(config)
    assert "postgres-secret" not in rendered
    assert "access-secret" not in rendered
    assert "storage-secret" not in rendered


def test_missing_configuration_is_reported_without_exposing_other_values() -> None:
    environment = dict(ENVIRONMENT)
    del environment["S3_BUCKET"]

    with pytest.raises(ConfigurationError, match="S3_BUCKET is required"):
        AppConfig.from_env(environment)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("POSTGRES_PORT", "0"),
        ("S3_TOTAL_MAX_ATTEMPTS", "0"),
        ("HTTP_MAX_ATTEMPTS", "many"),
        ("SNAPSHOT_PAGE_SIZE", "-1"),
    ],
)
def test_invalid_positive_integer_configuration_is_rejected(name: str, value: str) -> None:
    environment = dict(ENVIRONMENT)
    environment[name] = value

    with pytest.raises(ConfigurationError, match=name):
        AppConfig.from_env(environment)


@pytest.mark.parametrize(
    "name",
    ["S3_CONNECT_TIMEOUT_SECONDS", "S3_READ_TIMEOUT_SECONDS", "HTTP_READ_TIMEOUT_SECONDS"],
)
def test_non_finite_timeout_is_rejected(name: str) -> None:
    environment = dict(ENVIRONMENT)
    environment[name] = "nan"

    with pytest.raises(ConfigurationError, match=name):
        AppConfig.from_env(environment)
