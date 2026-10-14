import pytest

from batch_ingestion.config import AppConfig, ConfigurationError

ENVIRONMENT = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "batch_lab",
    "POSTGRES_USER": "batch_lab",
    "POSTGRES_PASSWORD": "postgres-secret",
    "POSTGRES_CONNECT_TIMEOUT_SECONDS": "3",
    "POSTGRES_STATEMENT_TIMEOUT_SECONDS": "30",
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
    "SNAPSHOT_MAX_ELAPSED_SECONDS": "120",
    "SNAPSHOT_CHUNK_ROWS": "100",
    "SNAPSHOT_MAX_CHUNKS": "100",
    "SNAPSHOT_MAX_CHUNK_BYTES": "8388608",
    "SNAPSHOT_SPOOL_THRESHOLD_BYTES": "1048576",
    "LOG_LEVEL": "INFO",
}


def test_configuration_is_loaded_only_from_explicit_mapping() -> None:
    config = AppConfig.from_env(ENVIRONMENT)

    assert config.postgres.port == 5432
    assert config.postgres.connect_timeout_seconds == 3.0
    assert config.postgres.statement_timeout_seconds == 30.0
    assert config.storage.addressing_style == "path"
    assert config.storage.total_max_attempts == 3
    assert config.storage.connect_timeout_seconds == 2.0
    assert config.storage.read_timeout_seconds == 5.0
    assert config.http.max_attempts == 4
    assert config.file.max_file_bytes == 8_388_608
    assert config.file.spool_threshold_bytes == 1_048_576
    assert config.file.max_records == 100_000
    assert config.file.parquet_batch_rows == 1_024
    assert config.file.max_diagnostic_details == 100
    assert config.snapshot.max_elapsed_seconds == 120.0
    assert config.snapshot.chunk_rows == 100
    assert config.snapshot.max_chunks == 100
    assert config.snapshot.max_records == 10_000
    assert config.snapshot.max_chunk_bytes == 8_388_608
    assert config.snapshot.spool_threshold_bytes == 1_048_576


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
        ("FILE_MAX_BYTES", "0"),
        ("FILE_SPOOL_THRESHOLD_BYTES", "-1"),
        ("FILE_MAX_RECORDS", "many"),
        ("FILE_PARQUET_BATCH_ROWS", "0"),
        ("FILE_MAX_DIAGNOSTIC_DETAILS", "-1"),
        ("SNAPSHOT_CHUNK_ROWS", "-1"),
        ("SNAPSHOT_MAX_CHUNKS", "0"),
        ("SNAPSHOT_MAX_CHUNK_BYTES", "many"),
        ("SNAPSHOT_SPOOL_THRESHOLD_BYTES", "-1"),
    ],
)
def test_invalid_positive_integer_configuration_is_rejected(name: str, value: str) -> None:
    environment = dict(ENVIRONMENT)
    environment[name] = value

    with pytest.raises(ConfigurationError, match=name):
        AppConfig.from_env(environment)


def test_snapshot_configuration_enforces_protocol_and_cross_field_bounds() -> None:
    environment = dict(ENVIRONMENT)
    environment["SNAPSHOT_MAX_CHUNKS"] = "10000"
    with pytest.raises(ConfigurationError, match="SNAPSHOT_MAX_CHUNKS must not exceed 9999"):
        AppConfig.from_env(environment)

    environment = dict(ENVIRONMENT)
    environment["SNAPSHOT_MAX_CHUNK_BYTES"] = "1024"
    environment["SNAPSHOT_SPOOL_THRESHOLD_BYTES"] = "1025"
    with pytest.raises(
        ConfigurationError,
        match=("SNAPSHOT_SPOOL_THRESHOLD_BYTES must not exceed SNAPSHOT_MAX_CHUNK_BYTES"),
    ):
        AppConfig.from_env(environment)


def test_file_spool_threshold_must_not_exceed_maximum_file_bytes() -> None:
    environment = dict(ENVIRONMENT)
    environment["FILE_MAX_BYTES"] = "1024"
    environment["FILE_SPOOL_THRESHOLD_BYTES"] = "1025"

    with pytest.raises(
        ConfigurationError,
        match="FILE_SPOOL_THRESHOLD_BYTES must not exceed FILE_MAX_BYTES",
    ):
        AppConfig.from_env(environment)


@pytest.mark.parametrize(
    "name",
    [
        "S3_CONNECT_TIMEOUT_SECONDS",
        "S3_READ_TIMEOUT_SECONDS",
        "HTTP_READ_TIMEOUT_SECONDS",
        "POSTGRES_CONNECT_TIMEOUT_SECONDS",
        "POSTGRES_STATEMENT_TIMEOUT_SECONDS",
        "SNAPSHOT_MAX_ELAPSED_SECONDS",
    ],
)
def test_non_finite_timeout_is_rejected(name: str) -> None:
    environment = dict(ENVIRONMENT)
    environment[name] = "nan"

    with pytest.raises(ConfigurationError, match=name):
        AppConfig.from_env(environment)
