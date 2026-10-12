"""Typed runtime configuration loaded only through an explicit call."""

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal, cast
from urllib.parse import urlsplit

AddressingStyle = Literal["path", "virtual"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class ConfigurationError(ValueError):
    """Raised when runtime environment configuration is invalid."""


@dataclass(frozen=True, slots=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class StorageConfig:
    endpoint_url: str
    region: str
    bucket: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    addressing_style: AddressingStyle = "path"


@dataclass(frozen=True, slots=True)
class HttpConfig:
    base_url: str
    connect_timeout_seconds: float
    read_timeout_seconds: float
    max_attempts: int
    backoff_base_seconds: float


@dataclass(frozen=True, slots=True)
class AppConfig:
    """Complete process configuration with redacted secret representations."""

    postgres: PostgresConfig
    storage: StorageConfig
    http: HttpConfig
    snapshot_page_size: int
    log_level: LogLevel

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "AppConfig":
        """Validate configuration from a mapping, defaulting to the process environment."""
        values = os.environ if environ is None else environ

        postgres_port = _positive_int(values, "POSTGRES_PORT")
        if postgres_port > 65535:
            raise ConfigurationError("POSTGRES_PORT must not exceed 65535")

        addressing_style_value = _required(values, "S3_ADDRESSING_STYLE")
        if addressing_style_value not in ("path", "virtual"):
            raise ConfigurationError("S3_ADDRESSING_STYLE must be 'path' or 'virtual'")
        addressing_style = cast(AddressingStyle, addressing_style_value)

        log_level_value = _required(values, "LOG_LEVEL").upper()
        valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if log_level_value not in valid_log_levels:
            raise ConfigurationError("LOG_LEVEL must be a standard Python log level")
        log_level = cast(LogLevel, log_level_value)

        endpoint_url = _url(values, "S3_ENDPOINT_URL")
        base_url = _url(values, "MOCK_API_BASE_URL")

        return cls(
            postgres=PostgresConfig(
                host=_required(values, "POSTGRES_HOST"),
                port=postgres_port,
                database=_required(values, "POSTGRES_DB"),
                user=_required(values, "POSTGRES_USER"),
                password=_required(values, "POSTGRES_PASSWORD"),
            ),
            storage=StorageConfig(
                endpoint_url=endpoint_url,
                region=_required(values, "S3_REGION"),
                bucket=_required(values, "S3_BUCKET"),
                access_key=_required(values, "S3_ACCESS_KEY"),
                secret_key=_required(values, "S3_SECRET_KEY"),
                addressing_style=addressing_style,
            ),
            http=HttpConfig(
                base_url=base_url,
                connect_timeout_seconds=_positive_float(values, "HTTP_CONNECT_TIMEOUT_SECONDS"),
                read_timeout_seconds=_positive_float(values, "HTTP_READ_TIMEOUT_SECONDS"),
                max_attempts=_positive_int(values, "HTTP_MAX_ATTEMPTS"),
                backoff_base_seconds=_positive_float(values, "HTTP_BACKOFF_BASE_SECONDS"),
            ),
            snapshot_page_size=_positive_int(values, "SNAPSHOT_PAGE_SIZE"),
            log_level=log_level,
        )


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"{name} is required")
    return value


def _positive_int(environ: Mapping[str, str], name: str) -> int:
    raw_value = _required(environ, name)
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a positive integer") from error
    if value <= 0:
        raise ConfigurationError(f"{name} must be a positive integer")
    return value


def _positive_float(environ: Mapping[str, str], name: str) -> float:
    raw_value = _required(environ, name)
    try:
        value = float(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a positive number") from error
    if not math.isfinite(value) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive number")
    return value


def _url(environ: Mapping[str, str], name: str) -> str:
    value = _required(environ, name).rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigurationError(f"{name} must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError(f"{name} must not contain credentials")
    return value
