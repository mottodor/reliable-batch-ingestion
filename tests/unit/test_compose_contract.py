"""Static contract tests for the BI-020 local Compose stack."""

import tomllib
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPOSITORY_ROOT / "compose.yaml"
GARAGE_CONFIG_PATH = REPOSITORY_ROOT / "infrastructure" / "garage.toml"
ENV_EXAMPLE_PATH = REPOSITORY_ROOT / ".env.example"


def test_compose_uses_exact_images_and_named_persistence() -> None:
    compose = COMPOSE_PATH.read_text(encoding="utf-8")

    assert "image: postgres:17.6-bookworm" in compose
    assert "image: dxflrs/garage:v2.4.1" in compose
    assert "postgres_data:" in compose
    assert "garage_metadata:" in compose
    assert "garage_data:" in compose
    assert "postgres_data:/var/lib/postgresql/data" in compose
    assert "garage_metadata:/var/lib/garage/meta" in compose
    assert "garage_data:/var/lib/garage/data" in compose
    assert "latest" not in compose


def test_compose_has_bounded_healthchecks_and_default_bucket_bootstrap() -> None:
    compose = COMPOSE_PATH.read_text(encoding="utf-8")

    assert compose.count("healthcheck:") == 3
    assert "pg_isready" in compose
    assert '["CMD", "/garage", "status"]' in compose
    assert compose.count("timeout: 5s") == 3
    assert compose.count("retries: 12") == 3
    assert 'command: ["/garage", "server", "--single-node", "--default-bucket"]' in compose
    assert "GARAGE_DEFAULT_ACCESS_KEY: ${S3_ACCESS_KEY:?" in compose
    assert "GARAGE_DEFAULT_SECRET_KEY: ${S3_SECRET_KEY:?" in compose
    assert "GARAGE_DEFAULT_BUCKET: ${S3_BUCKET:?" in compose
    assert "GARAGE_RPC_SECRET: ${GARAGE_RPC_SECRET:?" in compose


def test_garage_config_separates_metadata_and_object_data() -> None:
    config = tomllib.loads(GARAGE_CONFIG_PATH.read_text(encoding="utf-8"))

    assert config["metadata_dir"] == "/var/lib/garage/meta"
    assert config["data_dir"] == "/var/lib/garage/data"
    assert config["db_engine"] == "sqlite"
    assert config["replication_factor"] == 1
    assert config["rpc_bind_addr"] == "[::]:3901"
    assert config["rpc_public_addr"] == "garage:3901"
    assert config["s3_api"]["s3_region"] == "garage"
    assert config["s3_api"]["api_bind_addr"] == "[::]:3900"
    assert config["admin"]["api_bind_addr"] == "[::]:3903"
    assert "rpc_secret" not in config


def test_example_environment_declares_infrastructure_values_without_real_secrets() -> None:
    env_example = ENV_EXAMPLE_PATH.read_text(encoding="utf-8")

    assert "S3_PORT=3900" in env_example
    assert "GARAGE_ADMIN_PORT=3903" in env_example
    assert "GARAGE_RPC_SECRET=change-me" in env_example
    assert "GARAGE_RPC_SECRET=" not in env_example.replace("GARAGE_RPC_SECRET=change-me", "")
