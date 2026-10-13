"""Integration checks for the BI-020 PostgreSQL and Garage services."""

import hashlib
import os
from collections.abc import Iterator, Mapping
from contextlib import closing
from pathlib import Path

import boto3
import httpx
import psycopg
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from batch_ingestion.config import AppConfig

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SENTINEL_KEY = "_infrastructure/bi020-bootstrap-sentinel.txt"
SENTINEL_BODY = b"BI-020 bootstrap persistence sentinel\n"
SENTINEL_SHA256 = "b3ced96ccf85f2a526ec7f93c7ad9b84944838abb70a882830a7bd89c3d19923"
POSTGRES_MARKER_TABLE = "bi020_infrastructure_probe"
POSTGRES_MARKER_ID = "persistence-marker"
POSTGRES_MARKER_VALUE = "BI-020 PostgreSQL persistence marker"
POSTGRES_MARKER_SHA256 = hashlib.sha256(POSTGRES_MARKER_VALUE.encode()).hexdigest()


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name:
            raise ValueError(f"invalid environment line for {name or '<unknown>'}")
        values[name] = value
    return values


@pytest.fixture(scope="session")
def environment() -> Mapping[str, str]:
    configured_path = Path(os.environ.get("BI020_ENV_FILE", ".env"))
    env_path = (
        configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path
    )
    return _load_dotenv(env_path)


@pytest.fixture(scope="session")
def app_config(environment: Mapping[str, str]) -> AppConfig:
    return AppConfig.from_env(environment)


@pytest.fixture(scope="session")
def s3_client(app_config: AppConfig) -> Iterator[S3Client]:
    client = boto3.client(
        "s3",
        endpoint_url=app_config.storage.endpoint_url,
        region_name=app_config.storage.region,
        aws_access_key_id=app_config.storage.access_key,
        aws_secret_access_key=app_config.storage.secret_key,
        config=Config(
            connect_timeout=2,
            read_timeout=5,
            retries={"max_attempts": 2, "mode": "standard"},
            s3={"addressing_style": app_config.storage.addressing_style},
        ),
    )
    with closing(client):
        yield client


def test_postgres_accepts_queries(app_config: AppConfig) -> None:
    with (
        psycopg.connect(
            host=app_config.postgres.host,
            port=app_config.postgres.port,
            dbname=app_config.postgres.database,
            user=app_config.postgres.user,
            password=app_config.postgres.password,
            connect_timeout=5,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        assert cursor.fetchone() == (1,)


def test_postgres_persistence_marker(app_config: AppConfig) -> None:
    marker_mode = os.environ.get("BI020_MARKER_MODE", "create")
    if marker_mode not in {"create", "verify", "absent"}:
        raise ValueError("BI020_MARKER_MODE must be 'create', 'verify', or 'absent'")

    with (
        psycopg.connect(
            host=app_config.postgres.host,
            port=app_config.postgres.port,
            dbname=app_config.postgres.database,
            user=app_config.postgres.user,
            password=app_config.postgres.password,
            connect_timeout=5,
        ) as connection,
        connection.cursor() as cursor,
    ):
        if marker_mode == "absent":
            cursor.execute("SELECT to_regclass(%s)", (f"public.{POSTGRES_MARKER_TABLE}",))
            assert cursor.fetchone() == (None,)
            return

        if marker_mode == "create":
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {POSTGRES_MARKER_TABLE} (
                    marker_id text PRIMARY KEY,
                    marker_value text NOT NULL,
                    checksum_sha256 char(64) NOT NULL
                )
                """
            )
            cursor.execute(
                f"""
                INSERT INTO {POSTGRES_MARKER_TABLE}
                    (marker_id, marker_value, checksum_sha256)
                VALUES (%s, %s, %s)
                ON CONFLICT (marker_id) DO NOTHING
                """,
                (POSTGRES_MARKER_ID, POSTGRES_MARKER_VALUE, POSTGRES_MARKER_SHA256),
            )

        cursor.execute(
            f"""
            SELECT marker_value, checksum_sha256
            FROM {POSTGRES_MARKER_TABLE}
            WHERE marker_id = %s
            """,
            (POSTGRES_MARKER_ID,),
        )
        assert cursor.fetchone() == (POSTGRES_MARKER_VALUE, POSTGRES_MARKER_SHA256)


def test_garage_admin_health(environment: Mapping[str, str]) -> None:
    admin_port = int(environment["GARAGE_ADMIN_PORT"])

    response = httpx.get(f"http://localhost:{admin_port}/health", timeout=5)

    assert response.status_code == 200
    assert "operational" in response.text.lower()


def _read_sentinel(s3_client: S3Client, bucket: str) -> bytes | None:
    try:
        result = s3_client.get_object(Bucket=bucket, Key=SENTINEL_KEY)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
            return None
        raise

    body = result["Body"]
    try:
        content = body.read()
    finally:
        body.close()
    assert result["Metadata"].get("sha256") == SENTINEL_SHA256
    return content


def test_configured_bucket_and_bootstrap_sentinel(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    bucket = app_config.storage.bucket
    s3_client.head_bucket(Bucket=bucket)
    marker_mode = os.environ.get("BI020_MARKER_MODE", "create")
    if marker_mode not in {"create", "verify", "absent"}:
        raise ValueError("BI020_MARKER_MODE must be 'create', 'verify', or 'absent'")
    content = _read_sentinel(s3_client, bucket)

    if marker_mode == "absent":
        assert content is None
        return

    if marker_mode == "create" and content is None:
        s3_client.put_object(
            Bucket=bucket,
            Key=SENTINEL_KEY,
            Body=SENTINEL_BODY,
            ContentType="text/plain",
            Metadata={"sha256": SENTINEL_SHA256},
        )
        content = _read_sentinel(s3_client, bucket)

    if content is None:
        pytest.fail(f"bootstrap sentinel is unavailable in {marker_mode} mode")
    assert content == SENTINEL_BODY
