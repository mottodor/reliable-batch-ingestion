"""Isolated BI-093 acceptance for unavailable S3 across all source commands."""

from __future__ import annotations

import io
import json
import os
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing
from datetime import date
from hashlib import sha256
from pathlib import Path

import pytest
from mypy_boto3_s3 import S3Client

from batch_ingestion import cli as api_cli
from batch_ingestion.api.logical_manifest import api_logical_run_manifest_key
from batch_ingestion.api.sources import api_page_identity, get_source_descriptor, page_payload_name
from batch_ingestion.config import AppConfig
from batch_ingestion.files import DeliveryDescriptor, RejectionCode
from batch_ingestion.files import cli as file_cli
from batch_ingestion.identifiers import derive_batch_id, manifest_key_for, raw_object_key
from batch_ingestion.postgres_snapshot import SnapshotDescriptor
from batch_ingestion.postgres_snapshot import cli as snapshot_cli
from batch_ingestion.storage import create_s3_client

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SUMMARY_METRICS = {
    "records_read",
    "records_accepted",
    "records_rejected",
    "pages_fetched",
    "bytes_written",
    "retry_count",
    "duplicate_count",
    "duration_seconds",
    "source_date",
    "batch_id",
    "run_id",
}


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


@pytest.fixture(scope="module")
def app_config() -> AppConfig:
    configured_path = Path(os.environ.get("BI093_ENV_FILE", ".env"))
    env_path = (
        configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path
    )
    return AppConfig.from_env(_load_dotenv(env_path))


@pytest.fixture(scope="module")
def s3_client(app_config: AppConfig) -> Iterator[S3Client]:
    client = create_s3_client(app_config.storage)
    with closing(client):
        yield client


def _unavailable_environment(app_config: AppConfig) -> dict[str, str]:
    environment = _load_dotenv(Path(os.environ.get("BI093_ENV_FILE", REPOSITORY_ROOT / ".env")))
    environment.update(
        {
            "S3_ENDPOINT_URL": "http://127.0.0.1:1",
            "S3_TOTAL_MAX_ATTEMPTS": "1",
            "S3_CONNECT_TIMEOUT_SECONDS": "0.1",
            "S3_READ_TIMEOUT_SECONDS": "0.1",
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
        }
    )
    assert AppConfig.from_env(environment).storage.bucket == app_config.storage.bucket
    return environment


def _delete_exact_keys(client: S3Client, app_config: AppConfig, keys: Sequence[str]) -> None:
    for key in keys:
        client.delete_object(Bucket=app_config.storage.bucket, Key=key)


def _assert_exact_keys_absent(
    client: S3Client,
    app_config: AppConfig,
    keys: Sequence[str],
) -> None:
    for key in keys:
        contents = client.list_objects_v2(
            Bucket=app_config.storage.bucket,
            Prefix=key,
        ).get("Contents", [])
        assert not any(item.get("Key") == key for item in contents), key


def _events(output: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in output.getvalue().splitlines() if line]


def _assert_terminal_pair(
    events: Sequence[Mapping[str, object]],
    *,
    event_name: str,
    error_code: str,
    expected_batch_id: str,
    forbidden_values: Sequence[str],
) -> None:
    failures = [event for event in events if event.get("event") == event_name]
    summaries = [event for event in events if event.get("event") == "run_summary"]
    assert len(failures) == len(summaries) == 1
    assert events[-2:] == [failures[0], summaries[0]]
    assert failures[0]["error_code"] == error_code
    assert failures[0]["batch_id"] == summaries[0]["batch_id"] == expected_batch_id
    assert failures[0]["run_id"] == summaries[0]["run_id"]
    assert summaries[0].keys() >= SUMMARY_METRICS
    assert not any(event.get("event") == "cli_error" for event in events)
    serialized = "\n".join(json.dumps(event, sort_keys=True) for event in events)
    for value in forbidden_values:
        assert value not in serialized


def test_api_command_emits_terminal_pair_when_s3_is_unavailable(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 11, 11)
    descriptor = get_source_descriptor("fx-rates")
    identity = api_page_identity(
        descriptor,
        source_date,
        page_size=app_config.http.page_size,
    )
    raw_key = raw_object_key(
        identity,
        page_payload_name(1, max_pages=app_config.http.max_pages),
    )
    keys = (
        raw_key,
        manifest_key_for(raw_key),
        api_logical_run_manifest_key(identity),
    )
    environment = _unavailable_environment(app_config)
    output = io.StringIO()
    _delete_exact_keys(s3_client, app_config, keys)
    _assert_exact_keys_absent(s3_client, app_config, keys)

    started_at = time.monotonic()
    exit_code = api_cli.main(
        ["run", "--source", descriptor.name, "--date", source_date.isoformat()],
        environ=environment,
        output=output,
    )

    assert exit_code == api_cli.CliExitCode.INGESTION_FAILED
    assert time.monotonic() - started_at < 10.0
    events = _events(output)
    _assert_terminal_pair(
        events,
        event_name="api_ingestion",
        error_code="storage_error",
        expected_batch_id=derive_batch_id(identity),
        forbidden_values=(
            app_config.storage.access_key,
            app_config.storage.secret_key,
            app_config.postgres.password,
            "127.0.0.1:1",
        ),
    )
    assert events[-1]["records_read"] == 0
    assert events[-1]["bytes_written"] == 0
    _assert_exact_keys_absent(s3_client, app_config, keys)


def test_file_command_emits_terminal_pair_when_s3_is_unavailable(
    app_config: AppConfig,
    s3_client: S3Client,
    tmp_path: Path,
) -> None:
    source_date = date(2099, 11, 12)
    payload = (
        "supplier_id,product_id,price_minor_units,currency,effective_date\n"
        f"SUP-UNAVAILABLE,SKU-UNAVAILABLE,1250,USD,{source_date.isoformat()}\n"
    ).encode()
    fixture = tmp_path / f"supplier-prices-{source_date.isoformat()}.csv"
    fixture.write_bytes(payload)
    delivery = DeliveryDescriptor.create(
        source_name="supplier-prices",
        source_date=source_date,
        original_file_name=fixture.name,
        actual_sha256=sha256(payload).hexdigest(),
    )
    keys = (
        delivery.landing_key,
        delivery.raw_key,
        delivery.manifest_key,
        delivery.archive_key,
        *(delivery.rejected_key(code) for code in RejectionCode),
    )
    environment = _unavailable_environment(app_config)
    output = io.StringIO()
    _delete_exact_keys(s3_client, app_config, keys)
    _assert_exact_keys_absent(s3_client, app_config, keys)

    started_at = time.monotonic()
    exit_code = file_cli.main(
        [
            "run",
            "--source",
            "supplier-prices",
            "--date",
            source_date.isoformat(),
            "--path",
            str(fixture),
        ],
        environ=environment,
        output=output,
    )

    assert exit_code == file_cli.CliExitCode.INGESTION_FAILED
    assert time.monotonic() - started_at < 10.0
    events = _events(output)
    _assert_terminal_pair(
        events,
        event_name="file_ingestion",
        error_code="storage_failed",
        expected_batch_id=delivery.batch_id,
        forbidden_values=(
            app_config.storage.access_key,
            app_config.storage.secret_key,
            app_config.postgres.password,
            "127.0.0.1:1",
            "SUP-UNAVAILABLE",
            "SKU-UNAVAILABLE",
        ),
    )
    assert events[-1]["records_read"] == 0
    assert events[-1]["bytes_written"] == 0
    _assert_exact_keys_absent(s3_client, app_config, keys)


def test_snapshot_command_emits_terminal_pair_when_s3_is_unavailable(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 11, 13)
    descriptor = SnapshotDescriptor.empty(source_name="orders", source_date=source_date)
    raw_key = descriptor.raw_key(
        chunk_ordinal=1,
        chunk_rows=app_config.snapshot.chunk_rows,
    )
    keys = (raw_key, manifest_key_for(raw_key))
    environment = _unavailable_environment(app_config)
    output = io.StringIO()
    _delete_exact_keys(s3_client, app_config, keys)
    _assert_exact_keys_absent(s3_client, app_config, keys)

    started_at = time.monotonic()
    exit_code = snapshot_cli.main(
        ["run", "--source", "orders", "--date", source_date.isoformat()],
        environ=environment,
        output=output,
    )

    assert exit_code == snapshot_cli.CliExitCode.INGESTION_FAILED
    assert time.monotonic() - started_at < 10.0
    events = _events(output)
    assert [event["event"] for event in events] == [
        "snapshot_started",
        "snapshot_failed",
        "run_summary",
    ]
    _assert_terminal_pair(
        events,
        event_name="snapshot_failed",
        error_code="storage_failed",
        expected_batch_id=descriptor.batch_id(chunk_rows=app_config.snapshot.chunk_rows),
        forbidden_values=(
            app_config.storage.access_key,
            app_config.storage.secret_key,
            app_config.postgres.password,
            "127.0.0.1:1",
        ),
    )
    assert events[-1]["records_read"] == 0
    assert events[-1]["bytes_written"] == 0
    _assert_exact_keys_absent(s3_client, app_config, keys)
