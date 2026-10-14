"""Integration coverage for the shared S3 storage boundary against local Garage."""

import hashlib
import io
import os
import threading
import time
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast

import pytest
from botocore.exceptions import ClientError

from batch_ingestion.config import AppConfig, StorageConfig
from batch_ingestion.identifiers import BatchIdentity, SourceType, manifest_key_for, raw_object_key
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.storage import (
    ImmutableWriteStatus,
    MoveStatus,
    ObjectConflictError,
    ReconciliationState,
    S3Storage,
    create_s3_client,
)

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PAYLOAD = b'{"rates":[1,2,3]}\n'
PAYLOAD_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()
CONFLICTING_PAYLOAD = b"conflicting bytes\n"
PREFIX = "_integration/bi030"


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
    configured_path = Path(os.environ.get("BI030_ENV_FILE", ".env"))
    env_path = (
        configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path
    )
    return AppConfig.from_env(_load_dotenv(env_path))


@pytest.fixture(scope="module")
def storage(app_config: AppConfig) -> Iterator[S3Storage]:
    client = create_s3_client(app_config.storage)
    with closing(client):
        yield S3Storage(client=client, bucket=app_config.storage.bucket)


def _delete_keys(app_config: AppConfig, *keys: str) -> None:
    client = create_s3_client(app_config.storage)
    with closing(client):
        for key in keys:
            client.delete_object(Bucket=app_config.storage.bucket, Key=key)


def test_immutable_write_is_idempotent_and_conflict_preserves_original(
    app_config: AppConfig, storage: S3Storage
) -> None:
    key = f"{PREFIX}/immutable.json"
    _delete_keys(app_config, key)
    try:
        created = storage.write_immutable(
            key,
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )
        unchanged = storage.write_immutable(
            key,
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )

        assert created.status is ImmutableWriteStatus.CREATED
        assert unchanged.status is ImmutableWriteStatus.UNCHANGED
        with pytest.raises(ObjectConflictError):
            storage.write_immutable(
                key,
                io.BytesIO(CONFLICTING_PAYLOAD),
                content_length=len(CONFLICTING_PAYLOAD),
                checksum_sha256=hashlib.sha256(CONFLICTING_PAYLOAD).hexdigest(),
                content_type="application/json",
            )
        after_conflict = storage.write_immutable(
            key,
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )
        assert after_conflict.status is ImmutableWriteStatus.UNCHANGED
        assert after_conflict.object.checksum_sha256 == PAYLOAD_SHA256
    finally:
        _delete_keys(app_config, key)


def test_bounded_download_and_exact_inspection_verify_garage_bytes(
    app_config: AppConfig, storage: S3Storage
) -> None:
    key = f"{PREFIX}/bounded-download.json"
    _delete_keys(app_config, key)
    try:
        storage.write_immutable(
            key,
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )

        inspected = storage.inspect_object(
            key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )
        assert inspected is not None
        assert inspected.content_type == "application/json"

        with storage.download_spooled(
            key,
            maximum_bytes=len(PAYLOAD),
            spool_threshold_bytes=4,
            expected_content_length=len(PAYLOAD),
            expected_checksum_sha256=PAYLOAD_SHA256,
        ) as downloaded:
            assert downloaded.info == inspected
            assert downloaded.stream.read() == PAYLOAD
    finally:
        _delete_keys(app_config, key)


def test_move_preserves_metadata_and_converges_from_recovery_states(
    app_config: AppConfig, storage: S3Storage
) -> None:
    source_key = f"{PREFIX}/landing/input.json"
    destination_key = f"{PREFIX}/archive/input.json"
    _delete_keys(app_config, source_key, destination_key)
    client = create_s3_client(app_config.storage)
    try:
        storage.write_immutable(
            source_key,
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )
        source_only_moved = storage.move_verified(
            source_key,
            destination_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )
        assert source_only_moved.status is MoveStatus.MOVED

        _delete_keys(app_config, source_key, destination_key)
        storage.write_immutable(
            source_key,
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )
        client.copy_object(
            Bucket=app_config.storage.bucket,
            Key=destination_key,
            CopySource={"Bucket": app_config.storage.bucket, "Key": source_key},
            MetadataDirective="COPY",
        )

        both_identical_moved = storage.move_verified(
            source_key,
            destination_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )
        already_moved = storage.move_verified(
            source_key,
            destination_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )
        destination_head = client.head_object(Bucket=app_config.storage.bucket, Key=destination_key)

        assert both_identical_moved.status is MoveStatus.MOVED
        assert already_moved.status is MoveStatus.ALREADY_MOVED
        assert destination_head["ContentType"] == "application/json"
        assert destination_head["Metadata"]["checksum-sha256"] == PAYLOAD_SHA256
    finally:
        client.close()
        _delete_keys(app_config, source_key, destination_key)


def test_reconciliation_recovers_payload_without_manifest(
    app_config: AppConfig, storage: S3Storage
) -> None:
    identity = BatchIdentity.create("bi030-fx-rates", SourceType.API, date(2026, 9, 1))
    payload_key = raw_object_key(identity, "page-0001.json")
    manifest_key = manifest_key_for(payload_key)
    _delete_keys(app_config, payload_key, manifest_key)
    try:
        storage.write_immutable(
            payload_key,
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )
        payload_only = storage.reconcile_payload_manifest(
            payload_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )
        assert payload_only.state is ReconciliationState.PAYLOAD_ONLY

        manifest = Manifest(
            source_name=identity.source_name,
            source_type=identity.source_type,
            source_date=identity.source_date,
            batch_id=payload_key.split("batch_id=")[1].split("/")[0],
            run_id="run_00000000000000000000000000000001",
            object_key=payload_key,
            original_file_name=None,
            content_type="application/json",
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            schema_version=1,
            extracted_at=datetime(2026, 9, 1, tzinfo=UTC),
            status=ManifestStatus.RAW_STORED,
            attempt=2,
        )
        manifest_bytes = manifest.to_json().encode()
        storage.write_immutable(
            manifest_key,
            io.BytesIO(manifest_bytes),
            content_length=len(manifest_bytes),
            checksum_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            content_type="application/json",
        )

        complete = storage.reconcile_payload_manifest(
            payload_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )
        assert complete.state is ReconciliationState.COMPLETE
        assert complete.manifest == manifest
        assert complete.payload == payload_only.payload
    finally:
        _delete_keys(app_config, payload_key, manifest_key)


class _UnavailableHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        server = cast(_CountingServer, self.server)
        server.request_count += 1
        self.send_response(503)
        self.send_header("Content-Length", "0")
        self.send_header("x-amz-request-id", "bi030-fault")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


class _CountingServer(ThreadingHTTPServer):
    request_count: int = 0


def test_unavailable_endpoint_stops_at_configured_attempt_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    server = _CountingServer(("127.0.0.1", 0), _UnavailableHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = cast(tuple[str, int], server.server_address)[1]
    config = StorageConfig(
        endpoint_url=f"http://127.0.0.1:{port}",
        region="garage",
        bucket="unavailable",
        access_key="test-access",
        secret_key="test-secret",
        addressing_style="path",
        total_max_attempts=3,
        connect_timeout_seconds=1,
        read_timeout_seconds=1,
    )
    client = create_s3_client(config)
    started = time.monotonic()
    try:
        with pytest.raises(ClientError) as captured:
            client.get_object(Bucket=config.bucket, Key="object")
        elapsed = time.monotonic() - started
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert captured.value.response.get("Error", {}).get("Code") == "503"
    assert server.request_count == config.total_max_attempts
    assert elapsed < 10
    assert not thread.is_alive()
