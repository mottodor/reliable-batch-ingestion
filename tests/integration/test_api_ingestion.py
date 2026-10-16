"""Live BI-050 happy-path ingestion against the local mock API and Garage."""

import hashlib
import io
import os
from collections.abc import Iterator
from contextlib import closing
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import BinaryIO

import pytest
from mock_api.app import MALFORMED_JSON_BODY
from mypy_boto3_s3 import S3Client

from batch_ingestion.api.client import open_http_client
from batch_ingestion.api.logical_manifest import (
    API_LOGICAL_RUN_MANIFEST_CONTENT_TYPE,
    ApiLogicalRunManifest,
    api_logical_run_manifest_key,
)
from batch_ingestion.api.service import (
    ApiErrorCode,
    ApiIngestionError,
    ingest_api_backfill,
    ingest_api_date,
)
from batch_ingestion.api.sources import api_page_identity, get_source_descriptor, page_payload_name
from batch_ingestion.config import AppConfig, HttpConfig
from batch_ingestion.identifiers import manifest_key_for, raw_object_key
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.storage import ImmutableWriteResult, S3Storage, create_s3_client

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_HTTP_DEFAULTS = {
    "HTTP_BACKOFF_MAX_SECONDS": "4",
    "HTTP_MAX_ELAPSED_SECONDS": "20",
    "HTTP_PAGE_SIZE": "3",
    "HTTP_MAX_PAGES": "100",
    "HTTP_MAX_RESPONSE_BYTES": "1048576",
    "HTTP_MAX_BACKFILL_DAYS": "31",
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
    for name, value in _HTTP_DEFAULTS.items():
        values.setdefault(name, value)
    return values


@pytest.fixture(scope="module")
def app_config() -> AppConfig:
    configured_path = Path(os.environ.get("BI050_ENV_FILE", ".env"))
    env_path = (
        configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path
    )
    return AppConfig.from_env(_load_dotenv(env_path))


@pytest.fixture(scope="module")
def s3_client(app_config: AppConfig) -> Iterator[S3Client]:
    client = create_s3_client(app_config.storage)
    with closing(client):
        yield client


def _delete_exact_keys(client: S3Client, app_config: AppConfig, keys: tuple[str, ...]) -> None:
    for key in keys:
        client.delete_object(Bucket=app_config.storage.bucket, Key=key)


def _page_keys(
    descriptor_name: str,
    source_date: date,
    http_config: HttpConfig,
    pages: int,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    descriptor = get_source_descriptor(descriptor_name)
    identity = api_page_identity(descriptor, source_date, page_size=http_config.page_size)
    payload_keys = tuple(
        raw_object_key(identity, page_payload_name(page, max_pages=http_config.max_pages))
        for page in range(1, pages + 1)
    )
    return payload_keys, tuple(manifest_key_for(key) for key in payload_keys)


def _logical_run_key(
    descriptor_name: str,
    source_date: date,
    http_config: HttpConfig,
) -> str:
    descriptor = get_source_descriptor(descriptor_name)
    identity = api_page_identity(descriptor, source_date, page_size=http_config.page_size)
    return api_logical_run_manifest_key(identity)


def _object_bytes(client: S3Client, app_config: AppConfig, key: str) -> bytes:
    response = client.get_object(Bucket=app_config.storage.bucket, Key=key)
    with closing(response["Body"]):
        return response["Body"].read()


def _key_exists(client: S3Client, app_config: AppConfig, key: str) -> bool:
    contents = client.list_objects_v2(
        Bucket=app_config.storage.bucket,
        Prefix=key,
    ).get("Contents", [])
    return any(item.get("Key") == key for item in contents)


def _existing_inventory(
    client: S3Client,
    app_config: AppConfig,
    keys: tuple[str, ...],
) -> dict[str, bytes]:
    return {
        key: _object_bytes(client, app_config, key)
        for key in keys
        if _key_exists(client, app_config, key)
    }


def _exact_batch_inventory(
    client: S3Client,
    app_config: AppConfig,
    payload_keys: tuple[str, ...],
    manifest_keys: tuple[str, ...],
    logical_run_manifest_keys: tuple[str, ...],
) -> dict[str, str]:
    expected_keys = {*payload_keys, *manifest_keys, *logical_run_manifest_keys}
    prefixes = {key.rsplit("/", 1)[0] + "/" for key in expected_keys}
    found_keys: set[str] = set()
    for prefix in prefixes:
        objects = client.list_objects_v2(
            Bucket=app_config.storage.bucket,
            Prefix=prefix,
        ).get("Contents", [])
        found_keys.update(key for item in objects if (key := item.get("Key")) is not None)
    assert found_keys == expected_keys
    return {
        key: hashlib.sha256(_object_bytes(client, app_config, key)).hexdigest()
        for key in sorted(found_keys)
    }


class _AggregateWriteFaultStorage(S3Storage):
    def __init__(
        self,
        *,
        client: S3Client,
        bucket: str,
        write_then_raise: bool,
    ) -> None:
        super().__init__(client=client, bucket=bucket)
        self._write_then_raise = write_then_raise
        self.aggregate_write_attempts = 0

    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        if not key.endswith("/logical-run.manifest.json"):
            return super().write_immutable(
                key,
                stream,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                content_type=content_type,
            )
        self.aggregate_write_attempts += 1
        if self._write_then_raise and self.aggregate_write_attempts == 1:
            super().write_immutable(
                key,
                stream,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                content_type=content_type,
            )
        raise RuntimeError("injected aggregate write uncertainty")


@pytest.mark.parametrize(
    ("source_name", "source_date", "expected_records", "expected_pages", "run_id"),
    [
        ("fx-rates", date(2099, 12, 30), 4, 2, "run_50500000000000000000000000000001"),
        (
            "shipping-statuses",
            date(2099, 12, 31),
            12,
            4,
            "run_50500000000000000000000000000002",
        ),
    ],
)
def test_live_happy_path_preserves_exact_pages_and_manifests(
    app_config: AppConfig,
    s3_client: S3Client,
    source_name: str,
    source_date: date,
    expected_records: int,
    expected_pages: int,
    run_id: str,
) -> None:
    descriptor = get_source_descriptor(source_name)
    http_config: HttpConfig = replace(app_config.http, page_size=3)
    identity = api_page_identity(descriptor, source_date, page_size=http_config.page_size)
    payload_keys = tuple(
        raw_object_key(
            identity,
            page_payload_name(page, max_pages=http_config.max_pages),
        )
        for page in range(1, expected_pages + 1)
    )
    manifest_keys = tuple(manifest_key_for(key) for key in payload_keys)
    aggregate_key = api_logical_run_manifest_key(identity)
    all_keys = (*payload_keys, *manifest_keys, aggregate_key)
    _delete_exact_keys(s3_client, app_config, all_keys)

    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)
    try:
        with open_http_client(http_config) as client:
            result = ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("happy-path integration must not sleep"),
                run_id_factory=lambda: run_id,
            )

            expected_bodies: list[bytes] = []
            for page in range(1, expected_pages + 1):
                response = client.get(
                    descriptor.path,
                    params={
                        "source_date": source_date.isoformat(),
                        "page": page,
                        "page_size": http_config.page_size,
                    },
                )
                assert response.status_code == 200
                expected_bodies.append(response.content)

        assert result.payload_keys == payload_keys
        assert result.manifest_keys == manifest_keys
        assert result.logical_run_manifest_key == aggregate_key
        assert result.summary.records_read == expected_records
        assert result.summary.records_accepted == expected_records
        assert result.summary.pages_fetched == expected_pages
        assert result.summary.bytes_written == sum(map(len, expected_bodies))
        assert result.summary.duplicate_count == 0

        for page, (payload_key, manifest_key, expected_body) in enumerate(
            zip(payload_keys, manifest_keys, expected_bodies, strict=True),
            start=1,
        ):
            payload_response = s3_client.get_object(
                Bucket=app_config.storage.bucket,
                Key=payload_key,
            )
            with closing(payload_response["Body"]):
                assert payload_response["Body"].read() == expected_body

            manifest_response = s3_client.get_object(
                Bucket=app_config.storage.bucket,
                Key=manifest_key,
            )
            with closing(manifest_response["Body"]):
                manifest = Manifest.from_json(manifest_response["Body"].read().decode())
            assert manifest == result.manifests[page - 1]
            assert manifest.object_key == payload_key
            assert manifest.original_file_name is None
            assert manifest.status is ManifestStatus.SUCCEEDED
            assert manifest.attempt == 1
            assert manifest.run_id == run_id

        raw_prefix = payload_keys[0].rsplit("/", 1)[0] + "/"
        manifest_prefix = manifest_keys[0].rsplit("/", 1)[0] + "/"
        raw_inventory = s3_client.list_objects_v2(
            Bucket=app_config.storage.bucket,
            Prefix=raw_prefix,
        ).get("Contents", [])
        manifest_inventory = s3_client.list_objects_v2(
            Bucket=app_config.storage.bucket,
            Prefix=manifest_prefix,
        ).get("Contents", [])
        raw_keys = {key for item in raw_inventory if (key := item.get("Key")) is not None}
        manifest_keys_found = {
            key for item in manifest_inventory if (key := item.get("Key")) is not None
        }
        assert raw_keys == set(payload_keys)
        assert manifest_keys_found == {*manifest_keys, aggregate_key}

        aggregate = ApiLogicalRunManifest.from_bytes(
            _object_bytes(s3_client, app_config, aggregate_key)
        )
        assert aggregate.completion_run_id == run_id
        assert tuple(page.object_key for page in aggregate.pages) == payload_keys
        first_inventory = _existing_inventory(s3_client, app_config, all_keys)

        with open_http_client(http_config) as client:
            replay = ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("complete replay must not sleep"),
                run_id_factory=lambda: "run_50500000000000000000000000000099",
            )

        assert replay.summary.bytes_written == 0
        assert replay.summary.duplicate_count == expected_pages
        assert _existing_inventory(s3_client, app_config, all_keys) == first_inventory
        replayed = ApiLogicalRunManifest.from_bytes(
            _object_bytes(s3_client, app_config, aggregate_key)
        )
        assert replayed.completion_run_id == run_id
        assert replayed.completed_at == aggregate.completed_at
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_rate_limit_once_retries_with_selected_delay(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 12, 20)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3, max_attempts=3)
    payload_keys, manifest_keys = _page_keys("fx-rates", source_date, http_config, 2)
    aggregate_key = _logical_run_key("fx-rates", source_date, http_config)
    all_keys = (*payload_keys, *manifest_keys, aggregate_key)
    scenario_key = "bi050-slice3-rate-limit"
    delays: list[float] = []
    events: list[dict[str, object]] = []
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)

    try:
        with open_http_client(http_config) as client:
            reset = client.post("/__control/reset", params={"scenario_key": scenario_key})
            assert reset.status_code == 200
            result = ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=delays.append,
                query_parameter_provider=lambda _: {
                    "scenario": "rate_limit_once",
                    "scenario_key": scenario_key,
                },
                event_sink=events.append,
                run_id_factory=lambda: "run_50500000000000000000000000000003",
            )

        assert result.summary.retry_count == 1
        assert delays == [1.0]
        assert [event for event in events if event["event"] == "api_retry"] == [
            {
                "event": "api_retry",
                "source_date": source_date.isoformat(),
                "batch_id": result.summary.batch_id,
                "run_id": result.summary.run_id,
                "page": 1,
                "attempt": 2,
                "classification": "rate_limit",
                "http_status": 429,
                "delay_seconds": 1.0,
            }
        ]
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_permanent_500_exhausts_attempts_without_page_objects(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 12, 21)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3, max_attempts=3)
    payload_keys, manifest_keys = _page_keys("fx-rates", source_date, http_config, 2)
    aggregate_key = _logical_run_key("fx-rates", source_date, http_config)
    all_keys = (*payload_keys, *manifest_keys, aggregate_key)
    delays: list[float] = []
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)

    try:
        with (
            open_http_client(http_config) as client,
            pytest.raises(ApiIngestionError) as captured,
        ):
            ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=delays.append,
                query_parameter_provider=lambda _: {"scenario": "permanent_500"},
                run_id_factory=lambda: "run_50500000000000000000000000000004",
            )

        assert captured.value.code is ApiErrorCode.HTTP_RETRY_EXHAUSTED
        assert captured.value.summary.retry_count == 2
        assert not _key_exists(s3_client, app_config, aggregate_key)
        assert captured.value.summary.pages_fetched == 0
        assert delays == [http_config.backoff_base_seconds, http_config.backoff_base_seconds * 2]
        prefix = payload_keys[0].rsplit("/", 1)[0] + "/"
        assert (
            s3_client.list_objects_v2(Bucket=app_config.storage.bucket, Prefix=prefix).get(
                "Contents", []
            )
            == []
        )
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_malformed_json_preserves_exact_body_and_failed_manifest(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 12, 22)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3)
    payload_keys, manifest_keys = _page_keys("fx-rates", source_date, http_config, 2)
    aggregate_key = _logical_run_key("fx-rates", source_date, http_config)
    all_keys = (*payload_keys, *manifest_keys, aggregate_key)
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)

    try:
        with (
            open_http_client(http_config) as client,
            pytest.raises(ApiIngestionError) as captured,
        ):
            ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("malformed HTTP 200 must not retry"),
                query_parameter_provider=lambda _: {
                    "scenario": "malformed_json",
                    "fault_page": "1",
                },
                run_id_factory=lambda: "run_50500000000000000000000000000005",
            )

        assert captured.value.code is ApiErrorCode.INVALID_PAGE
        assert _object_bytes(s3_client, app_config, payload_keys[0]) == MALFORMED_JSON_BODY
        manifest = Manifest.from_json(
            _object_bytes(s3_client, app_config, manifest_keys[0]).decode()
        )
        assert manifest.status is ManifestStatus.FAILED
        assert manifest.checksum_sha256 == hashlib.sha256(MALFORMED_JSON_BODY).hexdigest()
        assert manifest.object_key == payload_keys[0]
        assert not _key_exists(s3_client, app_config, aggregate_key)
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_partial_pages_rerun_reuses_complete_page_and_finishes(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 12, 23)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3, max_attempts=2)
    payload_keys, manifest_keys = _page_keys("fx-rates", source_date, http_config, 2)
    aggregate_key = _logical_run_key("fx-rates", source_date, http_config)
    all_keys = (*payload_keys, *manifest_keys, aggregate_key)
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)

    try:
        with (
            open_http_client(http_config) as client,
            pytest.raises(ApiIngestionError) as captured,
        ):
            ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: None,
                query_parameter_provider=lambda _: {
                    "scenario": "partial_pages",
                    "fault_page": "2",
                },
                run_id_factory=lambda: "run_50500000000000000000000000000006",
            )
        assert captured.value.code is ApiErrorCode.HTTP_RETRY_EXHAUSTED
        page_one_before = _object_bytes(s3_client, app_config, payload_keys[0])
        manifest_one_before = _object_bytes(s3_client, app_config, manifest_keys[0])

        with open_http_client(http_config) as client:
            result = ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("fault-free rerun must not retry"),
                run_id_factory=lambda: "run_50500000000000000000000000000007",
            )

        assert result.summary.duplicate_count == 1
        assert result.summary.bytes_written == len(
            _object_bytes(s3_client, app_config, payload_keys[1])
        )
        assert _object_bytes(s3_client, app_config, payload_keys[0]) == page_one_before
        assert _object_bytes(s3_client, app_config, manifest_keys[0]) == manifest_one_before
        assert len(result.payload_keys) == 2
        aggregate = ApiLogicalRunManifest.from_bytes(
            _object_bytes(s3_client, app_config, aggregate_key)
        )
        page_manifests = tuple(
            Manifest.from_json(_object_bytes(s3_client, app_config, key).decode())
            for key in manifest_keys
        )
        assert tuple(manifest.run_id for manifest in page_manifests) == (
            "run_50500000000000000000000000000006",
            "run_50500000000000000000000000000007",
        )
        assert tuple(page.page_manifest_checksum_sha256 for page in aggregate.pages) == tuple(
            hashlib.sha256(_object_bytes(s3_client, app_config, key)).hexdigest()
            for key in manifest_keys
        )
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_payload_only_page_gains_manifest_without_payload_rewrite(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 12, 24)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3)
    payload_keys, manifest_keys = _page_keys("fx-rates", source_date, http_config, 2)
    aggregate_key = _logical_run_key("fx-rates", source_date, http_config)
    all_keys = (*payload_keys, *manifest_keys, aggregate_key)
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)

    try:
        with open_http_client(http_config) as client:
            response = client.get(
                descriptor.path,
                params={
                    "source_date": source_date.isoformat(),
                    "page": 1,
                    "page_size": http_config.page_size,
                },
            )
            assert response.status_code == 200
            page_one = response.content
            storage.write_immutable(
                payload_keys[0],
                io.BytesIO(page_one),
                content_length=len(page_one),
                checksum_sha256=hashlib.sha256(page_one).hexdigest(),
                content_type=response.headers["Content-Type"],
            )
            result = ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("payload-only recovery must not retry"),
                run_id_factory=lambda: "run_50500000000000000000000000000008",
            )

        assert _object_bytes(s3_client, app_config, payload_keys[0]) == page_one
        assert (
            Manifest.from_json(
                _object_bytes(s3_client, app_config, manifest_keys[0]).decode()
            ).status
            is ManifestStatus.SUCCEEDED
        )
        assert result.summary.bytes_written == len(
            _object_bytes(s3_client, app_config, payload_keys[1])
        )
        assert result.summary.duplicate_count == 0
        assert _key_exists(s3_client, app_config, aggregate_key)
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_final_page_interruption_and_legacy_page_only_replay_publish_only_aggregate(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 12, 25)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3)
    payload_keys, manifest_keys = _page_keys("fx-rates", source_date, http_config, 2)
    aggregate_key = _logical_run_key("fx-rates", source_date, http_config)
    page_keys = (*payload_keys, *manifest_keys)
    all_keys = (*page_keys, aggregate_key)
    _delete_exact_keys(s3_client, app_config, all_keys)
    interrupted = _AggregateWriteFaultStorage(
        client=s3_client,
        bucket=app_config.storage.bucket,
        write_then_raise=False,
    )

    try:
        with (
            open_http_client(http_config) as client,
            pytest.raises(ApiIngestionError) as captured,
        ):
            ingest_api_date(
                client=client,
                storage=interrupted,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("aggregate interruption must not retry HTTP"),
                run_id_factory=lambda: "run_50500000000000000000000000000009",
            )
        assert captured.value.code is ApiErrorCode.LOGICAL_RUN_MANIFEST_WRITE_UNCERTAIN
        assert interrupted.aggregate_write_attempts == 2
        assert not _key_exists(s3_client, app_config, aggregate_key)
        page_inventory = _existing_inventory(s3_client, app_config, page_keys)
        assert set(page_inventory) == set(page_keys)

        storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)
        with open_http_client(http_config) as client:
            recovered = ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("legacy recovery must not retry HTTP"),
                run_id_factory=lambda: "run_5050000000000000000000000000000a",
            )
        assert recovered.summary.bytes_written == 0
        assert recovered.summary.duplicate_count == 2
        assert _existing_inventory(s3_client, app_config, page_keys) == page_inventory
        first_aggregate = _object_bytes(s3_client, app_config, aggregate_key)

        s3_client.delete_object(Bucket=app_config.storage.bucket, Key=aggregate_key)
        with open_http_client(http_config) as client:
            upgraded = ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("legacy replay must not retry HTTP"),
                run_id_factory=lambda: "run_5050000000000000000000000000000b",
            )
        assert upgraded.summary.bytes_written == 0
        assert upgraded.summary.duplicate_count == 2
        assert _existing_inventory(s3_client, app_config, page_keys) == page_inventory
        assert _object_bytes(s3_client, app_config, aggregate_key) != first_aggregate
        assert (
            ApiLogicalRunManifest.from_bytes(
                _object_bytes(s3_client, app_config, aggregate_key)
            ).completion_run_id
            == "run_5050000000000000000000000000000b"
        )
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_write_then_uncertain_reuses_exact_garage_bytes(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    source_date = date(2099, 12, 26)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3)
    payload_keys, manifest_keys = _page_keys("fx-rates", source_date, http_config, 2)
    aggregate_key = _logical_run_key("fx-rates", source_date, http_config)
    all_keys = (*payload_keys, *manifest_keys, aggregate_key)
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = _AggregateWriteFaultStorage(
        client=s3_client,
        bucket=app_config.storage.bucket,
        write_then_raise=True,
    )

    try:
        with open_http_client(http_config) as client:
            result = ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("uncertain aggregate recovery must not retry HTTP"),
                run_id_factory=lambda: "run_5050000000000000000000000000000c",
            )
        assert storage.aggregate_write_attempts == 1
        assert result.logical_run_manifest_key == aggregate_key
        assert (
            ApiLogicalRunManifest.from_bytes(
                _object_bytes(s3_client, app_config, aggregate_key)
            ).completion_run_id
            == "run_5050000000000000000000000000000c"
        )
        _exact_batch_inventory(
            s3_client,
            app_config,
            payload_keys,
            manifest_keys,
            (aggregate_key,),
        )
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


@pytest.mark.parametrize(
    ("damage", "source_date", "expected_code"),
    [
        ("malformed", date(2099, 11, 27), ApiErrorCode.LOGICAL_RUN_MANIFEST_CORRUPT),
        ("noncanonical", date(2099, 11, 28), ApiErrorCode.LOGICAL_RUN_MANIFEST_CORRUPT),
        ("conflicting", date(2099, 11, 29), ApiErrorCode.LOGICAL_RUN_MANIFEST_CONFLICT),
        ("orphaned", date(2099, 11, 30), ApiErrorCode.LOGICAL_RUN_MANIFEST_ORPHANED),
    ],
)
def test_live_invalid_aggregate_blocks_mutation_and_retains_damaged_evidence(
    app_config: AppConfig,
    s3_client: S3Client,
    damage: str,
    source_date: date,
    expected_code: ApiErrorCode,
) -> None:
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3)
    payload_keys, manifest_keys = _page_keys("fx-rates", source_date, http_config, 2)
    aggregate_key = _logical_run_key("fx-rates", source_date, http_config)
    all_keys = (*payload_keys, *manifest_keys, aggregate_key)
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)

    try:
        with open_http_client(http_config) as client:
            ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("setup ingestion must not retry"),
                run_id_factory=lambda: "run_5050000000000000000000000000000d",
            )
        aggregate_bytes = _object_bytes(s3_client, app_config, aggregate_key)
        if damage == "malformed":
            damaged_aggregate = b'{"manifest_type":'
        elif damage == "noncanonical":
            damaged_aggregate = aggregate_bytes + b"\n"
        elif damage == "conflicting":
            aggregate = ApiLogicalRunManifest.from_bytes(aggregate_bytes)
            damaged_page = replace(
                aggregate.pages[0],
                payload_checksum_sha256="f" * 64,
            )
            damaged_aggregate = replace(
                aggregate,
                pages=(damaged_page, *aggregate.pages[1:]),
            ).to_bytes()
        else:
            damaged_aggregate = aggregate_bytes
            s3_client.delete_object(Bucket=app_config.storage.bucket, Key=payload_keys[0])
        s3_client.put_object(
            Bucket=app_config.storage.bucket,
            Key=aggregate_key,
            Body=damaged_aggregate,
            ContentLength=len(damaged_aggregate),
            ContentType=API_LOGICAL_RUN_MANIFEST_CONTENT_TYPE,
        )
        damaged_inventory = _existing_inventory(s3_client, app_config, all_keys)

        with (
            open_http_client(http_config) as client,
            pytest.raises(ApiIngestionError) as captured,
        ):
            ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                sleep=lambda _: pytest.fail("invalid aggregate preflight must not retry"),
                run_id_factory=lambda: "run_5050000000000000000000000000000e",
            )

        assert captured.value.code is expected_code
        assert _existing_inventory(s3_client, app_config, all_keys) == damaged_inventory
        if damage == "orphaned":
            assert not _key_exists(s3_client, app_config, payload_keys[0])
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_inclusive_backfill_repeat_has_identical_inventory_and_duplicate_metrics(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    from_date = date(2099, 12, 10)
    to_date = date(2099, 12, 12)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3)
    date_keys = [
        _page_keys("fx-rates", date(2099, 12, day), http_config, 2) for day in range(10, 13)
    ]
    payload_keys = tuple(key for payloads, _ in date_keys for key in payloads)
    manifest_keys = tuple(key for _, manifests in date_keys for key in manifests)
    aggregate_keys = tuple(
        _logical_run_key("fx-rates", date(2099, 12, day), http_config) for day in range(10, 13)
    )
    all_keys = (*payload_keys, *manifest_keys, *aggregate_keys)
    run_ids = iter(f"run_{index:032x}" for index in range(101, 107))
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)

    try:
        with open_http_client(http_config) as client:
            first = ingest_api_backfill(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                from_date=from_date,
                to_date=to_date,
                sleep=lambda _: pytest.fail("happy backfill must not retry"),
                run_id_factory=lambda: next(run_ids),
            )
        first_inventory = _exact_batch_inventory(
            s3_client,
            app_config,
            payload_keys,
            manifest_keys,
            aggregate_keys,
        )

        with open_http_client(http_config) as client:
            repeated = ingest_api_backfill(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                from_date=from_date,
                to_date=to_date,
                sleep=lambda _: pytest.fail("repeated backfill must not retry"),
                run_id_factory=lambda: next(run_ids),
            )
        repeated_inventory = _exact_batch_inventory(
            s3_client,
            app_config,
            payload_keys,
            manifest_keys,
            aggregate_keys,
        )

        assert [result.summary.source_date for result in first.results] == [
            date(2099, 12, 10),
            date(2099, 12, 11),
            date(2099, 12, 12),
        ]
        assert all(result.summary.bytes_written > 0 for result in first.results)
        assert all(result.summary.duplicate_count == 0 for result in first.results)
        assert all(result.summary.bytes_written == 0 for result in repeated.results)
        assert all(result.summary.duplicate_count == 2 for result in repeated.results)
        assert repeated_inventory == first_inventory
        assert tuple(result.logical_run_manifest_key for result in first.results) == aggregate_keys
        assert (
            tuple(result.logical_run_manifest_key for result in repeated.results) == aggregate_keys
        )
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_backfill_stops_at_conflicting_aggregate_before_later_date(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    from_date = date(2099, 12, 1)
    conflicting_date = date(2099, 12, 2)
    to_date = date(2099, 12, 3)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3)
    date_keys = {
        source_date: _page_keys("fx-rates", source_date, http_config, 2)
        for source_date in (from_date, conflicting_date, to_date)
    }
    aggregate_keys = {
        source_date: _logical_run_key("fx-rates", source_date, http_config)
        for source_date in date_keys
    }
    all_keys = tuple(
        key
        for source_date, (payloads, manifests) in date_keys.items()
        for key in (*payloads, *manifests, aggregate_keys[source_date])
    )
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)
    run_ids = iter(f"run_{index:032x}" for index in range(301, 305))

    try:
        with open_http_client(http_config) as client:
            ingest_api_date(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=conflicting_date,
                sleep=lambda _: pytest.fail("setup ingestion must not retry"),
                run_id_factory=lambda: next(run_ids),
            )
        conflicting_key = aggregate_keys[conflicting_date]
        aggregate = ApiLogicalRunManifest.from_bytes(
            _object_bytes(s3_client, app_config, conflicting_key)
        )
        damaged_page = replace(aggregate.pages[0], payload_checksum_sha256="e" * 64)
        damaged_aggregate = replace(
            aggregate,
            pages=(damaged_page, *aggregate.pages[1:]),
        ).to_bytes()
        s3_client.put_object(
            Bucket=app_config.storage.bucket,
            Key=conflicting_key,
            Body=damaged_aggregate,
            ContentLength=len(damaged_aggregate),
            ContentType=API_LOGICAL_RUN_MANIFEST_CONTENT_TYPE,
        )
        middle_keys = (
            *date_keys[conflicting_date][0],
            *date_keys[conflicting_date][1],
            conflicting_key,
        )
        middle_inventory = _existing_inventory(s3_client, app_config, middle_keys)

        with (
            open_http_client(http_config) as client,
            pytest.raises(ApiIngestionError) as captured,
        ):
            ingest_api_backfill(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                from_date=from_date,
                to_date=to_date,
                sleep=lambda _: pytest.fail("aggregate failure must not retry HTTP"),
                run_id_factory=lambda: next(run_ids),
            )

        assert captured.value.code is ApiErrorCode.LOGICAL_RUN_MANIFEST_CONFLICT
        assert captured.value.event.source_date == conflicting_date
        first_payloads, first_manifests = date_keys[from_date]
        _exact_batch_inventory(
            s3_client,
            app_config,
            first_payloads,
            first_manifests,
            (aggregate_keys[from_date],),
        )
        assert _existing_inventory(s3_client, app_config, middle_keys) == middle_inventory
        later_keys = (
            *date_keys[to_date][0],
            *date_keys[to_date][1],
            aggregate_keys[to_date],
        )
        assert _existing_inventory(s3_client, app_config, later_keys) == {}
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)


def test_live_backfill_stops_on_middle_date_then_full_rerun_resumes_safely(
    app_config: AppConfig,
    s3_client: S3Client,
) -> None:
    from_date = date(2099, 12, 7)
    failed_date = date(2099, 12, 8)
    to_date = date(2099, 12, 9)
    descriptor = get_source_descriptor("fx-rates")
    http_config = replace(app_config.http, page_size=3, max_attempts=2)
    date_keys = {
        source_date: _page_keys("fx-rates", source_date, http_config, 2)
        for source_date in (from_date, failed_date, to_date)
    }
    payload_keys = tuple(key for payloads, _ in date_keys.values() for key in payloads)
    manifest_keys = tuple(key for _, manifests in date_keys.values() for key in manifests)
    aggregate_keys = tuple(
        _logical_run_key("fx-rates", source_date, http_config) for source_date in date_keys
    )
    all_keys = (*payload_keys, *manifest_keys, *aggregate_keys)
    run_ids = iter(f"run_{index:032x}" for index in range(201, 206))
    _delete_exact_keys(s3_client, app_config, all_keys)
    storage = S3Storage(client=s3_client, bucket=app_config.storage.bucket)

    def faults_for_date(source_date: date, page: int) -> dict[str, str]:
        del page
        return {"scenario": "permanent_500"} if source_date == failed_date else {}

    try:
        with (
            open_http_client(http_config) as client,
            pytest.raises(ApiIngestionError) as captured,
        ):
            ingest_api_backfill(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                from_date=from_date,
                to_date=to_date,
                sleep=lambda _: None,
                run_id_factory=lambda: next(run_ids),
                query_parameter_provider=faults_for_date,
            )
        assert captured.value.event.source_date == failed_date
        first_payloads, first_manifests = date_keys[from_date]
        first_inventory = _exact_batch_inventory(
            s3_client,
            app_config,
            first_payloads,
            first_manifests,
            (_logical_run_key("fx-rates", from_date, http_config),),
        )
        for source_date in (failed_date, to_date):
            date_payloads, date_manifests = date_keys[source_date]
            for key in (
                *date_payloads,
                *date_manifests,
                _logical_run_key("fx-rates", source_date, http_config),
            ):
                assert (
                    s3_client.list_objects_v2(
                        Bucket=app_config.storage.bucket,
                        Prefix=key,
                    ).get("Contents", [])
                    == []
                )

        with open_http_client(http_config) as client:
            resumed = ingest_api_backfill(
                client=client,
                storage=storage,
                http_config=http_config,
                descriptor=descriptor,
                from_date=from_date,
                to_date=to_date,
                sleep=lambda _: pytest.fail("fault-free resume must not retry"),
                run_id_factory=lambda: next(run_ids),
            )

        assert [result.summary.source_date for result in resumed.results] == [
            from_date,
            failed_date,
            to_date,
        ]
        assert resumed.results[0].summary.bytes_written == 0
        assert resumed.results[0].summary.duplicate_count == 2
        assert (
            _exact_batch_inventory(
                s3_client,
                app_config,
                first_payloads,
                first_manifests,
                (_logical_run_key("fx-rates", from_date, http_config),),
            )
            == first_inventory
        )
        _exact_batch_inventory(
            s3_client,
            app_config,
            payload_keys,
            manifest_keys,
            aggregate_keys,
        )
    finally:
        _delete_exact_keys(s3_client, app_config, all_keys)
