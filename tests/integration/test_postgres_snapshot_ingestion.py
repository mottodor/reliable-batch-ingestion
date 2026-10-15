"""Focused Garage integration for BI-070 restart-safe snapshot chunks."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from io import BytesIO, StringIO
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
import pytest

from batch_ingestion.config import AppConfig, PostgresConfig, SnapshotConfig
from batch_ingestion.manifests import ManifestStatus
from batch_ingestion.postgres_snapshot import (
    ORDERS_ARROW_SCHEMA,
    PARQUET_CONTENT_TYPE,
    OrderSnapshotRead,
    SnapshotChunkStatus,
    SnapshotDescriptor,
    SnapshotErrorCode,
    SnapshotHandledError,
    SnapshotOrderRow,
    create_postgres_connection,
    open_order_snapshot,
    serialize_order_chunk,
)
from batch_ingestion.postgres_snapshot.service import ingest_order_snapshot
from batch_ingestion.source_records import generate_order_records
from batch_ingestion.source_seed import DEFAULT_SOURCE_SEED
from batch_ingestion.storage import ReconciliationState, S3Storage, create_s3_client

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DATE = date(2026, 9, 1)
RUN_ID = "run_70707070707070707070707070707070"
REPLAY_RUN_ID = "run_71717171717171717171717171717171"
EXTRACTED_AT = datetime(2026, 9, 1, 20, tzinfo=UTC)


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
    configured_path = Path(os.environ.get("BI070_ENV_FILE", ".env"))
    env_path = (
        configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path
    )
    return AppConfig.from_env(_load_dotenv(env_path))


@pytest.fixture(scope="module")
def storage(app_config: AppConfig) -> Iterator[S3Storage]:
    client = create_s3_client(app_config.storage)
    with closing(client):
        yield S3Storage(client=client, bucket=app_config.storage.bucket)


@dataclass
class _StaticSnapshotRead:
    descriptor: SnapshotDescriptor
    chunks: Sequence[tuple[SnapshotOrderRow, ...]]

    def iter_chunks(self) -> Iterator[tuple[SnapshotOrderRow, ...]]:
        yield from self.chunks


class _StaticSnapshotOpener:
    def __init__(
        self,
        descriptor: SnapshotDescriptor,
        chunks: Sequence[tuple[SnapshotOrderRow, ...]],
    ) -> None:
        self.descriptor = descriptor
        self.chunks = chunks

    @contextmanager
    def __call__(self, **kwargs: object) -> Iterator[_StaticSnapshotRead]:
        resume_descriptor = kwargs["resume_descriptor"]
        assert resume_descriptor is None or resume_descriptor == self.descriptor
        yield _StaticSnapshotRead(self.descriptor, self.chunks)


def _snapshot_rows() -> tuple[SnapshotOrderRow, ...]:
    return tuple(
        SnapshotOrderRow(
            order_id=record.order_id,
            customer_id=record.customer_id,
            order_status=record.order_status,
            shipping_status=record.shipping_status,
            currency=record.currency,
            total_amount=record.total_amount,
            updated_at=record.updated_at,
        )
        for record in generate_order_records(DEFAULT_SOURCE_SEED, SOURCE_DATE)
    )


def _snapshot_config(*, chunk_rows: int = 10) -> SnapshotConfig:
    return SnapshotConfig(
        max_elapsed_seconds=30.0,
        chunk_rows=chunk_rows,
        max_chunks=10,
        max_chunk_bytes=1_000_000,
        spool_threshold_bytes=1_000,
    )


def _serialize_chunks(
    chunks: Sequence[tuple[SnapshotOrderRow, ...]],
    config: SnapshotConfig,
) -> tuple[bytes, ...]:
    payloads: list[bytes] = []
    for rows in chunks:
        with serialize_order_chunk(rows, snapshot_config=config) as serialized:
            payloads.append(serialized.stream.read())
    return tuple(payloads)


def _read_object(storage: S3Storage, key: str, maximum_bytes: int) -> bytes:
    with storage.download_spooled(
        key,
        maximum_bytes=maximum_bytes,
        spool_threshold_bytes=min(maximum_bytes, 1_000),
    ) as downloaded:
        return downloaded.stream.read()


def _delete_keys(app_config: AppConfig, keys: Sequence[str]) -> None:
    client = create_s3_client(app_config.storage)
    with closing(client):
        for key in keys:
            client.delete_object(Bucket=app_config.storage.bucket, Key=key)


def _inventory(app_config: AppConfig, prefix: str) -> set[str]:
    client = create_s3_client(app_config.storage)
    with closing(client):
        response = client.list_objects_v2(Bucket=app_config.storage.bucket, Prefix=prefix)
    keys: set[str] = set()
    for entry in response.get("Contents", []):
        key = entry.get("Key")
        if key is None:
            raise AssertionError("listed S3 object omitted its key")
        keys.add(key)
    return keys


def test_live_multichunk_payload_only_repair_and_complete_replay(
    app_config: AppConfig,
    storage: S3Storage,
) -> None:
    rows = _snapshot_rows()
    config = _snapshot_config()
    chunks = tuple(
        tuple(rows[offset : offset + config.chunk_rows])
        for offset in range(0, len(rows), config.chunk_rows)
    )
    descriptor = SnapshotDescriptor.create(
        source_name="orders",
        source_date=SOURCE_DATE,
        upper_updated_at=rows[-1].updated_at,
        upper_order_id=rows[-1].order_id,
        expected_record_count=len(rows),
    )
    payloads = _serialize_chunks(chunks, config)
    raw_keys = tuple(
        descriptor.raw_key(chunk_ordinal=ordinal, chunk_rows=config.chunk_rows)
        for ordinal in range(1, len(chunks) + 1)
    )
    manifest_keys = tuple(
        descriptor.manifest_key(chunk_ordinal=ordinal, chunk_rows=config.chunk_rows)
        for ordinal in range(1, len(chunks) + 1)
    )
    all_keys = (*raw_keys, *manifest_keys)
    opener = _StaticSnapshotOpener(descriptor, chunks)
    _delete_keys(app_config, all_keys)

    try:
        payload_only = storage.write_immutable(
            raw_keys[1],
            BytesIO(payloads[1]),
            content_length=len(payloads[1]),
            checksum_sha256=sha256(payloads[1]).hexdigest(),
            content_type=PARQUET_CONTENT_TYPE,
        )
        assert payload_only.object.key == raw_keys[1]
        events: list[dict[str, object]] = []

        first = ingest_order_snapshot(
            storage=storage,
            postgres_config=app_config.postgres,
            snapshot_config=config,
            source_name="orders",
            source_date=SOURCE_DATE,
            run_id_factory=lambda: RUN_ID,
            utc_now=lambda: EXTRACTED_AT,
            snapshot_opener=opener,
            event_sink=events.append,
        )

        assert first.summary.records_read == first.summary.records_accepted == len(rows)
        assert first.summary.bytes_written == len(payloads[0]) + len(payloads[2])
        assert first.summary.duplicate_count == 0
        assert [
            event["chunk_status"]
            for event in events
            if event["event"] == "snapshot_chunk_completed"
        ] == [
            SnapshotChunkStatus.CREATED.value,
            SnapshotChunkStatus.PAYLOAD_REUSED.value,
            SnapshotChunkStatus.CREATED.value,
        ]

        original_manifests: dict[str, bytes] = {}
        for ordinal, (raw_key, manifest_key, expected_payload, expected_rows) in enumerate(
            zip(raw_keys, manifest_keys, payloads, chunks, strict=True),
            start=1,
        ):
            assert _read_object(storage, raw_key, config.max_chunk_bytes) == expected_payload
            parquet = pq.ParquetFile(BytesIO(expected_payload))
            assert parquet.schema_arrow == ORDERS_ARROW_SCHEMA
            assert parquet.read().to_pylist() == [
                {
                    "order_id": row.order_id,
                    "customer_id": row.customer_id,
                    "order_status": row.order_status,
                    "shipping_status": row.shipping_status,
                    "currency": row.currency,
                    "total_amount": row.total_amount,
                    "updated_at": row.updated_at,
                }
                for row in expected_rows
            ]
            reconciliation = storage.reconcile_payload_manifest(
                raw_key,
                content_length=len(expected_payload),
                checksum_sha256=sha256(expected_payload).hexdigest(),
            )
            assert reconciliation.state is ReconciliationState.COMPLETE
            assert reconciliation.manifest is not None
            assert reconciliation.manifest.status is ManifestStatus.SUCCEEDED
            assert reconciliation.manifest.run_id == RUN_ID
            assert reconciliation.manifest.object_key == raw_key
            assert ordinal <= 3
            original_manifests[manifest_key] = _read_object(storage, manifest_key, 64 * 1024)

        raw_prefix = raw_keys[0].rsplit("/", 1)[0] + "/"
        manifest_prefix = manifest_keys[0].rsplit("/", 1)[0] + "/"
        assert _inventory(app_config, raw_prefix) | _inventory(app_config, manifest_prefix) == set(
            all_keys
        )

        replay = ingest_order_snapshot(
            storage=storage,
            postgres_config=app_config.postgres,
            snapshot_config=config,
            source_name="orders",
            source_date=SOURCE_DATE,
            resume_descriptor=descriptor,
            run_id_factory=lambda: REPLAY_RUN_ID,
            utc_now=lambda: EXTRACTED_AT,
            snapshot_opener=opener,
        )

        assert replay.summary.records_read == replay.summary.records_accepted == len(rows)
        assert replay.summary.bytes_written == 0
        assert replay.summary.duplicate_count == len(chunks)
        assert {
            key: _read_object(storage, key, 64 * 1024) for key in manifest_keys
        } == original_manifests
        assert _inventory(app_config, raw_prefix) | _inventory(app_config, manifest_prefix) == set(
            all_keys
        )
    finally:
        _delete_keys(app_config, all_keys)


def _descriptor_for_rows(rows: Sequence[SnapshotOrderRow]) -> SnapshotDescriptor:
    return SnapshotDescriptor.create(
        source_name="orders",
        source_date=SOURCE_DATE,
        upper_updated_at=rows[-1].updated_at,
        upper_order_id=rows[-1].order_id,
        expected_record_count=len(rows),
    )


def _snapshot_keys(
    descriptor: SnapshotDescriptor,
    config: SnapshotConfig,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    ordinals = range(
        1,
        descriptor.expected_chunk_count(chunk_rows=config.chunk_rows) + 1,
    )
    raw_keys = tuple(
        descriptor.raw_key(chunk_ordinal=ordinal, chunk_rows=config.chunk_rows)
        for ordinal in ordinals
    )
    manifest_keys = tuple(
        descriptor.manifest_key(chunk_ordinal=ordinal, chunk_rows=config.chunk_rows)
        for ordinal in range(1, len(raw_keys) + 1)
    )
    return raw_keys, manifest_keys


def _parquet_rows(storage: S3Storage, raw_keys: Sequence[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for raw_key in raw_keys:
        body = _read_object(storage, raw_key, 1_000_000)
        parquet = pq.ParquetFile(BytesIO(body))
        assert parquet.schema_arrow == ORDERS_ARROW_SCHEMA
        assert parquet.metadata.num_row_groups == 1
        rows.extend(parquet.read().to_pylist())
    return rows


def _expected_pylist(rows: Sequence[SnapshotOrderRow]) -> list[dict[str, object]]:
    return [
        {
            "order_id": row.order_id,
            "customer_id": row.customer_id,
            "order_status": row.order_status,
            "shipping_status": row.shipping_status,
            "currency": row.currency,
            "total_amount": row.total_amount,
            "updated_at": row.updated_at,
        }
        for row in rows
    ]


def test_live_seeded_postgres_happy_path_and_exact_complete_replay(
    app_config: AppConfig,
    storage: S3Storage,
) -> None:
    rows = _snapshot_rows()
    config = _snapshot_config(chunk_rows=7)
    descriptor = _descriptor_for_rows(rows)
    raw_keys, manifest_keys = _snapshot_keys(descriptor, config)
    all_keys = (*raw_keys, *manifest_keys)
    _delete_keys(app_config, all_keys)

    try:
        events: list[dict[str, object]] = []
        first = ingest_order_snapshot(
            storage=storage,
            postgres_config=app_config.postgres,
            snapshot_config=config,
            source_name="orders",
            source_date=SOURCE_DATE,
            event_sink=events.append,
        )

        assert first.descriptor == descriptor
        assert first.summary.records_read == first.summary.records_accepted == 24
        assert first.summary.duplicate_count == 0
        assert first.summary.bytes_written > 0
        assert [
            event["chunk_status"]
            for event in events
            if event["event"] == "snapshot_chunk_completed"
        ] == [SnapshotChunkStatus.CREATED.value] * 4
        assert _parquet_rows(storage, raw_keys) == _expected_pylist(rows)

        original_inventory = {
            key: sha256(_read_object(storage, key, 1_000_000)).hexdigest() for key in all_keys
        }
        original_manifests = {key: _read_object(storage, key, 64 * 1024) for key in manifest_keys}
        for raw_key in raw_keys:
            body = _read_object(storage, raw_key, config.max_chunk_bytes)
            reconciliation = storage.reconcile_payload_manifest(
                raw_key,
                content_length=len(body),
                checksum_sha256=sha256(body).hexdigest(),
            )
            assert reconciliation.state is ReconciliationState.COMPLETE
            assert reconciliation.manifest is not None
            assert reconciliation.manifest.status is ManifestStatus.SUCCEEDED

        replay = ingest_order_snapshot(
            storage=storage,
            postgres_config=app_config.postgres,
            snapshot_config=config,
            source_name="orders",
            source_date=SOURCE_DATE,
            resume_descriptor=descriptor,
        )

        assert replay.summary.records_read == replay.summary.records_accepted == 24
        assert replay.summary.bytes_written == 0
        assert replay.summary.duplicate_count == 4
        assert {
            key: sha256(_read_object(storage, key, 1_000_000)).hexdigest() for key in all_keys
        } == original_inventory
        assert {
            key: _read_object(storage, key, 64 * 1024) for key in manifest_keys
        } == original_manifests
        raw_prefix = raw_keys[0].rsplit("/", 1)[0] + "/"
        manifest_prefix = manifest_keys[0].rsplit("/", 1)[0] + "/"
        assert _inventory(app_config, raw_prefix) | _inventory(app_config, manifest_prefix) == set(
            all_keys
        )
    finally:
        _delete_keys(app_config, all_keys)


class _CapturingSnapshotOpener:
    def __init__(self) -> None:
        self.backend_pid: int | None = None

    @contextmanager
    def __call__(
        self,
        *,
        postgres_config: PostgresConfig,
        snapshot_config: SnapshotConfig,
        source_name: str,
        source_date: date,
        resume_descriptor: SnapshotDescriptor | None,
        monotonic: Callable[[], float],
    ) -> Iterator[OrderSnapshotRead]:
        def connection_factory(config: PostgresConfig) -> psycopg.Connection[tuple[object, ...]]:
            connection = create_postgres_connection(config)
            self.backend_pid = connection.info.backend_pid
            return connection

        with open_order_snapshot(
            postgres_config=postgres_config,
            snapshot_config=snapshot_config,
            source_name=source_name,
            source_date=source_date,
            resume_descriptor=resume_descriptor,
            connection_factory=connection_factory,
            monotonic=monotonic,
        ) as snapshot:
            yield snapshot


def _connect(config: PostgresConfig) -> psycopg.Connection[tuple[object, ...]]:
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=max(1, int(config.connect_timeout_seconds)),
    )


def _delete_test_orders(config: PostgresConfig, order_ids: Sequence[int]) -> None:
    with _connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM omni_retail.orders WHERE order_id = ANY(%s)",
            (list(order_ids),),
        )


def _insert_test_order(
    config: PostgresConfig,
    *,
    order_id: int,
    updated_at: datetime,
) -> None:
    with _connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO omni_retail.orders (
                order_id,
                customer_id,
                order_status,
                shipping_status,
                currency,
                total_amount,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (order_id, 10999, "pending", "label_created", "USD", "12.34", updated_at),
        )


def _terminate_backend(config: PostgresConfig, backend_pid: int) -> None:
    with _connect(config) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))
        assert cursor.fetchone() == (True,)


def test_live_backend_termination_replays_from_start_excludes_above_bound_and_detects_drift(
    app_config: AppConfig,
    storage: S3Storage,
) -> None:
    rows = _snapshot_rows()
    config = _snapshot_config(chunk_rows=6)
    descriptor = _descriptor_for_rows(rows)
    raw_keys, manifest_keys = _snapshot_keys(descriptor, config)
    all_keys = (*raw_keys, *manifest_keys)
    above_bound_id = 9_999_260_901_001
    below_bound_id = 9_999_260_901_002
    opener = _CapturingSnapshotOpener()
    events: list[dict[str, object]] = []
    terminated = False
    _delete_test_orders(app_config.postgres, (above_bound_id, below_bound_id))
    _delete_keys(app_config, all_keys)

    def terminate_after_first_chunk(event: dict[str, object]) -> None:
        nonlocal terminated
        events.append(event)
        if event["event"] == "snapshot_chunk_completed" and not terminated:
            assert opener.backend_pid is not None
            _terminate_backend(app_config.postgres, opener.backend_pid)
            terminated = True

    try:
        with pytest.raises(SnapshotHandledError) as interrupted:
            ingest_order_snapshot(
                storage=storage,
                postgres_config=app_config.postgres,
                snapshot_config=config,
                source_name="orders",
                source_date=SOURCE_DATE,
                snapshot_opener=opener,
                event_sink=terminate_after_first_chunk,
            )
        assert interrupted.value.code is SnapshotErrorCode.POSTGRES_CONNECTION_FAILED
        assert terminated
        assert [event["event"] for event in events[:2]] == [
            "snapshot_started",
            "snapshot_chunk_completed",
        ]
        assert _inventory(app_config, raw_keys[0].rsplit("/", 1)[0] + "/") == {raw_keys[0]}
        assert _inventory(app_config, manifest_keys[0].rsplit("/", 1)[0] + "/") == {
            manifest_keys[0]
        }

        _insert_test_order(
            app_config.postgres,
            order_id=above_bound_id,
            updated_at=datetime(2026, 9, 1, 23, 59, tzinfo=UTC),
        )
        resumed = ingest_order_snapshot(
            storage=storage,
            postgres_config=app_config.postgres,
            snapshot_config=config,
            source_name="orders",
            source_date=SOURCE_DATE,
            resume_descriptor=descriptor,
        )

        assert resumed.summary.records_read == resumed.summary.records_accepted == 24
        assert resumed.summary.duplicate_count == 1
        assert _parquet_rows(storage, raw_keys) == _expected_pylist(rows)
        assert all(row["order_id"] != above_bound_id for row in _parquet_rows(storage, raw_keys))

        stable_inventory = {
            key: sha256(_read_object(storage, key, 1_000_000)).hexdigest() for key in all_keys
        }
        _insert_test_order(
            app_config.postgres,
            order_id=below_bound_id,
            updated_at=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        )
        with pytest.raises(SnapshotHandledError) as drift:
            ingest_order_snapshot(
                storage=storage,
                postgres_config=app_config.postgres,
                snapshot_config=config,
                source_name="orders",
                source_date=SOURCE_DATE,
                resume_descriptor=descriptor,
            )
        assert drift.value.code is SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED
        assert {
            key: sha256(_read_object(storage, key, 1_000_000)).hexdigest() for key in all_keys
        } == stable_inventory
    finally:
        _delete_test_orders(app_config.postgres, (above_bound_id, below_bound_id))
        _delete_keys(app_config, all_keys)


class _FailFirstManifestStorage:
    def __init__(self, storage: S3Storage) -> None:
        self.storage = storage
        self.failed = False

    def reconcile_payload_manifest(
        self,
        payload_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> object:
        return self.storage.reconcile_payload_manifest(
            payload_key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )

    def write_immutable(self, key: str, stream: object, **kwargs: object) -> object:
        if key.startswith("manifests/") and not self.failed:
            self.failed = True
            raise RuntimeError("injected post-payload process boundary")
        return self.storage.write_immutable(key, stream, **kwargs)  # type: ignore[arg-type]


def test_live_payload_only_interruption_is_repaired_on_resume(
    app_config: AppConfig,
    storage: S3Storage,
) -> None:
    rows = _snapshot_rows()
    config = _snapshot_config(chunk_rows=24)
    descriptor = _descriptor_for_rows(rows)
    raw_keys, manifest_keys = _snapshot_keys(descriptor, config)
    all_keys = (*raw_keys, *manifest_keys)
    _delete_keys(app_config, all_keys)

    try:
        interrupted_storage = _FailFirstManifestStorage(storage)
        with pytest.raises(SnapshotHandledError) as interrupted:
            ingest_order_snapshot(
                storage=interrupted_storage,  # type: ignore[arg-type]
                postgres_config=app_config.postgres,
                snapshot_config=config,
                source_name="orders",
                source_date=SOURCE_DATE,
            )
        assert interrupted.value.code is SnapshotErrorCode.STORAGE_FAILED
        assert interrupted_storage.failed
        assert _inventory(app_config, raw_keys[0].rsplit("/", 1)[0] + "/") == {raw_keys[0]}
        assert _inventory(app_config, manifest_keys[0].rsplit("/", 1)[0] + "/") == set()

        resumed = ingest_order_snapshot(
            storage=storage,
            postgres_config=app_config.postgres,
            snapshot_config=config,
            source_name="orders",
            source_date=SOURCE_DATE,
            resume_descriptor=descriptor,
        )
        assert resumed.summary.records_read == resumed.summary.records_accepted == 24
        assert resumed.summary.bytes_written == 0
        assert resumed.summary.duplicate_count == 0
        assert _parquet_rows(storage, raw_keys) == _expected_pylist(rows)
        assert _inventory(app_config, manifest_keys[0].rsplit("/", 1)[0] + "/") == {
            manifest_keys[0]
        }
    finally:
        _delete_keys(app_config, all_keys)


def test_cli_reports_bounded_storage_endpoint_failure(
    app_config: AppConfig,
    storage: S3Storage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from batch_ingestion.postgres_snapshot import cli

    rows = _snapshot_rows()
    descriptor = _descriptor_for_rows(rows)
    raw_keys, manifest_keys = _snapshot_keys(descriptor, app_config.snapshot)
    all_keys = (*raw_keys, *manifest_keys)
    configured_path = Path(os.environ.get("BI070_ENV_FILE", ".env"))
    env_path = (
        configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path
    )
    environment: Mapping[str, str] = {
        **_load_dotenv(env_path),
        "S3_ENDPOINT_URL": "http://127.0.0.1:1",
        "S3_TOTAL_MAX_ATTEMPTS": "1",
        "S3_CONNECT_TIMEOUT_SECONDS": "0.1",
        "S3_READ_TIMEOUT_SECONDS": "0.1",
    }
    output = StringIO()
    _delete_keys(app_config, all_keys)

    started_at = time.monotonic()
    exit_code = cli.main(
        ["run", "--source", "orders", "--date", SOURCE_DATE.isoformat()],
        environ=environment,
        output=output,
    )
    elapsed = time.monotonic() - started_at

    assert exit_code == cli.CliExitCode.INGESTION_FAILED
    assert elapsed < app_config.snapshot.max_elapsed_seconds
    emitted = [json.loads(line) for line in output.getvalue().splitlines()]
    assert emitted[0]["event"] == "snapshot_started"
    assert any(event.get("error_code") == "storage_failed" for event in emitted)
    assert _inventory(app_config, raw_keys[0].rsplit("/", 1)[0] + "/") == set()
    assert _inventory(app_config, manifest_keys[0].rsplit("/", 1)[0] + "/") == set()
