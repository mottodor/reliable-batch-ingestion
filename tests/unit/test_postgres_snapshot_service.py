"""Restart-safe PostgreSQL snapshot service coverage for BI-070 slice 3."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import BinaryIO

import pytest

from batch_ingestion.config import PostgresConfig, SnapshotConfig
from batch_ingestion.identifiers import SourceType, manifest_key_for
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.postgres_snapshot import (
    PARQUET_CONTENT_TYPE,
    SnapshotChunkStatus,
    SnapshotDescriptor,
    SnapshotErrorCode,
    SnapshotEventType,
    SnapshotHandledError,
    SnapshotOrderRow,
)
from batch_ingestion.postgres_snapshot.serializer import (
    SerializedOrderChunk,
    serialize_order_chunk,
)
from batch_ingestion.postgres_snapshot.service import (
    SnapshotIngestionResult,
    ingest_order_snapshot,
)
from batch_ingestion.storage import (
    ImmutableWriteResult,
    ImmutableWriteStatus,
    ObjectConflictError,
    ObjectVerificationError,
    ReconciliationResult,
    ReconciliationState,
    StoredObject,
)

SOURCE_DATE = date(2026, 9, 1)
RUN_ID = "run_11111111111111111111111111111111"
SECOND_RUN_ID = "run_22222222222222222222222222222222"
EXTRACTED_AT = datetime(2026, 9, 1, 18, tzinfo=UTC)
POSTGRES_CONFIG = PostgresConfig(
    host="postgres",
    port=5432,
    database="batch_lab",
    user="batch_lab",
    password="secret",
)
SNAPSHOT_CONFIG = SnapshotConfig(
    max_elapsed_seconds=30.0,
    chunk_rows=2,
    max_chunks=10,
    max_chunk_bytes=1_000_000,
    spool_threshold_bytes=1_000,
)


@dataclass
class _Object:
    body: bytes
    content_type: str


class ManualClock:
    def __init__(self) -> None:
        self.now = 10.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingStorage:
    """Exact-key in-memory implementation of the snapshot storage protocol."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.objects: dict[str, _Object] = {}
        self.fail_raw_write = False
        self.fail_manifest_write = False
        self.reconcile_error: Exception | None = None
        self.after_reconcile: Callable[[], None] | None = None

    def reconcile_payload_manifest(
        self,
        payload_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> ReconciliationResult:
        self.calls.append(f"reconcile:{payload_key}")
        if self.reconcile_error is not None:
            raise self.reconcile_error
        payload = self.objects.get(payload_key)
        manifest_object = self.objects.get(manifest_key_for(payload_key))
        if self.after_reconcile is not None:
            self.after_reconcile()
        if payload is None:
            state = (
                ReconciliationState.ABSENT
                if manifest_object is None
                else ReconciliationState.MANIFEST_ONLY
            )
            return ReconciliationResult(state=state, payload=None, manifest=None)
        if (
            len(payload.body) != content_length
            or sha256(payload.body).hexdigest() != checksum_sha256
        ):
            raise ObjectConflictError("raw conflict")
        payload_info = self._info(payload_key, payload)
        if manifest_object is None:
            return ReconciliationResult(
                state=ReconciliationState.PAYLOAD_ONLY,
                payload=payload_info,
                manifest=None,
            )
        manifest = Manifest.from_json(manifest_object.body.decode("utf-8"))
        return ReconciliationResult(
            state=ReconciliationState.COMPLETE,
            payload=payload_info,
            manifest=manifest,
        )

    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        self.calls.append(f"write:{key}")
        if key.startswith("raw/") and self.fail_raw_write:
            raise RuntimeError("injected raw write failure")
        if key.startswith("manifests/") and self.fail_manifest_write:
            raise RuntimeError("injected manifest write failure")
        body = stream.read()
        stream.seek(0)
        assert len(body) == content_length
        assert sha256(body).hexdigest() == checksum_sha256
        existing = self.objects.get(key)
        if existing is not None:
            if existing.body != body or existing.content_type != content_type:
                raise ObjectConflictError("immutable object conflict")
            return ImmutableWriteResult(ImmutableWriteStatus.UNCHANGED, self._info(key, existing))
        stored = _Object(body=body, content_type=content_type)
        self.objects[key] = stored
        return ImmutableWriteResult(ImmutableWriteStatus.CREATED, self._info(key, stored))

    def put_payload(self, key: str, body: bytes, content_type: str = PARQUET_CONTENT_TYPE) -> None:
        self.objects[key] = _Object(body=body, content_type=content_type)

    def put_manifest(self, manifest: Manifest) -> None:
        self.objects[manifest_key_for(manifest.object_key)] = _Object(
            body=manifest.to_json().encode("utf-8"),
            content_type="application/json",
        )

    @staticmethod
    def _info(key: str, stored: _Object) -> StoredObject:
        return StoredObject(
            key=key,
            content_length=len(stored.body),
            checksum_sha256=sha256(stored.body).hexdigest(),
            content_type=stored.content_type,
            metadata={"checksum-sha256": sha256(stored.body).hexdigest()},
        )


@dataclass
class FakeSnapshotRead:
    descriptor: SnapshotDescriptor
    chunks: Sequence[tuple[SnapshotOrderRow, ...]]
    calls: list[str]
    fail_after_chunks: int | None = None
    prior_streams: list[BinaryIO] | None = None

    def iter_chunks(self) -> Iterator[tuple[SnapshotOrderRow, ...]]:
        for index, chunk in enumerate(self.chunks, start=1):
            if self.fail_after_chunks is not None and index > self.fail_after_chunks:
                raise SnapshotHandledError(SnapshotErrorCode.POSTGRES_CONNECTION_FAILED)
            if self.prior_streams:
                assert self.prior_streams[-1].closed is True
            self.calls.append(f"fetch:{index}")
            yield chunk
        if self.fail_after_chunks is not None and self.fail_after_chunks >= len(self.chunks):
            raise SnapshotHandledError(SnapshotErrorCode.POSTGRES_CONNECTION_FAILED)


class FakeSnapshotOpener:
    def __init__(
        self,
        descriptor: SnapshotDescriptor,
        chunks: Sequence[tuple[SnapshotOrderRow, ...]],
        calls: list[str],
        *,
        fail_after_chunks: int | None = None,
        prior_streams: list[BinaryIO] | None = None,
    ) -> None:
        self.descriptor = descriptor
        self.chunks = chunks
        self.calls = calls
        self.fail_after_chunks = fail_after_chunks
        self.prior_streams = prior_streams
        self.open_count = 0
        self.resume_descriptors: list[SnapshotDescriptor | None] = []

    @contextmanager
    def __call__(self, **kwargs: object) -> Iterator[FakeSnapshotRead]:
        self.open_count += 1
        self.resume_descriptors.append(kwargs["resume_descriptor"])  # type: ignore[arg-type]
        self.calls.append("descriptor_validated")
        yield FakeSnapshotRead(
            descriptor=self.descriptor,
            chunks=self.chunks,
            calls=self.calls,
            fail_after_chunks=self.fail_after_chunks,
            prior_streams=self.prior_streams,
        )


def _row(ordinal: int) -> SnapshotOrderRow:
    return SnapshotOrderRow(
        order_id=202_609_010_000 + ordinal,
        customer_id=10_000 + ordinal,
        order_status="confirmed",
        shipping_status="in_transit",
        currency="USD",
        total_amount=Decimal(f"{ordinal}.00"),
        updated_at=datetime(2026, 9, 1, 10, ordinal, tzinfo=UTC),
    )


def _descriptor(rows: Sequence[SnapshotOrderRow]) -> SnapshotDescriptor:
    if not rows:
        return SnapshotDescriptor.empty(source_name="orders", source_date=SOURCE_DATE)
    return SnapshotDescriptor.create(
        source_name="orders",
        source_date=SOURCE_DATE,
        upper_updated_at=rows[-1].updated_at,
        upper_order_id=rows[-1].order_id,
        expected_record_count=len(rows),
    )


def _chunks(rows: Sequence[SnapshotOrderRow]) -> tuple[tuple[SnapshotOrderRow, ...], ...]:
    return tuple(
        tuple(rows[offset : offset + SNAPSHOT_CONFIG.chunk_rows])
        for offset in range(0, len(rows), SNAPSHOT_CONFIG.chunk_rows)
    ) or ((),)


def _serialized(rows: Sequence[SnapshotOrderRow]) -> bytes:
    with serialize_order_chunk(rows, snapshot_config=SNAPSHOT_CONFIG) as chunk:
        return chunk.stream.read()


def _manifest(
    descriptor: SnapshotDescriptor,
    ordinal: int,
    body: bytes,
    *,
    run_id: str = SECOND_RUN_ID,
    status: ManifestStatus = ManifestStatus.SUCCEEDED,
) -> Manifest:
    raw_key = descriptor.raw_key(
        chunk_ordinal=ordinal,
        chunk_rows=SNAPSHOT_CONFIG.chunk_rows,
    )
    return Manifest(
        source_name=descriptor.source_name,
        source_type=SourceType.POSTGRES_SNAPSHOT,
        source_date=descriptor.source_date,
        batch_id=descriptor.batch_id(chunk_rows=SNAPSHOT_CONFIG.chunk_rows),
        run_id=run_id,
        object_key=raw_key,
        original_file_name=None,
        content_type=PARQUET_CONTENT_TYPE,
        content_length=len(body),
        checksum_sha256=sha256(body).hexdigest(),
        schema_version=1,
        extracted_at=EXTRACTED_AT,
        status=status,
        attempt=1,
    )


def _ingest(
    storage: RecordingStorage,
    opener: FakeSnapshotOpener,
    *,
    clock: ManualClock | None = None,
    run_id: str = RUN_ID,
    event_sink: Callable[[dict[str, object]], None] | None = None,
    serializer: Callable[..., object] = serialize_order_chunk,
    resume_descriptor: SnapshotDescriptor | None = None,
) -> SnapshotIngestionResult:
    selected_clock = clock or ManualClock()
    return ingest_order_snapshot(
        storage=storage,
        postgres_config=POSTGRES_CONFIG,
        snapshot_config=SNAPSHOT_CONFIG,
        source_name="orders",
        source_date=SOURCE_DATE,
        resume_descriptor=resume_descriptor,
        monotonic=selected_clock,
        utc_now=lambda: EXTRACTED_AT,
        run_id_factory=lambda: run_id,
        snapshot_opener=opener,
        chunk_serializer=serializer,  # type: ignore[arg-type]
        event_sink=event_sink,
    )


def test_absent_chunks_follow_exact_commit_order_and_emit_exact_metrics() -> None:
    rows = (_row(1), _row(2), _row(3))
    descriptor = _descriptor(rows)
    calls: list[str] = []
    storage = RecordingStorage(calls)
    opener = FakeSnapshotOpener(descriptor, _chunks(rows), calls)
    emitted: list[dict[str, object]] = []

    @contextmanager
    def recording_serializer(
        values: Sequence[SnapshotOrderRow],
        *,
        snapshot_config: SnapshotConfig,
    ) -> Iterator[SerializedOrderChunk]:
        calls.append(f"serialize:{calls.count('fetch:1') + calls.count('fetch:2')}")
        with serialize_order_chunk(values, snapshot_config=snapshot_config) as chunk:
            yield chunk

    result = _ingest(
        storage,
        opener,
        event_sink=emitted.append,
        serializer=recording_serializer,
    )

    first_key = descriptor.raw_key(chunk_ordinal=1, chunk_rows=SNAPSHOT_CONFIG.chunk_rows)
    first_manifest_key = manifest_key_for(first_key)
    assert calls[:5] == [
        "descriptor_validated",
        "fetch:1",
        "serialize:1",
        f"reconcile:{first_key}",
        f"write:{first_key}",
    ]
    assert calls[5] == f"write:{first_manifest_key}"
    assert [event["event"] for event in emitted] == [
        SnapshotEventType.STARTED.value,
        SnapshotEventType.CHUNK_COMPLETED.value,
        SnapshotEventType.CHUNK_COMPLETED.value,
        SnapshotEventType.SUCCEEDED.value,
        "run_summary",
    ]
    assert emitted[1]["chunk_status"] == SnapshotChunkStatus.CREATED.value
    assert result.summary.records_read == 3
    assert result.summary.records_accepted == 3
    assert result.summary.bytes_written == sum(
        len(storage.objects[descriptor.raw_key(chunk_ordinal=index, chunk_rows=2)].body)
        for index in (1, 2)
    )
    assert result.summary.duplicate_count == 0
    assert result.summary.run_id == RUN_ID
    assert result.event.event_type is SnapshotEventType.SUCCEEDED


def test_payload_only_is_reused_and_only_missing_manifest_is_created() -> None:
    rows = (_row(1),)
    descriptor = _descriptor(rows)
    calls: list[str] = []
    storage = RecordingStorage(calls)
    raw_key = descriptor.raw_key(chunk_ordinal=1, chunk_rows=SNAPSHOT_CONFIG.chunk_rows)
    body = _serialized(rows)
    storage.put_payload(raw_key, body)
    opener = FakeSnapshotOpener(descriptor, _chunks(rows), calls)
    emitted: list[dict[str, object]] = []

    result = _ingest(storage, opener, event_sink=emitted.append)

    assert f"write:{raw_key}" not in calls
    assert calls[-1] == f"write:{manifest_key_for(raw_key)}"
    assert result.summary.bytes_written == 0
    assert result.summary.duplicate_count == 0
    assert emitted[1]["chunk_status"] == SnapshotChunkStatus.PAYLOAD_REUSED.value
    manifest = Manifest.from_json(storage.objects[manifest_key_for(raw_key)].body.decode())
    assert manifest.run_id == RUN_ID
    assert manifest.status is ManifestStatus.SUCCEEDED


def test_complete_replay_preserves_original_manifests_and_run_ids() -> None:
    rows = (_row(1), _row(2), _row(3))
    descriptor = _descriptor(rows)
    calls: list[str] = []
    storage = RecordingStorage(calls)
    for ordinal, chunk_rows in enumerate(_chunks(rows), start=1):
        body = _serialized(chunk_rows)
        raw_key = descriptor.raw_key(chunk_ordinal=ordinal, chunk_rows=2)
        storage.put_payload(raw_key, body)
        storage.put_manifest(_manifest(descriptor, ordinal, body))
    original_manifests = {
        key: stored.body for key, stored in storage.objects.items() if key.startswith("manifests/")
    }
    opener = FakeSnapshotOpener(descriptor, _chunks(rows), calls)

    result = _ingest(storage, opener)

    assert result.summary.records_accepted == 3
    assert result.summary.bytes_written == 0
    assert result.summary.duplicate_count == 2
    assert all(not call.startswith("write:") for call in calls)
    assert {
        key: stored.body for key, stored in storage.objects.items() if key.startswith("manifests/")
    } == original_manifests


@pytest.mark.parametrize(
    ("state", "expected_code"),
    [
        ("manifest_only", SnapshotErrorCode.RECONCILIATION_FAILED),
        ("malformed_manifest", SnapshotErrorCode.RECONCILIATION_FAILED),
        ("wrong_status", SnapshotErrorCode.RECONCILIATION_FAILED),
        ("wrong_link", SnapshotErrorCode.RECONCILIATION_FAILED),
        ("raw_conflict", SnapshotErrorCode.OBJECT_CONFLICT),
    ],
)
def test_conflicting_reconciliation_states_fail_without_writes(
    state: str,
    expected_code: SnapshotErrorCode,
) -> None:
    rows = (_row(1),)
    descriptor = _descriptor(rows)
    calls: list[str] = []
    storage = RecordingStorage(calls)
    raw_key = descriptor.raw_key(chunk_ordinal=1, chunk_rows=2)
    body = _serialized(rows)
    if state == "manifest_only":
        storage.put_manifest(_manifest(descriptor, 1, body))
    elif state == "malformed_manifest":
        storage.reconcile_error = ObjectVerificationError("invalid manifest")
    elif state == "wrong_status":
        storage.put_payload(raw_key, body)
        storage.put_manifest(_manifest(descriptor, 1, body, status=ManifestStatus.FAILED))
    elif state == "wrong_link":
        other_rows = (_row(4),)
        other_descriptor = _descriptor(other_rows)
        wrong_manifest = _manifest(other_descriptor, 1, _serialized(other_rows))
        storage.put_payload(raw_key, body)
        storage.objects[manifest_key_for(raw_key)] = _Object(
            body=wrong_manifest.to_json().encode("utf-8"),
            content_type="application/json",
        )
    else:
        storage.put_payload(raw_key, b"conflicting bytes")
    before = dict(storage.objects)
    emitted: list[dict[str, object]] = []

    with pytest.raises(SnapshotHandledError) as error:
        _ingest(
            storage,
            FakeSnapshotOpener(descriptor, _chunks(rows), calls),
            event_sink=emitted.append,
        )

    assert error.value.code is expected_code
    assert storage.objects == before
    assert all(not call.startswith("write:") for call in calls)
    assert emitted[-2]["event"] == SnapshotEventType.FAILED.value
    assert emitted[-2]["error_code"] == expected_code.value
    assert emitted[-1]["event"] == "run_summary"


@pytest.mark.parametrize("failure", ["raw", "manifest"])
def test_write_failures_leave_only_restart_safe_evidence(failure: str) -> None:
    rows = (_row(1),)
    descriptor = _descriptor(rows)
    calls: list[str] = []
    storage = RecordingStorage(calls)
    storage.fail_raw_write = failure == "raw"
    storage.fail_manifest_write = failure == "manifest"
    raw_key = descriptor.raw_key(chunk_ordinal=1, chunk_rows=2)

    with pytest.raises(SnapshotHandledError) as error:
        _ingest(storage, FakeSnapshotOpener(descriptor, _chunks(rows), calls))

    assert error.value.code is SnapshotErrorCode.STORAGE_FAILED
    assert (raw_key in storage.objects) is (failure == "manifest")
    assert manifest_key_for(raw_key) not in storage.objects

    storage.fail_raw_write = storage.fail_manifest_write = False
    resumed = _ingest(
        storage,
        FakeSnapshotOpener(descriptor, _chunks(rows), calls),
        run_id=SECOND_RUN_ID,
        resume_descriptor=descriptor,
    )
    assert resumed.summary.records_accepted == 1
    expected_bytes_written = len(storage.objects[raw_key].body) if failure == "raw" else 0
    assert resumed.summary.bytes_written == expected_bytes_written
    assert Manifest.from_json(storage.objects[manifest_key_for(raw_key)].body.decode()).run_id == (
        SECOND_RUN_ID
    )


def test_connection_failure_restarts_from_chunk_one_and_preserves_completed_manifest() -> None:
    rows = (_row(1), _row(2), _row(3))
    descriptor = _descriptor(rows)
    chunks = _chunks(rows)
    calls: list[str] = []
    storage = RecordingStorage(calls)
    failed_events: list[dict[str, object]] = []

    with pytest.raises(SnapshotHandledError) as error:
        _ingest(
            storage,
            FakeSnapshotOpener(descriptor, chunks, calls, fail_after_chunks=1),
            event_sink=failed_events.append,
        )
    assert error.value.code is SnapshotErrorCode.POSTGRES_CONNECTION_FAILED
    first_manifest_key = descriptor.manifest_key(chunk_ordinal=1, chunk_rows=2)
    original_manifest = storage.objects[first_manifest_key].body
    assert not any("chunk-0002" in key for key in storage.objects)

    calls.clear()
    resumed = _ingest(
        storage,
        FakeSnapshotOpener(descriptor, chunks, calls),
        run_id=SECOND_RUN_ID,
        resume_descriptor=descriptor,
    )

    assert calls[1] == "fetch:1"
    assert resumed.summary.duplicate_count == 1
    assert resumed.summary.records_read == resumed.summary.records_accepted == 3
    assert storage.objects[first_manifest_key].body == original_manifest
    assert Manifest.from_json(original_manifest.decode()).run_id == RUN_ID


def test_chunk_spool_closes_before_next_fetch_and_final_totals_are_validated() -> None:
    rows = (_row(1), _row(2), _row(3))
    descriptor = _descriptor(rows)
    calls: list[str] = []
    streams: list[BinaryIO] = []

    @contextmanager
    def tracking_serializer(
        values: Sequence[SnapshotOrderRow],
        *,
        snapshot_config: SnapshotConfig,
    ) -> Iterator[SerializedOrderChunk]:
        calls.append(f"serialize:{len(streams) + 1}")
        with serialize_order_chunk(values, snapshot_config=snapshot_config) as chunk:
            streams.append(chunk.stream)
            yield chunk

    storage = RecordingStorage(calls)
    opener = FakeSnapshotOpener(
        descriptor,
        _chunks(rows),
        calls,
        prior_streams=streams,
    )

    result = _ingest(storage, opener, serializer=tracking_serializer)

    assert result.summary.records_read == 3
    assert len(streams) == 2
    assert all(stream.closed for stream in streams)

    incomplete_calls: list[str] = []
    incomplete_storage = RecordingStorage(incomplete_calls)
    with pytest.raises(SnapshotHandledError) as error:
        _ingest(
            incomplete_storage,
            FakeSnapshotOpener(descriptor, (_chunks(rows)[0],), incomplete_calls),
        )
    assert error.value.code is SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED


def test_source_contract_chunk_overflow_and_deadline_fail_without_failed_manifest() -> None:
    rows = (_row(1),)
    descriptor = _descriptor(rows)

    invalid_calls: list[str] = []
    invalid_storage = RecordingStorage(invalid_calls)
    invalid_row = list(_row(1).as_tuple())
    invalid_row[5] = Decimal("1.2")
    with pytest.raises(SnapshotHandledError) as invalid_error:
        _ingest(
            invalid_storage,
            FakeSnapshotOpener(
                descriptor,
                ((invalid_row,),),  # type: ignore[arg-type]
                invalid_calls,
            ),
        )
    assert invalid_error.value.code is SnapshotErrorCode.SOURCE_CONTRACT_FAILED
    assert not any(key.startswith("manifests/") for key in invalid_storage.objects)

    overflow_calls: list[str] = []
    overflow_storage = RecordingStorage(overflow_calls)
    with pytest.raises(SnapshotHandledError) as overflow_error:
        _ingest(
            overflow_storage,
            FakeSnapshotOpener(descriptor, ((rows[0],), (rows[0],)), overflow_calls),
        )
    assert overflow_error.value.code is SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED

    deadline_calls: list[str] = []
    deadline_storage = RecordingStorage(deadline_calls)
    clock = ManualClock()
    deadline_storage.after_reconcile = lambda: clock.advance(31.0)
    with pytest.raises(SnapshotHandledError) as deadline_error:
        _ingest(
            deadline_storage,
            FakeSnapshotOpener(descriptor, _chunks(rows), deadline_calls),
            clock=clock,
        )
    assert deadline_error.value.code is SnapshotErrorCode.DEADLINE_EXCEEDED
    assert deadline_storage.objects == {}
