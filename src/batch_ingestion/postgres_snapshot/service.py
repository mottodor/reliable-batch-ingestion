"""Restart-safe per-chunk PostgreSQL snapshot ingestion over shared storage."""

from __future__ import annotations

import hashlib
import io
import time
from abc import abstractmethod
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import BinaryIO, Protocol

from batch_ingestion.config import PostgresConfig, SnapshotConfig
from batch_ingestion.identifiers import SourceType, manifest_key_for, new_run_id
from batch_ingestion.manifests import MANIFEST_SCHEMA_VERSION, Manifest, ManifestStatus
from batch_ingestion.storage import (
    ImmutableWriteResult,
    ImmutableWriteStatus,
    ObjectConflictError,
    ObjectVerificationError,
    ReconciliationResult,
    ReconciliationState,
    StorageError,
    StoredObject,
)
from batch_ingestion.summary import RunSummary, TerminalEmitter

from .contracts import PARQUET_CONTENT_TYPE, SnapshotDescriptor
from .events import (
    SnapshotChunkStatus,
    SnapshotCounters,
    SnapshotErrorCode,
    SnapshotEvent,
    SnapshotHandledError,
)
from .reader import open_order_snapshot
from .schema import SnapshotOrderRow
from .serializer import SerializedOrderChunk, serialize_order_chunk


class SnapshotStorage(Protocol):
    """Narrow shared-storage behavior used by the snapshot service."""

    @abstractmethod
    def reconcile_payload_manifest(
        self,
        payload_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> ReconciliationResult:
        raise AssertionError("protocol method has no runtime implementation")

    @abstractmethod
    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        raise AssertionError("protocol method has no runtime implementation")


class SnapshotRead(Protocol):
    descriptor: SnapshotDescriptor

    @abstractmethod
    def iter_chunks(self) -> Iterator[tuple[SnapshotOrderRow, ...]]:
        raise AssertionError("protocol method has no runtime implementation")


SnapshotOpener = Callable[..., AbstractContextManager[SnapshotRead]]


@dataclass(frozen=True, slots=True)
class SnapshotIngestionResult:
    """Successful physical-attempt output without retaining chunk rows or payloads."""

    descriptor: SnapshotDescriptor
    summary: RunSummary
    event: SnapshotEvent


@dataclass(slots=True)
class _MutableCounters:
    records_read: int = 0
    records_accepted: int = 0
    bytes_written: int = 0
    duplicate_count: int = 0

    def freeze(self) -> SnapshotCounters:
        return SnapshotCounters(
            records_read=self.records_read,
            records_accepted=self.records_accepted,
            bytes_written=self.bytes_written,
            duplicate_count=self.duplicate_count,
        )


@dataclass(slots=True)
class _Deadline:
    monotonic: Callable[[], float]
    started_at: float
    maximum_seconds: float

    def elapsed(self) -> float:
        elapsed = self.monotonic() - self.started_at
        if elapsed < 0:
            raise RuntimeError("monotonic clock moved backwards")
        return elapsed

    def check(self) -> None:
        if self.elapsed() > self.maximum_seconds:
            raise SnapshotHandledError(SnapshotErrorCode.DEADLINE_EXCEEDED)


def ingest_order_snapshot(
    *,
    postgres_config: PostgresConfig,
    snapshot_config: SnapshotConfig,
    source_name: str,
    source_date: date,
    storage: SnapshotStorage | None = None,
    storage_session_provider: (Callable[[], AbstractContextManager[SnapshotStorage]] | None) = None,
    resume_descriptor: SnapshotDescriptor | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    utc_now: Callable[[], datetime] | None = None,
    run_id_factory: Callable[[], str] = new_run_id,
    snapshot_opener: SnapshotOpener = open_order_snapshot,
    chunk_serializer: Callable[..., AbstractContextManager[SerializedOrderChunk]] = (
        serialize_order_chunk
    ),
    event_sink: Callable[[dict[str, object]], None] | None = None,
) -> SnapshotIngestionResult:
    """Capture a descriptor, own storage, and commit each bounded chunk raw-first."""
    started_at = monotonic()
    deadline = _Deadline(monotonic, started_at, snapshot_config.max_elapsed_seconds)
    run_id = run_id_factory()
    wall_clock = utc_now or _utc_now
    counters = _MutableCounters()
    terminal = TerminalEmitter(event_sink)
    descriptor: SnapshotDescriptor | None = None
    batch_id: str | None = None

    try:
        deadline.check()
        with snapshot_opener(
            postgres_config=postgres_config,
            snapshot_config=snapshot_config,
            source_name=source_name,
            source_date=source_date,
            resume_descriptor=resume_descriptor,
            monotonic=monotonic,
        ) as snapshot:
            deadline.check()
            descriptor = snapshot.descriptor
            _require_descriptor(
                descriptor,
                source_name=source_name,
                source_date=source_date,
                resume_descriptor=resume_descriptor,
                snapshot_config=snapshot_config,
            )
            batch_id = descriptor.batch_id(chunk_rows=snapshot_config.chunk_rows)
            expected_chunks = descriptor.expected_chunk_count(chunk_rows=snapshot_config.chunk_rows)
            _emit(
                event_sink,
                SnapshotEvent.started(
                    descriptor=descriptor,
                    chunk_rows=snapshot_config.chunk_rows,
                    batch_id=batch_id,
                    run_id=run_id,
                ),
            )

            produced_chunks = 0
            final_watermark: tuple[datetime, int] | None = None
            try:
                with _snapshot_storage_context(
                    storage=storage,
                    storage_session_provider=storage_session_provider,
                ) as active_storage:
                    for chunk_ordinal, rows in enumerate(snapshot.iter_chunks(), start=1):
                        deadline.check()
                        counters.records_read += len(rows)
                        if chunk_ordinal > expected_chunks:
                            raise SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)

                        with chunk_serializer(
                            rows,
                            snapshot_config=snapshot_config,
                        ) as serialized:
                            deadline.check()
                            if serialized.record_count != len(rows):
                                raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED)
                            if rows:
                                final_row = rows[-1]
                                if not isinstance(final_row, SnapshotOrderRow):
                                    raise SnapshotHandledError(
                                        SnapshotErrorCode.SOURCE_CONTRACT_FAILED
                                    )
                                final_watermark = (final_row.updated_at, final_row.order_id)
                            chunk_status = _commit_chunk(
                                storage=active_storage,
                                descriptor=descriptor,
                                snapshot_config=snapshot_config,
                                serialized=serialized,
                                chunk_ordinal=chunk_ordinal,
                                batch_id=batch_id,
                                run_id=run_id,
                                extracted_at=wall_clock(),
                                counters=counters,
                                deadline=deadline,
                            )
                            produced_chunks = chunk_ordinal
                            _emit(
                                event_sink,
                                SnapshotEvent.chunk_completed(
                                    descriptor=descriptor,
                                    chunk_rows=snapshot_config.chunk_rows,
                                    batch_id=batch_id,
                                    run_id=run_id,
                                    chunk_ordinal=chunk_ordinal,
                                    object_key=descriptor.raw_key(
                                        chunk_ordinal=chunk_ordinal,
                                        chunk_rows=snapshot_config.chunk_rows,
                                    ),
                                    chunk_status=chunk_status,
                                    records=serialized.record_count,
                                    content_length=serialized.content_length,
                                ),
                            )
                        deadline.check()

                    deadline.check()
                    _require_final_totals(
                        descriptor=descriptor,
                        expected_chunks=expected_chunks,
                        produced_chunks=produced_chunks,
                        records_read=counters.records_read,
                        final_watermark=final_watermark,
                    )
            except StorageError as error:
                raise SnapshotHandledError(SnapshotErrorCode.STORAGE_FAILED) from error

            frozen_counters = counters.freeze()
            summary = frozen_counters.to_run_summary(
                duration_seconds=deadline.elapsed(),
                source_date=source_date,
                batch_id=batch_id,
                run_id=run_id,
            )
            succeeded = SnapshotEvent.succeeded(
                descriptor=descriptor,
                chunk_rows=snapshot_config.chunk_rows,
                batch_id=batch_id,
                run_id=run_id,
                counters=frozen_counters,
            )
            terminal.emit(succeeded.to_dict(), summary)
            return SnapshotIngestionResult(
                descriptor=descriptor,
                summary=summary,
                event=succeeded,
            )
    except SnapshotHandledError as error:
        if descriptor is not None and batch_id is not None:
            frozen_counters = counters.freeze()
            summary = frozen_counters.to_run_summary(
                duration_seconds=deadline.elapsed(),
                source_date=source_date,
                batch_id=batch_id,
                run_id=run_id,
            )
            failed = SnapshotEvent.failed(
                descriptor=descriptor,
                chunk_rows=snapshot_config.chunk_rows,
                batch_id=batch_id,
                run_id=run_id,
                error_code=error.code,
                counters=frozen_counters,
            )
            terminal.emit(failed.to_dict(), summary)
            error.attach_terminal(failed, summary)
        raise


def _snapshot_storage_context(
    *,
    storage: SnapshotStorage | None,
    storage_session_provider: (Callable[[], AbstractContextManager[SnapshotStorage]] | None),
) -> AbstractContextManager[SnapshotStorage]:
    if (storage is None) == (storage_session_provider is None):
        raise ValueError("exactly one snapshot storage boundary must be supplied")
    if storage_session_provider is not None:
        return storage_session_provider()
    assert storage is not None
    return nullcontext(storage)


def _commit_chunk(
    *,
    storage: SnapshotStorage,
    descriptor: SnapshotDescriptor,
    snapshot_config: SnapshotConfig,
    serialized: SerializedOrderChunk,
    chunk_ordinal: int,
    batch_id: str,
    run_id: str,
    extracted_at: datetime,
    counters: _MutableCounters,
    deadline: _Deadline,
) -> SnapshotChunkStatus:
    raw_key = descriptor.raw_key(
        chunk_ordinal=chunk_ordinal,
        chunk_rows=snapshot_config.chunk_rows,
    )
    reconciliation = _reconcile(
        storage,
        raw_key=raw_key,
        content_length=serialized.content_length,
        checksum_sha256=serialized.checksum_sha256,
    )
    deadline.check()

    if reconciliation.state is ReconciliationState.MANIFEST_ONLY:
        raise SnapshotHandledError(SnapshotErrorCode.RECONCILIATION_FAILED)
    if reconciliation.state is ReconciliationState.COMPLETE:
        _require_complete_chunk(
            reconciliation,
            descriptor=descriptor,
            raw_key=raw_key,
            batch_id=batch_id,
        )
        counters.records_accepted += serialized.record_count
        counters.duplicate_count += 1
        return SnapshotChunkStatus.COMPLETE_REUSED

    if reconciliation.state is ReconciliationState.ABSENT:
        serialized.stream.seek(0)
        raw_result = _write_immutable(
            storage,
            raw_key,
            serialized.stream,
            content_length=serialized.content_length,
            checksum_sha256=serialized.checksum_sha256,
            content_type=PARQUET_CONTENT_TYPE,
        )
        deadline.check()
        if raw_result.status is not ImmutableWriteStatus.CREATED:
            raise SnapshotHandledError(SnapshotErrorCode.OBJECT_CONFLICT)
        _require_payload(
            raw_result.object,
            raw_key=raw_key,
            content_length=serialized.content_length,
            checksum_sha256=serialized.checksum_sha256,
        )
        counters.bytes_written += serialized.content_length
        chunk_status = SnapshotChunkStatus.CREATED
    elif reconciliation.state is ReconciliationState.PAYLOAD_ONLY:
        if reconciliation.payload is None:
            raise SnapshotHandledError(SnapshotErrorCode.RECONCILIATION_FAILED)
        _require_payload(
            reconciliation.payload,
            raw_key=raw_key,
            content_length=serialized.content_length,
            checksum_sha256=serialized.checksum_sha256,
        )
        chunk_status = SnapshotChunkStatus.PAYLOAD_REUSED
    else:
        raise SnapshotHandledError(SnapshotErrorCode.RECONCILIATION_FAILED)

    manifest = Manifest(
        source_name=descriptor.source_name,
        source_type=SourceType.POSTGRES_SNAPSHOT,
        source_date=descriptor.source_date,
        batch_id=batch_id,
        run_id=run_id,
        object_key=raw_key,
        original_file_name=None,
        content_type=PARQUET_CONTENT_TYPE,
        content_length=serialized.content_length,
        checksum_sha256=serialized.checksum_sha256,
        schema_version=MANIFEST_SCHEMA_VERSION,
        extracted_at=extracted_at,
        status=ManifestStatus.SUCCEEDED,
        attempt=1,
    )
    _write_manifest(storage, manifest)
    deadline.check()
    counters.records_accepted += serialized.record_count
    return chunk_status


def _reconcile(
    storage: SnapshotStorage,
    *,
    raw_key: str,
    content_length: int,
    checksum_sha256: str,
) -> ReconciliationResult:
    try:
        return storage.reconcile_payload_manifest(
            raw_key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )
    except ObjectConflictError as error:
        raise SnapshotHandledError(SnapshotErrorCode.OBJECT_CONFLICT) from error
    except ObjectVerificationError as error:
        raise SnapshotHandledError(SnapshotErrorCode.RECONCILIATION_FAILED) from error
    except StorageError as error:
        raise SnapshotHandledError(SnapshotErrorCode.STORAGE_FAILED) from error


def _write_immutable(
    storage: SnapshotStorage,
    key: str,
    stream: BinaryIO,
    *,
    content_length: int,
    checksum_sha256: str,
    content_type: str,
) -> ImmutableWriteResult:
    try:
        return storage.write_immutable(
            key,
            stream,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            content_type=content_type,
        )
    except ObjectConflictError as error:
        raise SnapshotHandledError(SnapshotErrorCode.OBJECT_CONFLICT) from error
    except StorageError as error:
        raise SnapshotHandledError(SnapshotErrorCode.STORAGE_FAILED) from error


def _write_manifest(storage: SnapshotStorage, manifest: Manifest) -> None:
    body = manifest.to_json().encode("utf-8")
    result = _write_immutable(
        storage,
        manifest_key_for(manifest.object_key),
        io.BytesIO(body),
        content_length=len(body),
        checksum_sha256=hashlib.sha256(body).hexdigest(),
        content_type="application/json",
    )
    if result.status is not ImmutableWriteStatus.CREATED:
        raise SnapshotHandledError(SnapshotErrorCode.OBJECT_CONFLICT)


def _require_payload(
    payload: StoredObject,
    *,
    raw_key: str,
    content_length: int,
    checksum_sha256: str,
) -> None:
    if (
        payload.key != raw_key
        or payload.content_type != PARQUET_CONTENT_TYPE
        or payload.content_length != content_length
        or payload.checksum_sha256 != checksum_sha256
    ):
        raise SnapshotHandledError(SnapshotErrorCode.OBJECT_CONFLICT)


def _require_complete_chunk(
    reconciliation: ReconciliationResult,
    *,
    descriptor: SnapshotDescriptor,
    raw_key: str,
    batch_id: str,
) -> Manifest:
    payload = reconciliation.payload
    manifest = reconciliation.manifest
    if payload is None or manifest is None:
        raise SnapshotHandledError(SnapshotErrorCode.RECONCILIATION_FAILED)
    if (
        manifest.source_name != descriptor.source_name
        or manifest.source_type is not SourceType.POSTGRES_SNAPSHOT
        or manifest.source_date != descriptor.source_date
        or manifest.batch_id != batch_id
        or manifest.object_key != raw_key
        or manifest.original_file_name is not None
        or manifest.content_type != PARQUET_CONTENT_TYPE
        or manifest.status is not ManifestStatus.SUCCEEDED
        or manifest.attempt != 1
    ):
        raise SnapshotHandledError(SnapshotErrorCode.RECONCILIATION_FAILED)
    _require_payload(
        payload,
        raw_key=raw_key,
        content_length=manifest.content_length,
        checksum_sha256=manifest.checksum_sha256,
    )
    return manifest


def _require_descriptor(
    descriptor: SnapshotDescriptor,
    *,
    source_name: str,
    source_date: date,
    resume_descriptor: SnapshotDescriptor | None,
    snapshot_config: SnapshotConfig,
) -> None:
    if descriptor.source_name != source_name or descriptor.source_date != source_date:
        raise SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)
    if resume_descriptor is not None and descriptor != resume_descriptor:
        raise SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)
    try:
        expected_chunks = descriptor.expected_chunk_count(chunk_rows=snapshot_config.chunk_rows)
    except ValueError as error:
        raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED) from error
    if (
        descriptor.expected_record_count > snapshot_config.max_records
        or expected_chunks > snapshot_config.max_chunks
    ):
        raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED)


def _require_final_totals(
    *,
    descriptor: SnapshotDescriptor,
    expected_chunks: int,
    produced_chunks: int,
    records_read: int,
    final_watermark: tuple[datetime, int] | None,
) -> None:
    expected_watermark = (
        None if descriptor.is_empty else (descriptor.upper_updated_at, descriptor.upper_order_id)
    )
    if (
        produced_chunks != expected_chunks
        or records_read != descriptor.expected_record_count
        or final_watermark != expected_watermark
    ):
        raise SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)


def _emit(
    event_sink: Callable[[dict[str, object]], None] | None,
    event: SnapshotEvent,
) -> None:
    if event_sink is not None:
        event_sink(event.to_dict())


def _utc_now() -> datetime:
    return datetime.now(UTC)
