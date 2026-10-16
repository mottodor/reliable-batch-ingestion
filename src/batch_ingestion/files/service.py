"""Restart-safe whole-file ingestion over the shared storage boundary."""

from __future__ import annotations

import io
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import BinaryIO, Protocol

from batch_ingestion.config import FileConfig
from batch_ingestion.identifiers import SourceType, manifest_key_for, new_run_id
from batch_ingestion.manifests import MANIFEST_SCHEMA_VERSION, Manifest, ManifestStatus
from batch_ingestion.storage import (
    CHECKSUM_METADATA_KEY,
    DownloadedObject,
    ImmutableWriteResult,
    ImmutableWriteStatus,
    MoveResult,
    ObjectNotFoundError,
    ReconciliationResult,
    ReconciliationState,
    StorageBackendError,
    StorageError,
    StoredObject,
)
from batch_ingestion.summary import RunSummary, TerminalEmitter

from .contracts import DeliveryDescriptor
from .diagnostics import BoundedDiagnostics, DuplicateClass, RejectionCode, classify_duplicate
from .events import FileErrorCode, FileIngestionEvent, FileOutcome
from .validators import FileValidationResult, validate_file


class FileIngestionStateError(StorageError):
    """Raised when observed file objects cannot converge without inventing state."""


_FILE_ERROR_MESSAGES = {
    FileErrorCode.STORAGE_FAILED: "Storage operation failed",
    FileErrorCode.STORAGE_STATE_FAILED: "Storage state could not be reconciled",
}


class FileIngestionError(RuntimeError):
    """Handled file-attempt failure carrying its terminal event and summary."""

    def __init__(
        self,
        *,
        code: FileErrorCode,
        summary: RunSummary,
        event: FileIngestionEvent,
    ) -> None:
        if event.outcome is not FileOutcome.FAILED or event.error_code is not code:
            raise ValueError("file failure event must match the handled error code")
        self.code = code
        self.summary = summary
        self.event = event
        super().__init__(_FILE_ERROR_MESSAGES[code])


class FileStorage(Protocol):
    """Format-independent shared-storage operations required by file ingestion."""

    def download_spooled(
        self,
        key: str,
        *,
        maximum_bytes: int,
        spool_threshold_bytes: int,
        expected_checksum_sha256: str | None = None,
        expected_content_length: int | None = None,
    ) -> AbstractContextManager[DownloadedObject]:
        raise AssertionError("protocol method has no runtime implementation")

    def inspect_object(
        self,
        key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> StoredObject | None:
        raise AssertionError("protocol method has no runtime implementation")

    def reconcile_payload_manifest(
        self,
        payload_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> ReconciliationResult:
        raise AssertionError("protocol method has no runtime implementation")

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

    def move_verified(
        self,
        source_key: str,
        destination_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> MoveResult:
        raise AssertionError("protocol method has no runtime implementation")


class FileValidator(Protocol):
    """Injected pure validator signature used by ordering and fault tests."""

    def __call__(
        self,
        stream: BinaryIO,
        *,
        delivery: DeliveryDescriptor,
        config: FileConfig,
    ) -> FileValidationResult:
        raise AssertionError("protocol method has no runtime implementation")


@dataclass(frozen=True, slots=True)
class FileIngestionResult:
    """Terminal accepted or retained-rejection result for one physical attempt."""

    summary: RunSummary
    event: FileIngestionEvent
    manifest: Manifest


@dataclass(slots=True)
class _FileAttemptState:
    records_read: int = 0
    records_accepted: int = 0
    records_rejected: int = 0
    bytes_written: int = 0
    duplicate_count: int = 0
    disposition_key: str | None = None
    duplicate_class: DuplicateClass | None = None


@dataclass(frozen=True, slots=True)
class _FileCompletion:
    event: FileIngestionEvent
    manifest: Manifest


def ingest_file_delivery(
    *,
    config: FileConfig,
    delivery: DeliveryDescriptor,
    storage: FileStorage | None = None,
    storage_session_provider: (Callable[[], AbstractContextManager[FileStorage]] | None) = None,
    landing_stream: BinaryIO | None = None,
    landing_content_length: int | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    utc_now: Callable[[], datetime] | None = None,
    run_id_factory: Callable[[], str] = new_run_id,
    validator: FileValidator = validate_file,
    event_sink: Callable[[dict[str, object]], None] | None = None,
) -> FileIngestionResult:
    """Own storage, optional landing staging, and one terminal pair for a file attempt."""
    if (landing_stream is None) != (landing_content_length is None):
        raise ValueError("landing stream and content length must be supplied together")
    if landing_content_length is not None and (
        isinstance(landing_content_length, bool) or landing_content_length < 0
    ):
        raise ValueError("landing content length must be a non-negative integer")

    started_at = monotonic()
    run_id = run_id_factory()
    wall_clock = utc_now or _utc_now
    terminal = TerminalEmitter(event_sink)
    state = _FileAttemptState()
    completion: _FileCompletion | None = None

    try:
        with _file_storage_context(
            storage=storage,
            storage_session_provider=storage_session_provider,
        ) as active_storage:
            if landing_stream is not None:
                assert landing_content_length is not None
                landing_stream.seek(0)
                active_storage.write_immutable(
                    delivery.landing_key,
                    landing_stream,
                    content_length=landing_content_length,
                    checksum_sha256=delivery.actual_sha256,
                    content_type=delivery.content_type,
                )
            completion = _ingest_file_delivery_with_storage(
                storage=active_storage,
                config=config,
                delivery=delivery,
                run_id=run_id,
                wall_clock=wall_clock,
                validator=validator,
                state=state,
            )
    except StorageError as error:
        code = (
            FileErrorCode.STORAGE_FAILED
            if isinstance(error, StorageBackendError)
            else FileErrorCode.STORAGE_STATE_FAILED
        )
        summary = _file_summary(
            state,
            started_at=started_at,
            monotonic=monotonic,
            delivery=delivery,
            run_id=run_id,
        )
        event = _failed_file_event(
            delivery=delivery,
            run_id=run_id,
            code=code,
            state=state,
        )
        terminal.emit(event.to_dict(), summary)
        raise FileIngestionError(code=code, summary=summary, event=event) from error

    if completion is None:
        raise AssertionError("successful file ingestion has no completion")
    summary = _file_summary(
        state,
        started_at=started_at,
        monotonic=monotonic,
        delivery=delivery,
        run_id=run_id,
    )
    terminal.emit(completion.event.to_dict(), summary)
    return FileIngestionResult(
        summary=summary,
        event=completion.event,
        manifest=completion.manifest,
    )


def _file_storage_context(
    *,
    storage: FileStorage | None,
    storage_session_provider: Callable[[], AbstractContextManager[FileStorage]] | None,
) -> AbstractContextManager[FileStorage]:
    if (storage is None) == (storage_session_provider is None):
        raise ValueError("exactly one file storage boundary must be supplied")
    if storage_session_provider is not None:
        return storage_session_provider()
    assert storage is not None
    return nullcontext(storage)


def _ingest_file_delivery_with_storage(
    *,
    storage: FileStorage,
    config: FileConfig,
    delivery: DeliveryDescriptor,
    run_id: str,
    wall_clock: Callable[[], datetime],
    validator: FileValidator,
    state: _FileAttemptState,
) -> _FileCompletion:
    """Preserve, validate, disposition, and manifest using acquired storage."""
    with _download_evidence(storage=storage, config=config, delivery=delivery) as downloaded:
        content_length = downloaded.info.content_length
        checksum_sha256 = downloaded.info.checksum_sha256
        _require_content_type(downloaded.info, delivery.content_type)

        reconciliation = storage.reconcile_payload_manifest(
            delivery.raw_key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )
        if reconciliation.state is ReconciliationState.MANIFEST_ONLY:
            raise FileIngestionStateError(
                "manifest-only state cannot be repaired without canonical raw payload evidence"
            )

        raw_object = reconciliation.payload
        if reconciliation.state is ReconciliationState.ABSENT:
            downloaded.stream.seek(0)
            raw_write = storage.write_immutable(
                delivery.raw_key,
                downloaded.stream,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                content_type=delivery.content_type,
            )
            if raw_write.status is not ImmutableWriteStatus.CREATED:
                raise FileIngestionStateError(
                    "absent raw payload was not created under the sequential-writer contract"
                )
            raw_object = raw_write.object
            state.bytes_written = content_length
        elif reconciliation.state in {
            ReconciliationState.PAYLOAD_ONLY,
            ReconciliationState.COMPLETE,
        }:
            if raw_object is None:
                raise FileIngestionStateError(
                    "reconciliation omitted existing canonical raw payload evidence"
                )
        else:
            raise FileIngestionStateError(
                f"unsupported reconciliation state: {reconciliation.state.value}"
            )

        assert raw_object is not None
        _require_canonical_raw(raw_object, delivery, checksum_sha256)

        downloaded.stream.seek(0)
        validation = validator(downloaded.stream, delivery=delivery, config=config)
        counters = validation.counters
        state.records_read = counters.records_read
        state.records_accepted = counters.records_accepted
        state.records_rejected = counters.records_rejected
        disposition_key = _disposition_key(delivery, validation)
        state.disposition_key = disposition_key

        _require_no_alternative_disposition(
            storage=storage,
            delivery=delivery,
            expected_key=disposition_key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )
        existing_disposition = storage.inspect_object(
            disposition_key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )
        if existing_disposition is not None:
            _require_matching_disposition(existing_disposition, raw_object)

        if downloaded.info.key != delivery.landing_key and existing_disposition is None:
            downloaded.stream.seek(0)
            landing_write = storage.write_immutable(
                delivery.landing_key,
                downloaded.stream,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                content_type=delivery.content_type,
            )
            if landing_write.status is not ImmutableWriteStatus.CREATED:
                raise FileIngestionStateError(
                    "missing landing object was not reconstructed under sequential-writer mode"
                )

        movement = storage.move_verified(
            delivery.landing_key,
            disposition_key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )
        _require_matching_disposition(movement.destination, raw_object)

        expected_status = (
            ManifestStatus.SUCCEEDED if validation.accepted else ManifestStatus.REJECTED
        )
        if reconciliation.state is ReconciliationState.COMPLETE:
            manifest = _require_complete_manifest(reconciliation, expected_status=expected_status)
            duplicate_class = classify_duplicate(
                manifest.original_file_name,
                delivery.original_file_name,
            )
            state.duplicate_count = 1
        else:
            manifest = _file_manifest(
                delivery=delivery,
                run_id=run_id,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                extracted_at=wall_clock(),
                status=expected_status,
            )
            _write_manifest(storage, manifest)
            duplicate_class = DuplicateClass.NEW_DELIVERY
        state.duplicate_class = duplicate_class

        event = FileIngestionEvent(
            outcome=FileOutcome.ACCEPTED if validation.accepted else FileOutcome.REJECTED,
            rejection_code=validation.rejection_code,
            diagnostics=validation.diagnostics,
            source_name=delivery.source_name,
            source_date=delivery.source_date,
            batch_id=delivery.batch_id,
            run_id=run_id,
            landing_key=delivery.landing_key,
            raw_key=delivery.raw_key,
            manifest_key=delivery.manifest_key,
            disposition_key=disposition_key,
            original_file_name=delivery.original_file_name,
            file_format=delivery.file_format,
            duplicate_class=duplicate_class,
        )
        return _FileCompletion(event=event, manifest=manifest)


def _file_summary(
    state: _FileAttemptState,
    *,
    started_at: float,
    monotonic: Callable[[], float],
    delivery: DeliveryDescriptor,
    run_id: str,
) -> RunSummary:
    duration = monotonic() - started_at
    if duration < 0:
        raise RuntimeError("monotonic clock moved backwards")
    return RunSummary(
        records_read=state.records_read,
        records_accepted=state.records_accepted,
        records_rejected=state.records_rejected,
        pages_fetched=0,
        bytes_written=state.bytes_written,
        retry_count=0,
        duplicate_count=state.duplicate_count,
        duration_seconds=duration,
        source_date=delivery.source_date,
        batch_id=delivery.batch_id,
        run_id=run_id,
    )


def _failed_file_event(
    *,
    delivery: DeliveryDescriptor,
    run_id: str,
    code: FileErrorCode,
    state: _FileAttemptState,
) -> FileIngestionEvent:
    return FileIngestionEvent(
        outcome=FileOutcome.FAILED,
        error_code=code,
        rejection_code=None,
        diagnostics=BoundedDiagnostics(details=(), truncated=False),
        source_name=delivery.source_name,
        source_date=delivery.source_date,
        batch_id=delivery.batch_id,
        run_id=run_id,
        landing_key=delivery.landing_key,
        raw_key=delivery.raw_key,
        manifest_key=delivery.manifest_key,
        disposition_key=state.disposition_key,
        original_file_name=delivery.original_file_name,
        file_format=delivery.file_format,
        duplicate_class=state.duplicate_class,
    )


@contextmanager
def _download_evidence(
    *,
    storage: FileStorage,
    config: FileConfig,
    delivery: DeliveryDescriptor,
) -> Iterator[DownloadedObject]:
    with ExitStack() as stack:
        try:
            downloaded = stack.enter_context(
                storage.download_spooled(
                    delivery.landing_key,
                    maximum_bytes=config.max_file_bytes,
                    spool_threshold_bytes=config.spool_threshold_bytes,
                    expected_checksum_sha256=delivery.actual_sha256,
                )
            )
        except ObjectNotFoundError:
            try:
                downloaded = stack.enter_context(
                    storage.download_spooled(
                        delivery.raw_key,
                        maximum_bytes=config.max_file_bytes,
                        spool_threshold_bytes=config.spool_threshold_bytes,
                        expected_checksum_sha256=delivery.actual_sha256,
                    )
                )
            except ObjectNotFoundError as raw_error:
                raise ObjectNotFoundError(
                    f"neither landing object {delivery.landing_key!r} nor raw object "
                    f"{delivery.raw_key!r} exists"
                ) from raw_error
        yield downloaded


def _disposition_key(
    delivery: DeliveryDescriptor,
    validation: FileValidationResult,
) -> str:
    if validation.accepted:
        if validation.rejection_code is not None:
            raise FileIngestionStateError("accepted validation unexpectedly carried a reason")
        return delivery.archive_key
    if validation.rejection_code is None:
        raise FileIngestionStateError("rejected validation omitted its primary reason")
    return delivery.rejected_key(validation.rejection_code)


def _all_disposition_keys(delivery: DeliveryDescriptor) -> tuple[str, ...]:
    return (
        delivery.archive_key,
        *(delivery.rejected_key(code) for code in RejectionCode),
    )


def _require_no_alternative_disposition(
    *,
    storage: FileStorage,
    delivery: DeliveryDescriptor,
    expected_key: str,
    content_length: int,
    checksum_sha256: str,
) -> None:
    for alternative_key in _all_disposition_keys(delivery):
        if alternative_key == expected_key:
            continue
        observed = storage.inspect_object(
            alternative_key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )
        if observed is not None:
            raise FileIngestionStateError(
                f"alternative disposition {alternative_key!r} already exists"
            )


def _require_content_type(stored: StoredObject, expected: str) -> None:
    if stored.content_type != expected:
        raise FileIngestionStateError(
            f"object {stored.key!r} has a content type that conflicts with its delivery"
        )


def _require_canonical_raw(
    stored: StoredObject,
    delivery: DeliveryDescriptor,
    checksum_sha256: str,
) -> None:
    _require_content_type(stored, delivery.content_type)
    if stored.metadata.get(CHECKSUM_METADATA_KEY) != checksum_sha256:
        raise FileIngestionStateError(
            f"raw object {stored.key!r} has conflicting checksum metadata"
        )


def _require_matching_disposition(destination: StoredObject, raw_object: StoredObject) -> None:
    if (
        destination.content_type != raw_object.content_type
        or destination.metadata != raw_object.metadata
    ):
        raise FileIngestionStateError(
            f"disposition object {destination.key!r} does not preserve raw metadata"
        )


def _require_complete_manifest(
    reconciliation: ReconciliationResult,
    *,
    expected_status: ManifestStatus,
) -> Manifest:
    manifest = reconciliation.manifest
    if manifest is None:
        raise FileIngestionStateError("complete reconciliation omitted its manifest")
    if manifest.status is not expected_status:
        raise FileIngestionStateError(
            "complete manifest status conflicts with current whole-file validation"
        )
    return manifest


def _file_manifest(
    *,
    delivery: DeliveryDescriptor,
    run_id: str,
    content_length: int,
    checksum_sha256: str,
    extracted_at: datetime,
    status: ManifestStatus,
) -> Manifest:
    return Manifest(
        source_name=delivery.source_name,
        source_type=SourceType.FILE,
        source_date=delivery.source_date,
        batch_id=delivery.batch_id,
        run_id=run_id,
        object_key=delivery.raw_key,
        original_file_name=delivery.original_file_name,
        content_type=delivery.content_type,
        content_length=content_length,
        checksum_sha256=checksum_sha256,
        schema_version=MANIFEST_SCHEMA_VERSION,
        extracted_at=extracted_at,
        status=status,
        attempt=1,
    )


def _write_manifest(storage: FileStorage, manifest: Manifest) -> None:
    manifest_bytes = manifest.to_json().encode("utf-8")
    result = storage.write_immutable(
        manifest_key_for(manifest.object_key),
        io.BytesIO(manifest_bytes),
        content_length=len(manifest_bytes),
        checksum_sha256=sha256(manifest_bytes).hexdigest(),
        content_type="application/json",
    )
    if result.status is not ImmutableWriteStatus.CREATED:
        raise FileIngestionStateError(
            "missing manifest was not created under the sequential-writer contract"
        )


def _utc_now() -> datetime:
    return datetime.now(UTC)
