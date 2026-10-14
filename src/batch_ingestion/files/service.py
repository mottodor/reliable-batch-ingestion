"""Restart-safe whole-file ingestion over the shared storage boundary."""

from __future__ import annotations

import io
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
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
    StorageError,
    StoredObject,
)
from batch_ingestion.summary import RunSummary

from .contracts import DeliveryDescriptor
from .diagnostics import DuplicateClass, RejectionCode, classify_duplicate
from .events import FileIngestionEvent, FileOutcome
from .validators import FileValidationResult, validate_file


class FileIngestionStateError(StorageError):
    """Raised when observed file objects cannot converge without inventing state."""


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


def ingest_file_delivery(
    *,
    storage: FileStorage,
    config: FileConfig,
    delivery: DeliveryDescriptor,
    monotonic: Callable[[], float] = time.monotonic,
    utc_now: Callable[[], datetime] | None = None,
    run_id_factory: Callable[[], str] = new_run_id,
    validator: FileValidator = validate_file,
    event_sink: Callable[[dict[str, object]], None] | None = None,
) -> FileIngestionResult:
    """Preserve, validate, disposition, and manifest one staged delivery."""
    started_at = monotonic()
    run_id = run_id_factory()
    wall_clock = utc_now or _utc_now
    bytes_written = 0

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
            bytes_written = content_length
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
        disposition_key = _disposition_key(delivery, validation)

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
            duplicate_count = 1
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
            duplicate_count = 0

        duration = monotonic() - started_at
        if duration < 0:
            raise RuntimeError("monotonic clock moved backwards")
        counters = validation.counters
        summary = RunSummary(
            records_read=counters.records_read,
            records_accepted=counters.records_accepted,
            records_rejected=counters.records_rejected,
            pages_fetched=0,
            bytes_written=bytes_written,
            retry_count=0,
            duplicate_count=duplicate_count,
            duration_seconds=duration,
            source_date=delivery.source_date,
            batch_id=delivery.batch_id,
            run_id=run_id,
        )
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
        if event_sink is not None:
            event_sink(event.to_dict())
            event_sink({"event": "run_summary", **summary.to_dict()})
        return FileIngestionResult(summary=summary, event=event, manifest=manifest)


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
