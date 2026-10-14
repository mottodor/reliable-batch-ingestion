"""Restart-safe file-ingestion service coverage for BI-060 slice 3."""

from __future__ import annotations

import io
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO

import pytest

from batch_ingestion.config import FileConfig
from batch_ingestion.files import (
    DeliveryDescriptor,
    DuplicateClass,
    FileOutcome,
    RejectionCode,
)
from batch_ingestion.files.service import (
    FileIngestionResult,
    FileIngestionStateError,
    ingest_file_delivery,
)
from batch_ingestion.files.validators import FileValidationResult, validate_file
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.storage import (
    DownloadedObject,
    ImmutableWriteResult,
    ImmutableWriteStatus,
    MoveResult,
    MoveStatus,
    ObjectConflictError,
    ObjectNotFoundError,
    ReconciliationResult,
    ReconciliationState,
    StoredObject,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = REPOSITORY_ROOT / "fixtures" / "supplier-prices" / "canonical"
SOURCE_DATE = date(2026, 9, 1)
RUN_ID = "run_11111111111111111111111111111111"
ORIGINAL_RUN_ID = "run_22222222222222222222222222222222"
EXTRACTED_AT = datetime(2026, 9, 1, 12, tzinfo=UTC)
CONFIG = FileConfig(
    max_file_bytes=8 * 1024 * 1024,
    spool_threshold_bytes=1024,
    max_records=100_000,
    parquet_batch_rows=2,
    max_diagnostic_details=10,
)


@dataclass
class _Object:
    body: bytes
    content_type: str
    metadata: dict[str, str]


class RecordingStorage:
    """Small exact-key fake implementing only the shared file-service boundary."""

    def __init__(self) -> None:
        self.objects: dict[str, _Object] = {}
        self.calls: list[str] = []
        self.fail_move = False
        self.fail_manifest_write = False
        self.fail_raw_write = False

    @contextmanager
    def download_spooled(
        self,
        key: str,
        *,
        maximum_bytes: int,
        spool_threshold_bytes: int,
        expected_checksum_sha256: str | None = None,
        expected_content_length: int | None = None,
    ) -> Iterator[DownloadedObject]:
        del spool_threshold_bytes
        self.calls.append(f"download:{key}")
        stored = self.objects.get(key)
        if stored is None:
            raise ObjectNotFoundError(f"object {key!r} does not exist")
        if len(stored.body) > maximum_bytes:
            raise AssertionError("test payload exceeded configured maximum")
        checksum = sha256(stored.body).hexdigest()
        if expected_checksum_sha256 is not None and checksum != expected_checksum_sha256:
            raise ObjectConflictError(f"object {key!r} does not match expected bytes")
        if expected_content_length is not None and len(stored.body) != expected_content_length:
            raise ObjectConflictError(f"object {key!r} does not match expected bytes")
        stream = io.BytesIO(stored.body)
        try:
            yield DownloadedObject(
                info=StoredObject(
                    key=key,
                    content_length=len(stored.body),
                    checksum_sha256=checksum,
                    content_type=stored.content_type,
                    metadata=dict(stored.metadata),
                ),
                stream=stream,
            )
        finally:
            stream.close()

    def inspect_object(
        self,
        key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> StoredObject | None:
        self.calls.append(f"inspect:{key}")
        stored = self.objects.get(key)
        if stored is None:
            return None
        if len(stored.body) != content_length or sha256(stored.body).hexdigest() != checksum_sha256:
            raise ObjectConflictError(f"object {key!r} does not match expected bytes")
        return self._info(key, stored)

    def reconcile_payload_manifest(
        self,
        payload_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> ReconciliationResult:
        self.calls.append(f"reconcile:{payload_key}")
        payload = self.objects.get(payload_key)
        manifest_object = self.objects.get(_manifest_key(payload_key))
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
            raise ObjectConflictError(f"object {payload_key!r} does not match expected bytes")
        info = self._info(payload_key, payload)
        if manifest_object is None:
            return ReconciliationResult(
                state=ReconciliationState.PAYLOAD_ONLY,
                payload=info,
                manifest=None,
            )
        manifest = Manifest.from_json(manifest_object.body.decode("utf-8"))
        return ReconciliationResult(
            state=ReconciliationState.COMPLETE,
            payload=info,
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
            if existing.body != body:
                raise ObjectConflictError(f"object {key!r} conflicts")
            return ImmutableWriteResult(ImmutableWriteStatus.UNCHANGED, self._info(key, existing))
        stored = _Object(
            body=body,
            content_type=content_type,
            metadata={"checksum-sha256": checksum_sha256},
        )
        self.objects[key] = stored
        return ImmutableWriteResult(ImmutableWriteStatus.CREATED, self._info(key, stored))

    def move_verified(
        self,
        source_key: str,
        destination_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> MoveResult:
        self.calls.append(f"move:{source_key}->{destination_key}")
        source = self.objects.get(source_key)
        destination = self.objects.get(destination_key)
        if source is None and destination is None:
            raise ObjectNotFoundError("movement evidence is absent")
        for key, stored in ((source_key, source), (destination_key, destination)):
            if stored is not None and (
                len(stored.body) != content_length
                or sha256(stored.body).hexdigest() != checksum_sha256
            ):
                raise ObjectConflictError(f"object {key!r} conflicts")
        if source is not None and destination is None:
            destination = _Object(
                body=source.body,
                content_type=source.content_type,
                metadata=dict(source.metadata),
            )
            self.objects[destination_key] = destination
        if self.fail_move:
            raise RuntimeError("injected move failure")
        if source is not None:
            self.objects.pop(source_key)
            status = MoveStatus.MOVED
        else:
            status = MoveStatus.ALREADY_MOVED
        assert destination is not None
        return MoveResult(status=status, destination=self._info(destination_key, destination))

    def put_payload(self, key: str, payload: bytes, content_type: str) -> None:
        self.objects[key] = _Object(
            body=payload,
            content_type=content_type,
            metadata={"checksum-sha256": sha256(payload).hexdigest()},
        )

    def put_manifest(self, manifest: Manifest) -> None:
        body = manifest.to_json().encode("utf-8")
        self.objects[_manifest_key(manifest.object_key)] = _Object(
            body=body,
            content_type="application/json",
            metadata={"checksum-sha256": sha256(body).hexdigest()},
        )

    @staticmethod
    def _info(key: str, stored: _Object) -> StoredObject:
        return StoredObject(
            key=key,
            content_length=len(stored.body),
            checksum_sha256=sha256(stored.body).hexdigest(),
            content_type=stored.content_type,
            metadata=dict(stored.metadata),
        )


def _manifest_key(payload_key: str) -> str:
    return payload_key.replace("raw/", "manifests/", 1) + ".manifest.json"


def _fixture(scenario: str = "valid") -> bytes:
    return (FIXTURE_ROOT / f"{scenario}.csv").read_bytes()


def _delivery(
    payload: bytes, *, name: str = "supplier-prices-2026-09-01.csv"
) -> DeliveryDescriptor:
    return DeliveryDescriptor.create(
        source_name="supplier-prices",
        source_date=SOURCE_DATE,
        original_file_name=name,
        actual_sha256=sha256(payload).hexdigest(),
    )


def _terminal_manifest(
    delivery: DeliveryDescriptor,
    payload: bytes,
    *,
    status: ManifestStatus,
    original_name: str | None = None,
) -> Manifest:
    return Manifest(
        source_name=delivery.source_name,
        source_type=delivery.identity.source_type,
        source_date=delivery.source_date,
        batch_id=delivery.batch_id,
        run_id=ORIGINAL_RUN_ID,
        object_key=delivery.raw_key,
        original_file_name=original_name or delivery.original_file_name,
        content_type=delivery.content_type,
        content_length=len(payload),
        checksum_sha256=sha256(payload).hexdigest(),
        schema_version=1,
        extracted_at=EXTRACTED_AT,
        status=status,
        attempt=1,
    )


def _ingest(
    storage: RecordingStorage,
    delivery: DeliveryDescriptor,
    *,
    validator: Callable[..., FileValidationResult] = validate_file,
) -> FileIngestionResult:
    ticks = iter((10.0, 12.5))
    return ingest_file_delivery(
        storage=storage,
        config=CONFIG,
        delivery=delivery,
        monotonic=lambda: next(ticks),
        utc_now=lambda: EXTRACTED_AT,
        run_id_factory=lambda: RUN_ID,
        validator=validator,
    )


def test_new_valid_delivery_preserves_raw_before_validation_and_manifests_last() -> None:
    payload = _fixture()
    delivery = _delivery(payload)
    storage = RecordingStorage()
    storage.put_payload(delivery.landing_key, payload, delivery.content_type)

    def recording_validator(*args: object, **kwargs: object) -> FileValidationResult:
        storage.calls.append("validate")
        return validate_file(*args, **kwargs)  # type: ignore[arg-type]

    result = _ingest(storage, delivery, validator=recording_validator)

    assert storage.calls.index(f"write:{delivery.raw_key}") < storage.calls.index("validate")
    assert storage.calls.index("validate") < storage.calls.index(
        f"move:{delivery.landing_key}->{delivery.archive_key}"
    )
    assert storage.calls[-1] == f"write:{delivery.manifest_key}"
    assert delivery.landing_key not in storage.objects
    assert storage.objects[delivery.raw_key].body == payload
    assert storage.objects[delivery.archive_key].body == payload
    manifest = Manifest.from_json(storage.objects[delivery.manifest_key].body.decode())
    assert manifest.status is ManifestStatus.SUCCEEDED
    assert result.manifest == manifest
    assert result.event.outcome is FileOutcome.ACCEPTED
    assert result.event.duplicate_class is DuplicateClass.NEW_DELIVERY
    assert result.summary.bytes_written == len(payload)
    assert result.summary.records_accepted == 15
    assert result.summary.duration_seconds == 2.5


def test_rejected_delivery_moves_exact_bytes_before_terminal_manifest() -> None:
    payload = _fixture("empty")
    delivery = _delivery(payload)
    storage = RecordingStorage()
    storage.put_payload(delivery.landing_key, payload, delivery.content_type)

    result = _ingest(storage, delivery)

    rejected_key = delivery.rejected_key(RejectionCode.EMPTY_FILE)
    assert storage.objects[delivery.raw_key].body == payload
    assert storage.objects[rejected_key].body == payload
    assert delivery.landing_key not in storage.objects
    assert storage.calls.index(
        f"move:{delivery.landing_key}->{rejected_key}"
    ) < storage.calls.index(f"write:{delivery.manifest_key}")
    assert result.event.outcome is FileOutcome.REJECTED
    assert result.event.rejection_code is RejectionCode.EMPTY_FILE
    assert result.summary.records_read == result.summary.records_rejected == 0
    assert result.manifest.status is ManifestStatus.REJECTED


@pytest.mark.parametrize(
    "initial_state",
    [
        "landing_only",
        "landing_raw",
        "landing_destination",
        "raw_destination",
        "raw_only",
        "both_after_delete_failure",
    ],
)
def test_retryable_recovery_states_converge_to_raw_destination_and_manifest(
    initial_state: str,
) -> None:
    payload = _fixture()
    delivery = _delivery(payload)
    storage = RecordingStorage()
    if initial_state in {
        "landing_only",
        "landing_raw",
        "landing_destination",
        "both_after_delete_failure",
    }:
        storage.put_payload(delivery.landing_key, payload, delivery.content_type)
    if initial_state in {"landing_raw", "raw_destination", "raw_only"}:
        storage.put_payload(delivery.raw_key, payload, delivery.content_type)
    if initial_state in {"landing_destination", "raw_destination", "both_after_delete_failure"}:
        storage.put_payload(delivery.archive_key, payload, delivery.content_type)

    result = _ingest(storage, delivery)

    assert delivery.landing_key not in storage.objects
    assert storage.objects[delivery.raw_key].body == payload
    assert storage.objects[delivery.archive_key].body == payload
    assert result.manifest.status is ManifestStatus.SUCCEEDED
    raw_was_present = initial_state in {"landing_raw", "raw_destination", "raw_only"}
    assert result.summary.bytes_written == (0 if raw_was_present else len(payload))
    if initial_state == "raw_only":
        assert f"write:{delivery.landing_key}" in storage.calls
    if initial_state == "raw_destination":
        assert f"write:{delivery.landing_key}" not in storage.calls


def test_complete_pair_redelivery_revalidates_and_preserves_original_manifest() -> None:
    payload = _fixture()
    original = _delivery(payload, name="supplier-prices-original-2026-09-01.csv")
    renamed = _delivery(payload, name="supplier-prices-renamed-2026-09-01.csv")
    storage = RecordingStorage()
    storage.put_payload(renamed.landing_key, payload, renamed.content_type)
    storage.put_payload(renamed.raw_key, payload, renamed.content_type)
    storage.put_payload(renamed.archive_key, payload, renamed.content_type)
    manifest = _terminal_manifest(
        original,
        payload,
        status=ManifestStatus.SUCCEEDED,
        original_name=original.original_file_name,
    )
    storage.put_manifest(manifest)
    original_manifest_bytes = storage.objects[renamed.manifest_key].body

    result = _ingest(storage, renamed)

    assert result.event.duplicate_class is DuplicateClass.RENAMED_CONTENT
    assert result.summary.duplicate_count == 1
    assert result.summary.bytes_written == 0
    assert result.manifest.run_id == ORIGINAL_RUN_ID
    assert storage.objects[renamed.manifest_key].body == original_manifest_bytes
    assert storage.calls.count(f"write:{renamed.manifest_key}") == 0


def test_complete_manifest_status_must_match_revalidation() -> None:
    payload = _fixture()
    delivery = _delivery(payload)
    storage = RecordingStorage()
    storage.put_payload(delivery.landing_key, payload, delivery.content_type)
    storage.put_payload(delivery.raw_key, payload, delivery.content_type)
    storage.put_payload(delivery.archive_key, payload, delivery.content_type)
    storage.put_manifest(_terminal_manifest(delivery, payload, status=ManifestStatus.REJECTED))

    with pytest.raises(FileIngestionStateError, match="status"):
        _ingest(storage, delivery)


def test_alternative_disposition_conflict_preserves_all_evidence() -> None:
    payload = _fixture()
    delivery = _delivery(payload)
    storage = RecordingStorage()
    alternative = delivery.rejected_key(RejectionCode.EMPTY_FILE)
    storage.put_payload(delivery.landing_key, payload, delivery.content_type)
    storage.put_payload(alternative, payload, delivery.content_type)

    with pytest.raises(FileIngestionStateError, match="alternative disposition"):
        _ingest(storage, delivery)

    assert delivery.landing_key in storage.objects
    assert delivery.raw_key in storage.objects
    assert alternative in storage.objects
    assert delivery.manifest_key not in storage.objects


def test_manifest_only_and_missing_evidence_fail_without_cleanup() -> None:
    payload = _fixture()
    delivery = _delivery(payload)

    manifest_only = RecordingStorage()
    manifest_only.put_payload(delivery.landing_key, payload, delivery.content_type)
    manifest_only.put_manifest(
        _terminal_manifest(delivery, payload, status=ManifestStatus.SUCCEEDED)
    )
    with pytest.raises(FileIngestionStateError, match="manifest-only"):
        _ingest(manifest_only, delivery)
    assert delivery.landing_key in manifest_only.objects

    missing = RecordingStorage()
    with pytest.raises(ObjectNotFoundError, match="landing.*raw"):
        _ingest(missing, delivery)
    assert missing.objects == {}


def test_raw_write_failure_leaves_landing_without_manifest() -> None:
    payload = _fixture()
    delivery = _delivery(payload)
    storage = RecordingStorage()
    storage.put_payload(delivery.landing_key, payload, delivery.content_type)
    storage.fail_raw_write = True

    with pytest.raises(RuntimeError, match="raw write"):
        _ingest(storage, delivery)

    assert delivery.landing_key in storage.objects
    assert delivery.raw_key not in storage.objects
    assert delivery.manifest_key not in storage.objects


def test_failure_after_disposition_before_manifest_is_restart_safe() -> None:
    payload = _fixture()
    delivery = _delivery(payload)
    storage = RecordingStorage()
    storage.put_payload(delivery.landing_key, payload, delivery.content_type)
    storage.fail_manifest_write = True

    with pytest.raises(RuntimeError, match="manifest write"):
        _ingest(storage, delivery)

    assert delivery.landing_key not in storage.objects
    assert storage.objects[delivery.raw_key].body == payload
    assert storage.objects[delivery.archive_key].body == payload
    assert delivery.manifest_key not in storage.objects

    storage.fail_manifest_write = False
    resumed = _ingest(storage, delivery)
    assert resumed.manifest.status is ManifestStatus.SUCCEEDED
    assert delivery.landing_key not in storage.objects


def test_move_failure_after_copy_leaves_retryable_raw_and_both_disposition_objects() -> None:
    payload = _fixture()
    delivery = _delivery(payload)
    storage = RecordingStorage()
    storage.put_payload(delivery.landing_key, payload, delivery.content_type)
    storage.fail_move = True

    with pytest.raises(RuntimeError, match="move failure"):
        _ingest(storage, delivery)

    assert storage.objects[delivery.landing_key].body == payload
    assert storage.objects[delivery.raw_key].body == payload
    assert storage.objects[delivery.archive_key].body == payload
    assert delivery.manifest_key not in storage.objects

    storage.fail_move = False
    resumed = _ingest(storage, delivery)
    assert resumed.manifest.status is ManifestStatus.SUCCEEDED
    assert delivery.landing_key not in storage.objects
