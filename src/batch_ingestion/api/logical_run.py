"""Exact-key evidence and reconciliation for API logical-run completion."""

from __future__ import annotations

import io
import json
from abc import abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from typing import BinaryIO, Protocol, cast

from batch_ingestion.identifiers import (
    BatchIdentity,
    derive_batch_id,
    manifest_key_for,
    raw_object_key,
)
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.storage import (
    DEFAULT_MAX_MANIFEST_BYTES,
    DownloadedObject,
    ImmutableWriteResult,
    ObjectNotFoundError,
    StorageBackendError,
    StorageError,
    StoredObject,
)

from .logical_manifest import (
    API_LOGICAL_RUN_MANIFEST_CONTENT_TYPE,
    API_LOGICAL_RUN_MANIFEST_MAX_BYTES,
    API_LOGICAL_RUN_MANIFEST_MAX_PAGES,
    API_LOGICAL_RUN_MANIFEST_SCHEMA_VERSION,
    API_LOGICAL_RUN_MANIFEST_STATUS,
    API_LOGICAL_RUN_MANIFEST_TYPE,
    ApiLogicalRunManifest,
    ApiLogicalRunManifestConflictError,
    ApiLogicalRunPage,
    api_logical_run_manifest_key,
)
from .sources import page_payload_name


class LogicalRunReconciliationState(StrEnum):
    """Stable ADR 0003 aggregate and page-evidence classifications."""

    ABSENT = "absent"
    INCOMPLETE = "incomplete"
    FAILED = "failed"
    ELIGIBLE = "eligible"
    COMPLETE = "complete"
    CORRUPT = "corrupt"
    CONFLICTING = "conflicting"
    ORPHANED = "orphaned"
    UNCERTAIN_WRITE = "uncertain_write"


class LogicalRunStorage(Protocol):
    """Minimal standard-S3 storage seam used by logical-run reconciliation."""

    @abstractmethod
    def download_spooled(
        self,
        key: str,
        *,
        maximum_bytes: int,
        spool_threshold_bytes: int,
        expected_checksum_sha256: str | None = None,
        expected_content_length: int | None = None,
    ) -> AbstractContextManager[DownloadedObject]:
        raise RuntimeError("protocol method has no implementation")

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
        raise RuntimeError("protocol method has no implementation")


@dataclass(frozen=True, slots=True)
class VerifiedLogicalRunManifest:
    """Canonical aggregate bytes plus their streamed exact-key observation."""

    key: str
    manifest: ApiLogicalRunManifest
    body: bytes
    content_length: int
    checksum_sha256: str


@dataclass(frozen=True, slots=True)
class PageEvidenceObservation:
    """One exact page-pair observation for post-traversal reconciliation."""

    page_number: int
    state: LogicalRunReconciliationState
    page: ApiLogicalRunPage | None = None


@dataclass(frozen=True, slots=True)
class LogicalRunReconciliation:
    """One non-mutating or publication reconciliation outcome."""

    state: LogicalRunReconciliationState
    aggregate: VerifiedLogicalRunManifest | None = None
    pages: tuple[ApiLogicalRunPage, ...] = ()
    cause: Exception | None = field(default=None, compare=False, repr=False)


@dataclass(frozen=True, slots=True)
class _ExactObject:
    info: StoredObject
    body: bytes


def inspect_logical_run(
    storage: LogicalRunStorage,
    identity: BatchIdentity,
) -> LogicalRunReconciliation:
    """Inspect one exact aggregate key and verify every exact referenced object."""
    key = api_logical_run_manifest_key(identity)
    try:
        aggregate_object = _download_exact(
            storage,
            key,
            maximum_bytes=API_LOGICAL_RUN_MANIFEST_MAX_BYTES,
            spool_threshold_bytes=DEFAULT_MAX_MANIFEST_BYTES,
        )
    except ObjectNotFoundError:
        return LogicalRunReconciliation(LogicalRunReconciliationState.ABSENT)
    except StorageBackendError:
        raise
    except StorageError as error:
        return LogicalRunReconciliation(
            LogicalRunReconciliationState.CORRUPT,
            cause=error,
        )

    try:
        manifest = ApiLogicalRunManifest.from_bytes(aggregate_object.body)
    except ApiLogicalRunManifestConflictError:
        return LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)
    except (TypeError, ValueError):
        return LogicalRunReconciliation(LogicalRunReconciliationState.CORRUPT)

    if (
        aggregate_object.info.content_type != API_LOGICAL_RUN_MANIFEST_CONTENT_TYPE
        or not _manifest_matches_identity(manifest, identity)
    ):
        return LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)

    for page in manifest.pages:
        page_state = _verify_aggregate_page(storage, identity=identity, page=page)
        if page_state is not None:
            return LogicalRunReconciliation(page_state)

    verified = VerifiedLogicalRunManifest(
        key=key,
        manifest=manifest,
        body=aggregate_object.body,
        content_length=aggregate_object.info.content_length,
        checksum_sha256=aggregate_object.info.checksum_sha256,
    )
    return LogicalRunReconciliation(
        LogicalRunReconciliationState.COMPLETE,
        aggregate=verified,
        pages=manifest.pages,
    )


def observe_page_evidence(
    storage: LogicalRunStorage,
    *,
    identity: BatchIdentity,
    page_number: int,
    record_count: int,
    maximum_payload_bytes: int,
) -> PageEvidenceObservation:
    """Collect one aggregate page entry from actual raw and manifest bytes."""
    if isinstance(maximum_payload_bytes, bool) or maximum_payload_bytes <= 0:
        raise ValueError("maximum_payload_bytes must be a positive integer")
    raw_key = raw_object_key(
        identity,
        page_payload_name(page_number, max_pages=API_LOGICAL_RUN_MANIFEST_MAX_PAGES),
    )
    page_manifest_key = manifest_key_for(raw_key)

    raw: _ExactObject | None
    page_manifest_object: _ExactObject | None
    try:
        raw = _download_exact(
            storage,
            raw_key,
            maximum_bytes=maximum_payload_bytes,
            spool_threshold_bytes=min(
                maximum_payload_bytes,
                DEFAULT_MAX_MANIFEST_BYTES,
            ),
            read_body=False,
        )
    except ObjectNotFoundError:
        raw = None
    except StorageBackendError:
        raise
    except StorageError:
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.CONFLICTING,
        )

    try:
        page_manifest_object = _download_exact(
            storage,
            page_manifest_key,
            maximum_bytes=DEFAULT_MAX_MANIFEST_BYTES,
            spool_threshold_bytes=DEFAULT_MAX_MANIFEST_BYTES,
        )
    except ObjectNotFoundError:
        page_manifest_object = None
    except StorageBackendError:
        raise
    except StorageError:
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.CONFLICTING,
        )

    if raw is None and page_manifest_object is None:
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.INCOMPLETE,
        )
    if raw is None:
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.CONFLICTING,
        )
    if page_manifest_object is None:
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.INCOMPLETE,
        )

    try:
        page_manifest = _parse_page_manifest(page_manifest_object.body)
    except (TypeError, ValueError):
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.CONFLICTING,
        )
    if not _page_manifest_matches(
        page_manifest,
        identity=identity,
        raw=raw.info,
        expected_raw_key=raw_key,
    ):
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.CONFLICTING,
        )
    if page_manifest.status is ManifestStatus.FAILED:
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.FAILED,
        )
    if page_manifest.status is not ManifestStatus.SUCCEEDED:
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.CONFLICTING,
        )

    try:
        page = ApiLogicalRunPage(
            page_number=page_number,
            object_key=raw_key,
            page_manifest_key=page_manifest_key,
            payload_content_length=raw.info.content_length,
            payload_checksum_sha256=raw.info.checksum_sha256,
            page_manifest_checksum_sha256=page_manifest_object.info.checksum_sha256,
            record_count=record_count,
        )
    except (TypeError, ValueError):
        return PageEvidenceObservation(
            page_number,
            LogicalRunReconciliationState.CONFLICTING,
        )
    return PageEvidenceObservation(
        page_number,
        LogicalRunReconciliationState.ELIGIBLE,
        page,
    )


def reconcile_page_evidence(
    *,
    identity: BatchIdentity,
    observations: Sequence[PageEvidenceObservation],
    completion_run_id: str,
    completed_at: datetime,
    existing_aggregate: VerifiedLogicalRunManifest | None = None,
) -> LogicalRunReconciliation:
    """Classify a traversed page set and freeze one eligible aggregate."""
    states = {observation.state for observation in observations}
    for state in (
        LogicalRunReconciliationState.CONFLICTING,
        LogicalRunReconciliationState.FAILED,
        LogicalRunReconciliationState.INCOMPLETE,
    ):
        if state in states:
            return LogicalRunReconciliation(state)
    if not observations or states != {LogicalRunReconciliationState.ELIGIBLE}:
        return LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)
    if any(
        observation.page is None or observation.page.page_number != observation.page_number
        for observation in observations
    ):
        return LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)
    pages = tuple(cast(ApiLogicalRunPage, observation.page) for observation in observations)
    try:
        manifest = ApiLogicalRunManifest(
            manifest_type=API_LOGICAL_RUN_MANIFEST_TYPE,
            schema_version=API_LOGICAL_RUN_MANIFEST_SCHEMA_VERSION,
            source_name=identity.source_name,
            source_type=identity.source_type,
            source_date=identity.source_date,
            batch_id=derive_batch_id(identity),
            identity_parameters=dict(identity.parameters),
            completion_run_id=completion_run_id,
            completed_at=completed_at,
            status=API_LOGICAL_RUN_MANIFEST_STATUS,
            page_count=len(pages),
            record_count=sum(page.record_count for page in pages),
            pages=pages,
        )
        frozen = _freeze_manifest(identity, manifest)
    except (TypeError, ValueError):
        return LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)
    if existing_aggregate is not None:
        existing_manifest = existing_aggregate.manifest
        if (
            not _manifest_matches_identity(existing_manifest, identity)
            or existing_manifest.pages != pages
            or existing_manifest.page_count != manifest.page_count
            or existing_manifest.record_count != manifest.record_count
        ):
            return LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)
        return LogicalRunReconciliation(
            LogicalRunReconciliationState.COMPLETE,
            aggregate=existing_aggregate,
            pages=pages,
        )
    return LogicalRunReconciliation(
        LogicalRunReconciliationState.ELIGIBLE,
        aggregate=frozen,
        pages=pages,
    )


def publish_logical_run(
    storage: LogicalRunStorage,
    *,
    identity: BatchIdentity,
    manifest: ApiLogicalRunManifest,
) -> LogicalRunReconciliation:
    """Publish frozen bytes with at most two writes and exact uncertain recovery."""
    try:
        frozen = _freeze_manifest(identity, manifest)
    except (TypeError, ValueError):
        return LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)

    for page in manifest.pages:
        page_state = _verify_aggregate_page(storage, identity=identity, page=page)
        if page_state is LogicalRunReconciliationState.ORPHANED:
            return LogicalRunReconciliation(LogicalRunReconciliationState.INCOMPLETE)
        if page_state is not None:
            return LogicalRunReconciliation(page_state)

    for attempt in range(2):
        stream = io.BytesIO(frozen.body)
        try:
            storage.write_immutable(
                frozen.key,
                stream,
                content_length=frozen.content_length,
                checksum_sha256=frozen.checksum_sha256,
                content_type=API_LOGICAL_RUN_MANIFEST_CONTENT_TYPE,
            )
        except Exception as error:
            observed = _inspect_uncertain_write(
                storage,
                identity=identity,
                frozen=frozen,
                write_error=error,
            )
            if observed.state is LogicalRunReconciliationState.ABSENT:
                if attempt == 0:
                    continue
                return LogicalRunReconciliation(
                    LogicalRunReconciliationState.UNCERTAIN_WRITE,
                    cause=observed.cause,
                )
            return observed

        observed = inspect_logical_run(storage, identity)
        if (
            observed.state is LogicalRunReconciliationState.COMPLETE
            and observed.aggregate is not None
            and observed.aggregate.body == frozen.body
        ):
            return observed
        if observed.state is LogicalRunReconciliationState.ABSENT:
            return LogicalRunReconciliation(LogicalRunReconciliationState.UNCERTAIN_WRITE)
        return observed

    return LogicalRunReconciliation(LogicalRunReconciliationState.UNCERTAIN_WRITE)


def _inspect_uncertain_write(
    storage: LogicalRunStorage,
    *,
    identity: BatchIdentity,
    frozen: VerifiedLogicalRunManifest,
    write_error: Exception,
) -> LogicalRunReconciliation:
    try:
        observed_object = _download_exact(
            storage,
            frozen.key,
            maximum_bytes=API_LOGICAL_RUN_MANIFEST_MAX_BYTES,
            spool_threshold_bytes=DEFAULT_MAX_MANIFEST_BYTES,
        )
    except ObjectNotFoundError:
        return LogicalRunReconciliation(
            LogicalRunReconciliationState.ABSENT,
            cause=write_error,
        )
    except StorageError as read_error:
        return LogicalRunReconciliation(
            LogicalRunReconciliationState.CONFLICTING,
            cause=read_error,
        )
    except Exception as read_error:
        return LogicalRunReconciliation(
            LogicalRunReconciliationState.UNCERTAIN_WRITE,
            cause=read_error,
        )
    if observed_object.body != frozen.body:
        return LogicalRunReconciliation(
            LogicalRunReconciliationState.CONFLICTING,
            cause=write_error,
        )
    return inspect_logical_run(storage, identity)


def _verify_aggregate_page(
    storage: LogicalRunStorage,
    *,
    identity: BatchIdentity,
    page: ApiLogicalRunPage,
) -> LogicalRunReconciliationState | None:
    try:
        raw = _download_exact(
            storage,
            page.object_key,
            maximum_bytes=max(1, page.payload_content_length),
            spool_threshold_bytes=min(
                max(1, page.payload_content_length),
                DEFAULT_MAX_MANIFEST_BYTES,
            ),
            expected_checksum_sha256=page.payload_checksum_sha256,
            expected_content_length=page.payload_content_length,
            read_body=False,
        )
        page_manifest_object = _download_exact(
            storage,
            page.page_manifest_key,
            maximum_bytes=DEFAULT_MAX_MANIFEST_BYTES,
            spool_threshold_bytes=DEFAULT_MAX_MANIFEST_BYTES,
            expected_checksum_sha256=page.page_manifest_checksum_sha256,
        )
    except ObjectNotFoundError:
        return LogicalRunReconciliationState.ORPHANED
    except StorageBackendError:
        raise
    except StorageError:
        return LogicalRunReconciliationState.CONFLICTING

    try:
        page_manifest = _parse_page_manifest(page_manifest_object.body)
    except (TypeError, ValueError):
        return LogicalRunReconciliationState.CONFLICTING
    if not _page_manifest_matches(
        page_manifest,
        identity=identity,
        raw=raw.info,
        expected_raw_key=page.object_key,
    ):
        return LogicalRunReconciliationState.CONFLICTING
    if page_manifest.status is not ManifestStatus.SUCCEEDED:
        return LogicalRunReconciliationState.CONFLICTING
    return None


def _download_exact(
    storage: LogicalRunStorage,
    key: str,
    *,
    maximum_bytes: int,
    spool_threshold_bytes: int,
    expected_checksum_sha256: str | None = None,
    expected_content_length: int | None = None,
    read_body: bool = True,
) -> _ExactObject:
    with storage.download_spooled(
        key,
        maximum_bytes=maximum_bytes,
        spool_threshold_bytes=spool_threshold_bytes,
        expected_checksum_sha256=expected_checksum_sha256,
        expected_content_length=expected_content_length,
    ) as downloaded:
        body = downloaded.stream.read() if read_body else b""
        return _ExactObject(downloaded.info, body)


def _parse_page_manifest(value: bytes) -> Manifest:
    try:
        decoded = value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("page manifest must be valid UTF-8") from error
    if decoded.startswith("\ufeff"):
        raise ValueError("page manifest must not contain a UTF-8 BOM")
    try:
        parsed = cast(
            object,
            json.loads(decoded, object_pairs_hook=_reject_duplicate_object),
        )
    except json.JSONDecodeError as error:
        raise ValueError("page manifest JSON is invalid") from error
    if not isinstance(parsed, dict):
        raise TypeError("page manifest JSON must contain one object")
    manifest = Manifest.from_dict(cast(Mapping[str, object], parsed))
    if manifest.to_json().encode("utf-8") != value:
        raise ValueError("page manifest JSON is not canonical")
    return manifest


def _reject_duplicate_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"page manifest JSON has duplicate member: {key}")
        value[key] = item
    return value


def _page_manifest_matches(
    manifest: Manifest,
    *,
    identity: BatchIdentity,
    raw: StoredObject,
    expected_raw_key: str,
) -> bool:
    return (
        manifest.source_name == identity.source_name
        and manifest.source_type is identity.source_type
        and manifest.source_date == identity.source_date
        and manifest.batch_id == derive_batch_id(identity)
        and manifest.object_key == expected_raw_key == raw.key
        and manifest.original_file_name is None
        and manifest.content_type == raw.content_type
        and manifest.content_length == raw.content_length
        and manifest.checksum_sha256 == raw.checksum_sha256
        and manifest.schema_version == 1
    )


def _manifest_matches_identity(
    manifest: ApiLogicalRunManifest,
    identity: BatchIdentity,
) -> bool:
    return (
        manifest.source_name == identity.source_name
        and manifest.source_type is identity.source_type
        and manifest.source_date == identity.source_date
        and manifest.batch_id == derive_batch_id(identity)
        and dict(manifest.identity_parameters) == dict(identity.parameters)
        and api_logical_run_manifest_key(identity)
        == api_logical_run_manifest_key(
            BatchIdentity.create(
                manifest.source_name,
                manifest.source_type,
                manifest.source_date,
                manifest.identity_parameters,
            )
        )
    )


def _freeze_manifest(
    identity: BatchIdentity,
    manifest: ApiLogicalRunManifest,
) -> VerifiedLogicalRunManifest:
    if not _manifest_matches_identity(manifest, identity):
        raise ValueError("logical-run manifest does not match the expected identity")
    body = manifest.to_bytes()
    return VerifiedLogicalRunManifest(
        key=api_logical_run_manifest_key(identity),
        manifest=manifest,
        body=body,
        content_length=len(body),
        checksum_sha256=sha256(body).hexdigest(),
    )
