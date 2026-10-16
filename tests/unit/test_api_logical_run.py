"""Exact-storage reconciliation for ADR 0003 REST completion manifests."""

from __future__ import annotations

import io
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from hashlib import sha256
from typing import BinaryIO

import pytest

from batch_ingestion.api.logical_manifest import (
    API_LOGICAL_RUN_MANIFEST_MAX_BYTES,
    ApiLogicalRunManifest,
    ApiLogicalRunPage,
    api_logical_run_manifest_key,
)
from batch_ingestion.api.logical_run import (
    LogicalRunReconciliationState,
    inspect_logical_run,
    observe_page_evidence,
    publish_logical_run,
    reconcile_page_evidence,
)
from batch_ingestion.api.sources import api_page_identity, get_source_descriptor, page_payload_name
from batch_ingestion.identifiers import (
    BatchIdentity,
    derive_batch_id,
    manifest_key_for,
    raw_object_key,
)
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.storage import (
    DownloadedObject,
    ImmutableWriteResult,
    ImmutableWriteStatus,
    ObjectConflictError,
    ObjectNotFoundError,
    ObjectSizeLimitError,
    StoredObject,
)

_SOURCE_DATE = date(2026, 9, 1)
_COMPLETED_AT = datetime(2026, 9, 1, 12, 30, 45, 123456, tzinfo=UTC)
_RUN_ID = "run_1234567812344abc9234567890abcdef"
_PAGE_RUN_ID = "run_00000000000000000000000000000001"
_RAW = b'{"items":[{"currency":"EUR"}],"page":1}'


@dataclass(frozen=True)
class _Object:
    body: bytes
    content_type: str = "application/json"
    declared_length: int | None = None
    metadata: Mapping[str, str] | None = None


class _TrackingStream(io.BytesIO):
    pass


class _MemoryStorage:
    def __init__(self) -> None:
        self.objects: dict[str, _Object] = {}
        self.download_calls: list[tuple[str, int, int, str | None, int | None]] = []
        self.closed_streams: list[_TrackingStream] = []
        self.write_actions: list[str] = []
        self.write_calls: list[tuple[str, bytes, int]] = []

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
        self.download_calls.append(
            (
                key,
                maximum_bytes,
                spool_threshold_bytes,
                expected_checksum_sha256,
                expected_content_length,
            )
        )
        stored = self.objects.get(key)
        if stored is None:
            raise ObjectNotFoundError(f"object {key!r} does not exist")
        declared = len(stored.body) if stored.declared_length is None else stored.declared_length
        if declared > maximum_bytes:
            raise ObjectSizeLimitError(f"object {key!r} exceeds the maximum download size")
        stream = _TrackingStream()
        try:
            for start in range(0, len(stored.body), 3):
                chunk = stored.body[start : start + 3]
                if stream.tell() + len(chunk) > maximum_bytes:
                    raise ObjectSizeLimitError(f"object {key!r} exceeds the maximum download size")
                stream.write(chunk)
            if declared != len(stored.body):
                raise ObjectConflictError(f"object {key!r} has inconsistent length")
            checksum = sha256(stored.body).hexdigest()
            if expected_checksum_sha256 is not None and checksum != expected_checksum_sha256:
                raise ObjectConflictError(f"object {key!r} does not match expected bytes")
            if expected_content_length is not None and len(stored.body) != expected_content_length:
                raise ObjectConflictError(f"object {key!r} does not match expected bytes")
            stream.seek(0)
            yield DownloadedObject(
                info=StoredObject(
                    key=key,
                    content_length=len(stored.body),
                    checksum_sha256=checksum,
                    content_type=stored.content_type,
                    metadata=dict(stored.metadata or {}),
                ),
                stream=stream,
            )
        finally:
            stream.close()
            self.closed_streams.append(stream)

    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        start = stream.tell()
        body = stream.read()
        self.write_calls.append((key, body, start))
        assert len(body) == content_length
        assert sha256(body).hexdigest() == checksum_sha256
        action = self.write_actions.pop(0) if self.write_actions else "success"
        if action == "raise_absent":
            raise RuntimeError("uncertain write")
        if action in {"raise_after", "different_then_raise"}:
            persisted = body if action == "raise_after" else b"different bytes"
            self.objects[key] = _Object(persisted, content_type)
            raise RuntimeError("uncertain write")
        existing = self.objects.get(key)
        if existing is not None and existing.body != body:
            raise ObjectConflictError(f"object {key!r} conflicts")
        status = (
            ImmutableWriteStatus.UNCHANGED if existing is not None else ImmutableWriteStatus.CREATED
        )
        self.objects[key] = _Object(body, content_type)
        return ImmutableWriteResult(
            status=status,
            object=StoredObject(
                key=key,
                content_length=len(body),
                checksum_sha256=sha256(body).hexdigest(),
                content_type=content_type,
                metadata={},
            ),
        )


def _identity(source_name: str = "fx-rates") -> BatchIdentity:
    return api_page_identity(
        get_source_descriptor(source_name),
        _SOURCE_DATE,
        page_size=25,
    )


def _page_manifest(
    identity: BatchIdentity,
    raw_key: str,
    raw: bytes,
    *,
    status: ManifestStatus = ManifestStatus.SUCCEEDED,
    run_id: str = _PAGE_RUN_ID,
) -> Manifest:
    return Manifest(
        source_name=identity.source_name,
        source_type=identity.source_type,
        source_date=identity.source_date,
        batch_id=derive_batch_id(identity),
        run_id=run_id,
        object_key=raw_key,
        original_file_name=None,
        content_type="application/json",
        content_length=len(raw),
        checksum_sha256=sha256(raw).hexdigest(),
        schema_version=1,
        extracted_at=_COMPLETED_AT,
        status=status,
        attempt=1,
    )


def _put_page_pair(
    storage: _MemoryStorage,
    *,
    identity: BatchIdentity | None = None,
    raw: bytes = _RAW,
    status: ManifestStatus = ManifestStatus.SUCCEEDED,
    run_id: str = _PAGE_RUN_ID,
) -> tuple[str, bytes]:
    identity = identity or _identity()
    raw_key = raw_object_key(identity, page_payload_name(1, max_pages=9_999))
    manifest = _page_manifest(identity, raw_key, raw, status=status, run_id=run_id)
    manifest_bytes = manifest.to_json().encode()
    storage.objects[raw_key] = _Object(raw, metadata={"etag": "not-a-sha256"})
    storage.objects[manifest_key_for(raw_key)] = _Object(manifest_bytes)
    return raw_key, manifest_bytes


def _aggregate(
    storage: _MemoryStorage,
    *,
    identity: BatchIdentity | None = None,
) -> ApiLogicalRunManifest:
    identity = identity or _identity()
    raw_key = raw_object_key(identity, page_payload_name(1, max_pages=9_999))
    raw = storage.objects[raw_key].body
    page_manifest_key = manifest_key_for(raw_key)
    page_manifest = storage.objects[page_manifest_key].body
    return ApiLogicalRunManifest(
        manifest_type="api_logical_run_manifest",
        schema_version=1,
        source_name=identity.source_name,
        source_type=identity.source_type,
        source_date=identity.source_date,
        batch_id=derive_batch_id(identity),
        identity_parameters=dict(identity.parameters),
        completion_run_id=_RUN_ID,
        completed_at=_COMPLETED_AT,
        status="succeeded",
        page_count=1,
        record_count=1,
        pages=(
            ApiLogicalRunPage(
                page_number=1,
                object_key=raw_key,
                page_manifest_key=page_manifest_key,
                payload_content_length=len(raw),
                payload_checksum_sha256=sha256(raw).hexdigest(),
                page_manifest_checksum_sha256=sha256(page_manifest).hexdigest(),
                record_count=1,
            ),
        ),
    )


def _put_complete(
    storage: _MemoryStorage,
    *,
    identity: BatchIdentity | None = None,
) -> ApiLogicalRunManifest:
    identity = identity or _identity()
    _put_page_pair(storage, identity=identity)
    aggregate = _aggregate(storage, identity=identity)
    storage.objects[api_logical_run_manifest_key(identity)] = _Object(aggregate.to_bytes())
    return aggregate


def test_absent_reader_uses_only_the_exact_derived_key_and_eight_mib_bound() -> None:
    storage = _MemoryStorage()
    identity = _identity()

    result = inspect_logical_run(storage, identity)

    assert result.state is LogicalRunReconciliationState.ABSENT
    assert storage.download_calls == [
        (
            api_logical_run_manifest_key(identity),
            API_LOGICAL_RUN_MANIFEST_MAX_BYTES,
            64 * 1024,
            None,
            None,
        )
    ]


@pytest.mark.parametrize("oversize_kind", ["declared", "actual"])
def test_aggregate_reader_rejects_declared_and_actual_oversize_and_closes_spool(
    oversize_kind: str,
) -> None:
    storage = _MemoryStorage()
    identity = _identity()
    key = api_logical_run_manifest_key(identity)
    body = b"{}"
    if oversize_kind == "declared":
        storage.objects[key] = _Object(body, declared_length=API_LOGICAL_RUN_MANIFEST_MAX_BYTES + 1)
    else:
        storage.objects[key] = _Object(b"x" * (API_LOGICAL_RUN_MANIFEST_MAX_BYTES + 1))

    result = inspect_logical_run(storage, identity)

    assert result.state is LogicalRunReconciliationState.CORRUPT
    assert all(stream.closed for stream in storage.closed_streams)


def test_complete_reader_streams_exact_raw_and_page_manifest_bytes_without_etag() -> None:
    storage = _MemoryStorage()
    identity = _identity()
    aggregate = _put_complete(storage, identity=identity)

    result = inspect_logical_run(storage, identity)

    assert result.state is LogicalRunReconciliationState.COMPLETE
    assert result.aggregate is not None
    assert result.aggregate.manifest == aggregate
    assert result.aggregate.body == aggregate.to_bytes()
    assert result.aggregate.checksum_sha256 == sha256(aggregate.to_bytes()).hexdigest()
    assert [call[0] for call in storage.download_calls] == [
        api_logical_run_manifest_key(identity),
        aggregate.pages[0].object_key,
        aggregate.pages[0].page_manifest_key,
    ]
    assert result.pages == aggregate.pages
    assert all(stream.closed for stream in storage.closed_streams)


def test_page_evidence_hashes_exact_manifest_bytes_and_allows_mixed_run_ids() -> None:
    storage = _MemoryStorage()
    identity = _identity()
    raw_key, manifest_bytes = _put_page_pair(
        storage,
        identity=identity,
        run_id="run_ffffffffffffffffffffffffffffffff",
    )

    observed = observe_page_evidence(
        storage,
        identity=identity,
        page_number=1,
        record_count=1,
        maximum_payload_bytes=1024,
    )

    assert observed.state is LogicalRunReconciliationState.ELIGIBLE
    assert observed.page is not None
    assert observed.page.object_key == raw_key
    assert observed.page.payload_checksum_sha256 == sha256(_RAW).hexdigest()
    assert observed.page.page_manifest_checksum_sha256 == sha256(manifest_bytes).hexdigest()


@pytest.mark.parametrize(
    ("setup", "expected"),
    [
        ("both_missing", LogicalRunReconciliationState.INCOMPLETE),
        ("payload_only", LogicalRunReconciliationState.INCOMPLETE),
        ("failed", LogicalRunReconciliationState.FAILED),
        ("manifest_only", LogicalRunReconciliationState.CONFLICTING),
        ("malformed_manifest", LogicalRunReconciliationState.CONFLICTING),
        ("noncanonical_manifest", LogicalRunReconciliationState.CONFLICTING),
        ("duplicate_manifest", LogicalRunReconciliationState.CONFLICTING),
        ("raw_mismatch", LogicalRunReconciliationState.CONFLICTING),
    ],
)
def test_page_evidence_reconciliation_states(
    setup: str,
    expected: LogicalRunReconciliationState,
) -> None:
    storage = _MemoryStorage()
    identity = _identity()
    raw_key = raw_object_key(identity, page_payload_name(1, max_pages=9_999))
    if setup == "payload_only":
        storage.objects[raw_key] = _Object(_RAW)
    elif setup == "failed":
        _put_page_pair(storage, identity=identity, status=ManifestStatus.FAILED)
    elif setup == "manifest_only":
        manifest = _page_manifest(identity, raw_key, _RAW)
        storage.objects[manifest_key_for(raw_key)] = _Object(manifest.to_json().encode())
    elif setup == "malformed_manifest":
        storage.objects[raw_key] = _Object(_RAW)
        storage.objects[manifest_key_for(raw_key)] = _Object(b'{"not":"a manifest"}')
    elif setup in {"noncanonical_manifest", "duplicate_manifest"}:
        storage.objects[raw_key] = _Object(_RAW)
        manifest_bytes = _page_manifest(identity, raw_key, _RAW).to_json().encode()
        if setup == "noncanonical_manifest":
            manifest_bytes += b"\n"
        else:
            manifest_bytes = manifest_bytes.replace(
                b'"attempt":1',
                b'"attempt":1,"attempt":1',
            )
        storage.objects[manifest_key_for(raw_key)] = _Object(manifest_bytes)
    elif setup == "raw_mismatch":
        storage.objects[raw_key] = _Object(_RAW)
        manifest = _page_manifest(identity, raw_key, b"different")
        storage.objects[manifest_key_for(raw_key)] = _Object(manifest.to_json().encode())

    observed = observe_page_evidence(
        storage,
        identity=identity,
        page_number=1,
        record_count=1,
        maximum_payload_bytes=1024,
    )

    assert observed.state is expected


def test_page_set_classifies_incomplete_failed_eligible_and_conflicting() -> None:
    storage = _MemoryStorage()
    identity = _identity()
    _put_page_pair(storage, identity=identity)
    eligible_page = observe_page_evidence(
        storage,
        identity=identity,
        page_number=1,
        record_count=1,
        maximum_payload_bytes=1024,
    )
    eligible = reconcile_page_evidence(
        identity=identity,
        observations=(eligible_page,),
        completion_run_id=_RUN_ID,
        completed_at=_COMPLETED_AT,
    )

    assert eligible.state is LogicalRunReconciliationState.ELIGIBLE
    assert eligible.aggregate is not None
    assert eligible.aggregate.manifest.pages == (eligible_page.page,)

    for state in (
        LogicalRunReconciliationState.INCOMPLETE,
        LogicalRunReconciliationState.FAILED,
        LogicalRunReconciliationState.CONFLICTING,
    ):
        changed = replace(eligible_page, state=state, page=None)
        result = reconcile_page_evidence(
            identity=identity,
            observations=(changed,),
            completion_run_id=_RUN_ID,
            completed_at=_COMPLETED_AT,
        )
        assert result.state is state
        assert result.aggregate is None


def test_complete_page_reconciliation_preserves_first_publication_facts() -> None:
    storage = _MemoryStorage()
    identity = _identity()
    _put_complete(storage, identity=identity)
    existing = inspect_logical_run(storage, identity)
    assert existing.aggregate is not None
    observed_page = observe_page_evidence(
        storage,
        identity=identity,
        page_number=1,
        record_count=1,
        maximum_payload_bytes=1024,
    )

    result = reconcile_page_evidence(
        identity=identity,
        observations=(observed_page,),
        completion_run_id="run_22222222222222222222222222222222",
        completed_at=datetime(2026, 9, 2, tzinfo=UTC),
        existing_aggregate=existing.aggregate,
    )

    assert result.state is LogicalRunReconciliationState.COMPLETE
    assert result.aggregate is not None
    assert result.aggregate is existing.aggregate
    assert result.aggregate.manifest.completion_run_id == _RUN_ID
    assert result.aggregate.manifest.completed_at == _COMPLETED_AT


@pytest.mark.parametrize("kind", ["malformed", "noncanonical", "wrong_count"])
def test_present_invalid_aggregate_is_classified_without_reading_page_evidence(kind: str) -> None:
    storage = _MemoryStorage()
    identity = _identity()
    _put_page_pair(storage, identity=identity)
    aggregate = _aggregate(storage, identity=identity)
    body = aggregate.to_bytes()
    if kind == "malformed":
        body = b"not json"
    elif kind == "noncanonical":
        body += b"\n"
    else:
        body = body.replace(b'"page_count":1', b'"page_count":2')
    storage.objects[api_logical_run_manifest_key(identity)] = _Object(body)

    result = inspect_logical_run(storage, identity)

    expected = (
        LogicalRunReconciliationState.CONFLICTING
        if kind == "wrong_count"
        else LogicalRunReconciliationState.CORRUPT
    )
    assert result.state is expected
    assert len(storage.download_calls) == 1
    assert storage.write_calls == []


def test_wrong_identity_raw_mismatch_page_manifest_mismatch_and_orphan_are_distinct() -> None:
    identity = _identity()

    wrong_identity = _MemoryStorage()
    shipping_identity = _identity("shipping-statuses")
    _put_complete(wrong_identity, identity=shipping_identity)
    wrong_identity.objects[api_logical_run_manifest_key(identity)] = wrong_identity.objects.pop(
        api_logical_run_manifest_key(shipping_identity)
    )
    wrong_identity_result = inspect_logical_run(wrong_identity, identity)
    assert wrong_identity_result.state is LogicalRunReconciliationState.CONFLICTING

    raw_mismatch = _MemoryStorage()
    aggregate = _put_complete(raw_mismatch, identity=identity)
    raw_mismatch.objects[aggregate.pages[0].object_key] = _Object(b"changed")
    raw_mismatch_result = inspect_logical_run(raw_mismatch, identity)
    assert raw_mismatch_result.state is LogicalRunReconciliationState.CONFLICTING

    manifest_mismatch = _MemoryStorage()
    aggregate = _put_complete(manifest_mismatch, identity=identity)
    manifest_mismatch.objects[aggregate.pages[0].page_manifest_key] = _Object(b"changed")
    manifest_mismatch_result = inspect_logical_run(manifest_mismatch, identity)
    assert manifest_mismatch_result.state is LogicalRunReconciliationState.CONFLICTING

    orphaned = _MemoryStorage()
    aggregate = _put_complete(orphaned, identity=identity)
    del orphaned.objects[aggregate.pages[0].object_key]
    assert inspect_logical_run(orphaned, identity).state is LogicalRunReconciliationState.ORPHANED


def test_publication_requires_all_page_evidence_before_the_first_write() -> None:
    source = _MemoryStorage()
    identity = _identity()
    _put_page_pair(source, identity=identity)
    aggregate = _aggregate(source, identity=identity)
    empty_storage = _MemoryStorage()

    result = publish_logical_run(empty_storage, identity=identity, manifest=aggregate)

    assert result.state is LogicalRunReconciliationState.INCOMPLETE
    assert empty_storage.write_calls == []
    assert api_logical_run_manifest_key(identity) not in empty_storage.objects


@pytest.mark.parametrize("mode", ["created", "unchanged", "write_then_raise"])
def test_publication_creates_reuses_or_recovers_the_same_frozen_bytes(mode: str) -> None:
    storage = _MemoryStorage()
    identity = _identity()
    _put_page_pair(storage, identity=identity)
    aggregate = _aggregate(storage, identity=identity)
    key = api_logical_run_manifest_key(identity)
    if mode == "unchanged":
        storage.objects[key] = _Object(aggregate.to_bytes())
    elif mode == "write_then_raise":
        storage.write_actions = ["raise_after"]

    result = publish_logical_run(storage, identity=identity, manifest=aggregate)

    assert result.state is LogicalRunReconciliationState.COMPLETE
    assert storage.objects[key].body == aggregate.to_bytes()
    assert len(storage.write_calls) == 1
    assert storage.write_calls[0][2] == 0


def test_publication_retries_absence_once_with_identical_rewound_frozen_bytes() -> None:
    storage = _MemoryStorage()
    identity = _identity()
    _put_page_pair(storage, identity=identity)
    aggregate = _aggregate(storage, identity=identity)
    storage.write_actions = ["raise_absent", "success"]

    result = publish_logical_run(storage, identity=identity, manifest=aggregate)

    assert result.state is LogicalRunReconciliationState.COMPLETE
    assert len(storage.write_calls) == 2
    assert storage.write_calls[0][1] == storage.write_calls[1][1] == aggregate.to_bytes()
    assert [call[2] for call in storage.write_calls] == [0, 0]


def test_publication_bounds_final_uncertainty_and_rejects_different_present_bytes() -> None:
    storage = _MemoryStorage()
    identity = _identity()
    _put_page_pair(storage, identity=identity)
    aggregate = _aggregate(storage, identity=identity)
    storage.write_actions = ["raise_absent", "raise_absent"]

    uncertain = publish_logical_run(storage, identity=identity, manifest=aggregate)

    assert uncertain.state is LogicalRunReconciliationState.UNCERTAIN_WRITE
    assert len(storage.write_calls) == 2

    conflicting = _MemoryStorage()
    _put_page_pair(conflicting, identity=identity)
    conflicting.write_actions = ["different_then_raise"]
    result = publish_logical_run(conflicting, identity=identity, manifest=aggregate)

    assert result.state is LogicalRunReconciliationState.CONFLICTING
    assert len(conflicting.write_calls) == 1
    assert conflicting.objects[api_logical_run_manifest_key(identity)].body == b"different bytes"
