"""Single-date happy-path and immutable page state-machine contracts."""

import hashlib
import io
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import BinaryIO

import httpx
import pytest

from batch_ingestion.api.logical_manifest import (
    ApiLogicalRunManifest,
    api_logical_run_manifest_key,
)
from batch_ingestion.api.service import (
    ApiErrorCode,
    ApiIngestionError,
    ApiIngestionResult,
    ApiRunStatus,
    ingest_api_date,
)
from batch_ingestion.api.sources import get_source_descriptor
from batch_ingestion.config import HttpConfig
from batch_ingestion.identifiers import (
    BatchIdentity,
    SourceType,
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
    ObjectVerificationError,
    ReconciliationResult,
    ReconciliationState,
    StorageBackendError,
    StorageError,
    StoredObject,
)

SOURCE_DATE = date(2026, 9, 1)
RUN_ID = "run_11111111111111111111111111111111"
ORIGINAL_RUN_ID = "run_22222222222222222222222222222222"
CONTENT_TYPE = "application/json; charset=utf-8"


def _config() -> HttpConfig:
    return HttpConfig(
        base_url="https://mock.invalid",
        connect_timeout_seconds=2.0,
        read_timeout_seconds=5.0,
        max_attempts=3,
        backoff_base_seconds=0.25,
        backoff_max_seconds=1.0,
        max_elapsed_seconds=10.0,
        page_size=2,
        max_pages=100,
        max_response_bytes=1024,
        max_backfill_days=31,
    )


def _fx_item(quote: str, rate: str) -> dict[str, object]:
    return {
        "base_currency": "USD",
        "quote_currency": quote,
        "rate": rate,
        "effective_date": SOURCE_DATE.isoformat(),
    }


def _body(page: int) -> bytes:
    items = (
        [_fx_item("AUD", "1.453321"), _fx_item("EUR", "0.854635")]
        if page == 1
        else [_fx_item("GBP", "0.754321")]
    )
    return (
        json.dumps(
            {
                "source": "fx-rates",
                "source_date": SOURCE_DATE.isoformat(),
                "page": page,
                "page_size": 2,
                "total_pages": 2,
                "total_records": 3,
                "next_page": 2 if page == 1 else None,
                "items": items,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode()


def _identity() -> BatchIdentity:
    return BatchIdentity.create(
        "fx-rates",
        SourceType.API,
        SOURCE_DATE,
        {"resource-path": "/v1/fx-rates", "page-size": "2"},
    )


def _manifest(
    page: int,
    *,
    run_id: str = ORIGINAL_RUN_ID,
    status: ManifestStatus = ManifestStatus.SUCCEEDED,
    body: bytes | None = None,
) -> Manifest:
    identity = _identity()
    payload = body if body is not None else _body(page)
    return Manifest(
        source_name=identity.source_name,
        source_type=identity.source_type,
        source_date=identity.source_date,
        batch_id=derive_batch_id(identity),
        run_id=run_id,
        object_key=raw_object_key(identity, f"page-{page:04d}.json"),
        original_file_name=None,
        content_type=CONTENT_TYPE,
        content_length=len(payload),
        checksum_sha256=hashlib.sha256(payload).hexdigest(),
        schema_version=1,
        extracted_at=datetime(2026, 9, 1, 11, tzinfo=UTC),
        status=status,
        attempt=1,
    )


def _stored(key: str, body: bytes, content_type: str = CONTENT_TYPE) -> StoredObject:
    return StoredObject(
        key=key,
        content_length=len(body),
        checksum_sha256=hashlib.sha256(body).hexdigest(),
        content_type=content_type,
        metadata={"checksum-sha256": hashlib.sha256(body).hexdigest()},
    )


class RecordingStorage:
    def __init__(
        self,
        states: dict[int, ReconciliationState],
        complete_manifests: dict[int, Manifest] | None = None,
        stored_bodies: dict[int, bytes] | None = None,
        reconciliation_errors: dict[int, StorageError] | None = None,
    ) -> None:
        self.states = states
        self.complete_manifests = complete_manifests or {}
        self.stored_bodies = stored_bodies or {}
        self.reconciliation_errors = reconciliation_errors or {}
        self.events: list[tuple[str, str]] = []
        self.timeline: list[tuple[str, str]] = []
        self.downloads: list[str] = []
        self.writes: dict[str, bytes] = {}
        self.objects: dict[str, tuple[bytes, str]] = {}
        for page, state in states.items():
            payload_key = raw_object_key(_identity(), f"page-{page:04d}.json")
            body = self.stored_bodies.get(page, _body(page))
            if state in {ReconciliationState.PAYLOAD_ONLY, ReconciliationState.COMPLETE}:
                self.objects[payload_key] = (body, CONTENT_TYPE)
            if state in {ReconciliationState.MANIFEST_ONLY, ReconciliationState.COMPLETE}:
                manifest = self.complete_manifests.get(page, _manifest(page, body=body))
                self.complete_manifests[page] = manifest
                self.objects[manifest_key_for(payload_key)] = (
                    manifest.to_json().encode("utf-8"),
                    "application/json",
                )

    def reconcile_payload_manifest(
        self,
        payload_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> ReconciliationResult:
        page = int(payload_key.rsplit("page-", 1)[1].split(".", 1)[0])
        state = self.states[page]
        self.events.append(("reconcile", payload_key))
        self.timeline.append(("reconcile", payload_key))
        if error := self.reconciliation_errors.get(page):
            raise error
        if state is ReconciliationState.ABSENT:
            return ReconciliationResult(state, None, None)
        body = self.stored_bodies.get(page, _body(page))
        payload = _stored(payload_key, body)
        manifest = self.complete_manifests.get(page)
        return ReconciliationResult(state, payload, manifest)

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
        self.downloads.append(key)
        self.timeline.append(("download", key))
        stored = self.objects.get(key)
        if stored is None:
            raise ObjectNotFoundError(f"object {key!r} does not exist")
        body, content_type = stored
        if len(body) > maximum_bytes:
            raise ObjectSizeLimitError(f"object {key!r} exceeds maximum bytes")
        checksum = hashlib.sha256(body).hexdigest()
        if expected_checksum_sha256 is not None and checksum != expected_checksum_sha256:
            raise ObjectConflictError(f"object {key!r} has an unexpected checksum")
        if expected_content_length is not None and len(body) != expected_content_length:
            raise ObjectConflictError(f"object {key!r} has an unexpected length")
        stream = io.BytesIO(body)
        try:
            yield DownloadedObject(
                info=_stored(key, body, content_type),
                stream=stream,
            )
        finally:
            stream.close()

    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        body = stream.read()
        assert len(body) == content_length
        assert hashlib.sha256(body).hexdigest() == checksum_sha256
        if key.endswith("/logical-run.manifest.json"):
            kind = "aggregate"
        elif key.startswith("manifests/"):
            kind = "manifest"
        else:
            kind = "payload"
        self.events.append((f"write_{kind}", key))
        self.timeline.append((f"write_{kind}", key))
        existing = self.objects.get(key)
        if existing is not None and existing[0] != body:
            raise ObjectConflictError(f"object {key!r} has different bytes")
        status = (
            ImmutableWriteStatus.UNCHANGED if existing is not None else ImmutableWriteStatus.CREATED
        )
        self.writes[key] = body
        self.objects[key] = (body, content_type)
        if "/page-" in key:
            page = int(key.rsplit("page-", 1)[1].split(".", 1)[0])
            if key.startswith("raw/"):
                self.states[page] = ReconciliationState.PAYLOAD_ONLY
            elif key.startswith("manifests/"):
                self.states[page] = ReconciliationState.COMPLETE
                self.complete_manifests[page] = Manifest.from_json(body.decode("utf-8"))
        return ImmutableWriteResult(status, _stored(key, body, content_type))


def _transport(request_pages: list[int]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        request_pages.append(page)
        return httpx.Response(
            200,
            headers={"Content-Type": CONTENT_TYPE},
            stream=httpx.ByteStream(_body(page)),
        )

    return httpx.MockTransport(handler)


def _ingest(
    storage: RecordingStorage,
    *,
    request_pages: list[int] | None = None,
    run_id: str = RUN_ID,
    completed_at: datetime = datetime(2026, 9, 1, 12, tzinfo=UTC),
    event_sink: Callable[[dict[str, object]], None] | None = None,
) -> ApiIngestionResult:
    pages = request_pages if request_pages is not None else []
    with httpx.Client(base_url="https://mock.invalid", transport=_transport(pages)) as client:
        return ingest_api_date(
            client=client,
            storage=storage,
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            monotonic=lambda: 50.0,
            sleep=lambda _: pytest.fail("happy-path ingestion must not sleep"),
            utc_now=lambda: completed_at,
            run_id_factory=lambda: run_id,
            event_sink=event_sink,
        )


def _manifest_writes(storage: RecordingStorage) -> list[Manifest]:
    return [
        Manifest.from_json(body.decode())
        for key, body in storage.writes.items()
        if key.startswith("manifests/") and not key.endswith("/logical-run.manifest.json")
    ]


def test_absent_pages_are_persisted_in_order_with_canonical_manifests_and_metrics() -> None:
    storage = RecordingStorage({1: ReconciliationState.ABSENT, 2: ReconciliationState.ABSENT})
    request_pages: list[int] = []

    result = _ingest(storage, request_pages=request_pages)

    assert request_pages == [1, 2]
    assert result.summary.to_dict() == {
        "records_read": 3,
        "records_accepted": 3,
        "records_rejected": 0,
        "pages_fetched": 2,
        "bytes_written": len(_body(1)) + len(_body(2)),
        "retry_count": 0,
        "duplicate_count": 0,
        "duration_seconds": 0.0,
        "source_date": "2026-09-01",
        "batch_id": derive_batch_id(_identity()),
        "run_id": RUN_ID,
    }
    assert len(result.payload_keys) == 2
    assert result.manifest_keys == tuple(manifest_key_for(key) for key in result.payload_keys)
    assert [event[0] for event in storage.events] == [
        "reconcile",
        "write_payload",
        "write_manifest",
        "reconcile",
        "write_payload",
        "write_manifest",
        "write_aggregate",
    ]
    assert result.logical_run_manifest_key == api_logical_run_manifest_key(_identity())

    manifests = _manifest_writes(storage)
    assert len(manifests) == 2
    assert all(manifest.original_file_name is None for manifest in manifests)
    assert all(manifest.status is ManifestStatus.SUCCEEDED for manifest in manifests)
    assert all(manifest.attempt == 1 for manifest in manifests)
    assert all(manifest.run_id == RUN_ID for manifest in manifests)
    assert all(manifest.content_type == CONTENT_TYPE for manifest in manifests)
    for manifest in manifests:
        serialized = storage.writes[manifest_key_for(manifest.object_key)]
        assert serialized == manifest.to_json().encode("utf-8")


def test_payload_only_recovery_writes_only_the_missing_manifest() -> None:
    storage = RecordingStorage(
        {1: ReconciliationState.PAYLOAD_ONLY, 2: ReconciliationState.PAYLOAD_ONLY}
    )

    result = _ingest(storage)

    assert [event[0] for event in storage.events] == [
        "reconcile",
        "write_manifest",
        "reconcile",
        "write_manifest",
        "write_aggregate",
    ]
    assert result.summary.bytes_written == 0
    assert result.summary.duplicate_count == 0
    assert all(manifest.run_id == RUN_ID for manifest in _manifest_writes(storage))


def test_legacy_complete_pages_publish_only_the_logical_run_manifest() -> None:
    original = {1: _manifest(1), 2: _manifest(2)}
    storage = RecordingStorage(
        {1: ReconciliationState.COMPLETE, 2: ReconciliationState.COMPLETE},
        original,
    )

    result = _ingest(storage)

    assert [event[0] for event in storage.events] == [
        "reconcile",
        "reconcile",
        "write_aggregate",
    ]
    assert set(storage.writes) == {api_logical_run_manifest_key(_identity())}
    assert result.manifests == (original[1], original[2])
    assert all(manifest.run_id == ORIGINAL_RUN_ID for manifest in result.manifests)
    assert result.summary.bytes_written == 0
    assert result.summary.duplicate_count == 2
    assert result.summary.records_read == 3
    assert result.event.status is ApiRunStatus.SUCCEEDED
    assert result.event.error_code is None


@pytest.mark.parametrize(
    "invalid_body",
    [b"\xff", b"not-json", b'{"items":[]}\n'],
    ids=["invalid-utf8", "malformed-json", "invalid-envelope"],
)
def test_invalid_http_200_is_preserved_before_failed_manifest_and_typed_outcome(
    invalid_body: bytes,
) -> None:
    storage = RecordingStorage({1: ReconciliationState.ABSENT})
    emitted: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": CONTENT_TYPE},
            stream=httpx.ByteStream(invalid_body),
        )

    with (
        httpx.Client(
            base_url="https://mock.invalid",
            transport=httpx.MockTransport(handler),
        ) as client,
        pytest.raises(ApiIngestionError) as captured,
    ):
        ingest_api_date(
            client=client,
            storage=storage,
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            monotonic=lambda: 50.0,
            sleep=lambda _: pytest.fail("validation failures must not sleep"),
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
            run_id_factory=lambda: RUN_ID,
            event_sink=emitted.append,
        )

    error = captured.value
    assert error.code is ApiErrorCode.INVALID_PAGE
    assert error.event.status is ApiRunStatus.FAILED
    assert error.event.page == 1
    assert error.summary.pages_fetched == 1
    assert error.summary.bytes_written == len(invalid_body)
    assert [event[0] for event in storage.events] == [
        "reconcile",
        "write_payload",
        "write_manifest",
    ]
    payload_writes = [body for key, body in storage.writes.items() if key.startswith("raw/")]
    assert payload_writes == [invalid_body]
    failed_manifest = _manifest_writes(storage)[0]
    assert failed_manifest.status is ManifestStatus.FAILED
    assert failed_manifest.run_id == RUN_ID
    assert emitted == [error.event.to_dict(), {"event": "run_summary", **error.summary.to_dict()}]


def test_complete_failed_page_is_replayed_as_failure_without_mutation() -> None:
    malformed = b'"unterminated'
    failed = _manifest(1, status=ManifestStatus.FAILED, body=malformed)
    storage = RecordingStorage(
        {1: ReconciliationState.COMPLETE},
        {1: failed},
        stored_bodies={1: malformed},
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": CONTENT_TYPE},
            stream=httpx.ByteStream(malformed),
        )

    with (
        httpx.Client(
            base_url="https://mock.invalid", transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiIngestionError) as captured,
    ):
        ingest_api_date(
            client=client,
            storage=storage,
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            monotonic=lambda: 50.0,
            sleep=lambda _: pytest.fail("failed replay must not sleep"),
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
            run_id_factory=lambda: RUN_ID,
        )

    assert captured.value.code is ApiErrorCode.INVALID_PAGE
    assert captured.value.summary.duplicate_count == 0
    assert storage.writes == {}
    assert [event[0] for event in storage.events] == ["reconcile"]


def test_manifest_only_state_fails_without_writes_or_cleanup() -> None:
    storage = RecordingStorage({1: ReconciliationState.MANIFEST_ONLY})

    with (
        httpx.Client(base_url="https://mock.invalid", transport=_transport([])) as client,
        pytest.raises(ApiIngestionError) as captured,
    ):
        ingest_api_date(
            client=client,
            storage=storage,
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            monotonic=lambda: 50.0,
            sleep=lambda _: pytest.fail("state failures must not sleep"),
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
            run_id_factory=lambda: RUN_ID,
        )

    assert captured.value.code is ApiErrorCode.MANIFEST_ONLY
    assert storage.writes == {}
    assert [event[0] for event in storage.events] == ["reconcile"]


@pytest.mark.parametrize(
    ("storage_error", "expected_code"),
    [
        (ObjectConflictError("injected payload conflict"), ApiErrorCode.STORAGE_CONFLICT),
        (
            ObjectVerificationError("injected malformed manifest"),
            ApiErrorCode.STORAGE_CORRUPTION,
        ),
    ],
)
def test_reconciliation_conflict_and_malformed_manifest_are_typed_without_repair(
    storage_error: StorageError,
    expected_code: ApiErrorCode,
) -> None:
    storage = RecordingStorage(
        {1: ReconciliationState.COMPLETE},
        reconciliation_errors={1: storage_error},
    )

    with (
        httpx.Client(base_url="https://mock.invalid", transport=_transport([])) as client,
        pytest.raises(ApiIngestionError) as captured,
    ):
        ingest_api_date(
            client=client,
            storage=storage,
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            monotonic=lambda: 50.0,
            sleep=lambda _: pytest.fail("storage failures must not sleep"),
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
            run_id_factory=lambda: RUN_ID,
        )

    assert captured.value.code is expected_code
    assert storage.writes == {}


class InterruptingStorage(RecordingStorage):
    def __init__(self) -> None:
        super().__init__({1: ReconciliationState.ABSENT, 2: ReconciliationState.ABSENT})
        self.fail_next_manifest = True

    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        if key.startswith("manifests/") and self.fail_next_manifest:
            self.fail_next_manifest = False
            raise StorageError("injected failure between payload and manifest")
        result = super().write_immutable(
            key,
            stream,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            content_type=content_type,
        )
        if "/page-" in key:
            page = int(key.rsplit("page-", 1)[1].split(".", 1)[0])
            if not key.startswith("manifests/"):
                self.states[page] = ReconciliationState.PAYLOAD_ONLY
        return result


def test_failure_after_raw_write_recovers_without_rewriting_payload() -> None:
    storage = InterruptingStorage()

    with pytest.raises(ApiIngestionError) as captured:
        _ingest(storage)
    assert captured.value.code is ApiErrorCode.STORAGE_ERROR
    assert captured.value.summary.bytes_written == len(_body(1))
    assert [event[0] for event in storage.events] == ["reconcile", "write_payload"]

    first_run_event_count = len(storage.events)
    result = _ingest(storage)

    assert [event[0] for event in storage.events[first_run_event_count:]] == [
        "reconcile",
        "write_manifest",
        "reconcile",
        "write_payload",
        "write_manifest",
        "write_aggregate",
    ]
    assert result.summary.bytes_written == len(_body(2))
    assert result.summary.duplicate_count == 0


def test_http_500_failure_has_summary_and_creates_no_page_object() -> None:
    storage = RecordingStorage({1: ReconciliationState.ABSENT})
    requests = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(500, content=b"error body")

    with (
        httpx.Client(
            base_url="https://mock.invalid", transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiIngestionError) as captured,
    ):
        ingest_api_date(
            client=client,
            storage=storage,
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            monotonic=lambda: 50.0,
            sleep=sleeps.append,
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
            run_id_factory=lambda: RUN_ID,
        )

    assert captured.value.code is ApiErrorCode.HTTP_RETRY_EXHAUSTED
    assert captured.value.summary.retry_count == 2
    assert captured.value.summary.pages_fetched == 0
    assert requests == 3
    assert sleeps == [0.25, 0.5]
    assert storage.events == []
    assert storage.writes == {}
    assert api_logical_run_manifest_key(_identity()) not in storage.objects


def test_aggregate_is_fully_verified_before_terminal_success_output() -> None:
    storage = RecordingStorage({1: ReconciliationState.ABSENT, 2: ReconciliationState.ABSENT})

    def emit(event: dict[str, object]) -> None:
        storage.timeline.append(("emit", str(event["event"])))

    result = _ingest(storage, event_sink=emit)

    aggregate_key = api_logical_run_manifest_key(_identity())
    aggregate_write = storage.timeline.index(("write_aggregate", aggregate_key))
    first_emit = next(index for index, item in enumerate(storage.timeline) if item[0] == "emit")
    aggregate_verification = max(
        index for index, item in enumerate(storage.timeline) if item == ("download", aggregate_key)
    )
    page_manifest_writes = [
        index for index, item in enumerate(storage.timeline) if item[0] == "write_manifest"
    ]

    assert max(page_manifest_writes) < aggregate_write < aggregate_verification < first_emit
    assert storage.timeline[first_emit:] == [
        ("emit", "api_ingestion"),
        ("emit", "run_summary"),
    ]
    assert result.logical_run_manifest_key == aggregate_key
    aggregate = ApiLogicalRunManifest.from_bytes(storage.objects[aggregate_key][0])
    assert aggregate.completion_run_id == RUN_ID
    assert aggregate.completed_at == datetime(2026, 9, 1, 12, tzinfo=UTC)


def test_complete_replay_preserves_aggregate_and_mixed_page_run_bytes() -> None:
    page_manifests = {
        1: _manifest(1, run_id=ORIGINAL_RUN_ID),
        2: _manifest(2, run_id="run_33333333333333333333333333333333"),
    }
    storage = RecordingStorage(
        {1: ReconciliationState.COMPLETE, 2: ReconciliationState.COMPLETE},
        page_manifests,
    )

    first = _ingest(storage)
    aggregate_key = first.logical_run_manifest_key
    before = dict(storage.objects)
    original_aggregate = ApiLogicalRunManifest.from_bytes(before[aggregate_key][0])
    storage.events.clear()
    storage.writes.clear()

    replay = _ingest(
        storage,
        run_id="run_44444444444444444444444444444444",
        completed_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
    )

    assert storage.objects == before
    assert storage.writes == {}
    assert replay.logical_run_manifest_key == aggregate_key
    assert replay.manifests == (page_manifests[1], page_manifests[2])
    assert replay.summary.bytes_written == 0
    assert replay.summary.duplicate_count == 2
    replayed_aggregate = ApiLogicalRunManifest.from_bytes(storage.objects[aggregate_key][0])
    assert replayed_aggregate.completion_run_id == original_aggregate.completion_run_id
    assert replayed_aggregate.completed_at == original_aggregate.completed_at


@pytest.mark.parametrize(
    ("damage", "expected_code"),
    [
        ("corrupt", ApiErrorCode.LOGICAL_RUN_MANIFEST_CORRUPT),
        ("conflicting", ApiErrorCode.LOGICAL_RUN_MANIFEST_CONFLICT),
        ("orphaned", ApiErrorCode.LOGICAL_RUN_MANIFEST_ORPHANED),
    ],
)
def test_invalid_present_aggregate_blocks_fetch_and_mutation(
    damage: str,
    expected_code: ApiErrorCode,
) -> None:
    storage = RecordingStorage({1: ReconciliationState.ABSENT, 2: ReconciliationState.ABSENT})
    first = _ingest(storage)
    aggregate_key = first.logical_run_manifest_key
    first_bytes = storage.objects[aggregate_key][0]
    first_objects = dict(storage.objects)
    if damage == "corrupt":
        storage.objects[aggregate_key] = (b"{not-json", "application/json")
    elif damage == "conflicting":
        raw_key = first.payload_keys[0]
        storage.objects[raw_key] = (b"different", CONTENT_TYPE)
    else:
        del storage.objects[first.payload_keys[0]]
    damaged_objects = dict(storage.objects)
    storage.writes.clear()
    requested: list[int] = []

    with pytest.raises(ApiIngestionError) as captured:
        _ingest(storage, request_pages=requested)

    assert captured.value.code is expected_code
    assert requested == []
    assert storage.writes == {}
    assert storage.objects == damaged_objects
    assert first_objects[aggregate_key][0] == first_bytes


class FailingAggregateStorage(RecordingStorage):
    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        if key.endswith("/logical-run.manifest.json"):
            raise RuntimeError("injected uncertain aggregate write")
        return super().write_immutable(
            key,
            stream,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            content_type=content_type,
        )


def test_aggregate_publication_failure_retains_complete_pages_without_success() -> None:
    storage = FailingAggregateStorage(
        {1: ReconciliationState.ABSENT, 2: ReconciliationState.ABSENT}
    )
    emitted: list[dict[str, object]] = []

    with pytest.raises(ApiIngestionError) as captured:
        _ingest(storage, event_sink=emitted.append)

    aggregate_key = api_logical_run_manifest_key(_identity())
    assert captured.value.code is ApiErrorCode.LOGICAL_RUN_MANIFEST_WRITE_UNCERTAIN
    assert aggregate_key not in storage.objects
    assert all(
        key in storage.objects
        for page in (1, 2)
        for key in (
            raw_object_key(_identity(), f"page-{page:04d}.json"),
            manifest_key_for(raw_object_key(_identity(), f"page-{page:04d}.json")),
        )
    )
    assert emitted == [
        captured.value.event.to_dict(),
        {"event": "run_summary", **captured.value.summary.to_dict()},
    ]


class BackendFailingStorage(RecordingStorage):
    def __init__(self, failure_point: str) -> None:
        super().__init__({1: ReconciliationState.ABSENT, 2: ReconciliationState.ABSENT})
        self.failure_point = failure_point
        self.backend_error = _storage_backend_error()
        self.aggregate_downloads = 0

    def _fail(self, point: str) -> None:
        if self.failure_point == point:
            raise self.backend_error

    def reconcile_payload_manifest(
        self,
        payload_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> ReconciliationResult:
        self._fail("page_reconciliation")
        return super().reconcile_payload_manifest(
            payload_key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )

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
        if key.endswith("/logical-run.manifest.json"):
            self.aggregate_downloads += 1
            if self.aggregate_downloads == 1:
                self._fail("aggregate_inspection")
            if self.aggregate_downloads == 3:
                self._fail("final_verification")
        elif key.startswith("raw/"):
            self._fail("page_read")
        with super().download_spooled(
            key,
            maximum_bytes=maximum_bytes,
            spool_threshold_bytes=spool_threshold_bytes,
            expected_checksum_sha256=expected_checksum_sha256,
            expected_content_length=expected_content_length,
        ) as downloaded:
            yield downloaded

    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        if key.endswith("/logical-run.manifest.json"):
            self._fail("aggregate_write")
        elif key.startswith("manifests/"):
            self._fail("page_manifest_write")
        else:
            self._fail("raw_write")
        return super().write_immutable(
            key,
            stream,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            content_type=content_type,
        )


def _storage_backend_error() -> StorageBackendError:
    try:
        raise RuntimeError("backend endpoint and credential sentinel")
    except RuntimeError as cause:
        try:
            raise StorageBackendError("storage backend operation failed") from cause
        except StorageBackendError as error:
            return error


@pytest.mark.parametrize(
    ("failure_point", "records_read", "pages_fetched", "bytes_written", "evidence"),
    [
        ("session_setup", 0, 0, 0, "none"),
        ("aggregate_inspection", 0, 0, 0, "none"),
        ("page_reconciliation", 0, 1, 0, "none"),
        ("raw_write", 0, 1, 0, "none"),
        ("page_manifest_write", 0, 1, len(_body(1)), "raw_one"),
        ("page_read", 2, 1, len(_body(1)), "page_one"),
        ("aggregate_write", 3, 2, len(_body(1)) + len(_body(2)), "pages"),
        ("final_verification", 3, 2, len(_body(1)) + len(_body(2)), "complete"),
        ("session_close", 3, 2, len(_body(1)) + len(_body(2)), "complete"),
    ],
)
def test_backend_storage_failures_emit_one_terminal_pair_with_confirmed_metrics(
    failure_point: str,
    records_read: int,
    pages_fetched: int,
    bytes_written: int,
    evidence: str,
) -> None:
    storage = BackendFailingStorage(failure_point)
    emitted: list[dict[str, object]] = []

    @contextmanager
    def open_storage() -> Iterator[RecordingStorage]:
        if failure_point == "session_setup":
            raise storage.backend_error
        yield storage
        if failure_point == "session_close":
            raise storage.backend_error

    with (
        httpx.Client(base_url="https://mock.invalid", transport=_transport([])) as client,
        pytest.raises(ApiIngestionError) as captured,
    ):
        ingest_api_date(
            client=client,
            storage_session_provider=open_storage,
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            monotonic=lambda: 50.0,
            sleep=lambda _: pytest.fail("storage failures must not add application retries"),
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
            run_id_factory=lambda: RUN_ID,
            event_sink=emitted.append,
        )

    failure = captured.value
    assert failure.code is ApiErrorCode.STORAGE_ERROR
    backend_cause = failure.__cause__
    assert isinstance(backend_cause, StorageBackendError)
    assert backend_cause is storage.backend_error
    assert backend_cause.__cause__ is not None
    assert failure.event.status is ApiRunStatus.FAILED
    assert failure.event.error_code is ApiErrorCode.STORAGE_ERROR
    assert failure.event.message == "storage operation failed"
    assert failure.event.source_date == failure.summary.source_date == SOURCE_DATE
    assert failure.event.batch_id == failure.summary.batch_id == derive_batch_id(_identity())
    assert failure.event.run_id == failure.summary.run_id == RUN_ID
    assert failure.summary.records_read == records_read
    assert failure.summary.records_accepted == records_read
    assert failure.summary.records_rejected == 0
    assert failure.summary.pages_fetched == pages_fetched
    assert failure.summary.bytes_written == bytes_written
    assert failure.summary.retry_count == 0
    assert failure.summary.duplicate_count == 0
    assert failure.summary.duration_seconds == 0.0
    assert emitted == [
        failure.event.to_dict(),
        {"event": "run_summary", **failure.summary.to_dict()},
    ]
    assert all(item.get("status") != "succeeded" for item in emitted)
    serialized = json.dumps(emitted, sort_keys=True)
    assert "backend endpoint" not in serialized
    assert "credential sentinel" not in serialized

    raw_keys = {raw_object_key(_identity(), f"page-{page:04d}.json") for page in (1, 2)}
    page_manifest_keys = {manifest_key_for(key) for key in raw_keys}
    aggregate_key = api_logical_run_manifest_key(_identity())
    expected_keys = {
        "none": set(),
        "raw_one": {raw_object_key(_identity(), "page-0001.json")},
        "page_one": {
            raw_object_key(_identity(), "page-0001.json"),
            manifest_key_for(raw_object_key(_identity(), "page-0001.json")),
        },
        "pages": raw_keys | page_manifest_keys,
        "complete": raw_keys | page_manifest_keys | {aggregate_key},
    }
    assert set(storage.objects) == expected_keys[evidence]


def test_service_fetches_only_the_strict_next_page() -> None:
    requested: list[int] = []
    storage = RecordingStorage({1: ReconciliationState.ABSENT, 2: ReconciliationState.ABSENT})

    _ingest(storage, request_pages=requested)

    assert requested == [1, 2]
    assert requested == list(range(1, len(requested) + 1))
