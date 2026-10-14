"""Single-date happy-path and immutable page state-machine contracts."""

import hashlib
import json
from datetime import UTC, date, datetime
from typing import BinaryIO

import httpx
import pytest

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
    ImmutableWriteResult,
    ImmutableWriteStatus,
    ObjectConflictError,
    ObjectVerificationError,
    ReconciliationResult,
    ReconciliationState,
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
        self.writes: dict[str, bytes] = {}

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
        if error := self.reconciliation_errors.get(page):
            raise error
        if state is ReconciliationState.ABSENT:
            return ReconciliationResult(state, None, None)
        body = self.stored_bodies.get(page, _body(page))
        payload = _stored(payload_key, body)
        manifest = self.complete_manifests.get(page)
        return ReconciliationResult(state, payload, manifest)

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
        kind = "manifest" if key.startswith("manifests/") else "payload"
        self.events.append((f"write_{kind}", key))
        self.writes[key] = body
        return ImmutableWriteResult(
            ImmutableWriteStatus.CREATED,
            _stored(key, body, content_type),
        )


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
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
            run_id_factory=lambda: RUN_ID,
        )


def _manifest_writes(storage: RecordingStorage) -> list[Manifest]:
    return [
        Manifest.from_json(body.decode())
        for key, body in storage.writes.items()
        if key.startswith("manifests/")
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
    ]

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
    ]
    assert result.summary.bytes_written == 0
    assert result.summary.duplicate_count == 0
    assert all(manifest.run_id == RUN_ID for manifest in _manifest_writes(storage))


def test_complete_success_reuses_original_manifest_and_writes_nothing() -> None:
    original = {1: _manifest(1), 2: _manifest(2)}
    storage = RecordingStorage(
        {1: ReconciliationState.COMPLETE, 2: ReconciliationState.COMPLETE},
        original,
    )

    result = _ingest(storage)

    assert [event[0] for event in storage.events] == ["reconcile", "reconcile"]
    assert storage.writes == {}
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


def test_service_fetches_only_the_strict_next_page() -> None:
    requested: list[int] = []
    storage = RecordingStorage({1: ReconciliationState.ABSENT, 2: ReconciliationState.ABSENT})

    _ingest(storage, request_pages=requested)

    assert requested == [1, 2]
    assert requested == list(range(1, len(requested) + 1))
