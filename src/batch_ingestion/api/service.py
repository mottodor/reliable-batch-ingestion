"""Restart-safe single-date REST ingestion over immutable shared storage."""

from __future__ import annotations

import hashlib
import io
import json
import time
from abc import abstractmethod
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import BinaryIO, Protocol

import httpx

from batch_ingestion.config import HttpConfig
from batch_ingestion.identifiers import (
    BatchIdentity,
    derive_batch_id,
    manifest_key_for,
    new_run_id,
    raw_object_key,
)
from batch_ingestion.manifests import MANIFEST_SCHEMA_VERSION, Manifest, ManifestStatus
from batch_ingestion.storage import (
    DownloadedObject,
    ImmutableWriteResult,
    ImmutableWriteStatus,
    ObjectConflictError,
    ObjectVerificationError,
    ReconciliationResult,
    ReconciliationState,
    StorageBackendError,
    StorageError,
)
from batch_ingestion.summary import RunSummary, TerminalEmitter

from .fetcher import (
    HttpPageFetchError,
    HttpProtocolError,
    HttpRetryExhausted,
    HttpStatusError,
    ResponseSizeLimitError,
    RetryEvent,
    fetch_page,
)
from .logical_run import (
    LogicalRunReconciliation,
    LogicalRunReconciliationState,
    PageEvidenceObservation,
    inspect_logical_run,
    observe_page_evidence,
    publish_logical_run,
    reconcile_page_evidence,
)
from .retry import RetryBudgetExhausted, RetryClassification
from .sources import (
    ApiPage,
    ApiSourceDescriptor,
    PageValidationError,
    api_page_identity,
    page_payload_name,
    validate_page_envelope,
)


class ApiErrorCode(StrEnum):
    """Stable handled-failure codes exposed above the ingestion boundary."""

    HTTP_PERMANENT_CLIENT_ERROR = "http_permanent_client_error"
    HTTP_PERMANENT_PROTOCOL_ERROR = "http_permanent_protocol_error"
    HTTP_RETRY_EXHAUSTED = "http_retry_exhausted"
    HTTP_RESPONSE_PROTOCOL_ERROR = "http_response_protocol_error"
    HTTP_RESPONSE_TOO_LARGE = "http_response_too_large"
    INVALID_PAGE = "invalid_page"
    MANIFEST_ONLY = "manifest_only"
    COMPLETE_STATUS_CONFLICT = "complete_status_conflict"
    STORAGE_CONFLICT = "storage_conflict"
    STORAGE_CORRUPTION = "storage_corruption"
    STORAGE_ERROR = "storage_error"
    LOGICAL_RUN_MANIFEST_CORRUPT = "logical_run_manifest_corrupt"
    LOGICAL_RUN_MANIFEST_CONFLICT = "logical_run_manifest_conflict"
    LOGICAL_RUN_MANIFEST_ORPHANED = "logical_run_manifest_orphaned"
    LOGICAL_RUN_MANIFEST_WRITE_UNCERTAIN = "logical_run_manifest_write_uncertain"


class ApiRunStatus(StrEnum):
    """Physical single-date invocation status."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


_FAILURE_MESSAGES: dict[ApiErrorCode, str] = {
    ApiErrorCode.HTTP_PERMANENT_CLIENT_ERROR: "API request failed permanently",
    ApiErrorCode.HTTP_PERMANENT_PROTOCOL_ERROR: "API returned an unsupported HTTP status",
    ApiErrorCode.HTTP_RETRY_EXHAUSTED: "API retry budget was exhausted",
    ApiErrorCode.HTTP_RESPONSE_PROTOCOL_ERROR: "HTTP 200 metadata violated the API contract",
    ApiErrorCode.HTTP_RESPONSE_TOO_LARGE: "HTTP 200 body exceeded the configured size bound",
    ApiErrorCode.INVALID_PAGE: "preserved HTTP 200 body failed page validation",
    ApiErrorCode.MANIFEST_ONLY: "manifest exists without its required raw payload",
    ApiErrorCode.COMPLETE_STATUS_CONFLICT: (
        "stored terminal manifest conflicts with fetched content"
    ),
    ApiErrorCode.STORAGE_CONFLICT: "stored object conflicts with deterministic page evidence",
    ApiErrorCode.STORAGE_CORRUPTION: "stored page evidence failed verification",
    ApiErrorCode.STORAGE_ERROR: "storage operation failed",
    ApiErrorCode.LOGICAL_RUN_MANIFEST_CORRUPT: (
        "logical-run manifest is malformed, noncanonical, oversized, or unsupported"
    ),
    ApiErrorCode.LOGICAL_RUN_MANIFEST_CONFLICT: (
        "logical-run manifest conflicts with exact page evidence"
    ),
    ApiErrorCode.LOGICAL_RUN_MANIFEST_ORPHANED: (
        "logical-run manifest references missing page evidence"
    ),
    ApiErrorCode.LOGICAL_RUN_MANIFEST_WRITE_UNCERTAIN: (
        "logical-run manifest publication outcome remains uncertain"
    ),
}


@dataclass(frozen=True, slots=True)
class ApiRunEvent:
    """Secret-free structured success or failure event for one physical run."""

    status: ApiRunStatus
    source_date: date
    batch_id: str
    run_id: str
    page: int | None
    error_code: ApiErrorCode | None
    http_status: int | None
    classification: RetryClassification | None
    message: str

    def to_dict(self) -> dict[str, object]:
        """Return a stable JSON-compatible event with explicit nullable fields."""
        return {
            "event": "api_ingestion",
            "status": self.status.value,
            "source_date": self.source_date.isoformat(),
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "page": self.page,
            "error_code": self.error_code.value if self.error_code is not None else None,
            "http_status": self.http_status,
            "classification": (
                self.classification.value if self.classification is not None else None
            ),
            "message": self.message,
        }


class ApiIngestionError(RuntimeError):
    """Handled single-date failure carrying its final event and run summary."""

    def __init__(
        self,
        *,
        code: ApiErrorCode,
        summary: RunSummary,
        event: ApiRunEvent,
    ) -> None:
        self.code = code
        self.summary = summary
        self.event = event
        super().__init__(event.message)


class ApiPageStateError(RuntimeError):
    """Raised when an observed payload/manifest state cannot safely advance."""

    def __init__(self, code: ApiErrorCode, message: str) -> None:
        self.code = code
        super().__init__(message)


class ApiStorage(Protocol):
    """Narrow shared-storage behavior consumed by API ingestion."""

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
        raise AssertionError("protocol method has no runtime implementation")

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


@dataclass(frozen=True, slots=True)
class ApiIngestionResult:
    """Successful single-date result with exact touched deterministic keys."""

    summary: RunSummary
    event: ApiRunEvent
    payload_keys: tuple[str, ...]
    manifest_keys: tuple[str, ...]
    logical_run_manifest_key: str
    manifests: tuple[Manifest, ...]


@dataclass(frozen=True, slots=True)
class ApiBackfillResult:
    """Successful inclusive backfill composed only of single-date results."""

    from_date: date
    to_date: date
    results: tuple[ApiIngestionResult, ...]


@dataclass(slots=True)
class _RunMetrics:
    records_read: int = 0
    pages_fetched: int = 0
    bytes_written: int = 0
    retry_count: int = 0
    duplicate_count: int = 0


def validate_backfill_range(*, from_date: date, to_date: date, max_days: int) -> int:
    """Validate one inclusive range and return its number of calendar dates."""
    if isinstance(from_date, datetime) or not isinstance(from_date, date):
        raise TypeError("from_date must be a date")
    if isinstance(to_date, datetime) or not isinstance(to_date, date):
        raise TypeError("to_date must be a date")
    if isinstance(max_days, bool) or not isinstance(max_days, int) or max_days <= 0:
        raise ValueError("max_days must be a positive integer")
    if from_date > to_date:
        raise ValueError("from date must not be after to date")
    day_count = (to_date - from_date).days + 1
    if day_count > max_days:
        raise ValueError(f"backfill range exceeds configured maximum of {max_days} days")
    return day_count


def ingest_api_date(
    *,
    client: httpx.Client,
    http_config: HttpConfig,
    descriptor: ApiSourceDescriptor,
    source_date: date,
    storage: ApiStorage | None = None,
    storage_session_provider: (Callable[[], AbstractContextManager[ApiStorage]] | None) = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    utc_now: Callable[[], datetime] | None = None,
    run_id_factory: Callable[[], str] = new_run_id,
    query_parameter_provider: Callable[[int], Mapping[str, str]] | None = None,
    event_sink: Callable[[dict[str, object]], None] | None = None,
) -> ApiIngestionResult:
    """Own one storage session and one terminal pair for a logical API attempt."""
    started_at = monotonic()
    identity = api_page_identity(descriptor, source_date, page_size=http_config.page_size)
    batch_id = derive_batch_id(identity)
    run_id = run_id_factory()
    terminal = TerminalEmitter(event_sink)
    result: ApiIngestionResult | None = None

    def relay_nonterminal(event: dict[str, object]) -> None:
        if event.get("event") in {"api_ingestion", "run_summary"}:
            return
        if event_sink is not None:
            event_sink(event)

    try:
        with _api_storage_context(
            storage=storage,
            storage_session_provider=storage_session_provider,
        ) as active_storage:
            result = _ingest_api_date_with_storage(
                client=client,
                storage=active_storage,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                monotonic=monotonic,
                sleep=sleep,
                utc_now=utc_now,
                run_id_factory=lambda: run_id,
                query_parameter_provider=query_parameter_provider,
                event_sink=relay_nonterminal,
                attempt_started_at=started_at,
            )
    except ApiIngestionError as error:
        terminal.emit(error.event.to_dict(), error.summary)
        raise
    except StorageError as error:
        if result is None:
            summary = _run_summary(
                _RunMetrics(),
                started_at=started_at,
                monotonic=monotonic,
                source_date=source_date,
                batch_id=batch_id,
                run_id=run_id,
            )
            page: int | None = 1
        else:
            duration = monotonic() - started_at
            if duration < 0:
                raise RuntimeError("monotonic clock moved backwards") from error
            summary = replace(result.summary, duration_seconds=duration)
            page = None
        event = ApiRunEvent(
            status=ApiRunStatus.FAILED,
            source_date=source_date,
            batch_id=batch_id,
            run_id=run_id,
            page=page,
            error_code=ApiErrorCode.STORAGE_ERROR,
            http_status=None,
            classification=None,
            message=_FAILURE_MESSAGES[ApiErrorCode.STORAGE_ERROR],
        )
        terminal.emit(event.to_dict(), summary)
        raise ApiIngestionError(
            code=ApiErrorCode.STORAGE_ERROR,
            summary=summary,
            event=event,
        ) from error

    if result is None:
        raise AssertionError("successful API ingestion has no result")
    terminal.emit(result.event.to_dict(), result.summary)
    return result


def _api_storage_context(
    *,
    storage: ApiStorage | None,
    storage_session_provider: Callable[[], AbstractContextManager[ApiStorage]] | None,
) -> AbstractContextManager[ApiStorage]:
    if (storage is None) == (storage_session_provider is None):
        raise ValueError("exactly one API storage boundary must be supplied")
    if storage_session_provider is not None:
        return storage_session_provider()
    assert storage is not None
    return nullcontext(storage)


def _ingest_api_date_with_storage(
    *,
    client: httpx.Client,
    storage: ApiStorage,
    http_config: HttpConfig,
    descriptor: ApiSourceDescriptor,
    source_date: date,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
    utc_now: Callable[[], datetime] | None,
    run_id_factory: Callable[[], str],
    query_parameter_provider: Callable[[int], Mapping[str, str]] | None,
    event_sink: Callable[[dict[str, object]], None] | None,
    attempt_started_at: float,
) -> ApiIngestionResult:
    """Run one API attempt against an already acquired storage boundary."""
    wall_clock = utc_now or _utc_now
    started_at = attempt_started_at
    identity = api_page_identity(descriptor, source_date, page_size=http_config.page_size)
    batch_id = derive_batch_id(identity)
    run_id = run_id_factory()
    metrics = _RunMetrics()

    page_number = 1
    expected_total_pages: int | None = None
    expected_total_records: int | None = None
    payload_keys: list[str] = []
    manifests: list[Manifest] = []
    page_evidence: list[PageEvidenceObservation] = []
    verified_logical_run_key: str | None = None

    def observe_retry(retry: RetryEvent) -> None:
        if event_sink is None:
            return
        event_sink(
            {
                "event": "api_retry",
                "source_date": source_date.isoformat(),
                "batch_id": batch_id,
                "run_id": run_id,
                "page": page_number,
                "attempt": retry.attempt,
                "classification": retry.classification.value,
                "http_status": retry.status_code,
                "delay_seconds": retry.delay_seconds,
            }
        )

    try:
        logical_run_preflight = inspect_logical_run(storage, identity)
        if logical_run_preflight.state not in {
            LogicalRunReconciliationState.ABSENT,
            LogicalRunReconciliationState.COMPLETE,
        }:
            raise _logical_run_state_error(logical_run_preflight)

        while True:
            extra_parameters = (
                query_parameter_provider(page_number)
                if query_parameter_provider is not None
                else None
            )
            with fetch_page(
                client=client,
                config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                page=page_number,
                monotonic=monotonic,
                sleep=sleep,
                utc_now=wall_clock,
                extra_query_parameters=extra_parameters,
                retry_observer=observe_retry,
            ) as fetched:
                metrics.pages_fetched += 1
                metrics.retry_count += fetched.retry_count
                payload_key = raw_object_key(
                    identity,
                    page_payload_name(page_number, max_pages=http_config.max_pages),
                )
                reconciliation = storage.reconcile_payload_manifest(
                    payload_key,
                    content_length=fetched.content_length,
                    checksum_sha256=fetched.checksum_sha256,
                )
                _validate_observed_payload(reconciliation, fetched.content_type)

                if reconciliation.state is ReconciliationState.ABSENT:
                    payload_write = storage.write_immutable(
                        payload_key,
                        fetched.stream,
                        content_length=fetched.content_length,
                        checksum_sha256=fetched.checksum_sha256,
                        content_type=fetched.content_type,
                    )
                    if payload_write.status is not ImmutableWriteStatus.CREATED:
                        raise ApiPageStateError(
                            ApiErrorCode.STORAGE_CONFLICT,
                            "absent payload was not created under single-writer mode",
                        )
                    metrics.bytes_written += fetched.content_length

                fetched.stream.seek(0)
                body = fetched.stream.read()
                try:
                    parsed_page = _decode_and_validate_page(
                        body,
                        descriptor=descriptor,
                        source_date=source_date,
                        requested_page=page_number,
                        page_size=http_config.page_size,
                        max_pages=http_config.max_pages,
                        expected_total_pages=expected_total_pages,
                        expected_total_records=expected_total_records,
                    )
                except PageValidationError:
                    if reconciliation.state is ReconciliationState.COMPLETE:
                        _require_complete_terminal(
                            reconciliation,
                            payload_key=payload_key,
                            content_type=fetched.content_type,
                            expected_status=ManifestStatus.FAILED,
                        )
                    else:
                        failed_manifest = _page_manifest(
                            identity=identity,
                            batch_id=batch_id,
                            run_id=run_id,
                            payload_key=payload_key,
                            content_type=fetched.content_type,
                            content_length=fetched.content_length,
                            checksum_sha256=fetched.checksum_sha256,
                            extracted_at=fetched.received_at,
                            status=ManifestStatus.FAILED,
                            attempt=fetched.attempt,
                        )
                        _write_manifest(storage, failed_manifest)
                    raise

                if reconciliation.state is ReconciliationState.COMPLETE:
                    manifest = _require_complete_terminal(
                        reconciliation,
                        payload_key=payload_key,
                        content_type=fetched.content_type,
                        expected_status=ManifestStatus.SUCCEEDED,
                    )
                    metrics.duplicate_count += 1
                else:
                    manifest = _page_manifest(
                        identity=identity,
                        batch_id=batch_id,
                        run_id=run_id,
                        payload_key=payload_key,
                        content_type=fetched.content_type,
                        content_length=fetched.content_length,
                        checksum_sha256=fetched.checksum_sha256,
                        extracted_at=fetched.received_at,
                        status=ManifestStatus.SUCCEEDED,
                        attempt=fetched.attempt,
                    )
                    _write_manifest(storage, manifest)

                payload_keys.append(payload_key)
                manifests.append(manifest)
                metrics.records_read += len(parsed_page.items)
                evidence = observe_page_evidence(
                    storage,
                    identity=identity,
                    page_number=page_number,
                    record_count=len(parsed_page.items),
                    maximum_payload_bytes=http_config.max_response_bytes,
                )
                if evidence.state is not LogicalRunReconciliationState.ELIGIBLE:
                    raise _logical_run_state_error(LogicalRunReconciliation(evidence.state))
                page_evidence.append(evidence)
                if expected_total_pages is None:
                    expected_total_pages = parsed_page.total_pages
                    expected_total_records = parsed_page.total_records
                if parsed_page.next_page is None:
                    break
                page_number = parsed_page.next_page

        completion = reconcile_page_evidence(
            identity=identity,
            observations=page_evidence,
            completion_run_id=run_id,
            completed_at=wall_clock(),
            existing_aggregate=(
                logical_run_preflight.aggregate
                if logical_run_preflight.state is LogicalRunReconciliationState.COMPLETE
                else None
            ),
        )
        if completion.state is LogicalRunReconciliationState.ELIGIBLE:
            if completion.aggregate is None:
                raise _logical_run_state_error(
                    LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)
                )
            completion = publish_logical_run(
                storage,
                identity=identity,
                manifest=completion.aggregate.manifest,
            )
        if (
            completion.state is not LogicalRunReconciliationState.COMPLETE
            or completion.aggregate is None
        ):
            raise _logical_run_state_error(completion)

        final_verification = inspect_logical_run(storage, identity)
        if (
            final_verification.state is not LogicalRunReconciliationState.COMPLETE
            or final_verification.aggregate is None
        ):
            raise _logical_run_state_error(final_verification)
        if final_verification.aggregate.body != completion.aggregate.body:
            raise _logical_run_state_error(
                LogicalRunReconciliation(LogicalRunReconciliationState.CONFLICTING)
            )
        verified_logical_run_key = final_verification.aggregate.key
    except (
        ApiPageStateError,
        HttpPageFetchError,
        RetryBudgetExhausted,
        PageValidationError,
        StorageError,
    ) as error:
        if isinstance(error, HttpPageFetchError):
            metrics.retry_count += error.retry_count
        summary = _run_summary(
            metrics,
            started_at=started_at,
            monotonic=monotonic,
            source_date=source_date,
            batch_id=batch_id,
            run_id=run_id,
        )
        code, http_status, classification = _classify_failure(error)
        event = ApiRunEvent(
            status=ApiRunStatus.FAILED,
            source_date=source_date,
            batch_id=batch_id,
            run_id=run_id,
            page=page_number,
            error_code=code,
            http_status=http_status,
            classification=classification,
            message=_FAILURE_MESSAGES[code],
        )
        _emit_final(event_sink, event, summary)
        raise ApiIngestionError(code=code, summary=summary, event=event) from error

    if verified_logical_run_key is None:
        raise AssertionError("successful API ingestion has no verified logical-run manifest")

    summary = _run_summary(
        metrics,
        started_at=started_at,
        monotonic=monotonic,
        source_date=source_date,
        batch_id=batch_id,
        run_id=run_id,
    )
    event = ApiRunEvent(
        status=ApiRunStatus.SUCCEEDED,
        source_date=source_date,
        batch_id=batch_id,
        run_id=run_id,
        page=None,
        error_code=None,
        http_status=None,
        classification=None,
        message="API ingestion succeeded",
    )
    _emit_final(event_sink, event, summary)
    return ApiIngestionResult(
        summary=summary,
        event=event,
        payload_keys=tuple(payload_keys),
        manifest_keys=tuple(manifest_key_for(key) for key in payload_keys),
        logical_run_manifest_key=verified_logical_run_key,
        manifests=tuple(manifests),
    )


def ingest_api_backfill(
    *,
    client: httpx.Client,
    http_config: HttpConfig,
    storage: ApiStorage | None = None,
    storage_session_provider: (Callable[[], AbstractContextManager[ApiStorage]] | None) = None,
    descriptor: ApiSourceDescriptor,
    from_date: date,
    to_date: date,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    utc_now: Callable[[], datetime] | None = None,
    run_id_factory: Callable[[], str] = new_run_id,
    query_parameter_provider: Callable[[date, int], Mapping[str, str]] | None = None,
    event_sink: Callable[[dict[str, object]], None] | None = None,
    ingest_date: Callable[..., ApiIngestionResult] = ingest_api_date,
) -> ApiBackfillResult:
    """Ingest an inclusive ascending date range, stopping at the first failure."""
    day_count = validate_backfill_range(
        from_date=from_date,
        to_date=to_date,
        max_days=http_config.max_backfill_days,
    )
    results: list[ApiIngestionResult] = []
    for offset in range(day_count):
        source_date = from_date + timedelta(days=offset)
        page_provider = (
            None
            if query_parameter_provider is None
            else lambda page, source_date=source_date: query_parameter_provider(source_date, page)
        )
        results.append(
            ingest_date(
                client=client,
                storage=storage,
                storage_session_provider=storage_session_provider,
                http_config=http_config,
                descriptor=descriptor,
                source_date=source_date,
                monotonic=monotonic,
                sleep=sleep,
                utc_now=utc_now,
                run_id_factory=run_id_factory,
                query_parameter_provider=page_provider,
                event_sink=event_sink,
            )
        )
    return ApiBackfillResult(from_date=from_date, to_date=to_date, results=tuple(results))


def _validate_observed_payload(
    reconciliation: ReconciliationResult,
    fetched_content_type: str,
) -> None:
    if reconciliation.state is ReconciliationState.MANIFEST_ONLY:
        raise ApiPageStateError(
            ApiErrorCode.MANIFEST_ONLY,
            "manifest-only state cannot be repaired automatically",
        )
    if reconciliation.state in {
        ReconciliationState.PAYLOAD_ONLY,
        ReconciliationState.COMPLETE,
    }:
        if reconciliation.payload is None:
            raise ApiPageStateError(
                ApiErrorCode.STORAGE_CORRUPTION,
                "reconciliation omitted existing payload evidence",
            )
        if reconciliation.payload.content_type != fetched_content_type:
            raise ApiPageStateError(
                ApiErrorCode.STORAGE_CONFLICT,
                "fetched Content-Type conflicts with stored payload",
            )


def _decode_and_validate_page(
    body: bytes,
    *,
    descriptor: ApiSourceDescriptor,
    source_date: date,
    requested_page: int,
    page_size: int,
    max_pages: int,
    expected_total_pages: int | None,
    expected_total_records: int | None,
) -> ApiPage:
    try:
        value: object = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PageValidationError("page body is not valid UTF-8 JSON") from error
    return validate_page_envelope(
        value,
        descriptor=descriptor,
        source_date=source_date,
        requested_page=requested_page,
        page_size=page_size,
        max_pages=max_pages,
        expected_total_pages=expected_total_pages,
        expected_total_records=expected_total_records,
    )


def _page_manifest(
    *,
    identity: BatchIdentity,
    batch_id: str,
    run_id: str,
    payload_key: str,
    content_type: str,
    content_length: int,
    checksum_sha256: str,
    extracted_at: datetime,
    status: ManifestStatus,
    attempt: int,
) -> Manifest:
    return Manifest(
        source_name=identity.source_name,
        source_type=identity.source_type,
        source_date=identity.source_date,
        batch_id=batch_id,
        run_id=run_id,
        object_key=payload_key,
        original_file_name=None,
        content_type=content_type,
        content_length=content_length,
        checksum_sha256=checksum_sha256,
        schema_version=MANIFEST_SCHEMA_VERSION,
        extracted_at=extracted_at,
        status=status,
        attempt=attempt,
    )


def _require_complete_terminal(
    reconciliation: ReconciliationResult,
    *,
    payload_key: str,
    content_type: str,
    expected_status: ManifestStatus,
) -> Manifest:
    manifest = reconciliation.manifest
    if manifest is None:
        raise ApiPageStateError(
            ApiErrorCode.STORAGE_CORRUPTION,
            "complete state omitted its manifest",
        )
    if manifest.object_key != payload_key or manifest.content_type != content_type:
        raise ApiPageStateError(
            ApiErrorCode.STORAGE_CONFLICT,
            "complete manifest conflicts with the fetched page",
        )
    if manifest.status is not expected_status:
        raise ApiPageStateError(
            ApiErrorCode.COMPLETE_STATUS_CONFLICT,
            "complete manifest terminal status conflicts with fetched page validation",
        )
    return manifest


def _write_manifest(storage: ApiStorage, manifest: Manifest) -> None:
    manifest_bytes = manifest.to_json().encode("utf-8")
    result = storage.write_immutable(
        manifest_key_for(manifest.object_key),
        io.BytesIO(manifest_bytes),
        content_length=len(manifest_bytes),
        checksum_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        content_type="application/json",
    )
    if result.status is not ImmutableWriteStatus.CREATED:
        raise ApiPageStateError(
            ApiErrorCode.STORAGE_CONFLICT,
            "missing manifest was not created under single-writer mode",
        )


def _logical_run_state_error(
    reconciliation: LogicalRunReconciliation,
) -> ApiPageStateError:
    if isinstance(reconciliation.cause, StorageBackendError):
        raise reconciliation.cause
    code_by_state = {
        LogicalRunReconciliationState.CORRUPT: ApiErrorCode.LOGICAL_RUN_MANIFEST_CORRUPT,
        LogicalRunReconciliationState.CONFLICTING: (ApiErrorCode.LOGICAL_RUN_MANIFEST_CONFLICT),
        LogicalRunReconciliationState.ORPHANED: ApiErrorCode.LOGICAL_RUN_MANIFEST_ORPHANED,
        LogicalRunReconciliationState.UNCERTAIN_WRITE: (
            ApiErrorCode.LOGICAL_RUN_MANIFEST_WRITE_UNCERTAIN
        ),
        LogicalRunReconciliationState.INCOMPLETE: (ApiErrorCode.LOGICAL_RUN_MANIFEST_CONFLICT),
        LogicalRunReconciliationState.FAILED: ApiErrorCode.COMPLETE_STATUS_CONFLICT,
        LogicalRunReconciliationState.ABSENT: (ApiErrorCode.LOGICAL_RUN_MANIFEST_WRITE_UNCERTAIN),
        LogicalRunReconciliationState.ELIGIBLE: (ApiErrorCode.LOGICAL_RUN_MANIFEST_CONFLICT),
    }
    code = code_by_state.get(reconciliation.state)
    if code is None:
        code = ApiErrorCode.LOGICAL_RUN_MANIFEST_CONFLICT
    return ApiPageStateError(code, _FAILURE_MESSAGES[code])


def _classify_failure(
    error: Exception,
) -> tuple[ApiErrorCode, int | None, RetryClassification | None]:
    if isinstance(error, HttpRetryExhausted):
        return ApiErrorCode.HTTP_RETRY_EXHAUSTED, error.status_code, error.classification
    if isinstance(error, HttpStatusError):
        code = (
            ApiErrorCode.HTTP_PERMANENT_CLIENT_ERROR
            if error.classification is RetryClassification.PERMANENT_CLIENT_ERROR
            else ApiErrorCode.HTTP_PERMANENT_PROTOCOL_ERROR
        )
        return code, error.status_code, error.classification
    if isinstance(error, ResponseSizeLimitError):
        return ApiErrorCode.HTTP_RESPONSE_TOO_LARGE, None, None
    if isinstance(error, HttpProtocolError):
        return ApiErrorCode.HTTP_RESPONSE_PROTOCOL_ERROR, None, None
    if isinstance(error, RetryBudgetExhausted):
        return ApiErrorCode.HTTP_RETRY_EXHAUSTED, None, None
    if isinstance(error, PageValidationError):
        return ApiErrorCode.INVALID_PAGE, None, None
    if isinstance(error, ApiPageStateError):
        return error.code, None, None
    if isinstance(error, ObjectConflictError):
        return ApiErrorCode.STORAGE_CONFLICT, None, None
    if isinstance(error, ObjectVerificationError):
        return ApiErrorCode.STORAGE_CORRUPTION, None, None
    if isinstance(error, StorageError):
        return ApiErrorCode.STORAGE_ERROR, None, None
    raise TypeError("unsupported handled API ingestion failure")


def _run_summary(
    metrics: _RunMetrics,
    *,
    started_at: float,
    monotonic: Callable[[], float],
    source_date: date,
    batch_id: str,
    run_id: str,
) -> RunSummary:
    duration = monotonic() - started_at
    if duration < 0:
        raise RuntimeError("monotonic clock moved backwards")
    return RunSummary(
        records_read=metrics.records_read,
        records_accepted=metrics.records_read,
        records_rejected=0,
        pages_fetched=metrics.pages_fetched,
        bytes_written=metrics.bytes_written,
        retry_count=metrics.retry_count,
        duplicate_count=metrics.duplicate_count,
        duration_seconds=duration,
        source_date=source_date,
        batch_id=batch_id,
        run_id=run_id,
    )


def _emit_final(
    event_sink: Callable[[dict[str, object]], None] | None,
    event: ApiRunEvent,
    summary: RunSummary,
) -> None:
    if event_sink is None:
        return
    event_sink(event.to_dict())
    event_sink({"event": "run_summary", **summary.to_dict()})


def _utc_now() -> datetime:
    return datetime.now(UTC)
