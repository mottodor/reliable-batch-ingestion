"""Bounded synchronous HTTP page fetching into seekable spooled streams."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from tempfile import SpooledTemporaryFile
from types import TracebackType
from typing import BinaryIO, Self, cast

import httpx

from batch_ingestion.config import HttpConfig

from .client import timeout_for_remaining
from .retry import (
    RetryBudget,
    RetryBudgetExhausted,
    RetryClassification,
    RetryPolicy,
    classify_exception,
    classify_status,
)
from .sources import ApiSourceDescriptor

_SPOOL_MEMORY_LIMIT_BYTES = 1024 * 1024


class HttpPageFetchError(RuntimeError):
    """Base error for a page that did not produce a preservable HTTP 200 body."""

    def __init__(self, message: str, *, attempt: int) -> None:
        self.attempt = attempt
        super().__init__(message)

    @property
    def retry_count(self) -> int:
        """Return the number of additional requests that actually started."""
        return self.attempt - 1


class HttpStatusError(HttpPageFetchError):
    """Raised immediately for a permanent non-200 HTTP status."""

    def __init__(
        self,
        status_code: int,
        classification: RetryClassification,
        *,
        attempt: int,
    ) -> None:
        self.status_code = status_code
        self.classification = classification
        super().__init__(
            f"HTTP status {status_code} classified as {classification.value}",
            attempt=attempt,
        )


class HttpRetryExhausted(HttpPageFetchError, RetryBudgetExhausted):
    """Raised when a retryable outcome cannot make another bounded request."""

    def __init__(
        self,
        *,
        classification: RetryClassification,
        status_code: int | None,
        attempt: int,
    ) -> None:
        self.status_code = status_code
        self.classification = classification
        detail = f"HTTP status {status_code}" if status_code is not None else "HTTP transport"
        super().__init__(
            f"{detail} retry budget exhausted after attempt {attempt}; "
            "elapsed-time budget or maximum attempts exhausted",
            attempt=attempt,
        )


class HttpProtocolError(HttpPageFetchError):
    """Raised when a successful response violates the required HTTP metadata contract."""


class ResponseSizeLimitError(HttpPageFetchError):
    """Raised as soon as a streamed body would exceed its configured byte bound."""


@dataclass(frozen=True, slots=True)
class RetryEvent:
    """One additional HTTP request that started after a classified failure."""

    attempt: int
    classification: RetryClassification
    status_code: int | None
    delay_seconds: float


@dataclass(slots=True)
class FetchedPage:
    """One complete bounded raw page body owned by a closeable seekable stream."""

    stream: BinaryIO
    content_length: int
    checksum_sha256: str
    content_type: str
    attempt: int
    received_at: datetime

    @property
    def retry_count(self) -> int:
        """Return the number of additional requests that produced this response."""
        return self.attempt - 1

    def close(self) -> None:
        """Close the owned spooled stream."""
        self.stream.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def fetch_page(
    *,
    client: httpx.Client,
    config: HttpConfig,
    descriptor: ApiSourceDescriptor,
    source_date: date,
    page: int,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
    utc_now: Callable[[], datetime],
    extra_query_parameters: Mapping[str, str] | None = None,
    retry_observer: Callable[[RetryEvent], None] | None = None,
) -> FetchedPage:
    """Fetch one complete HTTP 200 page under one attempt and elapsed budget."""
    if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= config.max_pages:
        raise ValueError(f"page must be between 1 and {config.max_pages}")

    policy = RetryPolicy(
        max_attempts=config.max_attempts,
        backoff_base_seconds=config.backoff_base_seconds,
        backoff_max_seconds=config.backoff_max_seconds,
        max_elapsed_seconds=config.max_elapsed_seconds,
    )
    budget = RetryBudget(
        policy,
        monotonic=monotonic,
        sleep=sleep,
        utc_now=utc_now,
    )
    parameters: dict[str, str | int] = {
        "source_date": source_date.isoformat(),
        "page": page,
        "page_size": config.page_size,
    }
    if extra_query_parameters:
        overlap = set(parameters).intersection(extra_query_parameters)
        if overlap:
            raise ValueError(f"extra query parameters replace required fields: {sorted(overlap)}")
        parameters.update(extra_query_parameters)

    attempt = 1
    pending_retry: tuple[RetryClassification, int | None, float] | None = None
    while True:
        try:
            budget.ensure_attempt_allowed(attempt=attempt)
        except RetryBudgetExhausted as error:
            if pending_retry is None:
                raise
            classification, status_code, _ = pending_retry
            raise HttpRetryExhausted(
                classification=classification,
                status_code=status_code,
                attempt=attempt - 1,
            ) from error

        if pending_retry is not None and retry_observer is not None:
            classification, status_code, delay_seconds = pending_retry
            retry_observer(
                RetryEvent(
                    attempt=attempt,
                    classification=classification,
                    status_code=status_code,
                    delay_seconds=delay_seconds,
                )
            )
        pending_retry = None

        try:
            with client.stream(
                "GET",
                descriptor.path,
                params=parameters,
                timeout=timeout_for_remaining(
                    config,
                    remaining_seconds=budget.remaining_seconds,
                ),
            ) as response:
                classification = classify_status(response.status_code)
                if classification is RetryClassification.SUCCESS:
                    return _read_success_response(
                        response,
                        config=config,
                        budget=budget,
                        utc_now=utc_now,
                        attempt=attempt,
                    )
                if classification not in {
                    RetryClassification.RATE_LIMIT,
                    RetryClassification.SERVER_ERROR,
                }:
                    raise HttpStatusError(
                        response.status_code,
                        classification,
                        attempt=attempt,
                    )
                retry_after = (
                    response.headers.get("Retry-After")
                    if classification is RetryClassification.RATE_LIMIT
                    else None
                )
                status_code = response.status_code
        except httpx.TransportError as error:
            classification = classify_exception(error)
            status_code = None
            retry_after = None

        try:
            delay = budget.wait_before_retry(
                attempt=attempt,
                retry_after=retry_after,
            )
        except RetryBudgetExhausted as error:
            raise HttpRetryExhausted(
                classification=classification,
                status_code=status_code,
                attempt=attempt,
            ) from error
        pending_retry = (classification, status_code, delay)
        attempt += 1


def _ensure_response_deadline(*, budget: RetryBudget, attempt: int) -> None:
    if budget.remaining_seconds <= 0:
        raise HttpRetryExhausted(
            classification=RetryClassification.TRANSPORT_ERROR,
            status_code=None,
            attempt=attempt,
        )


def _read_success_response(
    response: httpx.Response,
    *,
    config: HttpConfig,
    budget: RetryBudget,
    utc_now: Callable[[], datetime],
    attempt: int,
) -> FetchedPage:
    try:
        content_type = _json_content_type(response.headers.get("Content-Type"))
    except HttpProtocolError as error:
        raise HttpProtocolError(str(error), attempt=attempt) from error
    spool_limit = min(config.max_response_bytes, _SPOOL_MEMORY_LIMIT_BYTES)
    # Ownership is transferred to FetchedPage and closed by its context manager.
    stream = cast(
        BinaryIO,
        SpooledTemporaryFile(max_size=spool_limit, mode="w+b"),  # noqa: SIM115
    )
    digest = hashlib.sha256()
    content_length = 0
    chunks = iter(response.iter_raw(chunk_size=None))
    try:
        while True:
            _ensure_response_deadline(budget=budget, attempt=attempt)
            try:
                chunk = next(chunks)
            except StopIteration:
                _ensure_response_deadline(budget=budget, attempt=attempt)
                break
            _ensure_response_deadline(budget=budget, attempt=attempt)
            next_length = content_length + len(chunk)
            if next_length > config.max_response_bytes:
                raise ResponseSizeLimitError(
                    "response body exceeds configured maximum of "
                    f"{config.max_response_bytes} bytes",
                    attempt=attempt,
                )
            stream.write(chunk)
            digest.update(chunk)
            content_length = next_length
        received_at = utc_now()
        stream.seek(0)
        return FetchedPage(
            stream=stream,
            content_length=content_length,
            checksum_sha256=digest.hexdigest(),
            content_type=content_type,
            attempt=attempt,
            received_at=received_at,
        )
    except BaseException:
        stream.close()
        raise


def _json_content_type(value: str | None) -> str:
    if value is None:
        raise HttpProtocolError("HTTP 200 response is missing Content-Type", attempt=1)
    normalized = value.strip()
    base_media_type = normalized.partition(";")[0].strip().casefold()
    if not normalized or base_media_type != "application/json":
        raise HttpProtocolError(
            "HTTP 200 response Content-Type must be application/json",
            attempt=1,
        )
    return normalized
