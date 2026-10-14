"""REST API ingestion contracts with no import-time runtime I/O."""

from .fetcher import (
    FetchedPage,
    HttpPageFetchError,
    HttpProtocolError,
    HttpRetryExhausted,
    HttpStatusError,
    ResponseSizeLimitError,
    RetryEvent,
    fetch_page,
)
from .retry import RetryClassification, RetryPolicy
from .service import (
    ApiBackfillResult,
    ApiErrorCode,
    ApiIngestionError,
    ApiIngestionResult,
    ApiPageStateError,
    ApiRunEvent,
    ApiRunStatus,
    ingest_api_backfill,
    ingest_api_date,
    validate_backfill_range,
)
from .sources import ApiPage, ApiSourceDescriptor, PageValidationError, get_source_descriptor

__all__ = [
    "ApiBackfillResult",
    "ApiErrorCode",
    "ApiIngestionError",
    "ApiIngestionResult",
    "ApiPage",
    "ApiPageStateError",
    "ApiRunEvent",
    "ApiRunStatus",
    "ApiSourceDescriptor",
    "FetchedPage",
    "HttpPageFetchError",
    "HttpProtocolError",
    "HttpRetryExhausted",
    "HttpStatusError",
    "PageValidationError",
    "ResponseSizeLimitError",
    "RetryClassification",
    "RetryEvent",
    "RetryPolicy",
    "fetch_page",
    "get_source_descriptor",
    "ingest_api_backfill",
    "ingest_api_date",
    "validate_backfill_range",
]
