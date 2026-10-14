"""REST API source, retry, and bounded-fetch contracts with no import-time I/O."""

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
from .sources import ApiPage, ApiSourceDescriptor, PageValidationError, get_source_descriptor

__all__ = [
    "ApiPage",
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
]
