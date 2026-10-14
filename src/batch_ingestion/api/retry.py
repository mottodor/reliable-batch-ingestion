"""Pure retry classification and one-page attempt/elapsed budget policy."""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import StrEnum

import httpx

_ASCII_SECONDS = re.compile(r"[0-9]+", flags=re.ASCII)


class RetryClassification(StrEnum):
    """HTTP outcomes relevant to the application-owned retry loop."""

    SUCCESS = "success"
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
    TRANSPORT_ERROR = "transport_error"
    PERMANENT_CLIENT_ERROR = "permanent_client_error"
    PERMANENT_PROTOCOL_ERROR = "permanent_protocol_error"


class RetryBudgetExhausted(RuntimeError):
    """Raised when another HTTP request cannot fit both retry budgets."""


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Validated attempt and elapsed bounds for fetching one page."""

    max_attempts: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    max_elapsed_seconds: float

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or self.max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
        for name, value in (
            ("backoff_base_seconds", self.backoff_base_seconds),
            ("backoff_max_seconds", self.backoff_max_seconds),
            ("max_elapsed_seconds", self.max_elapsed_seconds),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a positive finite number")


def classify_status(status_code: int) -> RetryClassification:
    """Classify an HTTP status without collapsing it through raise_for_status()."""
    if isinstance(status_code, bool) or not isinstance(status_code, int):
        raise TypeError("status_code must be an integer")
    if status_code == 200:
        return RetryClassification.SUCCESS
    if status_code == 429:
        return RetryClassification.RATE_LIMIT
    if 400 <= status_code <= 499:
        return RetryClassification.PERMANENT_CLIENT_ERROR
    if 500 <= status_code <= 599:
        return RetryClassification.SERVER_ERROR
    return RetryClassification.PERMANENT_PROTOCOL_ERROR


def classify_exception(error: Exception) -> RetryClassification:
    """Classify only HTTPX transport failures as retryable."""
    if isinstance(error, httpx.TransportError):
        return RetryClassification.TRANSPORT_ERROR
    raise TypeError("not a retryable HTTP transport error")


def fallback_delay_seconds(
    attempt: int,
    *,
    base_seconds: float,
    maximum_seconds: float,
) -> float:
    """Return deterministic capped exponential delay after a failed attempt."""
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt <= 0:
        raise ValueError("attempt must be a positive integer")
    if not math.isfinite(base_seconds) or base_seconds <= 0:
        raise ValueError("base_seconds must be a positive finite number")
    if not math.isfinite(maximum_seconds) or maximum_seconds <= 0:
        raise ValueError("maximum_seconds must be a positive finite number")
    try:
        delay = base_seconds * (2.0 ** (attempt - 1))
    except OverflowError:
        return maximum_seconds
    return min(delay, maximum_seconds)


def parse_retry_after(value: str | None, *, now: datetime) -> float | None:
    """Parse non-negative delta-seconds or an HTTP date; return None if malformed."""
    utc_offset = now.utcoffset()
    if now.tzinfo is None or utc_offset is None or utc_offset.total_seconds() != 0:
        raise ValueError("now must be timezone-aware UTC")
    if value is None:
        return None

    stripped = value.strip()
    if _ASCII_SECONDS.fullmatch(stripped) is not None:
        try:
            return float(int(stripped))
        except OverflowError:
            return math.inf

    try:
        parsed = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    delay = (parsed.astimezone(UTC) - now.astimezone(UTC)).total_seconds()
    return max(0.0, delay)


class RetryBudget:
    """Stateful clock boundary for one page; policy calculations remain deterministic."""

    def __init__(
        self,
        policy: RetryPolicy,
        *,
        monotonic: Callable[[], float],
        sleep: Callable[[float], None],
        utc_now: Callable[[], datetime],
    ) -> None:
        self._policy = policy
        self._monotonic = monotonic
        self._sleep = sleep
        self._utc_now = utc_now
        self._deadline = monotonic() + policy.max_elapsed_seconds

    @property
    def remaining_seconds(self) -> float:
        """Return the non-negative time left before the page deadline."""
        return max(0.0, self._deadline - self._monotonic())

    def ensure_attempt_allowed(self, *, attempt: int) -> None:
        """Reject requests outside either the attempt or elapsed budget."""
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt <= 0:
            raise ValueError("attempt must be a positive integer")
        if attempt > self._policy.max_attempts:
            raise RetryBudgetExhausted("maximum attempts exhausted")
        if self.remaining_seconds <= 0:
            raise RetryBudgetExhausted("elapsed-time budget exhausted")

    def wait_before_retry(self, *, attempt: int, retry_after: str | None = None) -> float:
        """Sleep only when a complete selected delay fits before another attempt."""
        self.ensure_attempt_allowed(attempt=attempt)
        if attempt >= self._policy.max_attempts:
            raise RetryBudgetExhausted("maximum attempts exhausted")

        parsed_delay = parse_retry_after(retry_after, now=self._utc_now())
        delay = (
            parsed_delay
            if parsed_delay is not None
            else fallback_delay_seconds(
                attempt,
                base_seconds=self._policy.backoff_base_seconds,
                maximum_seconds=self._policy.backoff_max_seconds,
            )
        )
        if not math.isfinite(delay) or delay > self.remaining_seconds:
            raise RetryBudgetExhausted("elapsed-time budget exhausted before retry delay")
        if delay > 0:
            self._sleep(delay)
        return delay
