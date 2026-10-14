"""Pure HTTP retry classification and bounded-delay contracts."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from batch_ingestion.api.retry import (
    RetryBudget,
    RetryBudgetExhausted,
    RetryClassification,
    RetryPolicy,
    classify_exception,
    classify_status,
    fallback_delay_seconds,
    parse_retry_after,
)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (200, RetryClassification.SUCCESS),
        (429, RetryClassification.RATE_LIMIT),
        (400, RetryClassification.PERMANENT_CLIENT_ERROR),
        (401, RetryClassification.PERMANENT_CLIENT_ERROR),
        (404, RetryClassification.PERMANENT_CLIENT_ERROR),
        (499, RetryClassification.PERMANENT_CLIENT_ERROR),
        (500, RetryClassification.SERVER_ERROR),
        (503, RetryClassification.SERVER_ERROR),
        (599, RetryClassification.SERVER_ERROR),
        (301, RetryClassification.PERMANENT_PROTOCOL_ERROR),
        (204, RetryClassification.PERMANENT_PROTOCOL_ERROR),
        (199, RetryClassification.PERMANENT_PROTOCOL_ERROR),
        (600, RetryClassification.PERMANENT_PROTOCOL_ERROR),
    ],
)
def test_status_classification_is_explicit(
    status: int,
    expected: RetryClassification,
) -> None:
    assert classify_status(status) is expected


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectTimeout("connect"),
        httpx.ReadTimeout("read"),
        httpx.WriteTimeout("write"),
        httpx.PoolTimeout("pool"),
        httpx.ConnectError("connect"),
        httpx.ReadError("read"),
        httpx.WriteError("write"),
        httpx.RemoteProtocolError("protocol"),
    ],
)
def test_httpx_timeout_and_transport_failures_are_retryable(error: Exception) -> None:
    assert classify_exception(error) is RetryClassification.TRANSPORT_ERROR


def test_non_transport_exceptions_do_not_enter_retry_policy() -> None:
    with pytest.raises(TypeError, match="not a retryable HTTP transport error"):
        classify_exception(ValueError("programming failure"))


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 0.25), (2, 0.5), (3, 1.0), (4, 1.0), (20, 1.0)],
)
def test_fallback_backoff_is_exponential_and_capped(attempt: int, expected: float) -> None:
    assert fallback_delay_seconds(attempt, base_seconds=0.25, maximum_seconds=1.0) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0.0),
        ("12", 12.0),
        ("Wed, 21 Oct 2015 07:28:10 GMT", 10.0),
        ("Wed, 21 Oct 2015 07:27:00 GMT", 0.0),
        ("garbage", None),
        ("-1", None),
        ("1.5", None),
        (None, None),
    ],
)
def test_retry_after_supports_delta_seconds_and_http_dates(
    value: str | None,
    expected: float | None,
) -> None:
    now = datetime(2015, 10, 21, 7, 28, tzinfo=UTC)
    assert parse_retry_after(value, now=now) == expected


def test_retry_after_requires_an_aware_utc_clock() -> None:
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        parse_retry_after("1", now=datetime(2026, 9, 1))


class FakeClock:
    def __init__(self) -> None:
        self.elapsed = 100.0
        self.now = datetime(2026, 9, 1, tzinfo=UTC)
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.elapsed

    def utc_now(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.elapsed += seconds
        self.now += timedelta(seconds=seconds)


def _budget(clock: FakeClock, *, attempts: int = 4, elapsed: float = 10.0) -> RetryBudget:
    return RetryBudget(
        RetryPolicy(
            max_attempts=attempts,
            backoff_base_seconds=0.25,
            backoff_max_seconds=1.0,
            max_elapsed_seconds=elapsed,
        ),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        utc_now=clock.utc_now,
    )


def test_retry_budget_uses_valid_retry_after_exactly_and_malformed_value_falls_back() -> None:
    clock = FakeClock()
    budget = _budget(clock)

    assert budget.wait_before_retry(attempt=1, retry_after="2") == 2.0
    assert budget.wait_before_retry(attempt=2, retry_after="invalid") == 0.5
    assert clock.sleeps == [2.0, 0.5]


def test_retry_budget_stops_at_max_attempts_without_sleeping() -> None:
    clock = FakeClock()
    budget = _budget(clock, attempts=2)

    budget.wait_before_retry(attempt=1)
    with pytest.raises(RetryBudgetExhausted, match="maximum attempts"):
        budget.wait_before_retry(attempt=2)
    assert clock.sleeps == [0.25]


def test_retry_budget_rejects_delay_larger_than_remaining_elapsed_time() -> None:
    clock = FakeClock()
    budget = _budget(clock, elapsed=1.5)
    clock.elapsed += 1.0

    with pytest.raises(RetryBudgetExhausted, match="elapsed-time budget"):
        budget.wait_before_retry(attempt=1, retry_after="1")
    assert clock.sleeps == []


def test_elapsed_deadline_is_checked_before_an_attempt() -> None:
    clock = FakeClock()
    budget = _budget(clock, elapsed=1.0)
    clock.elapsed += 1.0

    with pytest.raises(RetryBudgetExhausted, match="elapsed-time budget"):
        budget.ensure_attempt_allowed(attempt=2)
    assert clock.sleeps == []


def test_http_date_delay_is_computed_from_injected_wall_clock() -> None:
    clock = FakeClock()
    budget = _budget(clock)
    retry_at = (clock.now + timedelta(seconds=3)).strftime("%a, %d %b %Y %H:%M:%S GMT")

    assert budget.wait_before_retry(attempt=1, retry_after=retry_at) == 3.0
    assert clock.sleeps == [3.0]
