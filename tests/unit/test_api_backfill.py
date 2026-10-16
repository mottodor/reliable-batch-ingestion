"""Inclusive sequential API backfill contracts."""

from datetime import date
from typing import cast

import httpx
import pytest

from batch_ingestion.api.service import (
    ApiErrorCode,
    ApiIngestionError,
    ApiIngestionResult,
    ApiRunEvent,
    ApiRunStatus,
    ApiStorage,
    ingest_api_backfill,
)
from batch_ingestion.api.sources import get_source_descriptor
from batch_ingestion.config import HttpConfig
from batch_ingestion.summary import RunSummary


def _config(*, max_backfill_days: int = 31) -> HttpConfig:
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
        max_backfill_days=max_backfill_days,
    )


def _result(source_date: date) -> ApiIngestionResult:
    batch_id = "1" * 64
    run_id = "run_" + "2" * 32
    summary = RunSummary(
        records_read=1,
        records_accepted=1,
        records_rejected=0,
        pages_fetched=1,
        bytes_written=10,
        retry_count=0,
        duplicate_count=0,
        duration_seconds=0.1,
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
    return ApiIngestionResult(
        summary,
        event,
        (),
        (),
        "manifests/api/test/logical-run.manifest.json",
        (),
    )


def _failure(source_date: date) -> ApiIngestionError:
    result = _result(source_date)
    event = ApiRunEvent(
        status=ApiRunStatus.FAILED,
        source_date=source_date,
        batch_id=result.summary.batch_id,
        run_id=result.summary.run_id,
        page=1,
        error_code=ApiErrorCode.HTTP_PERMANENT_CLIENT_ERROR,
        http_status=404,
        classification=None,
        message="API request failed permanently",
    )
    return ApiIngestionError(
        code=ApiErrorCode.HTTP_PERMANENT_CLIENT_ERROR,
        summary=result.summary,
        event=event,
    )


def test_backfill_calls_single_date_primitive_for_every_inclusive_date_in_order() -> None:
    called: list[date] = []

    def ingest_date(**kwargs: object) -> ApiIngestionResult:
        source_date = kwargs["source_date"]
        assert isinstance(source_date, date)
        called.append(source_date)
        return _result(source_date)

    with httpx.Client() as client:
        result = ingest_api_backfill(
            client=client,
            storage=cast(ApiStorage, object()),
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            from_date=date(2026, 9, 1),
            to_date=date(2026, 9, 3),
            ingest_date=ingest_date,
        )

    assert called == [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)]
    assert tuple(item.summary.source_date for item in result.results) == tuple(called)
    assert result.from_date == date(2026, 9, 1)
    assert result.to_date == date(2026, 9, 3)


def test_backfill_stops_at_storage_failure_without_duplicate_terminal_output() -> None:
    called: list[date] = []
    emitted: list[dict[str, object]] = []
    failed_date = date(2026, 9, 2)

    def ingest_date(**kwargs: object) -> ApiIngestionResult:
        source_date = kwargs["source_date"]
        event_sink = kwargs["event_sink"]
        assert isinstance(source_date, date)
        assert callable(event_sink)
        called.append(source_date)
        result = _result(source_date)
        if source_date == failed_date:
            event = ApiRunEvent(
                status=ApiRunStatus.FAILED,
                source_date=source_date,
                batch_id=result.summary.batch_id,
                run_id=result.summary.run_id,
                page=1,
                error_code=ApiErrorCode.STORAGE_ERROR,
                http_status=None,
                classification=None,
                message="storage operation failed",
            )
            event_sink(event.to_dict())
            event_sink({"event": "run_summary", **result.summary.to_dict()})
            raise ApiIngestionError(
                code=ApiErrorCode.STORAGE_ERROR,
                summary=result.summary,
                event=event,
            )
        event_sink(result.event.to_dict())
        event_sink({"event": "run_summary", **result.summary.to_dict()})
        return result

    with (
        httpx.Client() as client,
        pytest.raises(ApiIngestionError) as captured,
    ):
        ingest_api_backfill(
            client=client,
            storage=cast(ApiStorage, object()),
            http_config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            from_date=date(2026, 9, 1),
            to_date=date(2026, 9, 3),
            event_sink=emitted.append,
            ingest_date=ingest_date,
        )

    assert captured.value.code is ApiErrorCode.STORAGE_ERROR
    assert captured.value.event.source_date == failed_date
    assert called == [date(2026, 9, 1), failed_date]
    assert [item["event"] for item in emitted] == [
        "api_ingestion",
        "run_summary",
        "api_ingestion",
        "run_summary",
    ]
    assert [item.get("source_date") for item in emitted] == [
        "2026-09-01",
        "2026-09-01",
        "2026-09-02",
        "2026-09-02",
    ]


@pytest.mark.parametrize(
    ("from_date", "to_date", "maximum", "message"),
    [
        (date(2026, 9, 2), date(2026, 9, 1), 31, "from date must not be after to date"),
        (date(2026, 9, 1), date(2026, 9, 3), 2, "exceeds configured maximum"),
    ],
)
def test_invalid_backfill_range_is_rejected_before_single_date_calls(
    from_date: date,
    to_date: date,
    maximum: int,
    message: str,
) -> None:
    called = False

    def ingest_date(**kwargs: object) -> ApiIngestionResult:
        nonlocal called
        called = True
        raise AssertionError("invalid backfill must not call single-date ingestion")

    with (
        httpx.Client() as client,
        pytest.raises(ValueError, match=message),
    ):
        ingest_api_backfill(
            client=client,
            storage=cast(ApiStorage, object()),
            http_config=_config(max_backfill_days=maximum),
            descriptor=get_source_descriptor("fx-rates"),
            from_date=from_date,
            to_date=to_date,
            ingest_date=ingest_date,
        )

    assert called is False
