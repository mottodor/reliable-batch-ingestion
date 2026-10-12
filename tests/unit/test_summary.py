import json
from datetime import date

import pytest

from batch_ingestion import RunSummary


def test_run_summary_serializes_all_observable_metrics() -> None:
    summary = RunSummary(
        records_read=10,
        records_accepted=8,
        records_rejected=2,
        pages_fetched=3,
        bytes_written=1024,
        retry_count=1,
        duplicate_count=0,
        duration_seconds=1.25,
        source_date=date(2026, 9, 1),
        batch_id="a" * 64,
        run_id="run_1234567812344abc9234567890abcdef",
    )

    assert summary.to_dict() == {
        "records_read": 10,
        "records_accepted": 8,
        "records_rejected": 2,
        "pages_fetched": 3,
        "bytes_written": 1024,
        "retry_count": 1,
        "duplicate_count": 0,
        "duration_seconds": 1.25,
        "source_date": "2026-09-01",
        "batch_id": "a" * 64,
        "run_id": "run_1234567812344abc9234567890abcdef",
    }
    assert json.loads(summary.to_json()) == summary.to_dict()


def test_run_summary_rejects_negative_metrics() -> None:
    with pytest.raises(ValueError, match="records_rejected must be non-negative"):
        RunSummary(
            records_read=0,
            records_accepted=0,
            records_rejected=-1,
            pages_fetched=0,
            bytes_written=0,
            retry_count=0,
            duplicate_count=0,
            duration_seconds=0,
            source_date=date(2026, 9, 1),
            batch_id="a" * 64,
            run_id="run_1234567812344abc9234567890abcdef",
        )
