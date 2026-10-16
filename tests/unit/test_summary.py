import json
from datetime import date

import pytest

from batch_ingestion import RunSummary, TerminalEmitter


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


@pytest.mark.parametrize(
    "terminal_event",
    [
        {"event": "api_ingestion", "status": "failed"},
        {"event": "file_ingestion", "outcome": "failed"},
        {"event": "snapshot_failed", "error_code": "storage_failed"},
    ],
)
def test_terminal_emitter_emits_one_ordered_pair_for_each_source(
    terminal_event: dict[str, object],
) -> None:
    emitted: list[dict[str, object]] = []
    summary = RunSummary(
        records_read=0,
        records_accepted=0,
        records_rejected=0,
        pages_fetched=0,
        bytes_written=0,
        retry_count=0,
        duplicate_count=0,
        duration_seconds=0.5,
        source_date=date(2026, 9, 1),
        batch_id="a" * 64,
        run_id="run_1234567812344abc9234567890abcdef",
    )
    emitter = TerminalEmitter(emitted.append)

    emitter.emit(terminal_event, summary)

    assert emitter.emitted is True
    assert emitted == [
        terminal_event,
        {"event": "run_summary", **summary.to_dict()},
    ]
    with pytest.raises(RuntimeError, match="terminal output has already been emitted"):
        emitter.emit(terminal_event, summary)
    assert len(emitted) == 2


def test_terminal_emitter_is_one_shot_without_an_output_sink() -> None:
    summary = RunSummary(
        records_read=0,
        records_accepted=0,
        records_rejected=0,
        pages_fetched=0,
        bytes_written=0,
        retry_count=0,
        duplicate_count=0,
        duration_seconds=0,
        source_date=date(2026, 9, 1),
        batch_id="a" * 64,
        run_id="run_1234567812344abc9234567890abcdef",
    )
    emitter = TerminalEmitter(None)

    emitter.emit({"event": "file_ingestion", "outcome": "failed"}, summary)

    assert emitter.emitted is True
    with pytest.raises(RuntimeError, match="terminal output has already been emitted"):
        emitter.emit({"event": "file_ingestion", "outcome": "failed"}, summary)


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
