"""Structured final run-summary contract."""

import json
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime

from .identifiers import is_batch_id, is_run_id


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Observable counters and identifiers emitted after a physical run."""

    records_read: int
    records_accepted: int
    records_rejected: int
    pages_fetched: int
    bytes_written: int
    retry_count: int
    duplicate_count: int
    duration_seconds: float
    source_date: date
    batch_id: str
    run_id: str

    def __post_init__(self) -> None:
        integer_metrics = {
            "records_read": self.records_read,
            "records_accepted": self.records_accepted,
            "records_rejected": self.records_rejected,
            "pages_fetched": self.pages_fetched,
            "bytes_written": self.bytes_written,
            "retry_count": self.retry_count,
            "duplicate_count": self.duplicate_count,
        }
        for name, value in integer_metrics.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be non-negative")

        if (
            isinstance(self.duration_seconds, bool)
            or not isinstance(self.duration_seconds, (int, float))
            or not math.isfinite(self.duration_seconds)
            or self.duration_seconds < 0
        ):
            raise ValueError("duration_seconds must be finite and non-negative")
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise TypeError("source_date must be a date")
        if not is_batch_id(self.batch_id):
            raise ValueError("batch_id must be 64 lowercase hexadecimal characters")
        if not is_run_id(self.run_id):
            raise ValueError("run_id must use the run_<32 lowercase hex> format")

    def to_dict(self) -> dict[str, object]:
        """Return all observable metrics in JSON-compatible form."""
        return {
            "records_read": self.records_read,
            "records_accepted": self.records_accepted,
            "records_rejected": self.records_rejected,
            "pages_fetched": self.pages_fetched,
            "bytes_written": self.bytes_written,
            "retry_count": self.retry_count,
            "duplicate_count": self.duplicate_count,
            "duration_seconds": self.duration_seconds,
            "source_date": self.source_date.isoformat(),
            "batch_id": self.batch_id,
            "run_id": self.run_id,
        }

    def to_json(self) -> str:
        """Serialize as canonical compact JSON."""
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)


@dataclass(slots=True)
class TerminalEmitter:
    """Emit at most one ordered source-terminal-event and run-summary pair."""

    event_sink: Callable[[dict[str, object]], None] | None
    _emitted: bool = field(default=False, init=False)

    @property
    def emitted(self) -> bool:
        """Report whether the one terminal pair has already been committed to the sink."""
        return self._emitted

    def emit(self, event: Mapping[str, object], summary: RunSummary) -> None:
        """Emit one terminal pair, rejecting every duplicate attempt."""
        if self._emitted:
            raise RuntimeError("terminal output has already been emitted")
        self._emitted = True
        if self.event_sink is None:
            return
        self.event_sink(dict(event))
        self.event_sink({"event": "run_summary", **summary.to_dict()})
