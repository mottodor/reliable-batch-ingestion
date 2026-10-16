"""Secret-free structured events, handled errors, and snapshot metric adapters."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from batch_ingestion.identifiers import is_batch_id, is_run_id
from batch_ingestion.summary import RunSummary

from .contracts import SnapshotDescriptor


class SnapshotEventType(StrEnum):
    STARTED = "snapshot_started"
    CHUNK_COMPLETED = "snapshot_chunk_completed"
    SUCCEEDED = "snapshot_succeeded"
    FAILED = "snapshot_failed"


class SnapshotChunkStatus(StrEnum):
    """Observable result of reconciling one deterministic chunk."""

    CREATED = "created"
    PAYLOAD_REUSED = "payload_reused"
    COMPLETE_REUSED = "complete_reused"


class SnapshotErrorCode(StrEnum):
    """Stable handled-failure classes with no source exception text."""

    SOURCE_SNAPSHOT_CHANGED = "source_snapshot_changed"
    POSTGRES_CONNECTION_FAILED = "postgres_connection_failed"
    SOURCE_CONTRACT_FAILED = "source_contract_failed"
    SERIALIZATION_FAILED = "serialization_failed"
    RECONCILIATION_FAILED = "reconciliation_failed"
    OBJECT_CONFLICT = "object_conflict"
    STORAGE_FAILED = "storage_failed"
    DEADLINE_EXCEEDED = "deadline_exceeded"


class SnapshotHandledError(RuntimeError):
    """Handled snapshot failure with optional source-owned terminal output."""

    def __init__(self, code: SnapshotErrorCode) -> None:
        if not isinstance(code, SnapshotErrorCode):
            raise TypeError("code must be a SnapshotErrorCode")
        self.code = code
        self.event: SnapshotEvent | None = None
        self.summary: RunSummary | None = None
        super().__init__(code.value)

    def attach_terminal(self, event: SnapshotEvent, summary: RunSummary) -> None:
        """Record the exact terminal pair emitted by the source service."""
        if self.event is not None or self.summary is not None:
            raise RuntimeError("snapshot terminal output is already attached")
        if event.event_type is not SnapshotEventType.FAILED or event.error_code is not self.code:
            raise ValueError("terminal event must be a matching snapshot failure")
        if (
            summary.source_date != event.descriptor.source_date
            or summary.batch_id != event.batch_id
            or summary.run_id != event.run_id
        ):
            raise ValueError("terminal summary identifiers must match the snapshot event")
        self.event = event
        self.summary = summary


@dataclass(frozen=True, slots=True)
class SnapshotCounters:
    """Snapshot-specific counters; inapplicable summary metrics stay fixed at zero."""

    records_read: int = 0
    records_accepted: int = 0
    bytes_written: int = 0
    duplicate_count: int = 0

    def __post_init__(self) -> None:
        for name, value in self.to_dict().items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

    def to_dict(self) -> dict[str, int]:
        return {
            "records_read": self.records_read,
            "records_accepted": self.records_accepted,
            "bytes_written": self.bytes_written,
            "duplicate_count": self.duplicate_count,
        }

    def to_run_summary(
        self,
        *,
        duration_seconds: float,
        source_date: date,
        batch_id: str,
        run_id: str,
    ) -> RunSummary:
        """Map snapshot metrics to the unchanged repository-wide summary schema."""
        return RunSummary(
            records_read=self.records_read,
            records_accepted=self.records_accepted,
            records_rejected=0,
            pages_fetched=0,
            bytes_written=self.bytes_written,
            retry_count=0,
            duplicate_count=self.duplicate_count,
            duration_seconds=duration_seconds,
            source_date=source_date,
            batch_id=batch_id,
            run_id=run_id,
        )


@dataclass(frozen=True, slots=True)
class SnapshotEvent:
    """One bounded event whose fields cannot carry DSNs, SQL, or row values."""

    event_type: SnapshotEventType
    descriptor: SnapshotDescriptor
    chunk_rows: int
    batch_id: str
    run_id: str
    counters: SnapshotCounters | None = None
    chunk_ordinal: int | None = None
    object_key: str | None = None
    chunk_status: SnapshotChunkStatus | None = None
    chunk_records: int | None = None
    content_length: int | None = None
    error_code: SnapshotErrorCode | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.event_type, SnapshotEventType):
            raise TypeError("event_type must be a SnapshotEventType")
        expected_batch_id = self.descriptor.batch_id(chunk_rows=self.chunk_rows)
        if not is_batch_id(self.batch_id) or self.batch_id != expected_batch_id:
            raise ValueError("batch_id must match the snapshot descriptor and chunk_rows")
        if not is_run_id(self.run_id):
            raise ValueError("run_id must use the run_<32 lowercase hex> format")

        chunk_fields = (
            self.chunk_ordinal,
            self.object_key,
            self.chunk_status,
            self.chunk_records,
            self.content_length,
        )
        if self.event_type is SnapshotEventType.CHUNK_COMPLETED:
            if any(value is None for value in chunk_fields):
                raise ValueError("chunk completion event requires every chunk field")
            assert self.chunk_ordinal is not None
            assert self.object_key is not None
            assert self.chunk_records is not None
            assert self.content_length is not None
            if not isinstance(self.chunk_status, SnapshotChunkStatus):
                raise TypeError("chunk_status must be a SnapshotChunkStatus")
            expected_key = self.descriptor.raw_key(
                chunk_ordinal=self.chunk_ordinal,
                chunk_rows=self.chunk_rows,
            )
            if self.object_key != expected_key:
                raise ValueError("object_key must match the descriptor chunk")
            _validate_non_negative(self.chunk_records, field="chunk_records")
            _validate_non_negative(self.content_length, field="content_length")
        elif any(value is not None for value in chunk_fields):
            raise ValueError("non-chunk event must not carry chunk fields")

        if self.event_type is SnapshotEventType.FAILED:
            if not isinstance(self.error_code, SnapshotErrorCode):
                raise ValueError("failed event requires an error_code")
        elif self.error_code is not None:
            raise ValueError("non-failed event must not carry an error_code")

        if self.event_type in {SnapshotEventType.SUCCEEDED, SnapshotEventType.FAILED}:
            if not isinstance(self.counters, SnapshotCounters):
                raise ValueError("terminal event requires counters")
        elif self.counters is not None:
            raise ValueError("non-terminal event must not carry counters")

    @classmethod
    def started(
        cls,
        *,
        descriptor: SnapshotDescriptor,
        chunk_rows: int,
        batch_id: str,
        run_id: str,
    ) -> SnapshotEvent:
        return cls(SnapshotEventType.STARTED, descriptor, chunk_rows, batch_id, run_id)

    @classmethod
    def chunk_completed(
        cls,
        *,
        descriptor: SnapshotDescriptor,
        chunk_rows: int,
        batch_id: str,
        run_id: str,
        chunk_ordinal: int,
        object_key: str,
        chunk_status: SnapshotChunkStatus,
        records: int,
        content_length: int,
    ) -> SnapshotEvent:
        return cls(
            SnapshotEventType.CHUNK_COMPLETED,
            descriptor,
            chunk_rows,
            batch_id,
            run_id,
            chunk_ordinal=chunk_ordinal,
            object_key=object_key,
            chunk_status=chunk_status,
            chunk_records=records,
            content_length=content_length,
        )

    @classmethod
    def succeeded(
        cls,
        *,
        descriptor: SnapshotDescriptor,
        chunk_rows: int,
        batch_id: str,
        run_id: str,
        counters: SnapshotCounters,
    ) -> SnapshotEvent:
        return cls(
            SnapshotEventType.SUCCEEDED,
            descriptor,
            chunk_rows,
            batch_id,
            run_id,
            counters=counters,
        )

    @classmethod
    def failed(
        cls,
        *,
        descriptor: SnapshotDescriptor,
        chunk_rows: int,
        batch_id: str,
        run_id: str,
        error_code: SnapshotErrorCode,
        counters: SnapshotCounters,
    ) -> SnapshotEvent:
        return cls(
            SnapshotEventType.FAILED,
            descriptor,
            chunk_rows,
            batch_id,
            run_id,
            counters=counters,
            error_code=error_code,
        )

    def to_dict(self) -> dict[str, object]:
        value: dict[str, object] = {
            "event": self.event_type.value,
            "source_name": self.descriptor.source_name,
            "source_date": self.descriptor.source_date.isoformat(),
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "chunk_rows": self.chunk_rows,
            **self.descriptor.resume_fields(),
        }
        if self.event_type is SnapshotEventType.CHUNK_COMPLETED:
            assert self.chunk_status is not None
            value.update(
                {
                    "chunk_ordinal": self.chunk_ordinal,
                    "object_key": self.object_key,
                    "chunk_status": self.chunk_status.value,
                    "records": self.chunk_records,
                    "content_length": self.content_length,
                }
            )
        if self.counters is not None:
            value.update(self.counters.to_dict())
        if self.error_code is not None:
            value["error_code"] = self.error_code.value
        return value

    def to_json(self) -> str:
        """Serialize as compact, key-sorted newline-safe JSON."""
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)


def _validate_non_negative(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
