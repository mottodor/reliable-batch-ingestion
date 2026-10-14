"""Pure logical descriptor and deterministic key contracts for order snapshots."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from batch_ingestion.identifiers import (
    BatchIdentity,
    SourceType,
    derive_batch_id,
    manifest_key_for,
    raw_object_key,
)

ORDERS_SOURCE_NAME = "orders"
SNAPSHOT_CONTRACT = "omni-retail-orders-parquet-v1"
PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
MAX_CHUNK_ORDINAL = 9_999

_CANONICAL_UTC_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def is_supported_source(source_name: str) -> bool:
    """Return whether a source is part of the fixed snapshot allow-list."""
    return source_name == ORDERS_SOURCE_NAME


def canonical_utc_timestamp(value: datetime) -> str:
    """Render an aware UTC timestamp in the identity and resume format."""
    _validate_utc_datetime(value, field="timestamp")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_canonical_utc_timestamp(value: str) -> datetime:
    """Parse a timestamp only when it already uses the exact canonical UTC form."""
    if not isinstance(value, str) or _CANONICAL_UTC_PATTERN.fullmatch(value) is None:
        raise ValueError("timestamp must use canonical UTC YYYY-MM-DDTHH:MM:SS.ffffffZ form")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError(
            "timestamp must use canonical UTC YYYY-MM-DDTHH:MM:SS.ffffffZ form"
        ) from error
    if canonical_utc_timestamp(parsed) != value:
        raise ValueError("timestamp must use canonical UTC YYYY-MM-DDTHH:MM:SS.ffffffZ form")
    return parsed


def chunk_payload_name(chunk_ordinal: int) -> str:
    """Return the canonical one-based four-digit Parquet chunk name."""
    if (
        isinstance(chunk_ordinal, bool)
        or not isinstance(chunk_ordinal, int)
        or not 1 <= chunk_ordinal <= MAX_CHUNK_ORDINAL
    ):
        raise ValueError(f"chunk_ordinal must be between 1 and {MAX_CHUNK_ORDINAL}")
    return f"chunk-{chunk_ordinal:04d}.parquet"


@dataclass(frozen=True, slots=True)
class SnapshotDescriptor:
    """Captured logical bounds for one daily order snapshot."""

    source_name: str
    source_date: date
    upper_updated_at: datetime | None
    upper_order_id: int | None
    expected_record_count: int

    def __post_init__(self) -> None:
        if not is_supported_source(self.source_name):
            raise ValueError(f"unsupported snapshot source: {self.source_name!r}")
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise TypeError("source_date must be a date")
        if (
            isinstance(self.expected_record_count, bool)
            or not isinstance(self.expected_record_count, int)
            or self.expected_record_count < 0
        ):
            raise ValueError("expected_record_count must be a non-negative integer")

        if self.expected_record_count == 0:
            if self.upper_updated_at is not None or self.upper_order_id is not None:
                raise ValueError("empty descriptor must not carry an upper tuple")
            return

        if self.upper_updated_at is None or self.upper_order_id is None:
            raise ValueError("non-empty descriptor requires a complete upper tuple")
        _validate_utc_datetime(self.upper_updated_at, field="upper_updated_at")
        _validate_int64(self.upper_order_id, field="upper_order_id")
        if not self.window_start <= self.upper_updated_at < self.window_end_exclusive:
            raise ValueError("upper_updated_at must lie inside the source-date UTC window")

    @classmethod
    def create(
        cls,
        *,
        source_name: str,
        source_date: date,
        upper_updated_at: datetime | None,
        upper_order_id: int | None,
        expected_record_count: int,
    ) -> SnapshotDescriptor:
        """Create a validated captured descriptor without reading runtime state."""
        return cls(
            source_name=source_name,
            source_date=source_date,
            upper_updated_at=upper_updated_at,
            upper_order_id=upper_order_id,
            expected_record_count=expected_record_count,
        )

    @classmethod
    def empty(cls, *, source_name: str, source_date: date) -> SnapshotDescriptor:
        """Create an explicit zero-row descriptor whose bound remains empty on resume."""
        return cls(
            source_name=source_name,
            source_date=source_date,
            upper_updated_at=None,
            upper_order_id=None,
            expected_record_count=0,
        )

    @classmethod
    def from_resume_arguments(
        cls,
        *,
        source_name: str,
        source_date: date,
        upper_updated_at: str | None,
        upper_order_id: int | None,
        expected_record_count: int | None,
        empty: bool,
    ) -> SnapshotDescriptor:
        """Validate the mutually exclusive empty and non-empty resume forms."""
        if not isinstance(empty, bool):
            raise TypeError("empty must be a boolean")
        values = (upper_updated_at, upper_order_id, expected_record_count)
        if empty:
            if any(value is not None for value in values):
                raise ValueError("empty resume descriptor must not include upper/count fields")
            return cls.empty(source_name=source_name, source_date=source_date)
        if any(value is None for value in values):
            raise ValueError("non-empty resume descriptor requires upper/count fields")
        assert upper_updated_at is not None
        assert upper_order_id is not None
        assert expected_record_count is not None
        return cls.create(
            source_name=source_name,
            source_date=source_date,
            upper_updated_at=parse_canonical_utc_timestamp(upper_updated_at),
            upper_order_id=upper_order_id,
            expected_record_count=expected_record_count,
        )

    @property
    def window_start(self) -> datetime:
        """Inclusive midnight UTC bound for the selected source date."""
        return datetime.combine(self.source_date, time.min, tzinfo=UTC)

    @property
    def window_end_exclusive(self) -> datetime:
        """Exclusive midnight UTC bound for the day after the source date."""
        return self.window_start + timedelta(days=1)

    @property
    def canonical_window_start(self) -> str:
        return canonical_utc_timestamp(self.window_start)

    @property
    def canonical_window_end_exclusive(self) -> str:
        return canonical_utc_timestamp(self.window_end_exclusive)

    @property
    def canonical_upper_updated_at(self) -> str | None:
        if self.upper_updated_at is None:
            return None
        return canonical_utc_timestamp(self.upper_updated_at)

    @property
    def is_empty(self) -> bool:
        return self.expected_record_count == 0

    def resume_fields(self) -> dict[str, object]:
        """Return the complete public descriptor needed by a later resume attempt."""
        return {
            "empty": self.is_empty,
            "upper_updated_at": self.canonical_upper_updated_at,
            "upper_order_id": self.upper_order_id,
            "expected_record_count": self.expected_record_count,
        }

    def identity(
        self,
        *,
        chunk_rows: int,
        snapshot_contract: str = SNAPSHOT_CONTRACT,
    ) -> BatchIdentity:
        """Build the complete deterministic logical identity for this snapshot."""
        _validate_positive_int(chunk_rows, field="chunk_rows")
        self.expected_chunk_count(chunk_rows=chunk_rows)
        if not isinstance(snapshot_contract, str) or snapshot_contract.strip() != snapshot_contract:
            raise ValueError("snapshot_contract must be a non-empty canonical string")
        if not snapshot_contract:
            raise ValueError("snapshot_contract must be a non-empty canonical string")
        return BatchIdentity.create(
            source_name=self.source_name,
            source_type=SourceType.POSTGRES_SNAPSHOT,
            source_date=self.source_date,
            parameters={
                "snapshot-contract": snapshot_contract,
                "window-start": self.canonical_window_start,
                "window-end-exclusive": self.canonical_window_end_exclusive,
                "upper-updated-at": self.canonical_upper_updated_at or "none",
                "upper-order-id": (
                    str(self.upper_order_id) if self.upper_order_id is not None else "none"
                ),
                "expected-record-count": str(self.expected_record_count),
                "chunk-rows": str(chunk_rows),
            },
        )

    def batch_id(
        self,
        *,
        chunk_rows: int,
        snapshot_contract: str = SNAPSHOT_CONTRACT,
    ) -> str:
        return derive_batch_id(
            self.identity(chunk_rows=chunk_rows, snapshot_contract=snapshot_contract)
        )

    def expected_chunk_count(self, *, chunk_rows: int) -> int:
        """Precompute exact inventory cardinality, including one empty chunk."""
        _validate_positive_int(chunk_rows, field="chunk_rows")
        chunk_count = max(1, (self.expected_record_count + chunk_rows - 1) // chunk_rows)
        if chunk_count > MAX_CHUNK_ORDINAL:
            raise ValueError(
                f"descriptor requires more than {MAX_CHUNK_ORDINAL} addressable chunks"
            )
        return chunk_count

    def raw_key(
        self,
        *,
        chunk_ordinal: int,
        chunk_rows: int,
        snapshot_contract: str = SNAPSHOT_CONTRACT,
    ) -> str:
        payload_name = chunk_payload_name(chunk_ordinal)
        if chunk_ordinal > self.expected_chunk_count(chunk_rows=chunk_rows):
            raise ValueError("chunk_ordinal exceeds the descriptor's expected chunk count")
        return raw_object_key(
            self.identity(chunk_rows=chunk_rows, snapshot_contract=snapshot_contract),
            payload_name,
        )

    def manifest_key(
        self,
        *,
        chunk_ordinal: int,
        chunk_rows: int,
        snapshot_contract: str = SNAPSHOT_CONTRACT,
    ) -> str:
        return manifest_key_for(
            self.raw_key(
                chunk_ordinal=chunk_ordinal,
                chunk_rows=chunk_rows,
                snapshot_contract=snapshot_contract,
            )
        )


def _validate_positive_int(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")


def _validate_int64(value: int, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not _INT64_MIN <= value <= _INT64_MAX
    ):
        raise ValueError(f"{field} must be a signed 64-bit integer")


def _validate_utc_datetime(value: datetime, *, field: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be timezone-aware UTC")
