"""Typed payload-manifest schema and lifecycle contract."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Final

from .identifiers import (
    SourceType,
    is_batch_id,
    is_run_id,
    normalize_payload_name,
    normalize_source_name,
)

MANIFEST_SCHEMA_VERSION: Final = 1


class ManifestStatus(StrEnum):
    """Lifecycle states for one raw-payload manifest."""

    CREATED = "created"
    RAW_STORED = "raw_stored"
    SUCCEEDED = "succeeded"
    REJECTED = "rejected"
    DUPLICATE = "duplicate"
    FAILED = "failed"


class ManifestOutcome(StrEnum):
    """Processing outcomes mapped to persisted manifest states."""

    PRESERVED = "preserved"
    ACCEPTED = "accepted"
    VALIDATION_ERROR = "validation_error"
    DUPLICATE = "duplicate"
    SYSTEM_ERROR = "system_error"


TERMINAL_STATUSES: Final[frozenset[ManifestStatus]] = frozenset(
    {
        ManifestStatus.SUCCEEDED,
        ManifestStatus.REJECTED,
        ManifestStatus.DUPLICATE,
        ManifestStatus.FAILED,
    }
)

_TRANSITIONS: Final[dict[ManifestStatus, frozenset[ManifestStatus]]] = {
    ManifestStatus.CREATED: frozenset(
        {
            ManifestStatus.RAW_STORED,
            ManifestStatus.REJECTED,
            ManifestStatus.DUPLICATE,
            ManifestStatus.FAILED,
        }
    ),
    ManifestStatus.RAW_STORED: frozenset(
        {
            ManifestStatus.SUCCEEDED,
            ManifestStatus.REJECTED,
            ManifestStatus.DUPLICATE,
            ManifestStatus.FAILED,
        }
    ),
    ManifestStatus.SUCCEEDED: frozenset(),
    ManifestStatus.REJECTED: frozenset(),
    ManifestStatus.DUPLICATE: frozenset(),
    ManifestStatus.FAILED: frozenset(),
}

_OUTCOME_STATUSES: Final[dict[ManifestOutcome, ManifestStatus]] = {
    ManifestOutcome.PRESERVED: ManifestStatus.RAW_STORED,
    ManifestOutcome.ACCEPTED: ManifestStatus.SUCCEEDED,
    ManifestOutcome.VALIDATION_ERROR: ManifestStatus.REJECTED,
    ManifestOutcome.DUPLICATE: ManifestStatus.DUPLICATE,
    ManifestOutcome.SYSTEM_ERROR: ManifestStatus.FAILED,
}

_MANIFEST_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "source_name",
        "source_type",
        "source_date",
        "batch_id",
        "run_id",
        "object_key",
        "original_file_name",
        "content_type",
        "content_length",
        "checksum_sha256",
        "schema_version",
        "extracted_at",
        "status",
        "attempt",
    }
)


@dataclass(frozen=True, slots=True)
class Manifest:
    """Versioned metadata for exactly one immutable raw payload object."""

    source_name: str
    source_type: SourceType
    source_date: date
    batch_id: str
    run_id: str
    object_key: str
    original_file_name: str | None
    content_type: str
    content_length: int
    checksum_sha256: str
    schema_version: int
    extracted_at: datetime
    status: ManifestStatus
    attempt: int

    def __post_init__(self) -> None:
        if normalize_source_name(self.source_name) != self.source_name:
            raise ValueError("source_name must already be normalized")
        if not isinstance(self.source_type, SourceType):
            raise TypeError("source_type must be a SourceType")
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise TypeError("source_date must be a date")
        if not is_batch_id(self.batch_id):
            raise ValueError("batch_id must be 64 lowercase hexadecimal characters")
        if not is_run_id(self.run_id):
            raise ValueError("run_id must use the run_<32 lowercase hex> format")

        expected_prefix = (
            f"raw/{self.source_type.value}/{self.source_name}/"
            f"source_date={self.source_date.isoformat()}/batch_id={self.batch_id}/"
        )
        if not self.object_key.startswith(expected_prefix) or self.object_key == expected_prefix:
            raise ValueError("object_key does not match the manifest identity")
        payload_name = self.object_key.removeprefix(expected_prefix)
        try:
            canonical_payload_name = normalize_payload_name(payload_name)
        except ValueError as error:
            raise ValueError("object_key must end in one canonical payload-name segment") from error
        if canonical_payload_name != payload_name:
            raise ValueError("object_key must end in one canonical payload-name segment")

        if self.source_type is SourceType.FILE:
            if self.original_file_name is None or not self.original_file_name.strip():
                raise ValueError("original_file_name is required for file sources")
            try:
                normalized_file_name = normalize_payload_name(self.original_file_name)
            except ValueError as error:
                raise ValueError(
                    "original_file_name must be one normalized path segment"
                ) from error
            if normalized_file_name != self.original_file_name:
                raise ValueError("original_file_name must be one normalized path segment")
        elif self.original_file_name is not None:
            raise ValueError("original_file_name must be null for non-file sources")

        if not self.content_type.strip() or self.content_type != self.content_type.strip():
            raise ValueError("content_type must be a normalized non-empty value")
        _validate_non_negative_int(self.content_length, field="content_length")
        if len(self.checksum_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.checksum_sha256
        ):
            raise ValueError("checksum_sha256 must be 64 lowercase hexadecimal characters")
        if self.schema_version != MANIFEST_SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {MANIFEST_SCHEMA_VERSION}")
        if not isinstance(self.status, ManifestStatus):
            raise TypeError("status must be a ManifestStatus")
        _validate_positive_int(self.attempt, field="attempt")

        if not isinstance(self.extracted_at, datetime):
            raise TypeError("extracted_at must be a datetime")
        if self.extracted_at.tzinfo is None or self.extracted_at.utcoffset() is None:
            raise ValueError("extracted_at must be timezone-aware")
        object.__setattr__(self, "extracted_at", self.extracted_at.astimezone(UTC))

    def to_dict(self) -> dict[str, object]:
        """Return the complete JSON-compatible schema with stable UTC formatting."""
        return {
            "source_name": self.source_name,
            "source_type": self.source_type.value,
            "source_date": self.source_date.isoformat(),
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "object_key": self.object_key,
            "original_file_name": self.original_file_name,
            "content_type": self.content_type,
            "content_length": self.content_length,
            "checksum_sha256": self.checksum_sha256,
            "schema_version": self.schema_version,
            "extracted_at": self.extracted_at.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "status": self.status.value,
            "attempt": self.attempt,
        }

    def to_json(self) -> str:
        """Serialize as canonical compact JSON."""
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "Manifest":
        """Parse and strictly validate one manifest dictionary."""
        supplied_fields = set(value)
        missing_fields = _MANIFEST_FIELDS - supplied_fields
        unexpected_fields = supplied_fields - _MANIFEST_FIELDS
        if missing_fields:
            raise ValueError(f"manifest has missing fields: {sorted(missing_fields)}")
        if unexpected_fields:
            raise ValueError(f"manifest has unexpected fields: {sorted(unexpected_fields)}")

        original_file_name_value = value["original_file_name"]
        if original_file_name_value is not None and not isinstance(original_file_name_value, str):
            raise TypeError("original_file_name must be a string or null")

        try:
            source_type = SourceType(_string(value, "source_type"))
        except ValueError as error:
            raise ValueError("source_type is unsupported") from error
        try:
            status = ManifestStatus(_string(value, "status"))
        except ValueError as error:
            raise ValueError("status is unsupported") from error

        source_date_value = _string(value, "source_date")
        try:
            parsed_source_date = date.fromisoformat(source_date_value)
        except ValueError as error:
            raise ValueError("source_date must use YYYY-MM-DD") from error
        if parsed_source_date.isoformat() != source_date_value:
            raise ValueError("source_date must use YYYY-MM-DD")

        extracted_at_value = _string(value, "extracted_at")
        try:
            parsed_extracted_at = datetime.fromisoformat(extracted_at_value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("extracted_at must be an ISO 8601 timestamp") from error

        return cls(
            source_name=_string(value, "source_name"),
            source_type=source_type,
            source_date=parsed_source_date,
            batch_id=_string(value, "batch_id"),
            run_id=_string(value, "run_id"),
            object_key=_string(value, "object_key"),
            original_file_name=original_file_name_value,
            content_type=_string(value, "content_type"),
            content_length=_integer(value, "content_length"),
            checksum_sha256=_string(value, "checksum_sha256"),
            schema_version=_integer(value, "schema_version"),
            extracted_at=parsed_extracted_at,
            status=status,
            attempt=_integer(value, "attempt"),
        )

    @classmethod
    def from_json(cls, value: str) -> "Manifest":
        """Parse a JSON object into a strictly validated manifest."""
        try:
            parsed: object = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError("manifest JSON is invalid") from error
        if not isinstance(parsed, dict):
            raise TypeError("manifest JSON must contain an object")
        if not all(isinstance(key, str) for key in parsed):
            raise TypeError("manifest JSON keys must be strings")
        return cls.from_dict(parsed)


def can_transition(current: ManifestStatus, target: ManifestStatus) -> bool:
    """Return whether a lifecycle transition is allowed; same-state writes are idempotent."""
    return current is target or target in _TRANSITIONS[current]


def validate_status_transition(current: ManifestStatus, target: ManifestStatus) -> None:
    """Raise when a manifest lifecycle transition is invalid."""
    if not can_transition(current, target):
        raise ValueError(f"invalid manifest status transition: {current.value} -> {target.value}")


def status_for_outcome(outcome: ManifestOutcome) -> ManifestStatus:
    """Map a processing outcome to its persisted status."""
    return _OUTCOME_STATUSES[outcome]


def _string(value: Mapping[str, object], field: str) -> str:
    item = value[field]
    if not isinstance(item, str):
        raise TypeError(f"{field} must be a string")
    return item


def _integer(value: Mapping[str, object], field: str) -> int:
    item = value[field]
    if isinstance(item, bool) or not isinstance(item, int):
        raise TypeError(f"{field} must be an integer")
    return item


def _validate_non_negative_int(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


def _validate_positive_int(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
