"""Stable whole-file rejection, duplicate, diagnostic, and counter contracts."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from .contracts import DeliveryDescriptor, is_supported_source


class RejectionCode(StrEnum):
    """Stable primary file-rejection codes in semantic precedence order."""

    UNSUPPORTED_SOURCE = "unsupported_source"
    INVALID_FILE_NAME = "invalid_file_name"
    SOURCE_DATE_MISMATCH = "source_date_mismatch"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    MALFORMED_FILE = "malformed_file"
    SCHEMA_MISMATCH = "schema_mismatch"
    EMPTY_FILE = "empty_file"
    MISSING_REQUIRED_VALUE = "missing_required_value"
    INVALID_FIELD_TYPE = "invalid_field_type"
    BUSINESS_RULE_VIOLATION = "business_rule_violation"
    RECORD_LIMIT_EXCEEDED = "record_limit_exceeded"


REJECTION_PRECEDENCE: Final[tuple[RejectionCode, ...]] = tuple(RejectionCode)
_PRECEDENCE_INDEX: Final = {code: index for index, code in enumerate(REJECTION_PRECEDENCE)}
_SUPPLIER_FIELDS: Final[tuple[str, ...]] = (
    "supplier_id",
    "product_id",
    "price_minor_units",
    "currency",
    "effective_date",
)
_FIELD_INDEX: Final = {field: index for index, field in enumerate(_SUPPLIER_FIELDS)}


class DuplicateClass(StrEnum):
    """Classification of this delivery relative to an existing terminal manifest."""

    NEW_DELIVERY = "new_delivery"
    SAME_NAME_REDELIVERY = "same_name_redelivery"
    RENAMED_CONTENT = "renamed_content"


@dataclass(frozen=True, slots=True)
class FileDiagnostic:
    """One value-free bounded correction detail."""

    code: RejectionCode
    record_number: int | None = None
    field: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.code, RejectionCode):
            raise TypeError("code must be a RejectionCode")
        if self.record_number is not None and (
            isinstance(self.record_number, bool)
            or not isinstance(self.record_number, int)
            or self.record_number <= 0
        ):
            raise ValueError("record_number must be a positive integer or null")
        if self.field is not None and self.field not in _FIELD_INDEX:
            raise ValueError("field must be a canonical supplier-price field or null")

    def to_dict(self) -> dict[str, object]:
        """Return only stable code, record position, and canonical field name."""
        return {
            "code": self.code.value,
            "record_number": self.record_number,
            "field": self.field,
        }


@dataclass(frozen=True, slots=True)
class BoundedDiagnostics:
    """Ordered details plus an explicit omitted-detail indicator."""

    details: tuple[FileDiagnostic, ...]
    truncated: bool

    def to_list(self) -> list[dict[str, object]]:
        """Return JSON-compatible value-free diagnostic dictionaries."""
        return [detail.to_dict() for detail in self.details]


@dataclass(frozen=True, slots=True)
class FileRecordCounters:
    """Whole-file record counters before construction of a full RunSummary."""

    records_read: int
    records_accepted: int
    records_rejected: int


def delivery_rejection_codes(delivery: DeliveryDescriptor) -> tuple[RejectionCode, ...]:
    """Return applicable admission/business codes in fixed primary-code precedence."""
    codes: list[RejectionCode] = []
    if not is_supported_source(delivery.source_name):
        codes.append(RejectionCode.UNSUPPORTED_SOURCE)
    if not delivery.file_name_valid:
        codes.append(RejectionCode.INVALID_FILE_NAME)
    elif delivery.filename_date != delivery.source_date:
        codes.append(RejectionCode.SOURCE_DATE_MISMATCH)
    if delivery.expected_sha256 is not None and delivery.expected_sha256 != delivery.actual_sha256:
        codes.append(RejectionCode.CHECKSUM_MISMATCH)
    return tuple(codes)


def primary_rejection_code(codes: Iterable[RejectionCode]) -> RejectionCode | None:
    """Select the earliest stable code independently of discovery order."""
    supplied = set(codes)
    if not supplied:
        return None
    if not all(isinstance(code, RejectionCode) for code in supplied):
        raise TypeError("all rejection codes must be RejectionCode values")
    return min(supplied, key=_PRECEDENCE_INDEX.__getitem__)


def bounded_diagnostics(
    diagnostics: Iterable[FileDiagnostic],
    *,
    max_details: int,
) -> BoundedDiagnostics:
    """Sort details deterministically and retain no more than the configured bound."""
    if isinstance(max_details, bool) or not isinstance(max_details, int) or max_details <= 0:
        raise ValueError("max_details must be a positive integer")
    ordered = sorted(diagnostics, key=_diagnostic_sort_key)
    return BoundedDiagnostics(
        details=tuple(ordered[:max_details]),
        truncated=len(ordered) > max_details,
    )


def classify_duplicate(
    existing_original_file_name: str | None,
    current_name: str,
) -> DuplicateClass:
    """Classify filename provenance without mutating the original manifest."""
    if existing_original_file_name is None:
        return DuplicateClass.NEW_DELIVERY
    if existing_original_file_name == current_name:
        return DuplicateClass.SAME_NAME_REDELIVERY
    return DuplicateClass.RENAMED_CONTENT


def file_record_counters(*, records_read: int, accepted: bool) -> FileRecordCounters:
    """Apply ADR 0002 whole-file acceptance semantics to trusted record boundaries."""
    if isinstance(records_read, bool) or not isinstance(records_read, int) or records_read < 0:
        raise ValueError("records_read must be a non-negative integer")
    if not isinstance(accepted, bool):
        raise TypeError("accepted must be a boolean")
    return FileRecordCounters(
        records_read=records_read,
        records_accepted=records_read if accepted else 0,
        records_rejected=0 if accepted else records_read,
    )


def _diagnostic_sort_key(detail: FileDiagnostic) -> tuple[int, int, int]:
    record_number = detail.record_number if detail.record_number is not None else 0
    field_index = _FIELD_INDEX[detail.field] if detail.field is not None else -1
    return record_number, field_index, _PRECEDENCE_INDEX[detail.code]
