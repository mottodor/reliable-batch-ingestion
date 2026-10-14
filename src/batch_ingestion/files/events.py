"""Secret-free terminal event contract for supplier-file ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

from batch_ingestion.identifiers import is_batch_id, is_run_id, normalize_source_name

from .contracts import FileFormat
from .diagnostics import BoundedDiagnostics, DuplicateClass, RejectionCode


class FileOutcome(StrEnum):
    """Business outcome for one completely validated delivery."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class FileIngestionEvent:
    """Bounded terminal event containing keys and value-free validation details."""

    outcome: FileOutcome
    rejection_code: RejectionCode | None
    diagnostics: BoundedDiagnostics
    source_name: str
    source_date: date
    batch_id: str
    run_id: str
    landing_key: str
    raw_key: str
    manifest_key: str
    disposition_key: str
    original_file_name: str
    file_format: FileFormat
    duplicate_class: DuplicateClass

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, FileOutcome):
            raise TypeError("outcome must be a FileOutcome")
        if self.outcome is FileOutcome.ACCEPTED and self.rejection_code is not None:
            raise ValueError("accepted outcome must not carry a rejection_code")
        if self.outcome is FileOutcome.REJECTED and not isinstance(
            self.rejection_code, RejectionCode
        ):
            raise ValueError("rejected outcome must carry a rejection_code")
        if normalize_source_name(self.source_name) != self.source_name:
            raise ValueError("source_name must already be normalized")
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise TypeError("source_date must be a date")
        if not is_batch_id(self.batch_id):
            raise ValueError("batch_id must be 64 lowercase hexadecimal characters")
        if not is_run_id(self.run_id):
            raise ValueError("run_id must use the run_<32 lowercase hex> format")
        if not isinstance(self.file_format, FileFormat):
            raise TypeError("file_format must be a FileFormat")
        if not isinstance(self.duplicate_class, DuplicateClass):
            raise TypeError("duplicate_class must be a DuplicateClass")

    def to_dict(self) -> dict[str, object]:
        """Return stable JSON-compatible event fields with explicit nullable reason."""
        return {
            "event": "file_ingestion",
            "outcome": self.outcome.value,
            "rejection_code": (
                self.rejection_code.value if self.rejection_code is not None else None
            ),
            "details": self.diagnostics.to_list(),
            "details_truncated": self.diagnostics.truncated,
            "source_name": self.source_name,
            "source_date": self.source_date.isoformat(),
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "landing_key": self.landing_key,
            "raw_key": self.raw_key,
            "manifest_key": self.manifest_key,
            "disposition_key": self.disposition_key,
            "original_file_name": self.original_file_name,
            "format": self.file_format.value,
            "duplicate_class": self.duplicate_class.value,
        }

    def to_json(self) -> str:
        """Serialize the event as canonical compact JSON."""
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
