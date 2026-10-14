"""Pure supplier-file delivery identity and deterministic key contracts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Final

from batch_ingestion.identifiers import (
    BatchIdentity,
    SourceType,
    derive_batch_id,
    manifest_key_for,
    normalize_payload_name,
    normalize_source_name,
    raw_object_key,
)

SUPPLIER_SOURCE_NAME: Final = "supplier-prices"
DEFAULT_VALIDATION_CONTRACT: Final = "supplier-prices-v1"

_CHECKSUM_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_SUPPORTED_FILE_NAME_PATTERN = re.compile(
    r"supplier-prices(?:-[a-z0-9]+)*-(?P<source_date>[0-9]{4}-[0-9]{2}-[0-9]{2})"
    r"\.(?P<format>csv|json|parquet)",
    flags=re.ASCII,
)
_REASON_PATTERN = re.compile(r"[a-z][a-z0-9_]*", flags=re.ASCII)


class FileFormat(StrEnum):
    """Declared supplier payload formats derived only from a lowercase extension."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    UNKNOWN = "unknown"


_FORMAT_CONTENT_TYPES: Final[dict[FileFormat, str]] = {
    FileFormat.CSV: "text/csv",
    FileFormat.JSON: "application/json",
    FileFormat.PARQUET: "application/vnd.apache.parquet",
    FileFormat.UNKNOWN: "application/octet-stream",
}
_FORMAT_PAYLOAD_NAMES: Final[dict[FileFormat, str]] = {
    FileFormat.CSV: "payload.csv",
    FileFormat.JSON: "payload.json",
    FileFormat.PARQUET: "payload.parquet",
    FileFormat.UNKNOWN: "payload.bin",
}


@dataclass(frozen=True, slots=True)
class DeliveryDescriptor:
    """Admitted delivery metadata used without reading payload or storage state."""

    source_name: str
    source_date: date
    original_file_name: str
    actual_sha256: str
    expected_sha256: str | None
    validation_contract: str
    file_format: FileFormat
    filename_date: date | None
    file_name_valid: bool
    identity: BatchIdentity

    @classmethod
    def create(
        cls,
        *,
        source_name: str,
        source_date: date,
        original_file_name: str,
        actual_sha256: str,
        expected_sha256: str | None = None,
        validation_contract: str = DEFAULT_VALIDATION_CONTRACT,
    ) -> DeliveryDescriptor:
        """Validate admission-safe values and derive the content-based identity."""
        if normalize_source_name(source_name) != source_name:
            raise ValueError("source_name must already be normalized")
        if isinstance(source_date, datetime) or not isinstance(source_date, date):
            raise TypeError("source_date must be a date")
        try:
            represented_name = normalize_payload_name(original_file_name)
        except ValueError as error:
            raise ValueError("original_file_name must be one manifest-safe basename") from error
        if represented_name != original_file_name:
            raise ValueError("original_file_name must be one canonical basename")
        _validate_checksum(actual_sha256, field="actual_sha256")
        if expected_sha256 is not None:
            _validate_checksum(expected_sha256, field="expected_sha256")
        if not validation_contract or validation_contract != validation_contract.strip():
            raise ValueError("validation_contract must be normalized and non-empty")

        file_format = _format_from_filename(original_file_name)
        filename_date, file_name_valid = _inspect_supported_filename(original_file_name)
        expected_identity_value = expected_sha256 if expected_sha256 is not None else "none"
        identity = BatchIdentity.create(
            source_name=source_name,
            source_type=SourceType.FILE,
            source_date=source_date,
            parameters={
                "content-sha256": actual_sha256,
                "declared-format": file_format.value,
                "expected-sha256": expected_identity_value,
                "validation-contract": validation_contract,
            },
        )
        return cls(
            source_name=source_name,
            source_date=source_date,
            original_file_name=original_file_name,
            actual_sha256=actual_sha256,
            expected_sha256=expected_sha256,
            validation_contract=validation_contract,
            file_format=file_format,
            filename_date=filename_date,
            file_name_valid=file_name_valid,
            identity=identity,
        )

    @property
    def batch_id(self) -> str:
        """Return the deterministic logical batch identifier."""
        return derive_batch_id(self.identity)

    @property
    def content_type(self) -> str:
        """Return the stable content type for the declared format."""
        return _FORMAT_CONTENT_TYPES[self.file_format]

    @property
    def payload_name(self) -> str:
        """Return the canonical content-addressed raw payload name."""
        return _FORMAT_PAYLOAD_NAMES[self.file_format]

    @property
    def landing_key(self) -> str:
        """Return the filename-bearing deterministic intake key."""
        expected = self.expected_sha256 if self.expected_sha256 is not None else "none"
        return (
            f"landing/file/{self.source_name}/source_date={self.source_date.isoformat()}/"
            f"format={self.file_format.value}/content_sha256={self.actual_sha256}/"
            f"expected_sha256={expected}/{self.original_file_name}"
        )

    @property
    def raw_key(self) -> str:
        """Return the canonical immutable raw payload key."""
        return raw_object_key(self.identity, self.payload_name)

    @property
    def manifest_key(self) -> str:
        """Return the one-to-one canonical raw manifest key."""
        return manifest_key_for(self.raw_key)

    @property
    def archive_key(self) -> str:
        """Return the accepted final-disposition key."""
        return self._disposition_prefix("archive") + self.payload_name

    def rejected_key(self, reason: str) -> str:
        """Return the rejected final-disposition key carrying one stable reason."""
        reason_value = str(reason)
        if _REASON_PATTERN.fullmatch(reason_value) is None:
            raise ValueError("reason must be a stable machine-readable code")
        return self._disposition_prefix("rejected") + f"reason={reason_value}/{self.payload_name}"

    def _disposition_prefix(self, disposition: str) -> str:
        return (
            f"{disposition}/file/{self.source_name}/source_date={self.source_date.isoformat()}/"
            f"batch_id={self.batch_id}/"
        )


def parse_landing_key(value: str) -> DeliveryDescriptor:
    """Strictly reconstruct all non-secret delivery inputs from one landing key."""
    try:
        parts = value.split("/")
        if len(parts) != 8 or parts[:2] != ["landing", "file"]:
            raise ValueError("unexpected landing key layout")
        source_name = parts[2]
        source_date_text = _prefixed_value(parts[3], "source_date=")
        format_text = _prefixed_value(parts[4], "format=")
        actual_sha256 = _prefixed_value(parts[5], "content_sha256=")
        expected_text = _prefixed_value(parts[6], "expected_sha256=")
        original_file_name = parts[7]
        parsed_date = date.fromisoformat(source_date_text)
        if parsed_date.isoformat() != source_date_text:
            raise ValueError("source date is not canonical")
        expected_sha256 = None if expected_text == "none" else expected_text
        descriptor = DeliveryDescriptor.create(
            source_name=source_name,
            source_date=parsed_date,
            original_file_name=original_file_name,
            actual_sha256=actual_sha256,
            expected_sha256=expected_sha256,
        )
        if descriptor.file_format.value != format_text or descriptor.landing_key != value:
            raise ValueError("landing key components are inconsistent")
    except (TypeError, ValueError) as error:
        raise ValueError("landing key must be one canonical BI-060 key") from error
    return descriptor


def is_supported_source(source_name: str) -> bool:
    """Return whether a normalized source belongs to the production allow-list."""
    return source_name == SUPPLIER_SOURCE_NAME


def _prefixed_value(component: str, prefix: str) -> str:
    if not component.startswith(prefix) or component == prefix:
        raise ValueError("landing key component is missing")
    return component.removeprefix(prefix)


def _format_from_filename(original_file_name: str) -> FileFormat:
    suffix = original_file_name.rpartition(".")[2]
    try:
        return FileFormat(suffix)
    except ValueError:
        return FileFormat.UNKNOWN


def _inspect_supported_filename(original_file_name: str) -> tuple[date | None, bool]:
    match = _SUPPORTED_FILE_NAME_PATTERN.fullmatch(original_file_name)
    if match is None:
        return None, False
    date_text = match.group("source_date")
    try:
        parsed_date = date.fromisoformat(date_text)
    except ValueError:
        return None, False
    if parsed_date.isoformat() != date_text:
        return None, False
    return parsed_date, True


def _validate_checksum(value: str, *, field: str) -> None:
    if not isinstance(value, str) or _CHECKSUM_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field} must be 64 lowercase hexadecimal characters")
