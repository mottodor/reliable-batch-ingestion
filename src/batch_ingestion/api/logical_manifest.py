"""Pure strict contract for the ADR 0003 REST logical-run manifest."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from types import MappingProxyType
from typing import Final, NoReturn, cast

from batch_ingestion.identifiers import (
    BatchIdentity,
    SourceType,
    derive_batch_id,
    is_batch_id,
    is_run_id,
    manifest_key_for,
    raw_object_key,
)

from .sources import api_page_identity, get_source_descriptor, page_payload_name

API_LOGICAL_RUN_MANIFEST_TYPE: Final = "api_logical_run_manifest"
API_LOGICAL_RUN_MANIFEST_SCHEMA_VERSION: Final = 1
API_LOGICAL_RUN_MANIFEST_STATUS: Final = "succeeded"
API_LOGICAL_RUN_MANIFEST_CONTENT_TYPE: Final = "application/json"
API_LOGICAL_RUN_MANIFEST_MAX_BYTES: Final = 8 * 1024 * 1024
API_LOGICAL_RUN_MANIFEST_MAX_PAGES: Final = 9_999
API_LOGICAL_RUN_MANIFEST_MAX_RECORDS: Final = 999_900
API_LOGICAL_RUN_PAGE_SIZE_MAX: Final = 100
API_LOGICAL_RUN_PAYLOAD_LENGTH_MAX: Final = 2**63 - 1

_MANIFEST_FIELDS: Final = frozenset(
    {
        "manifest_type",
        "schema_version",
        "source_name",
        "source_type",
        "source_date",
        "batch_id",
        "identity_parameters",
        "completion_run_id",
        "completed_at",
        "status",
        "page_count",
        "record_count",
        "pages",
    }
)
_IDENTITY_PARAMETER_FIELDS: Final = frozenset({"page-size", "resource-path"})
_PAGE_FIELDS: Final = frozenset(
    {
        "page_number",
        "object_key",
        "page_manifest_key",
        "payload_content_length",
        "payload_checksum_sha256",
        "page_manifest_checksum_sha256",
        "record_count",
    }
)
_CANONICAL_DECIMAL = re.compile(r"[1-9][0-9]*", flags=re.ASCII)
_CANONICAL_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z",
    flags=re.ASCII,
)


class ApiLogicalRunManifestConflictError(ValueError):
    """A well-formed aggregate makes an internally conflicting completion claim."""


@dataclass(frozen=True, slots=True)
class ApiLogicalRunPage:
    """Checksummed linkage to one immutable raw/page-manifest pair."""

    page_number: int
    object_key: str
    page_manifest_key: str
    payload_content_length: int
    payload_checksum_sha256: str
    page_manifest_checksum_sha256: str
    record_count: int

    def __post_init__(self) -> None:
        _bounded_integer(
            self.page_number,
            field="page_number",
            minimum=1,
            maximum=API_LOGICAL_RUN_MANIFEST_MAX_PAGES,
        )
        _non_empty_string(self.object_key, field="object_key")
        _non_empty_string(self.page_manifest_key, field="page_manifest_key")
        _bounded_integer(
            self.payload_content_length,
            field="payload_content_length",
            minimum=0,
            maximum=API_LOGICAL_RUN_PAYLOAD_LENGTH_MAX,
        )
        _checksum(self.payload_checksum_sha256, field="payload_checksum_sha256")
        _checksum(
            self.page_manifest_checksum_sha256,
            field="page_manifest_checksum_sha256",
        )
        _bounded_integer(
            self.record_count,
            field="record_count",
            minimum=0,
            maximum=API_LOGICAL_RUN_PAGE_SIZE_MAX,
        )

    def to_dict(self) -> dict[str, object]:
        """Return this entry in its exact JSON-compatible schema."""
        return {
            "page_number": self.page_number,
            "object_key": self.object_key,
            "page_manifest_key": self.page_manifest_key,
            "payload_content_length": self.payload_content_length,
            "payload_checksum_sha256": self.payload_checksum_sha256,
            "page_manifest_checksum_sha256": self.page_manifest_checksum_sha256,
            "record_count": self.record_count,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApiLogicalRunPage:
        """Parse one strict page entry after duplicate-member detection."""
        _require_exact_fields(value, expected=_PAGE_FIELDS, subject="logical-run page")
        return cls(
            page_number=_integer(value, "page_number"),
            object_key=_string(value, "object_key"),
            page_manifest_key=_string(value, "page_manifest_key"),
            payload_content_length=_integer(value, "payload_content_length"),
            payload_checksum_sha256=_string(value, "payload_checksum_sha256"),
            page_manifest_checksum_sha256=_string(value, "page_manifest_checksum_sha256"),
            record_count=_integer(value, "record_count"),
        )


@dataclass(frozen=True, slots=True)
class ApiLogicalRunManifest:
    """One immutable success-only completion record for a logical API batch."""

    manifest_type: str
    schema_version: int
    source_name: str
    source_type: SourceType
    source_date: date
    batch_id: str
    identity_parameters: Mapping[str, str]
    completion_run_id: str
    completed_at: datetime
    status: str
    page_count: int
    record_count: int
    pages: tuple[ApiLogicalRunPage, ...]

    def __post_init__(self) -> None:
        if self.manifest_type != API_LOGICAL_RUN_MANIFEST_TYPE:
            raise ValueError(f"manifest_type must be {API_LOGICAL_RUN_MANIFEST_TYPE!r}")
        _exact_integer(
            self.schema_version,
            field="schema_version",
            expected=API_LOGICAL_RUN_MANIFEST_SCHEMA_VERSION,
        )
        identity, page_size = _manifest_identity(
            source_name=self.source_name,
            source_type=self.source_type,
            source_date=self.source_date,
            parameters=self.identity_parameters,
        )
        if not is_batch_id(self.batch_id):
            raise ValueError("batch_id must be 64 lowercase hexadecimal characters")
        if self.batch_id != derive_batch_id(identity):
            raise ApiLogicalRunManifestConflictError(
                "batch_id does not match the rederived API identity"
            )
        if not is_run_id(self.completion_run_id):
            raise ValueError("completion_run_id must use the run_<32 lowercase hex> format")
        if not isinstance(self.completed_at, datetime):
            raise TypeError("completed_at must be a datetime")
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("completed_at must be timezone-aware")
        object.__setattr__(self, "completed_at", self.completed_at.astimezone(UTC))
        if self.status != API_LOGICAL_RUN_MANIFEST_STATUS:
            raise ValueError(f"status must be {API_LOGICAL_RUN_MANIFEST_STATUS!r}")

        _bounded_integer(
            self.page_count,
            field="page_count",
            minimum=1,
            maximum=API_LOGICAL_RUN_MANIFEST_MAX_PAGES,
        )
        _bounded_integer(
            self.record_count,
            field="record_count",
            minimum=0,
            maximum=API_LOGICAL_RUN_MANIFEST_MAX_RECORDS,
        )
        if not isinstance(self.pages, tuple):
            raise TypeError("pages must be a tuple")
        if not all(isinstance(page, ApiLogicalRunPage) for page in self.pages):
            raise TypeError("pages must contain ApiLogicalRunPage entries")
        if self.page_count != len(self.pages):
            raise ApiLogicalRunManifestConflictError("page_count must equal the number of pages")

        expected_page_count = max(1, (self.record_count + page_size - 1) // page_size)
        if self.page_count != expected_page_count:
            raise ApiLogicalRunManifestConflictError(
                "page_count does not match record_count and page-size"
            )

        expected_total = 0
        for expected_number, page in enumerate(self.pages, start=1):
            if page.page_number != expected_number:
                raise ApiLogicalRunManifestConflictError(
                    "page_number entries must be ordered, unique, and gap-free"
                )
            expected_object_key = raw_object_key(
                identity,
                page_payload_name(
                    expected_number,
                    max_pages=API_LOGICAL_RUN_MANIFEST_MAX_PAGES,
                ),
            )
            if page.object_key != expected_object_key:
                raise ApiLogicalRunManifestConflictError(
                    "object_key does not match the rederived page key"
                )
            if page.page_manifest_key != manifest_key_for(expected_object_key):
                raise ApiLogicalRunManifestConflictError(
                    "page_manifest_key does not match the rederived page-manifest key"
                )
            expected_records = (
                page_size
                if expected_number < self.page_count
                else self.record_count - page_size * (self.page_count - 1)
            )
            if page.record_count != expected_records:
                raise ApiLogicalRunManifestConflictError(
                    "page record_count violates strict pagination"
                )
            expected_total += page.record_count
        if expected_total != self.record_count:
            raise ApiLogicalRunManifestConflictError(
                "record_count must equal the sum of page record counts"
            )

        object.__setattr__(
            self,
            "identity_parameters",
            MappingProxyType(dict(self.identity_parameters)),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the complete exact JSON-compatible aggregate schema."""
        return {
            "manifest_type": self.manifest_type,
            "schema_version": self.schema_version,
            "source_name": self.source_name,
            "source_type": self.source_type.value,
            "source_date": self.source_date.isoformat(),
            "batch_id": self.batch_id,
            "identity_parameters": dict(self.identity_parameters),
            "completion_run_id": self.completion_run_id,
            "completed_at": self.completed_at.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "status": self.status,
            "page_count": self.page_count,
            "record_count": self.record_count,
            "pages": [page.to_dict() for page in self.pages],
        }

    def to_bytes(self) -> bytes:
        """Serialize canonical UTF-8 JSON and enforce the separate 8 MiB bound."""
        value = json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(value) > API_LOGICAL_RUN_MANIFEST_MAX_BYTES:
            raise ValueError("logical-run manifest exceeds the 8388608-byte canonical limit")
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApiLogicalRunManifest:
        """Build and fully validate an aggregate from a duplicate-free object."""
        _require_exact_fields(
            value,
            expected=_MANIFEST_FIELDS,
            subject="logical-run manifest",
        )
        identity_parameters_value = value["identity_parameters"]
        if not isinstance(identity_parameters_value, dict):
            raise TypeError("identity_parameters must be an object")
        _require_exact_fields(
            identity_parameters_value,
            expected=_IDENTITY_PARAMETER_FIELDS,
            subject="identity_parameters",
        )
        identity_parameters = {
            "page-size": _string(identity_parameters_value, "page-size"),
            "resource-path": _string(identity_parameters_value, "resource-path"),
        }

        pages_value = value["pages"]
        if not isinstance(pages_value, list):
            raise TypeError("pages must be an array")
        pages: list[ApiLogicalRunPage] = []
        for page_value in pages_value:
            if not isinstance(page_value, dict):
                raise TypeError("each pages entry must be an object")
            pages.append(ApiLogicalRunPage.from_dict(page_value))

        source_type_value = _string(value, "source_type")
        try:
            source_type = SourceType(source_type_value)
        except ValueError as error:
            raise ValueError("source_type is unsupported") from error

        source_date_value = _string(value, "source_date")
        try:
            source_date = date.fromisoformat(source_date_value)
        except ValueError as error:
            raise ValueError("source_date must use YYYY-MM-DD") from error
        if source_date.isoformat() != source_date_value:
            raise ValueError("source_date must use YYYY-MM-DD")

        completed_at_value = _string(value, "completed_at")
        if _CANONICAL_TIMESTAMP.fullmatch(completed_at_value) is None:
            raise ValueError("completed_at must use YYYY-MM-DDTHH:MM:SS.ffffffZ")
        try:
            completed_at = datetime.strptime(
                completed_at_value,
                "%Y-%m-%dT%H:%M:%S.%fZ",
            ).replace(tzinfo=UTC)
        except ValueError as error:
            raise ValueError("completed_at is not a valid UTC instant") from error

        return cls(
            manifest_type=_string(value, "manifest_type"),
            schema_version=_integer(value, "schema_version"),
            source_name=_string(value, "source_name"),
            source_type=source_type,
            source_date=source_date,
            batch_id=_string(value, "batch_id"),
            identity_parameters=identity_parameters,
            completion_run_id=_string(value, "completion_run_id"),
            completed_at=completed_at,
            status=_string(value, "status"),
            page_count=_integer(value, "page_count"),
            record_count=_integer(value, "record_count"),
            pages=tuple(pages),
        )

    @classmethod
    def from_bytes(cls, value: bytes) -> ApiLogicalRunManifest:
        """Strictly parse canonical aggregate bytes with nested duplicate rejection."""
        if not isinstance(value, bytes):
            raise TypeError("logical-run manifest input must be bytes")
        if len(value) > API_LOGICAL_RUN_MANIFEST_MAX_BYTES:
            raise ValueError("logical-run manifest exceeds the 8388608-byte parser limit")
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("logical-run manifest must be valid UTF-8") from error
        if decoded.startswith("\ufeff"):
            raise ValueError("logical-run manifest must not contain a UTF-8 BOM")
        try:
            parsed = cast(
                object,
                json.loads(
                    decoded,
                    object_pairs_hook=_reject_duplicate_object,
                    parse_constant=_reject_json_constant,
                ),
            )
        except json.JSONDecodeError as error:
            raise ValueError("logical-run manifest JSON is invalid") from error
        if not isinstance(parsed, dict):
            raise TypeError("logical-run manifest JSON must contain one object")
        manifest = cls.from_dict(parsed)
        if manifest.to_bytes() != value:
            raise ValueError("logical-run manifest JSON is not canonical")
        return manifest


def api_logical_run_manifest_key(identity: BatchIdentity) -> str:
    """Derive the reserved aggregate key from one exact canonical API identity."""
    _validate_api_identity(identity)
    batch_id = derive_batch_id(identity)
    return (
        f"manifests/api/{identity.source_name}/"
        f"source_date={identity.source_date.isoformat()}/batch_id={batch_id}/"
        "logical-run.manifest.json"
    )


def _validate_api_identity(identity: BatchIdentity) -> int:
    if not isinstance(identity, BatchIdentity) or identity.source_type is not SourceType.API:
        raise ValueError("logical-run key requires an API identity")
    descriptor = get_source_descriptor(identity.source_name)
    parameters = dict(identity.parameters)
    page_size = _validated_page_size(parameters.get("page-size"))
    if set(parameters) != _IDENTITY_PARAMETER_FIELDS:
        raise ValueError("API identity parameters do not match the logical-run contract")
    if parameters["resource-path"] != descriptor.path:
        raise ValueError("API identity parameters do not match the source descriptor")
    expected = api_page_identity(descriptor, identity.source_date, page_size=page_size)
    if identity != expected:
        raise ValueError("API identity parameters are not canonical")
    return page_size


def _manifest_identity(
    *,
    source_name: str,
    source_type: SourceType,
    source_date: date,
    parameters: Mapping[str, str],
) -> tuple[BatchIdentity, int]:
    if not isinstance(source_name, str):
        raise TypeError("source_name must be a string")
    if source_type is not SourceType.API:
        raise ValueError("source_type must be 'api'")
    if isinstance(source_date, datetime) or not isinstance(source_date, date):
        raise TypeError("source_date must be a date")
    if not isinstance(parameters, Mapping):
        raise TypeError("identity_parameters must be an object")
    if not all(isinstance(key, str) and isinstance(item, str) for key, item in parameters.items()):
        raise TypeError("identity_parameters keys and values must be strings")
    if set(parameters) != _IDENTITY_PARAMETER_FIELDS:
        raise ValueError("identity_parameters fields do not match the contract")
    descriptor = get_source_descriptor(source_name)
    page_size = _validated_page_size(parameters["page-size"])
    if parameters["resource-path"] != descriptor.path:
        raise ApiLogicalRunManifestConflictError(
            "identity parameters do not match the source descriptor"
        )
    identity = api_page_identity(descriptor, source_date, page_size=page_size)
    return identity, page_size


def _validated_page_size(value: str | None) -> int:
    if not isinstance(value, str):
        raise TypeError("page-size must be a string")
    if _CANONICAL_DECIMAL.fullmatch(value) is None:
        raise ValueError("page-size must be a canonical decimal string")
    try:
        page_size = int(value)
    except ValueError as error:
        raise ValueError("page-size must be a canonical decimal string") from error
    if not 1 <= page_size <= API_LOGICAL_RUN_PAGE_SIZE_MAX:
        raise ValueError("page-size must be between 1 and 100")
    return page_size


def _require_exact_fields(
    value: Mapping[str, object],
    *,
    expected: frozenset[str],
    subject: str,
) -> None:
    fields = set(value)
    missing = expected - fields
    unexpected = fields - expected
    if missing:
        raise ValueError(f"{subject} has missing fields: {sorted(missing)}")
    if unexpected:
        raise ValueError(f"{subject} has unexpected fields: {sorted(unexpected)}")


def _reject_duplicate_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"logical-run manifest JSON has duplicate member: {key}")
        value[key] = item
    return value


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"logical-run manifest JSON constant is unsupported: {value}")


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


def _exact_integer(value: int, *, field: str, expected: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value != expected:
        raise ValueError(f"{field} must be {expected}")


def _bounded_integer(value: int, *, field: str, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{field} must be an integer between {minimum} and {maximum}")


def _non_empty_string(value: str, *, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")


def _checksum(value: str, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must be 64 lowercase hexadecimal characters")
