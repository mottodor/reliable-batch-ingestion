"""Deterministic logical-batch and object-key contracts."""

import json
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from hashlib import sha256
from uuid import uuid4

_SLUG_SEPARATOR = re.compile(r"[\s_]+")
_SLUG_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")
_PAYLOAD_NAME_PATTERN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?")
_BATCH_ID_PATTERN = re.compile(r"[0-9a-f]{64}")
_RUN_ID_PATTERN = re.compile(r"run_[0-9a-f]{32}")


class SourceType(StrEnum):
    """Supported source families."""

    API = "api"
    FILE = "file"
    POSTGRES_SNAPSHOT = "postgres_snapshot"


@dataclass(frozen=True, slots=True)
class BatchIdentity:
    """Normalized logical inputs used to derive a stable batch identifier."""

    source_name: str
    source_type: SourceType
    source_date: date
    parameters: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if normalize_source_name(self.source_name) != self.source_name:
            raise ValueError("source_name must already be normalized")
        if not isinstance(self.source_type, SourceType):
            raise TypeError("source_type must be a SourceType")
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise TypeError("source_date must be a date")

        normalized_parameters: list[tuple[str, str]] = []
        for key, value in self.parameters:
            normalized_key = _normalize_slug(key, field="parameter name")
            normalized_value = unicodedata.normalize("NFC", value).strip()
            if normalized_key != key or normalized_value != value or not value:
                raise ValueError("logical parameters must already be normalized")
            normalized_parameters.append((key, value))
        if tuple(sorted(normalized_parameters)) != self.parameters:
            raise ValueError("logical parameters must be uniquely sorted by name")
        if len({key for key, _ in normalized_parameters}) != len(normalized_parameters):
            raise ValueError("logical parameter names must be unique")

    @classmethod
    def create(
        cls,
        source_name: str,
        source_type: SourceType | str,
        source_date: date,
        parameters: Mapping[str, str] | None = None,
    ) -> "BatchIdentity":
        if isinstance(source_date, datetime) or not isinstance(source_date, date):
            raise TypeError("source_date must be a date")

        normalized_parameters: dict[str, str] = {}
        for raw_key, raw_value in (parameters or {}).items():
            key = _normalize_slug(raw_key, field="parameter name")
            value = unicodedata.normalize("NFC", raw_value).strip()
            if not value:
                raise ValueError(f"logical parameter {key!r} must not be empty")
            if key in normalized_parameters:
                raise ValueError(f"duplicate normalized logical parameter: {key}")
            normalized_parameters[key] = value

        try:
            parsed_source_type = SourceType(source_type)
        except ValueError as error:
            raise ValueError(f"unsupported source_type: {source_type!r}") from error

        return cls(
            source_name=_normalize_slug(source_name, field="source_name"),
            source_type=parsed_source_type,
            source_date=source_date,
            parameters=tuple(sorted(normalized_parameters.items())),
        )

    def canonical_bytes(self) -> bytes:
        """Serialize normalized identity fields as canonical UTF-8 JSON."""
        value = {
            "parameters": dict(self.parameters),
            "source_date": self.source_date.isoformat(),
            "source_name": self.source_name,
            "source_type": self.source_type.value,
        }
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")


def derive_batch_id(identity: BatchIdentity) -> str:
    """Derive the stable logical batch ID from canonical identity bytes."""
    return sha256(identity.canonical_bytes()).hexdigest()


def new_run_id() -> str:
    """Return a unique identifier for one physical execution attempt."""
    return f"run_{uuid4().hex}"


def raw_object_key(identity: BatchIdentity, payload_name: str) -> str:
    """Build the canonical raw object path for one payload in a logical batch."""
    normalized_name = normalize_payload_name(payload_name)

    batch_id = derive_batch_id(identity)
    return (
        f"raw/{identity.source_type.value}/{identity.source_name}/"
        f"source_date={identity.source_date.isoformat()}/batch_id={batch_id}/{normalized_name}"
    )


def manifest_key_for(raw_key: str) -> str:
    """Map one canonical raw key to its one-to-one manifest key."""
    if not raw_key.startswith("raw/") or ".." in raw_key.split("/"):
        raise ValueError("raw_key must be a canonical key below raw/")
    return f"manifests/{raw_key.removeprefix('raw/')}.manifest.json"


def is_batch_id(value: str) -> bool:
    """Return whether a value has the canonical batch-ID shape."""
    return _BATCH_ID_PATTERN.fullmatch(value) is not None


def is_run_id(value: str) -> bool:
    """Return whether a value has the canonical run-ID shape."""
    return _RUN_ID_PATTERN.fullmatch(value) is not None


def normalize_source_name(value: str) -> str:
    """Expose source-name normalization for boundary validation."""
    return _normalize_slug(value, field="source_name")


def normalize_payload_name(value: str) -> str:
    """Normalize and validate one safe payload-name path segment."""
    normalized = unicodedata.normalize("NFC", value).strip()
    if not _PAYLOAD_NAME_PATTERN.fullmatch(normalized):
        raise ValueError("payload_name must be one safe non-empty path segment")
    return normalized


def _normalize_slug(value: str, *, field: str) -> str:
    normalized = unicodedata.normalize("NFC", value).strip().casefold()
    normalized = _SLUG_SEPARATOR.sub("-", normalized)
    if not _SLUG_PATTERN.fullmatch(normalized):
        raise ValueError(f"{field} must normalize to lowercase letters, digits, dots, or hyphens")
    return normalized
