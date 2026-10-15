#!/usr/bin/env python3
"""Read-only exact S3 inventory verification for BI-080 demonstrations."""

from __future__ import annotations

import io
from abc import abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import PurePosixPath
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from batch_ingestion.identifiers import SourceType, manifest_key_for
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.postgres_snapshot.schema import ORDERS_ARROW_SCHEMA

_MAX_LIST_PAGES = 100
_MAX_MANIFEST_BYTES = 1_048_576
_DEFAULT_CHUNK_SIZE = 64 * 1024


class DemoInventoryError(RuntimeError):
    """One exact read-only inventory assertion failed."""


class _StreamingBody(Protocol):
    @abstractmethod
    def read(self, amount: int | None = None) -> bytes:
        """Read at most one bounded chunk."""
        return b""

    @abstractmethod
    def close(self) -> None:
        """Release the response connection."""
        return None


class DemoS3Client(Protocol):
    """Narrow standard-S3 surface used only by the read-only verifier."""

    @abstractmethod
    def list_objects_v2(self, **kwargs: object) -> Mapping[str, object]:
        """List one bounded page below an exact prefix."""
        return {}

    @abstractmethod
    def get_object(self, **kwargs: object) -> Mapping[str, object]:
        """Open one exact object for bounded streaming."""
        return {}


def _safe_key(value: str, *, field: str = "object key") -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty safe relative key")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ValueError(f"{field} must be a safe relative key")
    return value


def _canonical_date(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("source_date must be a string")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("source_date must use YYYY-MM-DD")
    return value


def _optional_string(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string or null")
    return value


def _string(value: Mapping[str, object], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str):
        raise TypeError(f"{field} must be a string")
    return result


def _string_tuple(value: Mapping[str, object], field: str) -> tuple[str, ...]:
    raw = value.get(field)
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise TypeError(f"{field} must be a list of strings")
    return tuple(cast(list[str], raw))


def _optional_non_negative_integer(value: object, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer or null")
    return value


@dataclass(frozen=True, slots=True)
class DemoBatchRecord:
    """Strict non-secret descriptor for one known demo batch inventory."""

    scenario: str
    source_name: str
    source_type: SourceType
    source_date: str
    batch_id: str
    payload_keys: tuple[str, ...]
    expected_status: ManifestStatus
    disposition_key: str | None = None
    landing_prefixes: tuple[str, ...] = ()
    expected_record_count: int | None = None
    upper_updated_at: str | None = None
    upper_order_id: int | None = None
    chunk_rows: int | None = None

    def __post_init__(self) -> None:
        if not self.scenario or not self.scenario.replace("-", "_").isidentifier():
            raise ValueError("scenario must be a canonical identifier")
        if not self.source_name or not self.source_name.replace("-", "_").isidentifier():
            raise ValueError("source_name must be a canonical identifier")
        if not isinstance(self.source_type, SourceType):
            raise TypeError("source_type must be a SourceType")
        _canonical_date(self.source_date)
        if len(self.batch_id) != 64 or any(
            character not in "0123456789abcdef" for character in self.batch_id
        ):
            raise ValueError("batch_id must be 64 lowercase hexadecimal characters")
        if not self.payload_keys:
            raise ValueError("payload_keys must not be empty")
        if len(set(self.payload_keys)) != len(self.payload_keys):
            raise ValueError("payload_keys must be unique")
        expected_prefix = (
            f"raw/{self.source_type.value}/{self.source_name}/"
            f"source_date={self.source_date}/batch_id={self.batch_id}/"
        )
        for key in self.payload_keys:
            _safe_key(key)
            if not key.startswith(expected_prefix):
                raise ValueError("payload key does not match the record identity")
        if not isinstance(self.expected_status, ManifestStatus):
            raise TypeError("expected_status must be a ManifestStatus")
        if self.disposition_key is not None:
            _safe_key(self.disposition_key, field="disposition_key")
        for prefix in self.landing_prefixes:
            if not prefix.endswith("/"):
                raise ValueError("landing prefix must end with slash")
            _safe_key(prefix.removesuffix("/"), field="landing prefix")
        _optional_non_negative_integer(self.expected_record_count, field="expected_record_count")
        _optional_non_negative_integer(self.upper_order_id, field="upper_order_id")
        _optional_non_negative_integer(self.chunk_rows, field="chunk_rows")
        if self.source_type is SourceType.POSTGRES_SNAPSHOT:
            if (
                self.expected_record_count is None
                or self.chunk_rows is None
                or self.chunk_rows == 0
            ):
                raise ValueError("snapshot record requires expected count and positive chunk_rows")
            if self.expected_record_count == 0:
                if self.upper_updated_at is not None or self.upper_order_id is not None:
                    raise ValueError("empty snapshot record must not carry an upper tuple")
            elif self.upper_updated_at is None or self.upper_order_id is None:
                raise ValueError("non-empty snapshot record requires an upper tuple")
        elif any(
            value is not None
            for value in (
                self.expected_record_count,
                self.upper_updated_at,
                self.upper_order_id,
                self.chunk_rows,
            )
        ):
            raise ValueError("only snapshot records may carry row-bound fields")

    @property
    def manifest_keys(self) -> tuple[str, ...]:
        return tuple(manifest_key_for(key) for key in self.payload_keys)

    def to_dict(self) -> dict[str, object]:
        return {
            "scenario": self.scenario,
            "source_name": self.source_name,
            "source_type": self.source_type.value,
            "source_date": self.source_date,
            "batch_id": self.batch_id,
            "payload_keys": list(self.payload_keys),
            "expected_status": self.expected_status.value,
            "disposition_key": self.disposition_key,
            "landing_prefixes": list(self.landing_prefixes),
            "expected_record_count": self.expected_record_count,
            "upper_updated_at": self.upper_updated_at,
            "upper_order_id": self.upper_order_id,
            "chunk_rows": self.chunk_rows,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> DemoBatchRecord:
        fields = {
            "scenario",
            "source_name",
            "source_type",
            "source_date",
            "batch_id",
            "payload_keys",
            "expected_status",
            "disposition_key",
            "landing_prefixes",
            "expected_record_count",
            "upper_updated_at",
            "upper_order_id",
            "chunk_rows",
        }
        if set(value) != fields:
            raise ValueError("demo batch record fields do not match the schema")
        try:
            source_type = SourceType(_string(value, "source_type"))
            expected_status = ManifestStatus(_string(value, "expected_status"))
        except ValueError as error:
            raise ValueError("demo batch record enum value is unsupported") from error
        return cls(
            scenario=_string(value, "scenario"),
            source_name=_string(value, "source_name"),
            source_type=source_type,
            source_date=_string(value, "source_date"),
            batch_id=_string(value, "batch_id"),
            payload_keys=_string_tuple(value, "payload_keys"),
            expected_status=expected_status,
            disposition_key=_optional_string(value.get("disposition_key"), field="disposition_key"),
            landing_prefixes=_string_tuple(value, "landing_prefixes"),
            expected_record_count=_optional_non_negative_integer(
                value.get("expected_record_count"), field="expected_record_count"
            ),
            upper_updated_at=_optional_string(
                value.get("upper_updated_at"), field="upper_updated_at"
            ),
            upper_order_id=_optional_non_negative_integer(
                value.get("upper_order_id"), field="upper_order_id"
            ),
            chunk_rows=_optional_non_negative_integer(value.get("chunk_rows"), field="chunk_rows"),
        )


@dataclass(frozen=True, slots=True)
class DemoObjectEvidence:
    """Independent streamed evidence for one exact object."""

    content_length: int
    checksum_sha256: str


@dataclass(slots=True)
class _SnapshotEvidence:
    rows: int = 0
    previous: tuple[datetime, int] | None = None
    final: tuple[datetime, int] | None = None


@dataclass(frozen=True, slots=True)
class DemoInventoryEvidence:
    """Closed read-only evidence set suitable for replay comparison."""

    objects: Mapping[str, DemoObjectEvidence]
    raw_count: int
    manifest_count: int
    landing_count: int
    archive_count: int
    rejected_count: int

    def require_unchanged(self, other: DemoInventoryEvidence) -> None:
        if dict(self.objects) != dict(other.objects):
            raise DemoInventoryError("canonical inventory changed during idempotent replay")


class DemoInventoryVerifier:
    """Verify calculated prefixes and actual bytes without mutating storage."""

    def __init__(
        self,
        *,
        client: DemoS3Client,
        bucket: str,
        maximum_object_bytes: int,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> None:
        if not bucket:
            raise ValueError("bucket must not be empty")
        if (
            isinstance(maximum_object_bytes, bool)
            or not isinstance(maximum_object_bytes, int)
            or maximum_object_bytes <= 0
        ):
            raise ValueError("maximum_object_bytes must be positive")
        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self._client = client
        self._bucket = bucket
        self._maximum_object_bytes = maximum_object_bytes
        self._chunk_size = chunk_size

    def verify(self, records: Sequence[DemoBatchRecord]) -> DemoInventoryEvidence:
        if not records:
            raise DemoInventoryError("no recorded demo batches are available for inspection")
        evidence: dict[str, DemoObjectEvidence] = {}
        raw_keys: set[str] = set()
        manifest_keys: set[str] = set()
        archive_count = 0
        rejected_count = 0

        for record in records:
            expected_payloads = set(record.payload_keys)
            expected_manifests = set(record.manifest_keys)
            raw_prefix = record.payload_keys[0].rsplit("/", 1)[0] + "/"
            manifest_prefix = record.manifest_keys[0].rsplit("/", 1)[0] + "/"
            self._require_exact_prefix(raw_prefix, expected_payloads)
            self._require_exact_prefix(manifest_prefix, expected_manifests)

            snapshot_evidence = (
                _SnapshotEvidence() if record.source_type is SourceType.POSTGRES_SNAPSHOT else None
            )
            first_payload_evidence: DemoObjectEvidence | None = None
            for payload_key, manifest_key in zip(
                record.payload_keys, record.manifest_keys, strict=True
            ):
                manifest_bytes, manifest_evidence = self._read_object(
                    manifest_key, maximum_bytes=_MAX_MANIFEST_BYTES
                )
                try:
                    manifest = Manifest.from_json(manifest_bytes.decode("utf-8"))
                except (UnicodeDecodeError, TypeError, ValueError) as error:
                    raise DemoInventoryError(
                        f"manifest is invalid for key {manifest_key}"
                    ) from error
                if (
                    manifest.object_key != payload_key
                    or manifest.batch_id != record.batch_id
                    or manifest.source_name != record.source_name
                    or manifest.source_type is not record.source_type
                    or manifest.source_date.isoformat() != record.source_date
                    or manifest.status is not record.expected_status
                ):
                    raise DemoInventoryError(
                        f"manifest linkage/status mismatch for key {manifest_key}"
                    )
                payload, payload_evidence = self._read_object(
                    payload_key, maximum_bytes=self._maximum_object_bytes
                )
                if (
                    payload_evidence.content_length != manifest.content_length
                    or payload_evidence.checksum_sha256 != manifest.checksum_sha256
                ):
                    raise DemoInventoryError(
                        f"payload bytes do not match manifest for key {payload_key}"
                    )
                evidence[payload_key] = payload_evidence
                evidence[manifest_key] = manifest_evidence
                raw_keys.add(payload_key)
                manifest_keys.add(manifest_key)
                if first_payload_evidence is None:
                    first_payload_evidence = payload_evidence
                if snapshot_evidence is not None:
                    self._inspect_snapshot_payload(record, payload, snapshot_evidence)

            if snapshot_evidence is not None:
                self._require_snapshot_complete(record, snapshot_evidence)

            if record.disposition_key is not None:
                disposition, disposition_evidence = self._read_object(
                    record.disposition_key,
                    maximum_bytes=self._maximum_object_bytes,
                )
                del disposition
                if (
                    len(record.payload_keys) != 1
                    or first_payload_evidence is None
                    or disposition_evidence != first_payload_evidence
                ):
                    raise DemoInventoryError(
                        "file disposition bytes differ from canonical raw bytes"
                    )
                evidence[record.disposition_key] = disposition_evidence
                if record.disposition_key.startswith("archive/"):
                    archive_count += 1
                    archive_prefix = record.disposition_key.rsplit("/", 1)[0] + "/"
                    rejected_prefix = archive_prefix.replace("archive/", "rejected/", 1)
                    self._require_exact_prefix(archive_prefix, {record.disposition_key})
                    self._require_exact_prefix(rejected_prefix, set())
                elif record.disposition_key.startswith("rejected/"):
                    rejected_count += 1
                    rejected_prefix = record.disposition_key.split("reason=", 1)[0]
                    archive_prefix = rejected_prefix.replace("rejected/", "archive/", 1)
                    self._require_exact_prefix(rejected_prefix, {record.disposition_key})
                    self._require_exact_prefix(archive_prefix, set())
                else:
                    raise DemoInventoryError("file disposition key is neither archive nor rejected")

            for landing_prefix in record.landing_prefixes:
                self._require_exact_prefix(landing_prefix, set())

        return DemoInventoryEvidence(
            objects=dict(sorted(evidence.items())),
            raw_count=len(raw_keys),
            manifest_count=len(manifest_keys),
            landing_count=0,
            archive_count=archive_count,
            rejected_count=rejected_count,
        )

    def _list_prefix(self, prefix: str) -> set[str]:
        keys: set[str] = set()
        continuation_token: str | None = None
        for _ in range(_MAX_LIST_PAGES):
            arguments: dict[str, object] = {"Bucket": self._bucket, "Prefix": prefix}
            if continuation_token is not None:
                arguments["ContinuationToken"] = continuation_token
            page = self._client.list_objects_v2(**arguments)
            contents = page.get("Contents", [])
            if not isinstance(contents, list):
                raise DemoInventoryError("S3 listing Contents has an invalid type")
            for entry in contents:
                if not isinstance(entry, Mapping):
                    raise DemoInventoryError("S3 listing entry has an invalid type")
                key = entry.get("Key")
                if not isinstance(key, str) or not key.startswith(prefix):
                    raise DemoInventoryError("S3 listing returned an invalid key")
                keys.add(key)
            if page.get("IsTruncated") is not True:
                return keys
            token = page.get("NextContinuationToken")
            if not isinstance(token, str) or not token:
                raise DemoInventoryError("truncated S3 listing omitted its continuation token")
            continuation_token = token
        raise DemoInventoryError("S3 listing exceeded the bounded page limit")

    def _require_exact_prefix(self, prefix: str, expected: set[str]) -> None:
        actual = self._list_prefix(prefix)
        if actual != expected:
            raise DemoInventoryError(
                f"exact inventory mismatch below {prefix}: "
                f"expected {len(expected)}, found {len(actual)}"
            )

    def _read_object(
        self,
        key: str,
        *,
        maximum_bytes: int,
    ) -> tuple[bytes, DemoObjectEvidence]:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        declared = response.get("ContentLength")
        if isinstance(declared, bool) or not isinstance(declared, int) or declared < 0:
            raise DemoInventoryError(f"object has invalid ContentLength: {key}")
        if declared > maximum_bytes:
            raise DemoInventoryError(f"object exceeds inspection byte bound: {key}")
        body = response.get("Body")
        if body is None or not hasattr(body, "read") or not hasattr(body, "close"):
            raise DemoInventoryError(f"object response body is invalid: {key}")
        stream = cast(_StreamingBody, body)
        digest = sha256()
        content = io.BytesIO()
        observed = 0
        with closing(stream):
            while True:
                chunk = stream.read(self._chunk_size)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise DemoInventoryError(f"object body returned non-bytes: {key}")
                observed += len(chunk)
                if observed > maximum_bytes:
                    raise DemoInventoryError(f"object exceeds inspection byte bound: {key}")
                digest.update(chunk)
                content.write(chunk)
        if observed != declared:
            raise DemoInventoryError(f"object ContentLength differs from streamed bytes: {key}")
        return content.getvalue(), DemoObjectEvidence(observed, digest.hexdigest())

    @staticmethod
    def _inspect_snapshot_payload(
        record: DemoBatchRecord,
        payload: bytes,
        evidence: _SnapshotEvidence,
    ) -> None:
        try:
            parquet = pq.ParquetFile(io.BytesIO(payload))
        except (OSError, pa.ArrowException) as error:
            raise DemoInventoryError("snapshot payload is not readable Parquet") from error
        if not parquet.schema_arrow.equals(ORDERS_ARROW_SCHEMA, check_metadata=True):
            raise DemoInventoryError("snapshot Parquet schema does not match the contract")
        if parquet.num_row_groups != 1:
            raise DemoInventoryError("snapshot Parquet chunk must contain one row group")
        for batch in parquet.iter_batches(batch_size=max(1, record.chunk_rows or 1)):
            updated_values = batch.column("updated_at").to_pylist()
            order_values = batch.column("order_id").to_pylist()
            for updated_at, order_id in zip(updated_values, order_values, strict=True):
                if (
                    not isinstance(updated_at, datetime)
                    or isinstance(order_id, bool)
                    or not isinstance(order_id, int)
                ):
                    raise DemoInventoryError("snapshot Parquet ordering columns have invalid types")
                current = (updated_at, order_id)
                if evidence.previous is not None and current <= evidence.previous:
                    raise DemoInventoryError("snapshot Parquet rows are not strictly ordered")
                evidence.previous = current
                evidence.final = current
                evidence.rows += 1

    @staticmethod
    def _require_snapshot_complete(
        record: DemoBatchRecord,
        evidence: _SnapshotEvidence,
    ) -> None:
        if evidence.rows != record.expected_record_count:
            raise DemoInventoryError("snapshot Parquet row count does not match descriptor")
        if evidence.rows == 0:
            if evidence.final is not None:
                raise DemoInventoryError("empty snapshot unexpectedly contains a final tuple")
            return
        if (
            evidence.final is None
            or record.upper_updated_at is None
            or record.upper_order_id is None
        ):
            raise DemoInventoryError("non-empty snapshot descriptor omitted its upper tuple")
        canonical_final = (
            evidence.final[0].isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
        if canonical_final != record.upper_updated_at or evidence.final[1] != record.upper_order_id:
            raise DemoInventoryError("snapshot Parquet final tuple does not match descriptor")
