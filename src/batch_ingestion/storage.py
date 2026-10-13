"""Shared S3 primitives with byte-level verification and recoverable state transitions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from typing import BinaryIO, cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from .checksums import DEFAULT_CHUNK_SIZE
from .config import StorageConfig
from .identifiers import manifest_key_for
from .manifests import Manifest

CHECKSUM_METADATA_KEY = "checksum-sha256"
DEFAULT_MAX_MANIFEST_BYTES = 64 * 1024
_MISSING_ERROR_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class StorageError(RuntimeError):
    """Base class for storage contract failures created by this module."""


class ObjectConflictError(StorageError):
    """Raised when a deterministic key contains different bytes or metadata."""


class ObjectVerificationError(StorageError):
    """Raised when bytes or metadata cannot be verified after an operation."""


class ObjectNotFoundError(StorageError):
    """Raised when an operation requires an object that is absent."""


class ImmutableWriteStatus(StrEnum):
    """Outcomes for a verified immutable write."""

    CREATED = "created"
    UNCHANGED = "unchanged"


class MoveStatus(StrEnum):
    """Outcomes for a verified copy-delete movement."""

    MOVED = "moved"
    ALREADY_MOVED = "already_moved"


class ReconciliationState(StrEnum):
    """Observed payload/manifest states that do not mutate storage."""

    ABSENT = "absent"
    PAYLOAD_ONLY = "payload_only"
    COMPLETE = "complete"
    MANIFEST_ONLY = "manifest_only"


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Verified evidence derived from actual object bytes."""

    key: str
    content_length: int
    checksum_sha256: str
    content_type: str
    metadata: dict[str, str]


@dataclass(frozen=True, slots=True)
class ImmutableWriteResult:
    """Result of creating or reusing one immutable object."""

    status: ImmutableWriteStatus
    object: StoredObject


@dataclass(frozen=True, slots=True)
class MoveResult:
    """Result of converging one source/destination pair."""

    status: MoveStatus
    destination: StoredObject


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Read-only payload/manifest observation."""

    state: ReconciliationState
    payload: StoredObject | None
    manifest: Manifest | None


@dataclass(frozen=True, slots=True)
class _SmallObject:
    info: StoredObject
    body: bytes


def create_s3_client(config: StorageConfig) -> S3Client:
    """Construct an explicit SigV4 S3 client without performing network I/O."""
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        config=Config(
            signature_version="s3v4",
            connect_timeout=config.connect_timeout_seconds,
            read_timeout=config.read_timeout_seconds,
            retries={
                "mode": "standard",
                "total_max_attempts": config.total_max_attempts,
            },
            s3={"addressing_style": config.addressing_style},
        ),
    )


class S3Storage:
    """Single-bucket S3 boundary for sequential ingestion writers."""

    def __init__(
        self,
        *,
        client: S3Client,
        bucket: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_manifest_bytes: int = DEFAULT_MAX_MANIFEST_BYTES,
    ) -> None:
        if not bucket.strip():
            raise ValueError("bucket must not be empty")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if max_manifest_bytes <= 0:
            raise ValueError("max_manifest_bytes must be positive")
        self._client = client
        self._bucket = bucket
        self._chunk_size = chunk_size
        self._max_manifest_bytes = max_manifest_bytes

    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        """Create an object once, or reuse it only when actual bytes are identical."""
        _validate_key(key)
        _validate_expected(content_length, checksum_sha256)
        _validate_content_type(content_type)
        self._verify_local_stream(
            key,
            stream,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )

        existing = self._read_object(key)
        if existing is not None:
            self._require_match(
                existing,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                conflict=True,
            )
            return ImmutableWriteResult(ImmutableWriteStatus.UNCHANGED, existing)

        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=stream,
            ContentLength=content_length,
            ContentType=content_type,
            Metadata={CHECKSUM_METADATA_KEY: checksum_sha256},
        )
        stored = self._require_object(key)
        self._require_match(
            stored,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            conflict=False,
        )
        if stored.content_type != content_type:
            raise ObjectVerificationError(f"object {key!r} has unexpected content type")
        if stored.metadata.get(CHECKSUM_METADATA_KEY) != checksum_sha256:
            raise ObjectVerificationError(f"object {key!r} has unexpected checksum metadata")
        return ImmutableWriteResult(ImmutableWriteStatus.CREATED, stored)

    def move_verified(
        self,
        source_key: str,
        destination_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> MoveResult:
        """Converge source-only, both-identical, or destination-only to destination-only."""
        _validate_key(source_key)
        _validate_key(destination_key)
        if source_key == destination_key:
            raise ValueError("source_key and destination_key must differ")
        _validate_expected(content_length, checksum_sha256)

        source = self._read_object(source_key)
        destination = self._read_object(destination_key)

        if source is None:
            if destination is None:
                raise ObjectNotFoundError(
                    f"neither source {source_key!r} nor destination {destination_key!r} exists"
                )
            self._require_match(
                destination,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                conflict=True,
            )
            return MoveResult(MoveStatus.ALREADY_MOVED, destination)

        self._require_match(
            source,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            conflict=False,
        )

        if destination is None:
            self._client.copy_object(
                Bucket=self._bucket,
                Key=destination_key,
                CopySource={"Bucket": self._bucket, "Key": source_key},
                MetadataDirective="COPY",
            )
            destination = self._require_object(destination_key)
            self._require_match(
                destination,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                conflict=False,
            )
        else:
            self._require_match(
                destination,
                content_length=content_length,
                checksum_sha256=checksum_sha256,
                conflict=True,
            )

        if (
            destination.content_type != source.content_type
            or destination.metadata != source.metadata
        ):
            raise ObjectConflictError(
                f"destination object {destination_key!r} does not preserve source metadata"
            )

        self._client.delete_object(Bucket=self._bucket, Key=source_key)
        if self._read_object(source_key) is not None:
            raise ObjectVerificationError(f"source object {source_key!r} still exists after delete")

        final_destination = self._require_object(destination_key)
        self._require_match(
            final_destination,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            conflict=False,
        )
        if (
            final_destination.content_type != source.content_type
            or final_destination.metadata != source.metadata
        ):
            raise ObjectVerificationError(
                f"destination object {destination_key!r} changed during movement"
            )
        return MoveResult(MoveStatus.MOVED, final_destination)

    def reconcile_payload_manifest(
        self,
        payload_key: str,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> ReconciliationResult:
        """Classify one deterministic payload/manifest pair without changing either object."""
        _validate_expected(content_length, checksum_sha256)
        derived_manifest_key = manifest_key_for(payload_key)
        payload = self._read_object(payload_key)
        manifest_object = self._read_small_object(
            derived_manifest_key,
            maximum_bytes=self._max_manifest_bytes,
        )

        if payload is None:
            state = (
                ReconciliationState.ABSENT
                if manifest_object is None
                else ReconciliationState.MANIFEST_ONLY
            )
            return ReconciliationResult(state, None, None)

        self._require_match(
            payload,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            conflict=True,
        )
        if manifest_object is None:
            return ReconciliationResult(ReconciliationState.PAYLOAD_ONLY, payload, None)

        try:
            manifest_json = manifest_object.body.decode("utf-8")
            manifest = Manifest.from_json(manifest_json)
        except (UnicodeDecodeError, TypeError, ValueError) as error:
            raise ObjectVerificationError(
                f"manifest object {derived_manifest_key!r} is invalid"
            ) from error

        if (
            manifest.object_key != payload_key
            or manifest.content_length != payload.content_length
            or manifest.checksum_sha256 != payload.checksum_sha256
            or manifest.content_type != payload.content_type
        ):
            raise ObjectConflictError(
                f"manifest object {derived_manifest_key!r} does not match payload {payload_key!r}"
            )
        return ReconciliationResult(ReconciliationState.COMPLETE, payload, manifest)

    def _verify_local_stream(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
    ) -> None:
        if not stream.seekable():
            raise ValueError("immutable writes require a seekable binary stream")
        if stream.tell() != 0:
            raise ValueError("immutable write stream must be positioned at its start")

        digest = sha256()
        actual_length = 0
        try:
            while chunk := stream.read(self._chunk_size):
                digest.update(chunk)
                actual_length += len(chunk)
        finally:
            stream.seek(0)
        if actual_length != content_length or digest.hexdigest() != checksum_sha256:
            raise ObjectVerificationError(f"local payload for object {key!r} does not match")

    def _require_object(self, key: str) -> StoredObject:
        stored = self._read_object(key)
        if stored is None:
            raise ObjectVerificationError(f"object {key!r} is absent after storage operation")
        return stored

    def _read_object(self, key: str) -> StoredObject | None:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as error:
            if _is_missing(error):
                return None
            raise

        body = response["Body"]
        digest = sha256()
        actual_length = 0
        try:
            while chunk := body.read(self._chunk_size):
                digest.update(chunk)
                actual_length += len(chunk)
        finally:
            body.close()

        declared_length = response.get("ContentLength")
        if declared_length is None or declared_length != actual_length:
            raise ObjectVerificationError(f"object {key!r} returned inconsistent content length")
        content_type = response.get("ContentType", "application/octet-stream")
        metadata = dict(response.get("Metadata", {}))
        return StoredObject(
            key=key,
            content_length=actual_length,
            checksum_sha256=digest.hexdigest(),
            content_type=content_type,
            metadata=metadata,
        )

    def _read_small_object(self, key: str, *, maximum_bytes: int) -> _SmallObject | None:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as error:
            if _is_missing(error):
                return None
            raise

        body = response["Body"]
        declared_length = response.get("ContentLength")
        if declared_length is None:
            body.close()
            raise ObjectVerificationError(f"object {key!r} has no content length")
        if declared_length > maximum_bytes:
            body.close()
            raise ObjectVerificationError(f"object {key!r} exceeds the maximum manifest size")

        try:
            content = body.read(maximum_bytes + 1)
        finally:
            body.close()
        if len(content) != declared_length or len(content) > maximum_bytes:
            raise ObjectVerificationError(f"object {key!r} returned inconsistent content length")

        metadata = dict(response.get("Metadata", {}))
        content_type = response.get("ContentType", "application/octet-stream")
        info = StoredObject(
            key=key,
            content_length=len(content),
            checksum_sha256=sha256(content).hexdigest(),
            content_type=content_type,
            metadata=metadata,
        )
        return _SmallObject(info, content)

    @staticmethod
    def _require_match(
        stored: StoredObject,
        *,
        content_length: int,
        checksum_sha256: str,
        conflict: bool,
    ) -> None:
        if stored.content_length == content_length and stored.checksum_sha256 == checksum_sha256:
            return
        exception_type = ObjectConflictError if conflict else ObjectVerificationError
        raise exception_type(f"object {stored.key!r} does not match expected bytes")


def _is_missing(error: ClientError) -> bool:
    response = cast(dict[str, object], error.response)
    error_details = response.get("Error")
    code = error_details.get("Code") if isinstance(error_details, dict) else None
    response_metadata = response.get("ResponseMetadata")
    status = (
        response_metadata.get("HTTPStatusCode") if isinstance(response_metadata, dict) else None
    )
    return code in _MISSING_ERROR_CODES or status == 404


def _validate_key(key: str) -> None:
    if not key or key != key.strip():
        raise ValueError("object key must be a non-empty normalized string")


def _validate_expected(content_length: int, checksum_sha256: str) -> None:
    if isinstance(content_length, bool) or content_length < 0:
        raise ValueError("content_length must be a non-negative integer")
    if len(checksum_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in checksum_sha256
    ):
        raise ValueError("checksum_sha256 must be 64 lowercase hexadecimal characters")


def _validate_content_type(content_type: str) -> None:
    if not content_type or content_type != content_type.strip():
        raise ValueError("content_type must be a normalized non-empty value")
