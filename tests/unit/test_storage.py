import io
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import BinaryIO, Protocol, cast

import pytest
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from batch_ingestion.config import StorageConfig
from batch_ingestion.identifiers import BatchIdentity, SourceType, manifest_key_for, raw_object_key
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.storage import (
    ImmutableWriteStatus,
    MoveStatus,
    ObjectConflictError,
    ObjectNotFoundError,
    ObjectVerificationError,
    ReconciliationState,
    S3Storage,
    create_s3_client,
)

PAYLOAD = b'{"rates":[1,2,3]}\n'
PAYLOAD_SHA256 = "3d31e59ed66d50e2860d0af3c2cbbe9f4e6c07819adee061a115475359eb940b"
OTHER_PAYLOAD = b"different bytes\n"
OTHER_SHA256 = "78442dda3e5642ce405f6dc6a36797ab67d4c6bef4f46eb5e19d2c66d7f1595c"
BUCKET = "batch-lab"


@dataclass
class _Object:
    body: bytes
    content_type: str
    metadata: dict[str, str]


class _InspectableConfig(Protocol):
    signature_version: str
    s3: dict[str, str]
    retries: dict[str, object]
    connect_timeout: float
    read_timeout: float


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, _Object] = {}
        self.put_count = 0
        self.copy_count = 0
        self.delete_count = 0
        self.get_errors: dict[str, ClientError] = {}
        self.fail_copy = False
        self.fail_delete = False
        self.corrupt_copy = False
        self.keep_source_on_delete = False

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == BUCKET
        if error := self.get_errors.get(Key):
            raise error
        stored = self.objects.get(Key)
        if stored is None:
            raise _client_error("NoSuchKey", 404, "GetObject")
        return {
            "Body": io.BytesIO(stored.body),
            "ContentLength": len(stored.body),
            "ContentType": stored.content_type,
            "Metadata": dict(stored.metadata),
        }

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: BinaryIO,
        ContentLength: int,
        ContentType: str,
        Metadata: Mapping[str, str],
    ) -> dict[str, object]:
        assert Bucket == BUCKET
        body = Body.read()
        assert len(body) == ContentLength
        self.put_count += 1
        self.objects[Key] = _Object(body, ContentType, dict(Metadata))
        return {}

    def copy_object(
        self,
        *,
        Bucket: str,
        Key: str,
        CopySource: Mapping[str, str],
        MetadataDirective: str,
    ) -> dict[str, object]:
        assert Bucket == BUCKET
        assert CopySource["Bucket"] == BUCKET
        assert MetadataDirective == "COPY"
        if self.fail_copy:
            raise _client_error("ServiceUnavailable", 503, "CopyObject")
        source = self.objects[CopySource["Key"]]
        body = source.body + (b"corrupt" if self.corrupt_copy else b"")
        self.copy_count += 1
        self.objects[Key] = _Object(body, source.content_type, dict(source.metadata))
        return {}

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == BUCKET
        if self.fail_delete:
            raise _client_error("ServiceUnavailable", 503, "DeleteObject")
        self.delete_count += 1
        if not self.keep_source_on_delete:
            self.objects.pop(Key, None)
        return {}


def _client_error(code: str, status: int, operation: str) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {
                "HTTPStatusCode": status,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
                "RequestId": "test-request",
                "HostId": "test-host",
            },
        },
        operation,
    )


def _storage(fake: FakeS3Client, *, max_manifest_bytes: int = 64 * 1024) -> S3Storage:
    return S3Storage(
        client=cast(S3Client, fake),
        bucket=BUCKET,
        max_manifest_bytes=max_manifest_bytes,
    )


def _put_fake(
    fake: FakeS3Client,
    key: str,
    body: bytes = PAYLOAD,
    *,
    content_type: str = "application/json",
    metadata: Mapping[str, str] | None = None,
) -> None:
    fake.objects[key] = _Object(
        body,
        content_type,
        dict(metadata or {"checksum-sha256": PAYLOAD_SHA256}),
    )


def _manifest(payload_key: str) -> Manifest:
    identity = BatchIdentity.create("fx-rates", SourceType.API, date(2026, 9, 1))
    return Manifest(
        source_name=identity.source_name,
        source_type=identity.source_type,
        source_date=identity.source_date,
        batch_id=payload_key.split("batch_id=")[1].split("/")[0],
        run_id="run_00000000000000000000000000000000",
        object_key=payload_key,
        original_file_name=None,
        content_type="application/json",
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
        schema_version=1,
        extracted_at=datetime(2026, 9, 1, tzinfo=UTC),
        status=ManifestStatus.RAW_STORED,
        attempt=1,
    )


def _payload_key() -> str:
    identity = BatchIdentity.create("fx-rates", SourceType.API, date(2026, 9, 1))
    return raw_object_key(identity, "page-0001.json")


def test_client_uses_explicit_path_style_sigv4_and_bounded_retries() -> None:
    config = StorageConfig(
        endpoint_url="http://localhost:3900",
        region="garage",
        bucket=BUCKET,
        access_key="access-secret",
        secret_key="storage-secret",
        addressing_style="path",
        total_max_attempts=3,
        connect_timeout_seconds=2,
        read_timeout_seconds=5,
    )

    client = create_s3_client(config)
    try:
        client_config = cast(_InspectableConfig, client.meta.config)
        assert client_config.signature_version == "s3v4"
        assert client_config.s3 == {"addressing_style": "path"}
        assert client_config.retries == {"mode": "standard", "total_max_attempts": 3}
        assert client_config.connect_timeout == 2
        assert client_config.read_timeout == 5
        assert client.meta.endpoint_url == "http://localhost:3900"
    finally:
        client.close()


def test_immutable_write_creates_then_accepts_identical_replay() -> None:
    fake = FakeS3Client()
    storage = _storage(fake)

    created = storage.write_immutable(
        "raw/object.json",
        io.BytesIO(PAYLOAD),
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
        content_type="application/json",
    )
    unchanged = storage.write_immutable(
        "raw/object.json",
        io.BytesIO(PAYLOAD),
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
        content_type="application/json",
    )

    assert created.status is ImmutableWriteStatus.CREATED
    assert unchanged.status is ImmutableWriteStatus.UNCHANGED
    assert fake.put_count == 1
    assert fake.objects["raw/object.json"].body == PAYLOAD


def test_immutable_write_rejects_different_existing_content_without_overwrite() -> None:
    fake = FakeS3Client()
    _put_fake(fake, "raw/object.json", OTHER_PAYLOAD)

    with pytest.raises(ObjectConflictError, match="raw/object.json"):
        _storage(fake).write_immutable(
            "raw/object.json",
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )

    assert fake.put_count == 0
    assert fake.objects["raw/object.json"].body == OTHER_PAYLOAD


@pytest.mark.parametrize(
    ("content_length", "checksum"),
    [(len(PAYLOAD) + 1, PAYLOAD_SHA256), (len(PAYLOAD), OTHER_SHA256)],
)
def test_immutable_write_validates_local_stream_before_s3(
    content_length: int, checksum: str
) -> None:
    fake = FakeS3Client()

    with pytest.raises(ObjectVerificationError):
        _storage(fake).write_immutable(
            "raw/object.json",
            io.BytesIO(PAYLOAD),
            content_length=content_length,
            checksum_sha256=checksum,
            content_type="application/json",
        )

    assert fake.put_count == 0


def test_immutable_write_rejects_non_seekable_stream() -> None:
    class NonSeekable(io.BytesIO):
        def seekable(self) -> bool:
            return False

    with pytest.raises(ValueError, match="seekable"):
        _storage(FakeS3Client()).write_immutable(
            "raw/object.json",
            NonSeekable(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )


def test_non_missing_s3_error_is_not_classified_as_absence() -> None:
    fake = FakeS3Client()
    fake.get_errors["raw/object.json"] = _client_error("AccessDenied", 403, "GetObject")

    with pytest.raises(ClientError) as captured:
        _storage(fake).write_immutable(
            "raw/object.json",
            io.BytesIO(PAYLOAD),
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
            content_type="application/json",
        )

    assert captured.value.response.get("Error", {}).get("Code") == "AccessDenied"
    assert fake.put_count == 0


def test_move_copies_verifies_preserves_metadata_then_deletes() -> None:
    fake = FakeS3Client()
    _put_fake(fake, "landing/input.json", metadata={"source": "supplier"})

    result = _storage(fake).move_verified(
        "landing/input.json",
        "archive/input.json",
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
    )

    assert result.status is MoveStatus.MOVED
    assert "landing/input.json" not in fake.objects
    assert fake.objects["archive/input.json"] == _Object(
        PAYLOAD, "application/json", {"source": "supplier"}
    )
    assert fake.copy_count == 1
    assert fake.delete_count == 1


def test_move_resumes_when_both_identical_objects_exist() -> None:
    fake = FakeS3Client()
    _put_fake(fake, "landing/input.json")
    _put_fake(fake, "archive/input.json")

    result = _storage(fake).move_verified(
        "landing/input.json",
        "archive/input.json",
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
    )

    assert result.status is MoveStatus.MOVED
    assert fake.copy_count == 0
    assert fake.delete_count == 1
    assert "landing/input.json" not in fake.objects


def test_move_resumes_when_only_verified_destination_exists() -> None:
    fake = FakeS3Client()
    _put_fake(fake, "archive/input.json")

    result = _storage(fake).move_verified(
        "landing/input.json",
        "archive/input.json",
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
    )

    assert result.status is MoveStatus.ALREADY_MOVED
    assert fake.copy_count == 0
    assert fake.delete_count == 0


def test_move_conflicting_destination_leaves_source() -> None:
    fake = FakeS3Client()
    _put_fake(fake, "landing/input.json")
    _put_fake(fake, "archive/input.json", OTHER_PAYLOAD)

    with pytest.raises(ObjectConflictError, match="archive/input.json"):
        _storage(fake).move_verified(
            "landing/input.json",
            "archive/input.json",
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )

    assert fake.objects["landing/input.json"].body == PAYLOAD
    assert fake.objects["archive/input.json"].body == OTHER_PAYLOAD
    assert fake.delete_count == 0


def test_move_copy_failure_leaves_source_only() -> None:
    fake = FakeS3Client()
    fake.fail_copy = True
    _put_fake(fake, "landing/input.json")

    with pytest.raises(ClientError):
        _storage(fake).move_verified(
            "landing/input.json",
            "archive/input.json",
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )

    assert "landing/input.json" in fake.objects
    assert "archive/input.json" not in fake.objects
    assert fake.delete_count == 0


def test_move_copy_verification_failure_leaves_source() -> None:
    fake = FakeS3Client()
    fake.corrupt_copy = True
    _put_fake(fake, "landing/input.json")

    with pytest.raises(ObjectVerificationError, match="archive/input.json"):
        _storage(fake).move_verified(
            "landing/input.json",
            "archive/input.json",
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )

    assert "landing/input.json" in fake.objects
    assert fake.delete_count == 0


def test_move_delete_failure_leaves_both_objects_for_retry() -> None:
    fake = FakeS3Client()
    fake.fail_delete = True
    _put_fake(fake, "landing/input.json")

    with pytest.raises(ClientError):
        _storage(fake).move_verified(
            "landing/input.json",
            "archive/input.json",
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )

    assert fake.objects["landing/input.json"].body == PAYLOAD
    assert fake.objects["archive/input.json"].body == PAYLOAD


def test_move_detects_source_that_remains_after_delete() -> None:
    fake = FakeS3Client()
    fake.keep_source_on_delete = True
    _put_fake(fake, "landing/input.json")

    with pytest.raises(ObjectVerificationError, match="still exists"):
        _storage(fake).move_verified(
            "landing/input.json",
            "archive/input.json",
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )

    assert "landing/input.json" in fake.objects
    assert "archive/input.json" in fake.objects


def test_move_requires_at_least_one_matching_object() -> None:
    with pytest.raises(ObjectNotFoundError):
        _storage(FakeS3Client()).move_verified(
            "landing/input.json",
            "archive/input.json",
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )


def test_reconcile_absent_and_payload_only_states() -> None:
    fake = FakeS3Client()
    storage = _storage(fake)
    payload_key = _payload_key()

    absent = storage.reconcile_payload_manifest(
        payload_key,
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
    )
    _put_fake(fake, payload_key)
    payload_only = storage.reconcile_payload_manifest(
        payload_key,
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
    )

    assert absent.state is ReconciliationState.ABSENT
    assert payload_only.state is ReconciliationState.PAYLOAD_ONLY
    assert payload_only.manifest is None


def test_reconcile_complete_state_validates_manifest_linkage() -> None:
    fake = FakeS3Client()
    payload_key = _payload_key()
    _put_fake(fake, payload_key)
    manifest = _manifest(payload_key)
    _put_fake(
        fake,
        manifest_key_for(payload_key),
        manifest.to_json().encode(),
        content_type="application/json",
        metadata={},
    )

    result = _storage(fake).reconcile_payload_manifest(
        payload_key,
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
    )

    assert result.state is ReconciliationState.COMPLETE
    assert result.manifest == manifest


def test_reconcile_rejects_manifest_linked_to_another_payload() -> None:
    fake = FakeS3Client()
    payload_key = _payload_key()
    other_key = payload_key.replace("page-0001.json", "page-0002.json")
    _put_fake(fake, payload_key)
    _put_fake(
        fake,
        manifest_key_for(payload_key),
        _manifest(other_key).to_json().encode(),
        metadata={},
    )

    with pytest.raises(ObjectConflictError, match="does not match payload"):
        _storage(fake).reconcile_payload_manifest(
            payload_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )


def test_reconcile_reports_manifest_only_without_mutation() -> None:
    fake = FakeS3Client()
    payload_key = _payload_key()
    _put_fake(fake, manifest_key_for(payload_key), b"not parsed", metadata={})

    result = _storage(fake).reconcile_payload_manifest(
        payload_key,
        content_length=len(PAYLOAD),
        checksum_sha256=PAYLOAD_SHA256,
    )

    assert result.state is ReconciliationState.MANIFEST_ONLY
    assert len(fake.objects) == 1


@pytest.mark.parametrize("manifest_body", [b"not json", b"{}"])
def test_reconcile_rejects_malformed_manifest(manifest_body: bytes) -> None:
    fake = FakeS3Client()
    payload_key = _payload_key()
    _put_fake(fake, payload_key)
    _put_fake(fake, manifest_key_for(payload_key), manifest_body, metadata={})

    with pytest.raises(ObjectVerificationError, match="manifest"):
        _storage(fake).reconcile_payload_manifest(
            payload_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )


def test_reconcile_rejects_oversized_manifest_before_parsing() -> None:
    fake = FakeS3Client()
    payload_key = _payload_key()
    _put_fake(fake, payload_key)
    _put_fake(fake, manifest_key_for(payload_key), b"x" * 65, metadata={})

    with pytest.raises(ObjectVerificationError, match="maximum"):
        _storage(fake, max_manifest_bytes=64).reconcile_payload_manifest(
            payload_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )


def test_reconcile_rejects_payload_checksum_conflict() -> None:
    fake = FakeS3Client()
    payload_key = _payload_key()
    _put_fake(fake, payload_key, OTHER_PAYLOAD)

    with pytest.raises(ObjectConflictError, match=payload_key):
        _storage(fake).reconcile_payload_manifest(
            payload_key,
            content_length=len(PAYLOAD),
            checksum_sha256=PAYLOAD_SHA256,
        )
