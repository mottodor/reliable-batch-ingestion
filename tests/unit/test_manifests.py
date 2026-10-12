import json
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from batch_ingestion.identifiers import BatchIdentity, SourceType, derive_batch_id, raw_object_key
from batch_ingestion.manifests import (
    TERMINAL_STATUSES,
    Manifest,
    ManifestOutcome,
    ManifestStatus,
    can_transition,
    status_for_outcome,
    validate_status_transition,
)


def api_manifest(*, status: ManifestStatus = ManifestStatus.SUCCEEDED) -> Manifest:
    identity = BatchIdentity.create(
        "fx-rates",
        SourceType.API,
        date(2026, 9, 1),
        {"currency": "EUR"},
    )
    return Manifest(
        source_name=identity.source_name,
        source_type=identity.source_type,
        source_date=identity.source_date,
        batch_id=derive_batch_id(identity),
        run_id="run_1234567812344abc9234567890abcdef",
        object_key=raw_object_key(identity, "page-0001.json"),
        original_file_name=None,
        content_type="application/json",
        content_length=128,
        checksum_sha256="a" * 64,
        schema_version=1,
        extracted_at=datetime(2026, 9, 1, 12, 30, 45, 123456, tzinfo=UTC),
        status=status,
        attempt=1,
    )


def test_manifest_contains_and_serializes_every_required_field() -> None:
    manifest = api_manifest()

    serialized = manifest.to_dict()

    assert set(serialized) == {
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
    assert serialized["source_type"] == "api"
    assert serialized["source_date"] == "2026-09-01"
    assert serialized["extracted_at"] == "2026-09-01T12:30:45.123456Z"
    assert serialized["original_file_name"] is None
    assert Manifest.from_json(manifest.to_json()) == manifest


def test_manifest_normalizes_aware_timestamp_to_utc() -> None:
    manifest = api_manifest()
    local_time = datetime(2026, 9, 1, 14, 30, 45, tzinfo=timezone(timedelta(hours=2)))
    values = manifest.to_dict()
    values["extracted_at"] = local_time.isoformat()

    restored = Manifest.from_dict(values)

    assert restored.extracted_at.tzinfo is UTC
    assert restored.to_dict()["extracted_at"] == "2026-09-01T12:30:45.000000Z"


def test_manifest_rejects_naive_timestamp() -> None:
    manifest = api_manifest()

    with pytest.raises(ValueError, match="extracted_at must be timezone-aware"):
        Manifest(
            source_name=manifest.source_name,
            source_type=manifest.source_type,
            source_date=manifest.source_date,
            batch_id=manifest.batch_id,
            run_id=manifest.run_id,
            object_key=manifest.object_key,
            original_file_name=manifest.original_file_name,
            content_type=manifest.content_type,
            content_length=manifest.content_length,
            checksum_sha256=manifest.checksum_sha256,
            schema_version=manifest.schema_version,
            extracted_at=datetime(2026, 9, 1, 12, 30),
            status=manifest.status,
            attempt=manifest.attempt,
        )


def test_original_file_name_is_required_only_for_file_sources() -> None:
    values = api_manifest().to_dict()
    file_identity = BatchIdentity.create(
        "supplier-prices", SourceType.FILE, date(2026, 9, 1), {"delivery": "daily"}
    )
    values.update(
        source_name=file_identity.source_name,
        source_type=file_identity.source_type.value,
        batch_id=derive_batch_id(file_identity),
        object_key=raw_object_key(file_identity, "prices.csv"),
    )

    with pytest.raises(ValueError, match="original_file_name is required"):
        Manifest.from_dict(values)

    values = api_manifest().to_dict()
    values["original_file_name"] = "invented.json"
    with pytest.raises(ValueError, match="must be null"):
        Manifest.from_dict(values)


def test_manifest_serialization_does_not_gain_configured_secrets() -> None:
    serialized = api_manifest().to_json()

    assert "password" not in serialized.casefold()
    assert "authorization" not in serialized.casefold()
    assert "secret" not in serialized.casefold()


def test_manifest_parser_rejects_unknown_or_missing_fields() -> None:
    values = api_manifest().to_dict()
    values["unexpected"] = "value"

    with pytest.raises(ValueError, match="unexpected fields"):
        Manifest.from_dict(values)

    values = api_manifest().to_dict()
    del values["attempt"]
    with pytest.raises(ValueError, match="missing fields"):
        Manifest.from_dict(values)


def test_status_transition_contract_and_terminal_states() -> None:
    expected_terminal_statuses = {
        ManifestStatus.SUCCEEDED,
        ManifestStatus.REJECTED,
        ManifestStatus.DUPLICATE,
        ManifestStatus.FAILED,
    }
    assert expected_terminal_statuses == TERMINAL_STATUSES
    assert can_transition(ManifestStatus.CREATED, ManifestStatus.RAW_STORED)
    assert can_transition(ManifestStatus.RAW_STORED, ManifestStatus.REJECTED)
    assert can_transition(ManifestStatus.SUCCEEDED, ManifestStatus.SUCCEEDED)
    assert not can_transition(ManifestStatus.SUCCEEDED, ManifestStatus.FAILED)

    with pytest.raises(ValueError, match="invalid manifest status transition"):
        validate_status_transition(ManifestStatus.REJECTED, ManifestStatus.CREATED)


@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        (ManifestOutcome.PRESERVED, ManifestStatus.RAW_STORED),
        (ManifestOutcome.ACCEPTED, ManifestStatus.SUCCEEDED),
        (ManifestOutcome.VALIDATION_ERROR, ManifestStatus.REJECTED),
        (ManifestOutcome.DUPLICATE, ManifestStatus.DUPLICATE),
        (ManifestOutcome.SYSTEM_ERROR, ManifestStatus.FAILED),
    ],
)
def test_outcomes_map_to_statuses(outcome: ManifestOutcome, status: ManifestStatus) -> None:
    assert status_for_outcome(outcome) is status


def test_manifest_json_is_canonical() -> None:
    serialized = api_manifest().to_json()

    assert serialized == json.dumps(json.loads(serialized), sort_keys=True, separators=(",", ":"))
