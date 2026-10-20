"""Pure delivery and diagnostic contracts for BI-060 slice 1."""

import json
from datetime import date
from hashlib import sha256
from pathlib import Path

import pytest

from batch_ingestion.files import (
    DeliveryDescriptor,
    DuplicateClass,
    FileDiagnostic,
    FileErrorCode,
    FileFormat,
    FileIngestionError,
    FileIngestionEvent,
    FileOutcome,
    RejectionCode,
    bounded_diagnostics,
    classify_duplicate,
    delivery_rejection_codes,
    file_record_counters,
    parse_landing_key,
    primary_rejection_code,
)
from batch_ingestion.summary import RunSummary

SOURCE_DATE = date(2026, 9, 1)
PAYLOAD = b"exact supplier payload"
CHECKSUM = sha256(PAYLOAD).hexdigest()
RUN_ID = "run_1234567812344abc9234567890abcdef"


def _delivery(
    *,
    source_name: str = "supplier-prices",
    original_file_name: str = "supplier-prices-2026-09-01.csv",
    expected_sha256: str | None = None,
    actual_sha256: str = CHECKSUM,
    validation_contract: str = "supplier-prices-v1",
) -> DeliveryDescriptor:
    return DeliveryDescriptor.create(
        source_name=source_name,
        source_date=SOURCE_DATE,
        original_file_name=original_file_name,
        actual_sha256=actual_sha256,
        expected_sha256=expected_sha256,
        validation_contract=validation_contract,
    )


@pytest.mark.parametrize(
    ("name", "file_format", "content_type", "payload_name"),
    [
        ("supplier-prices-2026-09-01.csv", FileFormat.CSV, "text/csv", "payload.csv"),
        (
            "supplier-prices-original-2026-09-01.json",
            FileFormat.JSON,
            "application/json",
            "payload.json",
        ),
        (
            "supplier-prices-region1-extra2-2026-09-01.parquet",
            FileFormat.PARQUET,
            "application/vnd.apache.parquet",
            "payload.parquet",
        ),
    ],
)
def test_supplier_filename_parsing_and_canonical_payload_contract(
    name: str,
    file_format: FileFormat,
    content_type: str,
    payload_name: str,
) -> None:
    delivery = _delivery(original_file_name=name)

    assert delivery.file_name_valid is True
    assert delivery.filename_date == SOURCE_DATE
    assert delivery.file_format is file_format
    assert delivery.content_type == content_type
    assert delivery.payload_name == payload_name
    assert delivery_rejection_codes(delivery) == ()


def test_representable_business_invalid_names_are_retained_as_unknown_or_declared_format() -> None:
    unsupported_extension = _delivery(original_file_name="supplier-prices-2026-09-01.txt")
    unrelated_name = _delivery(original_file_name="prices-2026-09-01.csv")

    assert unsupported_extension.file_format is FileFormat.UNKNOWN
    assert unsupported_extension.payload_name == "payload.bin"
    assert delivery_rejection_codes(unsupported_extension) == (RejectionCode.INVALID_FILE_NAME,)
    assert unrelated_name.file_format is FileFormat.CSV
    assert delivery_rejection_codes(unrelated_name) == (RejectionCode.INVALID_FILE_NAME,)


@pytest.mark.parametrize(
    "name",
    [
        "../supplier-prices-2026-09-01.csv",
        "nested/supplier-prices-2026-09-01.csv",
        " supplier-prices-2026-09-01.csv ",
        "supplier-prices-2026-09-01.CSV",
    ],
)
def test_unrepresentable_or_noncanonical_basenames_fail_admission(name: str) -> None:
    if name.endswith(".CSV"):
        delivery = _delivery(original_file_name=name)
        assert delivery.file_format is FileFormat.UNKNOWN
        assert delivery_rejection_codes(delivery) == (RejectionCode.INVALID_FILE_NAME,)
    else:
        with pytest.raises(ValueError, match="original_file_name"):
            _delivery(original_file_name=name)


def test_source_allow_list_and_source_date_mismatch_are_business_rejections() -> None:
    unsupported = _delivery(source_name="other-supplier")
    date_mismatch = _delivery(original_file_name="supplier-prices-2026-09-02.csv")

    assert delivery_rejection_codes(unsupported) == (RejectionCode.UNSUPPORTED_SOURCE,)
    assert delivery_rejection_codes(date_mismatch) == (RejectionCode.SOURCE_DATE_MISMATCH,)

    with pytest.raises(ValueError, match="source_name"):
        _delivery(source_name="unsafe/source")


@pytest.mark.parametrize("value", ["A" * 64, "a" * 63, "not-a-checksum"])
def test_expected_and_actual_checksums_must_be_lowercase_sha256(value: str) -> None:
    with pytest.raises(ValueError, match="actual_sha256"):
        _delivery(actual_sha256=value)
    with pytest.raises(ValueError, match="expected_sha256"):
        _delivery(expected_sha256=value)


def test_identity_parameters_and_exact_keys_are_deterministic() -> None:
    delivery = _delivery(expected_sha256=CHECKSUM)
    batch_id = delivery.batch_id

    assert dict(delivery.identity.parameters) == {
        "content-sha256": CHECKSUM,
        "declared-format": "csv",
        "expected-sha256": CHECKSUM,
        "validation-contract": "supplier-prices-v1",
    }
    assert delivery.landing_key == (
        "landing/file/supplier-prices/source_date=2026-09-01/format=csv/"
        f"content_sha256={CHECKSUM}/expected_sha256={CHECKSUM}/"
        "supplier-prices-2026-09-01.csv"
    )
    assert delivery.raw_key == (
        f"raw/file/supplier-prices/source_date=2026-09-01/batch_id={batch_id}/payload.csv"
    )
    assert delivery.manifest_key == (
        "manifests/file/supplier-prices/source_date=2026-09-01/"
        f"batch_id={batch_id}/payload.csv.manifest.json"
    )
    assert delivery.archive_key == (
        f"archive/file/supplier-prices/source_date=2026-09-01/batch_id={batch_id}/payload.csv"
    )
    assert delivery.rejected_key(RejectionCode.CHECKSUM_MISMATCH) == (
        "rejected/file/supplier-prices/source_date=2026-09-01/"
        f"batch_id={batch_id}/reason=checksum_mismatch/payload.csv"
    )


def test_landing_key_strictly_round_trips_all_identity_inputs() -> None:
    delivery = _delivery(
        source_name="other-supplier",
        original_file_name="supplier-prices-original-2026-09-01.json",
        expected_sha256="c" * 64,
    )

    assert parse_landing_key(delivery.landing_key) == delivery


@pytest.mark.parametrize(
    "replacement",
    [
        ("landing/file/", "raw/file/"),
        ("source_date=2026-09-01", "source_date=2026-9-1"),
        ("format=csv", "format=json"),
        (f"content_sha256={CHECKSUM}", "content_sha256=not-a-checksum"),
        ("expected_sha256=none", "expected_sha256=NONE"),
    ],
)
def test_landing_key_parser_rejects_noncanonical_or_inconsistent_keys(
    replacement: tuple[str, str],
) -> None:
    key = _delivery().landing_key.replace(*replacement)

    with pytest.raises(ValueError, match="landing key"):
        parse_landing_key(key)


def test_same_name_and_renamed_content_key_relationships() -> None:
    first = _delivery(original_file_name="supplier-prices-original-2026-09-01.csv")
    same_name = _delivery(original_file_name="supplier-prices-original-2026-09-01.csv")
    renamed = _delivery(original_file_name="supplier-prices-renamed-2026-09-01.csv")

    assert first.landing_key == same_name.landing_key
    assert first.raw_key == same_name.raw_key == renamed.raw_key
    assert first.archive_key == same_name.archive_key == renamed.archive_key
    assert first.rejected_key(RejectionCode.EMPTY_FILE) == renamed.rejected_key(
        RejectionCode.EMPTY_FILE
    )
    assert first.landing_key != renamed.landing_key


@pytest.mark.parametrize(
    "changed",
    [
        {"actual_sha256": "b" * 64},
        {"original_file_name": "supplier-prices-2026-09-01.json"},
        {"expected_sha256": "c" * 64},
        {"validation_contract": "supplier-prices-v2"},
    ],
)
def test_content_format_expected_checksum_and_contract_change_batch(
    changed: dict[str, str],
) -> None:
    assert _delivery().batch_id != _delivery(**changed).batch_id


@pytest.mark.parametrize(
    ("existing_name", "current_name", "expected"),
    [
        (None, "supplier-prices-2026-09-01.csv", DuplicateClass.NEW_DELIVERY),
        (
            "supplier-prices-2026-09-01.csv",
            "supplier-prices-2026-09-01.csv",
            DuplicateClass.SAME_NAME_REDELIVERY,
        ),
        (
            "supplier-prices-original-2026-09-01.csv",
            "supplier-prices-renamed-2026-09-01.csv",
            DuplicateClass.RENAMED_CONTENT,
        ),
    ],
)
def test_duplicate_classification_is_separate_from_validation(
    existing_name: str | None,
    current_name: str,
    expected: DuplicateClass,
) -> None:
    assert classify_duplicate(existing_name, current_name) is expected


def test_rejection_precedence_is_stable_independent_of_input_order() -> None:
    codes = [
        RejectionCode.BUSINESS_RULE_VIOLATION,
        RejectionCode.EMPTY_FILE,
        RejectionCode.CHECKSUM_MISMATCH,
    ]

    assert primary_rejection_code(codes) is RejectionCode.CHECKSUM_MISMATCH
    assert primary_rejection_code(reversed(codes)) is RejectionCode.CHECKSUM_MISMATCH
    assert primary_rejection_code([]) is None


def test_diagnostics_are_value_free_ordered_bounded_and_serializable() -> None:
    diagnostics = [
        FileDiagnostic(
            code=RejectionCode.BUSINESS_RULE_VIOLATION,
            record_number=3,
            field="price_minor_units",
        ),
        FileDiagnostic(
            code=RejectionCode.INVALID_FIELD_TYPE,
            record_number=2,
            field="price_minor_units",
        ),
        FileDiagnostic(
            code=RejectionCode.MISSING_REQUIRED_VALUE,
            record_number=2,
            field="supplier_id",
        ),
    ]

    bounded = bounded_diagnostics(diagnostics, max_details=2)

    assert bounded.details == (diagnostics[2], diagnostics[1])
    assert bounded.truncated is True
    assert bounded.to_list() == [
        {
            "code": "missing_required_value",
            "record_number": 2,
            "field": "supplier_id",
        },
        {
            "code": "invalid_field_type",
            "record_number": 2,
            "field": "price_minor_units",
        },
    ]
    assert "supplier-secret-value" not in json.dumps(bounded.to_list())

    with pytest.raises(ValueError, match="max_details"):
        bounded_diagnostics(diagnostics, max_details=0)
    with pytest.raises(ValueError, match="field"):
        FileDiagnostic(
            code=RejectionCode.INVALID_FIELD_TYPE,
            record_number=1,
            field="raw supplier value",
        )


@pytest.mark.parametrize(
    ("records_read", "accepted", "expected"),
    [
        (15, True, (15, 15, 0)),
        (3, False, (3, 0, 3)),
        (0, False, (0, 0, 0)),
    ],
)
def test_whole_file_counters(
    records_read: int,
    accepted: bool,
    expected: tuple[int, int, int],
) -> None:
    counters = file_record_counters(records_read=records_read, accepted=accepted)

    assert (counters.records_read, counters.records_accepted, counters.records_rejected) == expected


def test_terminal_event_is_bounded_structured_and_contains_no_secret_or_local_path() -> None:
    delivery = _delivery()
    bounded = bounded_diagnostics(
        [
            FileDiagnostic(
                code=RejectionCode.BUSINESS_RULE_VIOLATION,
                record_number=2,
                field="price_minor_units",
            )
        ],
        max_details=5,
    )
    event = FileIngestionEvent(
        outcome=FileOutcome.REJECTED,
        rejection_code=RejectionCode.BUSINESS_RULE_VIOLATION,
        diagnostics=bounded,
        source_name=delivery.source_name,
        source_date=delivery.source_date,
        batch_id=delivery.batch_id,
        run_id=RUN_ID,
        landing_key=delivery.landing_key,
        raw_key=delivery.raw_key,
        manifest_key=delivery.manifest_key,
        disposition_key=delivery.rejected_key(RejectionCode.BUSINESS_RULE_VIOLATION),
        original_file_name=delivery.original_file_name,
        file_format=delivery.file_format,
        duplicate_class=DuplicateClass.RENAMED_CONTENT,
    )

    value = event.to_dict()
    serialized = event.to_json()

    assert value["event"] == "file_ingestion"
    assert value["outcome"] == "rejected"
    assert value["rejection_code"] == "business_rule_violation"
    assert value["duplicate_class"] == "renamed_content"
    assert value["details_truncated"] is False
    assert value["details"] == bounded.to_list()
    assert "/tmp/secret-delivery" not in serialized
    assert "storage-secret" not in serialized


def test_failed_terminal_event_and_typed_error_are_system_only_and_secret_free() -> None:
    delivery = _delivery()
    diagnostics = bounded_diagnostics([], max_details=1)
    event = FileIngestionEvent(
        outcome=FileOutcome.FAILED,
        rejection_code=None,
        diagnostics=diagnostics,
        source_name=delivery.source_name,
        source_date=delivery.source_date,
        batch_id=delivery.batch_id,
        run_id=RUN_ID,
        landing_key=delivery.landing_key,
        raw_key=delivery.raw_key,
        manifest_key=delivery.manifest_key,
        disposition_key=None,
        original_file_name=delivery.original_file_name,
        file_format=delivery.file_format,
        duplicate_class=None,
        error_code=FileErrorCode.STORAGE_FAILED,
    )
    summary = RunSummary(
        records_read=0,
        records_accepted=0,
        records_rejected=0,
        pages_fetched=0,
        bytes_written=0,
        retry_count=0,
        duplicate_count=0,
        duration_seconds=0.25,
        source_date=delivery.source_date,
        batch_id=delivery.batch_id,
        run_id=RUN_ID,
    )
    error = FileIngestionError(
        code=FileErrorCode.STORAGE_FAILED,
        summary=summary,
        event=event,
    )

    assert event.to_dict() == {
        "event": "file_ingestion",
        "outcome": "failed",
        "error_code": "storage_failed",
        "rejection_code": None,
        "details": [],
        "details_truncated": False,
        "source_name": delivery.source_name,
        "source_date": "2026-09-01",
        "batch_id": delivery.batch_id,
        "run_id": RUN_ID,
        "landing_key": delivery.landing_key,
        "raw_key": delivery.raw_key,
        "manifest_key": delivery.manifest_key,
        "disposition_key": None,
        "original_file_name": delivery.original_file_name,
        "format": "csv",
        "duplicate_class": None,
    }
    assert error.code is FileErrorCode.STORAGE_FAILED
    assert error.summary is summary
    assert error.event is event
    assert str(error) == "Storage operation failed"
    assert "backend-secret" not in event.to_json()
    assert "backend-secret" not in str(error)


def test_file_terminal_event_rejects_mixed_business_and_system_failure_fields() -> None:
    delivery = _delivery()

    def event(
        outcome: FileOutcome,
        *,
        rejection_code: RejectionCode | None,
        error_code: FileErrorCode | None,
    ) -> FileIngestionEvent:
        return FileIngestionEvent(
            outcome=outcome,
            rejection_code=rejection_code,
            diagnostics=bounded_diagnostics([], max_details=1),
            source_name=delivery.source_name,
            source_date=delivery.source_date,
            batch_id=delivery.batch_id,
            run_id=RUN_ID,
            landing_key=delivery.landing_key,
            raw_key=delivery.raw_key,
            manifest_key=delivery.manifest_key,
            disposition_key=None,
            original_file_name=delivery.original_file_name,
            file_format=delivery.file_format,
            duplicate_class=None,
            error_code=error_code,
        )

    with pytest.raises(ValueError, match="failed outcome must carry an error_code"):
        event(FileOutcome.FAILED, rejection_code=None, error_code=None)
    with pytest.raises(ValueError, match="failed outcome must not carry a rejection_code"):
        event(
            FileOutcome.FAILED,
            rejection_code=RejectionCode.MALFORMED_FILE,
            error_code=FileErrorCode.STORAGE_STATE_FAILED,
        )
    with pytest.raises(ValueError, match="business outcome must not carry an error_code"):
        event(
            FileOutcome.ACCEPTED,
            rejection_code=None,
            error_code=FileErrorCode.STORAGE_FAILED,
        )


def test_file_ingestion_documentation_covers_public_contract_and_recovery() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    contract = (repository_root / "docs/ingestion/file-ingestion.md").read_text(encoding="utf-8")
    readme = (repository_root / "README.md").read_text(encoding="utf-8")
    operator_guide = (repository_root / "docs" / "guides" / "operator-guide.md").read_text(
        encoding="utf-8"
    )
    fixture = repository_root / "fixtures/supplier-prices/canonical/valid.csv"
    delivery = _delivery(actual_sha256=sha256(fixture.read_bytes()).hexdigest())

    required_contract_text = {
        'make ingest-file ARGS="run',
        'make ingest-file ARGS="resume',
        delivery.landing_key,
        delivery.raw_key,
        delivery.manifest_key,
        delivery.archive_key,
        "copy -> verify -> delete",
        "not an atomic rename",
        "not an exactly-once claim",
        "concurrent writers",
        "FILE_MAX_BYTES",
        "FILE_SPOOL_THRESHOLD_BYTES",
        "FILE_MAX_RECORDS",
        "FILE_PARQUET_BATCH_ROWS",
        "FILE_MAX_DIAGNOSTIC_DETAILS",
        "same_name_redelivery",
        "renamed_content",
    }
    required_contract_text.update(code.value for code in RejectionCode)

    assert all(value in contract for value in required_contract_text)
    assert "docs/ingestion/file-ingestion.md" in readme
    assert "File failure and recovery exercises" in operator_guide
