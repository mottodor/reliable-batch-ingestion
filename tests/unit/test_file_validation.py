"""Bounded CSV, JSON, and Parquet validation for BI-060 slice 2."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import replace
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from batch_ingestion.config import FileConfig
from batch_ingestion.files import DeliveryDescriptor, FileFormat, RejectionCode
from batch_ingestion.files.validators import (
    FileSizeLimitError,
    FileValidationResult,
    validate_file,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = REPOSITORY_ROOT / "fixtures" / "supplier-prices" / "canonical"
SOURCE_DATE = date(2026, 9, 1)
FORMATS = (FileFormat.CSV, FileFormat.JSON, FileFormat.PARQUET)
COLUMNS = (
    "supplier_id",
    "product_id",
    "price_minor_units",
    "currency",
    "effective_date",
)
VALID_ROW: dict[str, object] = {
    "supplier_id": "SUP-001",
    "product_id": "SKU-0001",
    "price_minor_units": 1250,
    "currency": "USD",
    "effective_date": "2026-09-01",
}
PARQUET_SCHEMA = pa.schema(
    [
        pa.field("supplier_id", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("price_minor_units", pa.int64(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("effective_date", pa.date32(), nullable=False),
    ]
)
DEFAULT_CONFIG = FileConfig(
    max_file_bytes=8 * 1024 * 1024,
    spool_threshold_bytes=1024 * 1024,
    max_records=100_000,
    parquet_batch_rows=2,
    max_diagnostic_details=100,
)


def _descriptor(
    payload: bytes,
    file_format: FileFormat,
    *,
    source_date: date = SOURCE_DATE,
    expected_sha256: str | None = None,
) -> DeliveryDescriptor:
    return DeliveryDescriptor.create(
        source_name="supplier-prices",
        source_date=source_date,
        original_file_name=f"supplier-prices-{source_date.isoformat()}.{file_format.value}",
        actual_sha256=sha256(payload).hexdigest(),
        expected_sha256=expected_sha256,
    )


def _validate_payload(
    payload: bytes,
    file_format: FileFormat,
    *,
    config: FileConfig = DEFAULT_CONFIG,
    source_date: date = SOURCE_DATE,
    expected_sha256: str | None = None,
) -> FileValidationResult:
    stream = io.BytesIO(payload)
    return validate_file(
        stream,
        delivery=_descriptor(
            payload,
            file_format,
            source_date=source_date,
            expected_sha256=expected_sha256,
        ),
        config=config,
    )


def _fixture(scenario: str, file_format: FileFormat) -> bytes:
    return (FIXTURE_ROOT / f"{scenario}.{file_format.value}").read_bytes()


def _payload(
    file_format: FileFormat,
    rows: Sequence[Mapping[str, object]],
    *,
    parquet_schema: pa.Schema = PARQUET_SCHEMA,
    columns: tuple[str, ...] = COLUMNS,
) -> bytes:
    if file_format is FileFormat.CSV:
        text = io.StringIO(newline="")
        writer = csv.writer(text, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row[column] for column in columns])
        return text.getvalue().encode("utf-8")
    if file_format is FileFormat.JSON:
        projected = [{column: row[column] for column in columns} for row in rows]
        return (json.dumps(projected, separators=(",", ":")) + "\n").encode()

    selected_schema = pa.schema(
        [
            parquet_schema.field(column)
            if column in parquet_schema.names
            else pa.field(column, pa.string(), nullable=False)
            for column in columns
        ]
    )
    parquet_rows: list[dict[str, object]] = []
    for row in rows:
        converted = {column: row[column] for column in columns}
        effective_date = converted.get("effective_date")
        if isinstance(effective_date, str):
            converted["effective_date"] = date.fromisoformat(effective_date)
        parquet_rows.append(converted)
    sink = io.BytesIO()
    table = pa.Table.from_pylist(parquet_rows, schema=selected_schema)
    pq.write_table(table, sink, compression="none", row_group_size=2)
    return sink.getvalue()


@pytest.mark.parametrize("file_format", FORMATS)
def test_valid_committed_fixture_is_accepted_as_one_whole_file(
    file_format: FileFormat,
) -> None:
    payload = _fixture("valid", file_format)

    result = _validate_payload(payload, file_format)

    assert result.accepted is True
    assert result.rejection_code is None
    assert result.diagnostics.details == ()
    assert result.diagnostics.truncated is False
    assert (
        result.counters.records_read,
        result.counters.records_accepted,
        result.counters.records_rejected,
    ) == (15, 15, 0)
    assert not hasattr(result, "records")


@pytest.mark.parametrize("file_format", FORMATS)
def test_empty_committed_fixture_is_a_zero_record_whole_file_rejection(
    file_format: FileFormat,
) -> None:
    result = _validate_payload(_fixture("empty", file_format), file_format)

    assert result.accepted is False
    assert result.rejection_code is RejectionCode.EMPTY_FILE
    assert (
        result.counters.records_read,
        result.counters.records_accepted,
        result.counters.records_rejected,
    ) == (0, 0, 0)


@pytest.mark.parametrize("file_format", FORMATS)
def test_schema_invalid_committed_fixture_fails_before_trustworthy_records(
    file_format: FileFormat,
) -> None:
    result = _validate_payload(_fixture("schema_invalid", file_format), file_format)

    assert result.rejection_code is RejectionCode.SCHEMA_MISMATCH
    assert result.counters.records_read == 0
    assert result.counters.records_accepted == 0


@pytest.mark.parametrize("file_format", FORMATS)
def test_mixed_fixture_uses_exact_adr_0002_three_zero_three_counters(
    file_format: FileFormat,
) -> None:
    result = _validate_payload(_fixture("mixed", file_format), file_format)

    assert result.accepted is False
    assert result.rejection_code is RejectionCode.BUSINESS_RULE_VIOLATION
    assert (
        result.counters.records_read,
        result.counters.records_accepted,
        result.counters.records_rejected,
    ) == (3, 0, 3)
    assert [detail.field for detail in result.diagnostics.details] == ["price_minor_units"]


@pytest.mark.parametrize("file_format", FORMATS)
def test_effective_date_must_match_explicit_source_date(file_format: FileFormat) -> None:
    payload = _fixture("valid", file_format)

    result = _validate_payload(payload, file_format, source_date=date(2026, 9, 2))

    assert result.rejection_code is RejectionCode.BUSINESS_RULE_VIOLATION
    assert result.counters.records_read == 15
    assert result.counters.records_rejected == 15
    assert all(detail.field == "effective_date" for detail in result.diagnostics.details)


@pytest.mark.parametrize("file_format", FORMATS)
def test_expected_checksum_mismatch_stops_before_format_parsing(
    file_format: FileFormat,
) -> None:
    payload = _fixture("valid", file_format)

    result = _validate_payload(payload, file_format, expected_sha256="0" * 64)

    assert result.rejection_code is RejectionCode.CHECKSUM_MISMATCH
    assert result.counters.records_read == 0
    assert result.diagnostics.details[0].record_number is None


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("supplier_id", "", RejectionCode.MISSING_REQUIRED_VALUE),
        ("supplier_id", 7, RejectionCode.INVALID_FIELD_TYPE),
        ("supplier_id", "supplier-1", RejectionCode.BUSINESS_RULE_VIOLATION),
        ("product_id", "", RejectionCode.MISSING_REQUIRED_VALUE),
        ("product_id", 7, RejectionCode.INVALID_FIELD_TYPE),
        ("product_id", "SKU-1", RejectionCode.BUSINESS_RULE_VIOLATION),
        ("price_minor_units", None, RejectionCode.MISSING_REQUIRED_VALUE),
        ("price_minor_units", True, RejectionCode.INVALID_FIELD_TYPE),
        ("price_minor_units", "1250", RejectionCode.INVALID_FIELD_TYPE),
        ("price_minor_units", 0, RejectionCode.BUSINESS_RULE_VIOLATION),
        ("currency", "", RejectionCode.MISSING_REQUIRED_VALUE),
        ("currency", 7, RejectionCode.INVALID_FIELD_TYPE),
        ("currency", "CAD", RejectionCode.BUSINESS_RULE_VIOLATION),
        ("effective_date", "", RejectionCode.MISSING_REQUIRED_VALUE),
        ("effective_date", 20260901, RejectionCode.INVALID_FIELD_TYPE),
        ("effective_date", "2026-9-1", RejectionCode.INVALID_FIELD_TYPE),
        ("effective_date", "2026-09-02", RejectionCode.BUSINESS_RULE_VIOLATION),
    ],
)
def test_json_enforces_every_required_value_type_and_business_rule(
    field: str,
    value: object,
    code: RejectionCode,
) -> None:
    row = dict(VALID_ROW)
    row[field] = value
    payload = _payload(FileFormat.JSON, [row])

    result = _validate_payload(payload, FileFormat.JSON)

    assert result.rejection_code is code
    assert result.counters.records_read == 1
    assert result.diagnostics.details[0].field == field
    assert result.diagnostics.details[0].record_number == 1


@pytest.mark.parametrize("value", ["+12", "01", " 12", "12 ", "1.0", "true"])
def test_csv_requires_canonical_unsigned_integer_text(value: str) -> None:
    row = dict(VALID_ROW)
    row["price_minor_units"] = value
    payload = _payload(FileFormat.CSV, [row])

    result = _validate_payload(payload, FileFormat.CSV)

    assert result.rejection_code is RejectionCode.INVALID_FIELD_TYPE
    assert result.diagnostics.details[0].field == "price_minor_units"


@pytest.mark.parametrize(
    ("file_format", "payload"),
    [
        (FileFormat.CSV, b"\xff\n"),
        (
            FileFormat.CSV,
            b'supplier_id,product_id,price_minor_units,currency,effective_date\n"unterminated',
        ),
        (FileFormat.JSON, b"\xff"),
        (FileFormat.JSON, b"[{"),
        (FileFormat.PARQUET, b"not a parquet file"),
    ],
)
def test_malformed_payloads_become_stable_rejections_not_parser_exceptions(
    file_format: FileFormat,
    payload: bytes,
) -> None:
    result = _validate_payload(payload, file_format)

    assert result.rejection_code is RejectionCode.MALFORMED_FILE
    assert result.accepted is False


@pytest.mark.parametrize("file_format", FORMATS)
@pytest.mark.parametrize("schema_case", ["missing", "extra"])
def test_missing_or_extra_fields_are_schema_mismatches(
    file_format: FileFormat,
    schema_case: str,
) -> None:
    row = dict(VALID_ROW)
    if schema_case == "missing":
        columns = tuple(column for column in COLUMNS if column != "currency")
    else:
        columns = (*COLUMNS, "unexpected")
        row["unexpected"] = "value"
    payload = _payload(file_format, [row], columns=columns)

    result = _validate_payload(payload, file_format)

    assert result.rejection_code is RejectionCode.SCHEMA_MISMATCH
    assert result.counters.records_read == 0


@pytest.mark.parametrize("file_format", FORMATS)
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("supplier_id", "supplier-1"),
        ("product_id", "SKU-1"),
        ("price_minor_units", 0),
        ("currency", "CAD"),
        ("effective_date", "2026-09-02"),
    ],
)
def test_every_business_rule_rejects_the_complete_file_in_every_format(
    file_format: FileFormat,
    field: str,
    value: object,
) -> None:
    row = dict(VALID_ROW)
    row[field] = value
    payload = _payload(file_format, [row])

    result = _validate_payload(payload, file_format)

    assert result.rejection_code is RejectionCode.BUSINESS_RULE_VIOLATION
    assert result.counters.records_read == 1
    assert result.counters.records_accepted == 0
    assert result.counters.records_rejected == 1
    assert result.diagnostics.details[0].field == field


def test_parquet_requires_exact_field_type_and_nullability() -> None:
    wrong_type_schema = pa.schema(
        [
            field
            if field.name != "price_minor_units"
            else pa.field("price_minor_units", pa.string(), nullable=False)
            for field in PARQUET_SCHEMA
        ]
    )
    wrong_type_row = dict(VALID_ROW)
    wrong_type_row["price_minor_units"] = "1250"
    nullable_schema = pa.schema(
        [
            field if field.name != "currency" else pa.field("currency", pa.string(), nullable=True)
            for field in PARQUET_SCHEMA
        ]
    )

    for payload in (
        _payload(FileFormat.PARQUET, [wrong_type_row], parquet_schema=wrong_type_schema),
        _payload(FileFormat.PARQUET, [VALID_ROW], parquet_schema=nullable_schema),
    ):
        result = _validate_payload(payload, FileFormat.PARQUET)
        assert result.rejection_code is RejectionCode.SCHEMA_MISMATCH
        assert result.counters.records_read == 0


@pytest.mark.parametrize("file_format", FORMATS)
def test_record_limit_reads_only_one_record_past_the_bound(file_format: FileFormat) -> None:
    rows = [dict(VALID_ROW) for _ in range(4)]
    payload = _payload(file_format, rows)
    config = replace(DEFAULT_CONFIG, max_records=2)

    result = _validate_payload(payload, file_format, config=config)

    assert result.rejection_code is RejectionCode.RECORD_LIMIT_EXCEEDED
    assert (
        result.counters.records_read,
        result.counters.records_accepted,
        result.counters.records_rejected,
    ) == (3, 0, 3)


@pytest.mark.parametrize("file_format", FORMATS)
def test_diagnostic_details_are_bounded_without_stopping_record_count(
    file_format: FileFormat,
) -> None:
    rows: list[Mapping[str, object]] = []
    for index in range(4):
        row = dict(VALID_ROW)
        row["price_minor_units"] = 0
        row["product_id"] = f"bad-{index}"
        rows.append(row)
    payload = _payload(file_format, rows)
    config = replace(DEFAULT_CONFIG, max_diagnostic_details=2)

    result = _validate_payload(payload, file_format, config=config)

    assert result.counters.records_read == 4
    assert result.counters.records_rejected == 4
    assert len(result.diagnostics.details) == 2
    assert result.diagnostics.truncated is True
    assert [detail.field for detail in result.diagnostics.details] == [
        "product_id",
        "price_minor_units",
    ]


def test_json_never_loads_a_payload_beyond_the_file_byte_bound() -> None:
    payload = _payload(FileFormat.JSON, [VALID_ROW])
    stream = _ReadProbe(payload)
    config = replace(
        DEFAULT_CONFIG,
        max_file_bytes=len(payload) - 1,
        spool_threshold_bytes=min(DEFAULT_CONFIG.spool_threshold_bytes, len(payload) - 1),
    )

    with pytest.raises(FileSizeLimitError, match="maximum"):
        validate_file(
            stream,
            delivery=_descriptor(payload, FileFormat.JSON),
            config=config,
        )

    assert stream.read_sizes == []


def test_csv_is_consumed_without_an_unbounded_binary_read() -> None:
    payload = _payload(FileFormat.CSV, [VALID_ROW for _ in range(20)])
    stream = _ReadProbe(payload)

    result = validate_file(
        stream,
        delivery=_descriptor(payload, FileFormat.CSV),
        config=DEFAULT_CONFIG,
    )

    assert result.accepted is True
    assert stream.read_sizes
    assert all(size is not None and size >= 0 for size in stream.read_sizes)


def test_parquet_uses_configured_iter_batches_and_never_read_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _payload(FileFormat.PARQUET, [VALID_ROW for _ in range(5)])
    actual_parquet_file = pq.ParquetFile
    batch_sizes: list[int] = []

    class ParquetFileProbe:
        def __init__(self, source: Any) -> None:
            self._inner = actual_parquet_file(source)
            self.schema_arrow = self._inner.schema_arrow

        def iter_batches(self, *, batch_size: int) -> Iterator[pa.RecordBatch]:
            batch_sizes.append(batch_size)
            return cast(
                Iterator[pa.RecordBatch],
                self._inner.iter_batches(batch_size=batch_size),
            )

    def forbidden_read_table(*args: object, **kwargs: object) -> None:
        raise AssertionError("whole-table Parquet reads are forbidden")

    monkeypatch.setattr(pq, "ParquetFile", ParquetFileProbe)
    monkeypatch.setattr(pq, "read_table", forbidden_read_table)

    result = _validate_payload(
        payload,
        FileFormat.PARQUET,
        config=replace(DEFAULT_CONFIG, parquet_batch_rows=2),
    )

    assert result.accepted is True
    assert batch_sizes == [2]


@pytest.mark.parametrize("file_format", FORMATS)
def test_validation_preserves_exact_bytes_and_restores_stream_position(
    file_format: FileFormat,
) -> None:
    payload = _fixture("mixed", file_format)
    stream = io.BytesIO(payload)
    stream.seek(min(2, len(payload)))
    original_position = stream.tell()

    result = validate_file(
        stream,
        delivery=_descriptor(payload, file_format),
        config=DEFAULT_CONFIG,
    )

    assert result.accepted is False
    assert stream.getvalue() == payload
    assert stream.tell() == original_position


class _ReadProbe(io.BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.read_sizes: list[int | None] = []

    def read(self, size: int | None = -1) -> bytes:
        self.read_sizes.append(size)
        if size is None or size < 0:
            raise AssertionError("unbounded binary read is forbidden")
        return super().read(size)

    def read1(self, size: int | None = -1) -> bytes:
        self.read_sizes.append(size)
        if size is None or size < 0:
            raise AssertionError("unbounded binary read is forbidden")
        return super().read(size)
