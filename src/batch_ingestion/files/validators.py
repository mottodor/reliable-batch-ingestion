"""Bounded whole-file validation for supplier CSV, JSON, and Parquet payloads."""

from __future__ import annotations

import csv
import io
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import BinaryIO, Final

import pyarrow as pa
import pyarrow.parquet as pq

from batch_ingestion.config import FileConfig

from .contracts import DeliveryDescriptor, FileFormat
from .diagnostics import (
    BoundedDiagnostics,
    FileDiagnostic,
    FileRecordCounters,
    RejectionCode,
    delivery_rejection_codes,
    file_record_counters,
    primary_rejection_code,
)

SUPPLIER_PRICE_FIELDS: Final[tuple[str, ...]] = (
    "supplier_id",
    "product_id",
    "price_minor_units",
    "currency",
    "effective_date",
)
_SUPPLIER_ID_PATTERN = re.compile(r"SUP-[0-9]{3}", flags=re.ASCII)
_PRODUCT_ID_PATTERN = re.compile(r"SKU-[0-9]{4}", flags=re.ASCII)
_CANONICAL_INTEGER_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)", flags=re.ASCII)
_CURRENCIES: Final = frozenset({"EUR", "GBP", "USD"})
_EXPECTED_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("supplier_id", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("price_minor_units", pa.int64(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("effective_date", pa.date32(), nullable=False),
    ]
)


class FileSizeLimitError(ValueError):
    """Raised at admission when a payload exceeds the configured byte bound."""


@dataclass(frozen=True, slots=True)
class FileValidationResult:
    """Whole-file outcome without publishing or returning accepted row subsets."""

    accepted: bool
    rejection_code: RejectionCode | None
    diagnostics: BoundedDiagnostics
    counters: FileRecordCounters


class _DiagnosticCollector:
    def __init__(self, maximum: int) -> None:
        self._maximum = maximum
        self._details: list[FileDiagnostic] = []
        self._seen = 0
        self.codes: set[RejectionCode] = set()

    def add(
        self,
        code: RejectionCode,
        *,
        record_number: int | None = None,
        field: str | None = None,
    ) -> None:
        self.codes.add(code)
        self._seen += 1
        if len(self._details) < self._maximum:
            self._details.append(
                FileDiagnostic(code=code, record_number=record_number, field=field)
            )

    def bounded(self) -> BoundedDiagnostics:
        return BoundedDiagnostics(
            details=tuple(self._details),
            truncated=self._seen > len(self._details),
        )


def validate_file(
    stream: BinaryIO,
    *,
    delivery: DeliveryDescriptor,
    config: FileConfig,
) -> FileValidationResult:
    """Validate one seekable preserved stream and restore its original position."""
    try:
        original_position = stream.tell()
    except (AttributeError, OSError) as error:
        raise ValueError("file validation requires a seekable binary stream") from error

    try:
        content_length = _content_length(stream)
        if content_length > config.max_file_bytes:
            raise FileSizeLimitError("file exceeds the configured maximum admitted bytes")
        stream.seek(0)

        collector = _DiagnosticCollector(config.max_diagnostic_details)
        delivery_codes = delivery_rejection_codes(delivery)
        if delivery_codes:
            for code in delivery_codes:
                collector.add(code)
            return _result(records_read=0, collector=collector)

        if delivery.file_format is FileFormat.CSV:
            records_read = _validate_csv(stream, delivery, config, collector)
        elif delivery.file_format is FileFormat.JSON:
            records_read = _validate_json(stream, delivery, config, collector)
        elif delivery.file_format is FileFormat.PARQUET:
            records_read = _validate_parquet(stream, delivery, config, collector)
        else:
            collector.add(RejectionCode.INVALID_FILE_NAME)
            records_read = 0

        if records_read == 0 and not collector.codes:
            collector.add(RejectionCode.EMPTY_FILE)
        return _result(records_read=records_read, collector=collector)
    finally:
        stream.seek(original_position)


def _validate_csv(
    stream: BinaryIO,
    delivery: DeliveryDescriptor,
    config: FileConfig,
    collector: _DiagnosticCollector,
) -> int:
    text = io.TextIOWrapper(stream, encoding="utf-8", errors="strict", newline="")
    records_read = 0
    try:
        reader = csv.reader(text, strict=True)
        try:
            header = next(reader)
        except StopIteration:
            return 0
        if tuple(header) != SUPPLIER_PRICE_FIELDS:
            collector.add(RejectionCode.SCHEMA_MISMATCH)
            return 0

        for values in reader:
            if tuple(values) == SUPPLIER_PRICE_FIELDS:
                collector.add(RejectionCode.SCHEMA_MISMATCH)
                break
            if len(values) != len(SUPPLIER_PRICE_FIELDS):
                collector.add(RejectionCode.MALFORMED_FILE)
                break
            records_read += 1
            if records_read > config.max_records:
                collector.add(
                    RejectionCode.RECORD_LIMIT_EXCEEDED,
                    record_number=records_read,
                )
                break
            row: dict[str, object] = {
                field: value for field, value in zip(SUPPLIER_PRICE_FIELDS, values, strict=True)
            }
            _validate_row(
                row,
                file_format=FileFormat.CSV,
                source_date=delivery.source_date,
                record_number=records_read,
                collector=collector,
            )
    except (UnicodeDecodeError, csv.Error):
        collector.add(RejectionCode.MALFORMED_FILE)
    finally:
        text.detach()
    return records_read


def _validate_json(
    stream: BinaryIO,
    delivery: DeliveryDescriptor,
    config: FileConfig,
    collector: _DiagnosticCollector,
) -> int:
    records_read = 0
    try:
        payload = stream.read(config.max_file_bytes + 1)
        if len(payload) > config.max_file_bytes:
            raise FileSizeLimitError("file exceeds the configured maximum admitted bytes")
        decoded = payload.decode("utf-8", errors="strict")
        value: object = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        collector.add(RejectionCode.MALFORMED_FILE)
        return records_read

    if not isinstance(value, list):
        collector.add(RejectionCode.MALFORMED_FILE)
        return records_read

    for item in value:
        record_number = records_read + 1
        if not isinstance(item, Mapping) or tuple(item) != SUPPLIER_PRICE_FIELDS:
            collector.add(
                RejectionCode.SCHEMA_MISMATCH,
                record_number=record_number,
            )
            break
        records_read += 1
        if records_read > config.max_records:
            collector.add(
                RejectionCode.RECORD_LIMIT_EXCEEDED,
                record_number=records_read,
            )
            break
        _validate_row(
            item,
            file_format=FileFormat.JSON,
            source_date=delivery.source_date,
            record_number=records_read,
            collector=collector,
        )
    return records_read


def _validate_parquet(
    stream: BinaryIO,
    delivery: DeliveryDescriptor,
    config: FileConfig,
    collector: _DiagnosticCollector,
) -> int:
    records_read = 0
    try:
        parquet_file = pq.ParquetFile(stream)
        if parquet_file.schema_arrow != _EXPECTED_PARQUET_SCHEMA:
            collector.add(RejectionCode.SCHEMA_MISMATCH)
            return 0

        limit_reached = False
        for batch in parquet_file.iter_batches(batch_size=config.parquet_batch_rows):
            for row in batch.to_pylist():
                records_read += 1
                if records_read > config.max_records:
                    collector.add(
                        RejectionCode.RECORD_LIMIT_EXCEEDED,
                        record_number=records_read,
                    )
                    limit_reached = True
                    break
                _validate_row(
                    row,
                    file_format=FileFormat.PARQUET,
                    source_date=delivery.source_date,
                    record_number=records_read,
                    collector=collector,
                )
            if limit_reached:
                break
    except pa.ArrowInvalid:
        collector.add(RejectionCode.MALFORMED_FILE)
    return records_read


def _validate_row(
    row: Mapping[str, object],
    *,
    file_format: FileFormat,
    source_date: date,
    record_number: int,
    collector: _DiagnosticCollector,
) -> None:
    supplier_id = row["supplier_id"]
    _validate_required_string(
        supplier_id,
        field="supplier_id",
        pattern=_SUPPLIER_ID_PATTERN,
        record_number=record_number,
        collector=collector,
    )

    product_id = row["product_id"]
    _validate_required_string(
        product_id,
        field="product_id",
        pattern=_PRODUCT_ID_PATTERN,
        record_number=record_number,
        collector=collector,
    )

    _validate_price(
        row["price_minor_units"],
        file_format=file_format,
        record_number=record_number,
        collector=collector,
    )

    currency = row["currency"]
    if _required_value_failure(
        currency,
        field="currency",
        record_number=record_number,
        collector=collector,
    ):
        pass
    elif not isinstance(currency, str):
        collector.add(
            RejectionCode.INVALID_FIELD_TYPE,
            record_number=record_number,
            field="currency",
        )
    elif currency not in _CURRENCIES:
        collector.add(
            RejectionCode.BUSINESS_RULE_VIOLATION,
            record_number=record_number,
            field="currency",
        )

    _validate_effective_date(
        row["effective_date"],
        file_format=file_format,
        source_date=source_date,
        record_number=record_number,
        collector=collector,
    )


def _validate_required_string(
    value: object,
    *,
    field: str,
    pattern: re.Pattern[str],
    record_number: int,
    collector: _DiagnosticCollector,
) -> None:
    if _required_value_failure(
        value,
        field=field,
        record_number=record_number,
        collector=collector,
    ):
        return
    if not isinstance(value, str):
        collector.add(
            RejectionCode.INVALID_FIELD_TYPE,
            record_number=record_number,
            field=field,
        )
    elif pattern.fullmatch(value) is None:
        collector.add(
            RejectionCode.BUSINESS_RULE_VIOLATION,
            record_number=record_number,
            field=field,
        )


def _validate_price(
    value: object,
    *,
    file_format: FileFormat,
    record_number: int,
    collector: _DiagnosticCollector,
) -> None:
    field = "price_minor_units"
    if _required_value_failure(
        value,
        field=field,
        record_number=record_number,
        collector=collector,
    ):
        return
    if file_format is FileFormat.CSV:
        if not isinstance(value, str) or _CANONICAL_INTEGER_PATTERN.fullmatch(value) is None:
            collector.add(
                RejectionCode.INVALID_FIELD_TYPE,
                record_number=record_number,
                field=field,
            )
            return
        is_positive = value != "0"
    else:
        if isinstance(value, bool) or not isinstance(value, int):
            collector.add(
                RejectionCode.INVALID_FIELD_TYPE,
                record_number=record_number,
                field=field,
            )
            return
        is_positive = value > 0
    if not is_positive:
        collector.add(
            RejectionCode.BUSINESS_RULE_VIOLATION,
            record_number=record_number,
            field=field,
        )


def _validate_effective_date(
    value: object,
    *,
    file_format: FileFormat,
    source_date: date,
    record_number: int,
    collector: _DiagnosticCollector,
) -> None:
    field = "effective_date"
    if _required_value_failure(
        value,
        field=field,
        record_number=record_number,
        collector=collector,
    ):
        return

    if file_format is FileFormat.PARQUET:
        if not isinstance(value, date):
            collector.add(
                RejectionCode.INVALID_FIELD_TYPE,
                record_number=record_number,
                field=field,
            )
            return
        parsed_date = value
    else:
        if not isinstance(value, str):
            collector.add(
                RejectionCode.INVALID_FIELD_TYPE,
                record_number=record_number,
                field=field,
            )
            return
        try:
            parsed_date = date.fromisoformat(value)
        except ValueError:
            collector.add(
                RejectionCode.INVALID_FIELD_TYPE,
                record_number=record_number,
                field=field,
            )
            return
        if parsed_date.isoformat() != value:
            collector.add(
                RejectionCode.INVALID_FIELD_TYPE,
                record_number=record_number,
                field=field,
            )
            return

    if parsed_date != source_date:
        collector.add(
            RejectionCode.BUSINESS_RULE_VIOLATION,
            record_number=record_number,
            field=field,
        )


def _required_value_failure(
    value: object,
    *,
    field: str,
    record_number: int,
    collector: _DiagnosticCollector,
) -> bool:
    if value is None or value == "":
        collector.add(
            RejectionCode.MISSING_REQUIRED_VALUE,
            record_number=record_number,
            field=field,
        )
        return True
    return False


def _content_length(stream: BinaryIO) -> int:
    stream.seek(0, io.SEEK_END)
    length = stream.tell()
    stream.seek(0)
    if length < 0:
        raise ValueError("stream length must be non-negative")
    return length


def _result(
    *,
    records_read: int,
    collector: _DiagnosticCollector,
) -> FileValidationResult:
    rejection_code = primary_rejection_code(collector.codes)
    accepted = rejection_code is None
    return FileValidationResult(
        accepted=accepted,
        rejection_code=rejection_code,
        diagnostics=collector.bounded(),
        counters=file_record_counters(records_read=records_read, accepted=accepted),
    )
