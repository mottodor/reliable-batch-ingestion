"""Deterministic CSV, JSON, and Parquet supplier-price fixture generation."""

import csv
import io
import json
import shutil
import tempfile
from dataclasses import asdict, dataclass, replace
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Literal, cast

import pyarrow as pa
import pyarrow.parquet as pq

from .source_records import SupplierPriceRecord, generate_supplier_price_records
from .source_seed import validate_source_date, validate_source_seed

FIXTURE_SCHEMA_VERSION = 1
DEFAULT_FIXTURE_SOURCE_DATE = date(2026, 9, 1)
SUPPLIER_PRICE_COLUMNS = (
    "supplier_id",
    "product_id",
    "price_minor_units",
    "currency",
    "effective_date",
)
type FixtureFormat = Literal["csv", "json", "parquet"]
type FixtureScenario = Literal[
    "valid",
    "empty",
    "schema_invalid",
    "mixed",
    "same_name_duplicate",
    "renamed_content",
]

_SUPPLIER_PRICE_SCHEMA = pa.schema(
    [
        pa.field("supplier_id", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("price_minor_units", pa.int64(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("effective_date", pa.date32(), nullable=False),
    ]
)
_SCHEMA_INVALID_COLUMNS = tuple(column for column in SUPPLIER_PRICE_COLUMNS if column != "currency")
_SCHEMA_INVALID_PARQUET_SCHEMA = pa.schema(
    [field for field in _SUPPLIER_PRICE_SCHEMA if field.name != "currency"]
)
_FORMATS: tuple[FixtureFormat, ...] = ("csv", "json", "parquet")


@dataclass(frozen=True, slots=True)
class FixtureIndexEntry:
    """Checksum evidence for one generated fixture payload."""

    path: str
    byte_length: int
    checksum_sha256: str
    format: FixtureFormat
    scenario: FixtureScenario


@dataclass(frozen=True, slots=True)
class FixtureIndex:
    """Typed representation of the generated checksum index."""

    fixture_schema_version: int
    seed: int
    source_date: str
    files: tuple[FixtureIndexEntry, ...]


def _record_mapping(record: SupplierPriceRecord) -> dict[str, object]:
    return {
        "supplier_id": record.supplier_id,
        "product_id": record.product_id,
        "price_minor_units": record.price_minor_units,
        "currency": record.currency,
        "effective_date": record.effective_date,
    }


def _json_mapping(record: SupplierPriceRecord) -> dict[str, object]:
    row = _record_mapping(record)
    row["effective_date"] = record.effective_date.isoformat()
    return row


def _write_csv(
    path: Path,
    records: tuple[SupplierPriceRecord, ...],
    *,
    columns: tuple[str, ...] = SUPPLIER_PRICE_COLUMNS,
) -> None:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(columns)
    for record in records:
        row = _json_mapping(record)
        writer.writerow(tuple(row[column] for column in columns))
    path.write_bytes(stream.getvalue().encode("utf-8"))


def _write_json(
    path: Path,
    records: tuple[SupplierPriceRecord, ...],
    *,
    columns: tuple[str, ...] = SUPPLIER_PRICE_COLUMNS,
) -> None:
    rows = [{column: _json_mapping(record)[column] for column in columns} for record in records]
    serialized = json.dumps(
        rows,
        ensure_ascii=True,
        indent=2,
        separators=(",", ": "),
    )
    path.write_bytes(f"{serialized}\n".encode())


def _write_parquet(
    path: Path,
    records: tuple[SupplierPriceRecord, ...],
    *,
    schema: pa.Schema = _SUPPLIER_PRICE_SCHEMA,
) -> None:
    rows = [_record_mapping(record) for record in records]
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(
        table,
        path,
        version="2.6",
        row_group_size=1_024,
        compression="none",
        use_dictionary=False,
        write_statistics=False,
        data_page_version="2.0",
        store_schema=True,
        write_page_index=False,
        write_page_checksum=False,
    )


def _write_format(
    path: Path,
    file_format: FixtureFormat,
    records: tuple[SupplierPriceRecord, ...],
    *,
    schema_invalid: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if file_format == "csv":
        columns = _SCHEMA_INVALID_COLUMNS if schema_invalid else SUPPLIER_PRICE_COLUMNS
        _write_csv(path, records, columns=columns)
    elif file_format == "json":
        columns = _SCHEMA_INVALID_COLUMNS if schema_invalid else SUPPLIER_PRICE_COLUMNS
        _write_json(path, records, columns=columns)
    else:
        schema = _SCHEMA_INVALID_PARQUET_SCHEMA if schema_invalid else _SUPPLIER_PRICE_SCHEMA
        _write_parquet(path, records, schema=schema)


def _build_index(
    root: Path,
    *,
    seed: int,
    source_date: date,
    scenarios: dict[str, FixtureScenario],
) -> FixtureIndex:
    entries: list[FixtureIndexEntry] = []
    for relative_path in sorted(scenarios):
        payload = (root / relative_path).read_bytes()
        suffix = Path(relative_path).suffix.removeprefix(".")
        entries.append(
            FixtureIndexEntry(
                path=relative_path,
                byte_length=len(payload),
                checksum_sha256=sha256(payload).hexdigest(),
                format=cast(FixtureFormat, suffix),
                scenario=scenarios[relative_path],
            )
        )
    return FixtureIndex(
        fixture_schema_version=FIXTURE_SCHEMA_VERSION,
        seed=seed,
        source_date=source_date.isoformat(),
        files=tuple(entries),
    )


def _write_index(path: Path, index: FixtureIndex) -> None:
    payload = {
        "fixture_schema_version": index.fixture_schema_version,
        "seed": index.seed,
        "source_date": index.source_date,
        "files": [asdict(entry) for entry in index.files],
    }
    serialized = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    path.write_bytes(f"{serialized}\n".encode())


def _populate_corpus(root: Path, *, seed: int, source_date: date) -> FixtureIndex:
    valid_records = generate_supplier_price_records(seed, source_date)
    schema_invalid_records = valid_records[:2]
    mixed_records = tuple(
        sorted(
            (
                valid_records[0],
                replace(valid_records[1], price_minor_units=0),
                valid_records[2],
            ),
            key=lambda record: (
                record.supplier_id,
                record.product_id,
                record.effective_date,
            ),
        )
    )
    scenarios: dict[str, FixtureScenario] = {}
    canonical_records = {
        "valid": valid_records,
        "empty": (),
        "schema_invalid": schema_invalid_records,
        "mixed": mixed_records,
    }

    for scenario, records in canonical_records.items():
        typed_scenario = cast(FixtureScenario, scenario)
        for file_format in _FORMATS:
            relative_path = f"supplier-prices/canonical/{scenario}.{file_format}"
            _write_format(
                root / relative_path,
                file_format,
                records,
                schema_invalid=scenario == "schema_invalid",
            )
            scenarios[relative_path] = typed_scenario

    date_text = source_date.isoformat()
    for file_format in _FORMATS:
        valid_path = root / f"supplier-prices/canonical/valid.{file_format}"
        same_name = f"supplier-prices-{date_text}.{file_format}"
        for delivery in ("same-name-delivery-a", "same-name-delivery-b"):
            relative_path = f"supplier-prices/{delivery}/{same_name}"
            destination = root / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(valid_path, destination)
            scenarios[relative_path] = "same_name_duplicate"

        for name in ("original", "renamed"):
            relative_path = (
                f"supplier-prices/renamed-content/supplier-prices-{name}-{date_text}.{file_format}"
            )
            destination = root / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(valid_path, destination)
            scenarios[relative_path] = "renamed_content"

    index = _build_index(root, seed=seed, source_date=source_date, scenarios=scenarios)
    _write_index(root / "checksums.json", index)
    return index


def generate_supplier_fixture_corpus(
    output_directory: Path,
    *,
    seed: int,
    source_date: date,
) -> FixtureIndex:
    """Replace an output directory with a validated deterministic fixture corpus."""
    validated_seed = validate_source_seed(seed)
    validated_date = validate_source_date(source_date)
    output = Path(output_directory)
    if output.name in {"", ".", ".."}:
        raise ValueError("output_directory must name a dedicated directory")

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}-", dir=output.parent))
    try:
        index = _populate_corpus(
            temporary,
            seed=validated_seed,
            source_date=validated_date,
        )
        if output.is_dir() and not output.is_symlink():
            shutil.rmtree(output)
        elif output.exists() or output.is_symlink():
            output.unlink()
        temporary.replace(output)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return index
