"""Supplier CSV/JSON/Parquet corpus contracts for BI-040 slice 4."""

import csv
import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from batch_ingestion.source_records import generate_supplier_price_records
from batch_ingestion.source_seed import DEFAULT_SOURCE_SEED, SourceSeedError
from batch_ingestion.supplier_fixtures import (
    DEFAULT_FIXTURE_SOURCE_DATE,
    FIXTURE_SCHEMA_VERSION,
    SUPPLIER_PRICE_COLUMNS,
    generate_supplier_fixture_corpus,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
COMMITTED_FIXTURES = REPOSITORY_ROOT / "fixtures"
FORMATS = ("csv", "json", "parquet")
CORE_SCENARIOS = ("valid", "empty", "schema_invalid", "mixed")


def _canonical_path(root: Path, scenario: str, file_format: str) -> Path:
    return root / "supplier-prices" / "canonical" / f"{scenario}.{file_format}"


def _normalized_rows(path: Path) -> tuple[tuple[object, ...], ...]:
    if path.suffix == ".csv":
        with path.open(encoding="utf-8", newline="") as stream:
            rows: list[dict[str, Any]] = list(csv.DictReader(stream))
        return tuple(
            (
                row["supplier_id"],
                row["product_id"],
                int(row["price_minor_units"]),
                row["currency"],
                row["effective_date"],
            )
            for row in rows
        )
    if path.suffix == ".json":
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(loaded, list)
        return tuple(
            (
                row["supplier_id"],
                row["product_id"],
                row["price_minor_units"],
                row["currency"],
                row["effective_date"],
            )
            for row in loaded
        )

    return tuple(
        (
            row["supplier_id"],
            row["product_id"],
            row["price_minor_units"],
            row["currency"],
            row["effective_date"].isoformat(),
        )
        for row in pq.read_table(path).to_pylist()
    )


def _column_names(path: Path) -> tuple[str, ...]:
    if path.suffix == ".csv":
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream)
            return tuple(next(reader))
    if path.suffix == ".json":
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(loaded, list)
        return tuple(loaded[0]) if loaded else SUPPLIER_PRICE_COLUMNS
    return tuple(pq.read_schema(path).names)


def _inventory(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_generated_corpus_has_every_required_format_and_scenario(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    index = generate_supplier_fixture_corpus(
        root,
        seed=DEFAULT_SOURCE_SEED,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )

    indexed = {(entry.scenario, entry.format) for entry in index.files}
    required = {(scenario, file_format) for scenario in CORE_SCENARIOS for file_format in FORMATS}
    assert required <= indexed
    assert {"same_name_duplicate", "renamed_content"} <= {entry.scenario for entry in index.files}
    for scenario in CORE_SCENARIOS:
        for file_format in FORMATS:
            assert _canonical_path(root, scenario, file_format).is_file()


def test_valid_rows_and_schema_are_equal_across_formats(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    generate_supplier_fixture_corpus(
        root,
        seed=DEFAULT_SOURCE_SEED,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )

    valid_paths = tuple(_canonical_path(root, "valid", file_format) for file_format in FORMATS)
    expected = tuple(
        (
            record.supplier_id,
            record.product_id,
            record.price_minor_units,
            record.currency,
            record.effective_date.isoformat(),
        )
        for record in generate_supplier_price_records(
            DEFAULT_SOURCE_SEED, DEFAULT_FIXTURE_SOURCE_DATE
        )
    )
    assert all(_column_names(path) == SUPPLIER_PRICE_COLUMNS for path in valid_paths)
    assert all(_normalized_rows(path) == expected for path in valid_paths)
    parquet_schema = pq.read_schema(valid_paths[2])
    assert parquet_schema == pa.schema(
        [
            pa.field("supplier_id", pa.string(), nullable=False),
            pa.field("product_id", pa.string(), nullable=False),
            pa.field("price_minor_units", pa.int64(), nullable=False),
            pa.field("currency", pa.string(), nullable=False),
            pa.field("effective_date", pa.date32(), nullable=False),
        ]
    )


def test_empty_invalid_and_mixed_files_are_inspectable(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    generate_supplier_fixture_corpus(
        root,
        seed=DEFAULT_SOURCE_SEED,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )

    for file_format in FORMATS:
        empty_path = _canonical_path(root, "empty", file_format)
        invalid_path = _canonical_path(root, "schema_invalid", file_format)
        mixed_path = _canonical_path(root, "mixed", file_format)

        assert _column_names(empty_path) == SUPPLIER_PRICE_COLUMNS
        assert _normalized_rows(empty_path) == ()
        assert "currency" not in _column_names(invalid_path)
        mixed_rows = _normalized_rows(mixed_path)
        mixed_prices = tuple(row[2] for row in mixed_rows)
        assert _column_names(mixed_path) == SUPPLIER_PRICE_COLUMNS
        assert any(isinstance(price, int) and price > 0 for price in mixed_prices)
        assert any(isinstance(price, int) and price <= 0 for price in mixed_prices)


def test_duplicate_delivery_cases_have_the_required_names_and_bytes(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    generate_supplier_fixture_corpus(
        root,
        seed=DEFAULT_SOURCE_SEED,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )

    for file_format in FORMATS:
        same_name_a = (
            root
            / "supplier-prices"
            / "same-name-delivery-a"
            / f"supplier-prices-2026-09-01.{file_format}"
        )
        same_name_b = (
            root
            / "supplier-prices"
            / "same-name-delivery-b"
            / f"supplier-prices-2026-09-01.{file_format}"
        )
        renamed_original = (
            root
            / "supplier-prices"
            / "renamed-content"
            / f"supplier-prices-original-2026-09-01.{file_format}"
        )
        renamed_copy = (
            root
            / "supplier-prices"
            / "renamed-content"
            / f"supplier-prices-renamed-2026-09-01.{file_format}"
        )

        assert same_name_a.name == same_name_b.name
        assert same_name_a.read_bytes() == same_name_b.read_bytes()
        assert renamed_original.name != renamed_copy.name
        assert renamed_original.read_bytes() == renamed_copy.read_bytes()


def test_checksum_index_matches_every_payload_file(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    generated = generate_supplier_fixture_corpus(
        root,
        seed=DEFAULT_SOURCE_SEED,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )
    serialized = json.loads((root / "checksums.json").read_text(encoding="utf-8"))

    assert serialized["fixture_schema_version"] == FIXTURE_SCHEMA_VERSION
    assert serialized["seed"] == DEFAULT_SOURCE_SEED
    assert serialized["source_date"] == DEFAULT_FIXTURE_SOURCE_DATE.isoformat()
    assert [entry["path"] for entry in serialized["files"]] == sorted(
        entry["path"] for entry in serialized["files"]
    )
    assert len(serialized["files"]) == len(generated.files)
    indexed_paths = {entry["path"] for entry in serialized["files"]}
    payload_paths = set(_inventory(root)) - {"checksums.json"}
    assert indexed_paths == payload_paths
    for entry in serialized["files"]:
        payload = (root / entry["path"]).read_bytes()
        assert entry["byte_length"] == len(payload)
        assert entry["checksum_sha256"] == sha256(payload).hexdigest()
        assert entry["format"] == Path(entry["path"]).suffix.removeprefix(".")


def test_two_generations_and_committed_default_corpus_are_byte_identical(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate_supplier_fixture_corpus(
        first,
        seed=DEFAULT_SOURCE_SEED,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )
    generate_supplier_fixture_corpus(
        second,
        seed=DEFAULT_SOURCE_SEED,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )

    assert _inventory(first) == _inventory(second) == _inventory(COMMITTED_FIXTURES)


def test_invalid_seed_does_not_change_an_existing_output_directory(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    root.mkdir()
    sentinel = root / "sentinel.txt"
    sentinel.write_text("preserve me\n", encoding="utf-8")

    with pytest.raises(SourceSeedError, match="SOURCE_SEED"):
        generate_supplier_fixture_corpus(
            root,
            seed=-1,
            source_date=DEFAULT_FIXTURE_SOURCE_DATE,
        )

    assert _inventory(root) == {"sentinel.txt": b"preserve me\n"}


def test_alternate_seed_changes_rows_without_changing_paths_or_schemas(tmp_path: Path) -> None:
    default_root = tmp_path / "default"
    alternate_root = tmp_path / "alternate"
    generate_supplier_fixture_corpus(
        default_root,
        seed=DEFAULT_SOURCE_SEED,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )
    generate_supplier_fixture_corpus(
        alternate_root,
        seed=DEFAULT_SOURCE_SEED + 1,
        source_date=DEFAULT_FIXTURE_SOURCE_DATE,
    )

    assert set(_inventory(default_root)) == set(_inventory(alternate_root))
    for file_format in FORMATS:
        default_valid = _canonical_path(default_root, "valid", file_format)
        alternate_valid = _canonical_path(alternate_root, "valid", file_format)
        assert _column_names(default_valid) == _column_names(alternate_valid)
        assert _normalized_rows(default_valid) != _normalized_rows(alternate_valid)
