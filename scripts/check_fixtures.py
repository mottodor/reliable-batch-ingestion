#!/usr/bin/env python3
"""Regenerate supplier fixtures twice and verify committed bytes and semantics."""

import argparse
import csv
import json
import os
import sys
import tempfile
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from batch_ingestion.source_records import generate_supplier_price_records
from batch_ingestion.source_seed import SourceSeedError, parse_source_seed
from batch_ingestion.supplier_fixtures import (
    DEFAULT_FIXTURE_SOURCE_DATE,
    FIXTURE_SCHEMA_VERSION,
    SUPPLIER_PRICE_COLUMNS,
    generate_supplier_fixture_corpus,
)

_FORMATS = ("csv", "json", "parquet")
_SCENARIOS = ("valid", "empty", "schema_invalid", "mixed")


class FixtureCheckError(RuntimeError):
    """Raised when a generated or committed corpus violates its contract."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--committed", type=Path, required=True)
    return parser


def _inventory(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _canonical_path(root: Path, scenario: str, file_format: str) -> Path:
    return root / "supplier-prices" / "canonical" / f"{scenario}.{file_format}"


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureCheckError(f"cannot parse JSON fixture: {path}") from error


def _column_names(path: Path) -> tuple[str, ...]:
    if path.suffix == ".csv":
        with path.open(encoding="utf-8", newline="") as stream:
            return tuple(next(csv.reader(stream)))
    if path.suffix == ".json":
        loaded = _load_json(path)
        if not isinstance(loaded, list):
            raise FixtureCheckError(f"JSON fixture is not an array: {path}")
        if not loaded:
            return SUPPLIER_PRICE_COLUMNS
        first = loaded[0]
        if not isinstance(first, dict):
            raise FixtureCheckError(f"JSON fixture row is not an object: {path}")
        return tuple(first)
    return tuple(pq.read_schema(path).names)


def _parse_price(value: str, path: Path) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise FixtureCheckError(f"invalid integer price in fixture: {path}") from error


def _rows(path: Path) -> tuple[tuple[object, ...], ...]:
    if path.suffix == ".csv":
        with path.open(encoding="utf-8", newline="") as stream:
            loaded: list[dict[str, Any]] = list(csv.DictReader(stream))
        return tuple(
            (
                row["supplier_id"],
                row["product_id"],
                _parse_price(row["price_minor_units"], path),
                row["currency"],
                row["effective_date"],
            )
            for row in loaded
        )
    if path.suffix == ".json":
        loaded_json = _load_json(path)
        if not isinstance(loaded_json, list):
            raise FixtureCheckError(f"JSON fixture is not an array: {path}")
        return tuple(
            (
                row["supplier_id"],
                row["product_id"],
                row["price_minor_units"],
                row["currency"],
                row["effective_date"],
            )
            for row in loaded_json
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


def _verify_index(root: Path, *, seed: int) -> None:
    index_path = root / "checksums.json"
    serialized = _load_json(index_path)
    if not isinstance(serialized, dict):
        raise FixtureCheckError(f"checksum index is not an object: {index_path}")
    if serialized.get("fixture_schema_version") != FIXTURE_SCHEMA_VERSION:
        raise FixtureCheckError(f"unexpected fixture schema version in {index_path}")
    if serialized.get("seed") != seed:
        raise FixtureCheckError(f"unexpected seed in {index_path}")
    if serialized.get("source_date") != DEFAULT_FIXTURE_SOURCE_DATE.isoformat():
        raise FixtureCheckError(f"unexpected source date in {index_path}")

    entries = serialized.get("files")
    if not isinstance(entries, list):
        raise FixtureCheckError(f"missing file entries in {index_path}")
    if not all(isinstance(entry, dict) for entry in entries):
        raise FixtureCheckError(f"invalid file entry in {index_path}")
    paths = [entry.get("path") for entry in entries]
    if not all(isinstance(path, str) for path in paths):
        raise FixtureCheckError(f"invalid indexed path in {index_path}")
    typed_paths = [path for path in paths if isinstance(path, str)]
    if typed_paths != sorted(typed_paths):
        raise FixtureCheckError(f"file entries are not sorted in {index_path}")
    if set(typed_paths) != set(_inventory(root)) - {"checksums.json"}:
        raise FixtureCheckError(f"indexed paths differ from corpus paths in {root}")
    for entry in entries:
        relative_path = entry["path"]
        payload = (root / relative_path).read_bytes()
        if entry.get("byte_length") != len(payload):
            raise FixtureCheckError(f"byte length mismatch for {relative_path}")
        if entry.get("checksum_sha256") != sha256(payload).hexdigest():
            raise FixtureCheckError(f"checksum mismatch for {relative_path}")
        if entry.get("format") != Path(relative_path).suffix.removeprefix("."):
            raise FixtureCheckError(f"format mismatch for {relative_path}")


def _verify_semantics(root: Path, *, seed: int) -> None:
    expected_rows = tuple(
        (
            record.supplier_id,
            record.product_id,
            record.price_minor_units,
            record.currency,
            record.effective_date.isoformat(),
        )
        for record in generate_supplier_price_records(seed, DEFAULT_FIXTURE_SOURCE_DATE)
    )
    for file_format in _FORMATS:
        paths = {scenario: _canonical_path(root, scenario, file_format) for scenario in _SCENARIOS}
        if _column_names(paths["valid"]) != SUPPLIER_PRICE_COLUMNS:
            raise FixtureCheckError(f"valid schema mismatch: {paths['valid']}")
        if _rows(paths["valid"]) != expected_rows:
            raise FixtureCheckError(f"valid row mismatch: {paths['valid']}")
        if _column_names(paths["empty"]) != SUPPLIER_PRICE_COLUMNS:
            raise FixtureCheckError(f"empty schema mismatch: {paths['empty']}")
        if _rows(paths["empty"]):
            raise FixtureCheckError(f"empty fixture contains rows: {paths['empty']}")
        if "currency" in _column_names(paths["schema_invalid"]):
            raise FixtureCheckError(
                f"invalid fixture retained required column: {paths['schema_invalid']}"
            )
        if _column_names(paths["mixed"]) != SUPPLIER_PRICE_COLUMNS:
            raise FixtureCheckError(f"mixed schema mismatch: {paths['mixed']}")
        mixed_prices = [row[2] for row in _rows(paths["mixed"])]
        if not any(isinstance(value, int) and value > 0 for value in mixed_prices):
            raise FixtureCheckError(f"mixed fixture has no valid row: {paths['mixed']}")
        if not any(isinstance(value, int) and value <= 0 for value in mixed_prices):
            raise FixtureCheckError(f"mixed fixture has no business-invalid row: {paths['mixed']}")

        same_name = f"supplier-prices-{DEFAULT_FIXTURE_SOURCE_DATE.isoformat()}.{file_format}"
        same_a = root / "supplier-prices" / "same-name-delivery-a" / same_name
        same_b = root / "supplier-prices" / "same-name-delivery-b" / same_name
        renamed_root = root / "supplier-prices" / "renamed-content"
        original = renamed_root / (
            f"supplier-prices-original-{DEFAULT_FIXTURE_SOURCE_DATE.isoformat()}.{file_format}"
        )
        renamed = renamed_root / (
            f"supplier-prices-renamed-{DEFAULT_FIXTURE_SOURCE_DATE.isoformat()}.{file_format}"
        )
        if same_a.name != same_b.name or same_a.read_bytes() != same_b.read_bytes():
            raise FixtureCheckError(f"same-name delivery mismatch for {file_format}")
        if original.name == renamed.name or original.read_bytes() != renamed.read_bytes():
            raise FixtureCheckError(f"renamed-content mismatch for {file_format}")


def _verify_root(root: Path, *, seed: int) -> None:
    _verify_index(root, seed=seed)
    _verify_semantics(root, seed=seed)


def main(argv: Sequence[str] | None = None) -> int:
    """Return zero only when two runs and committed fixture bytes all agree."""
    arguments = _parser().parse_args(argv)
    try:
        seed = parse_source_seed(os.environ.get("SOURCE_SEED", ""))
        with tempfile.TemporaryDirectory(prefix="batch-fixtures-check-") as temporary:
            temporary_root = Path(temporary)
            first = temporary_root / "first"
            second = temporary_root / "second"
            generate_supplier_fixture_corpus(
                first,
                seed=seed,
                source_date=DEFAULT_FIXTURE_SOURCE_DATE,
            )
            generate_supplier_fixture_corpus(
                second,
                seed=seed,
                source_date=DEFAULT_FIXTURE_SOURCE_DATE,
            )
            for root in (first, second, arguments.committed):
                _verify_root(root, seed=seed)
            if not (_inventory(first) == _inventory(second) == _inventory(arguments.committed)):
                raise FixtureCheckError(
                    "temporary generations and committed corpus differ by path or bytes"
                )
    except (SourceSeedError, FixtureCheckError, OSError, ValueError) as error:
        print(f"Fixture check failed: {error}", file=sys.stderr)
        return 1

    print(
        f"Fixture check: PASS ({len(_inventory(arguments.committed)) - 1} payload files, "
        f"SOURCE_SEED={seed})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
