from datetime import date

import pytest

from batch_ingestion.identifiers import (
    BatchIdentity,
    SourceType,
    derive_batch_id,
    manifest_key_for,
    new_run_id,
    raw_object_key,
)


def test_normalized_logical_input_has_deterministic_batch_and_object_keys() -> None:
    first = BatchIdentity.create(
        source_name=" FX_Rates ",
        source_type=SourceType.API,
        source_date=date(2026, 9, 1),
        parameters={" Currency ": " EUR ", "page-size": " 100 "},
    )
    second = BatchIdentity.create(
        source_name="fx-rates",
        source_type="api",
        source_date=date(2026, 9, 1),
        parameters={"page-size": "100", "currency": "EUR"},
    )

    assert first == second
    assert derive_batch_id(first) == derive_batch_id(second)
    assert raw_object_key(first, " page-0001.json ") == raw_object_key(second, "page-0001.json")
    assert raw_object_key(first, "page-0001.json") == (
        f"raw/api/fx-rates/source_date=2026-09-01/batch_id={derive_batch_id(first)}/page-0001.json"
    )


def test_logical_parameter_changes_batch_id() -> None:
    first = BatchIdentity.create("fx-rates", SourceType.API, date(2026, 9, 1), {"currency": "EUR"})
    second = BatchIdentity.create("fx-rates", SourceType.API, date(2026, 9, 1), {"currency": "USD"})

    assert derive_batch_id(first) != derive_batch_id(second)


def test_physical_attempts_have_unique_run_ids() -> None:
    assert new_run_id() != new_run_id()


def test_manifest_key_has_one_to_one_mapping_with_raw_key() -> None:
    identity = BatchIdentity.create("supplier-prices", "file", date(2026, 9, 1))
    raw_key = raw_object_key(identity, "prices.csv")

    assert manifest_key_for(raw_key) == (
        raw_key.replace("raw/", "manifests/", 1) + ".manifest.json"
    )


@pytest.mark.parametrize("payload_name", ["../payload.json", "nested/payload.json", ""])
def test_object_key_rejects_unsafe_payload_names(payload_name: str) -> None:
    identity = BatchIdentity.create("fx-rates", "api", date(2026, 9, 1))

    with pytest.raises(ValueError, match="payload_name"):
        raw_object_key(identity, payload_name)
