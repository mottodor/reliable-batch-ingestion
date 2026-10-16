"""Pure ADR 0003 REST logical-run manifest contracts."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

from batch_ingestion.api.logical_manifest import (
    API_LOGICAL_RUN_MANIFEST_MAX_BYTES,
    API_LOGICAL_RUN_MANIFEST_MAX_PAGES,
    ApiLogicalRunManifest,
    ApiLogicalRunPage,
    api_logical_run_manifest_key,
)
from batch_ingestion.api.sources import (
    api_page_identity,
    get_source_descriptor,
    page_payload_name,
)
from batch_ingestion.identifiers import (
    BatchIdentity,
    SourceType,
    derive_batch_id,
    manifest_key_for,
    raw_object_key,
)

_RUN_ID = "run_1234567812344abc9234567890abcdef"
_COMPLETED_AT = datetime(2026, 9, 1, 12, 30, 45, 123456, tzinfo=UTC)


def _manifest(
    *,
    source_name: str = "fx-rates",
    source_date: date = date(2026, 9, 1),
    page_size: int = 25,
    record_count: int = 2,
    maximum_width: bool = False,
) -> ApiLogicalRunManifest:
    descriptor = get_source_descriptor(source_name)
    identity = api_page_identity(descriptor, source_date, page_size=page_size)
    page_count = max(1, (record_count + page_size - 1) // page_size)
    pages: list[ApiLogicalRunPage] = []
    remaining = record_count
    for page_number in range(1, page_count + 1):
        page_records = min(page_size, remaining)
        remaining -= page_records
        object_key = raw_object_key(
            identity,
            page_payload_name(page_number, max_pages=API_LOGICAL_RUN_MANIFEST_MAX_PAGES),
        )
        pages.append(
            ApiLogicalRunPage(
                page_number=page_number,
                object_key=object_key,
                page_manifest_key=manifest_key_for(object_key),
                payload_content_length=(2**63 - 1 if maximum_width else 100 + page_number),
                payload_checksum_sha256="a" * 64,
                page_manifest_checksum_sha256="b" * 64,
                record_count=page_records,
            )
        )
    return ApiLogicalRunManifest(
        manifest_type="api_logical_run_manifest",
        schema_version=1,
        source_name=source_name,
        source_type=SourceType.API,
        source_date=source_date,
        batch_id=derive_batch_id(identity),
        identity_parameters={
            "page-size": str(page_size),
            "resource-path": descriptor.path,
        },
        completion_run_id=_RUN_ID,
        completed_at=_COMPLETED_AT,
        status="succeeded",
        page_count=page_count,
        record_count=record_count,
        pages=tuple(pages),
    )


def _canonical_value() -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(_manifest().to_bytes()))


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def test_logical_manifest_key_matches_both_adr_examples_and_is_a_separate_namespace() -> None:
    expected = {
        "fx-rates": (
            "manifests/api/fx-rates/source_date=2026-09-01/"
            "batch_id=150b7a9c57cadf18d4d1f5e51fc66575da0d3934b97cf79dac3fc91f3c2becf6/"
            "logical-run.manifest.json"
        ),
        "shipping-statuses": (
            "manifests/api/shipping-statuses/source_date=2026-09-01/"
            "batch_id=d1b4d7361db024cb942708a5d45ca1731addb831f9551299f623f8e4d5a2a185/"
            "logical-run.manifest.json"
        ),
    }

    for source_name, expected_key in expected.items():
        identity = api_page_identity(
            get_source_descriptor(source_name), date(2026, 9, 1), page_size=25
        )
        raw_key = raw_object_key(identity, "page-0001.json")
        aggregate_key = api_logical_run_manifest_key(identity)

        assert aggregate_key == expected_key
        assert aggregate_key != manifest_key_for(raw_key)
        assert not aggregate_key.startswith("raw/")


def test_key_helper_rejects_non_api_and_non_descriptor_identities() -> None:
    file_identity = BatchIdentity.create(
        "fx-rates", SourceType.FILE, date(2026, 9, 1), {"page-size": "25"}
    )
    wrong_path = BatchIdentity.create(
        "fx-rates",
        SourceType.API,
        date(2026, 9, 1),
        {"page-size": "25", "resource-path": "/v1/not-fx"},
    )

    with pytest.raises(ValueError, match="API identity"):
        api_logical_run_manifest_key(file_identity)
    with pytest.raises(ValueError, match="identity parameters"):
        api_logical_run_manifest_key(wrong_path)


def test_exact_fields_canonical_bytes_and_strict_round_trip() -> None:
    manifest = _manifest()
    serialized = manifest.to_bytes()
    value = json.loads(serialized)

    assert set(value) == {
        "manifest_type",
        "schema_version",
        "source_name",
        "source_type",
        "source_date",
        "batch_id",
        "identity_parameters",
        "completion_run_id",
        "completed_at",
        "status",
        "page_count",
        "record_count",
        "pages",
    }
    assert set(value["identity_parameters"]) == {"page-size", "resource-path"}
    assert set(value["pages"][0]) == {
        "page_number",
        "object_key",
        "page_manifest_key",
        "payload_content_length",
        "payload_checksum_sha256",
        "page_manifest_checksum_sha256",
        "record_count",
    }
    assert value["completed_at"] == "2026-09-01T12:30:45.123456Z"
    assert serialized == _canonical_bytes(value)
    assert not serialized.startswith(b"\xef\xbb\xbf")
    assert not serialized.endswith(b"\n")
    assert ApiLogicalRunManifest.from_bytes(serialized) == manifest


@pytest.mark.parametrize("depth", ["top", "identity", "page"])
@pytest.mark.parametrize("change", ["missing", "unknown"])
def test_parser_rejects_missing_and_unknown_fields(depth: str, change: str) -> None:
    value = _canonical_value()
    target: dict[str, Any]
    required: str
    if depth == "top":
        target, required = value, "status"
    elif depth == "identity":
        target, required = value["identity_parameters"], "page-size"
    else:
        target, required = value["pages"][0], "record_count"

    if change == "missing":
        del target[required]
    else:
        target["unexpected"] = "value"

    with pytest.raises(ValueError, match="fields"):
        ApiLogicalRunManifest.from_bytes(_canonical_bytes(value))


@pytest.mark.parametrize(
    ("path", "invalid"),
    [
        (("schema_version",), True),
        (("page_count",), True),
        (("record_count",), True),
        (("pages", 0, "page_number"), True),
        (("pages", 0, "payload_content_length"), True),
        (("pages", 0, "record_count"), True),
        (("schema_version",), "1"),
        (("page_count",), 1.0),
        (("identity_parameters", "page-size"), 25),
        (("pages",), {}),
    ],
)
def test_parser_rejects_wrong_json_types_and_booleans_as_integers(
    path: tuple[str | int, ...], invalid: object
) -> None:
    value = _canonical_value()
    target: Any = value
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = invalid

    with pytest.raises((TypeError, ValueError)):
        ApiLogicalRunManifest.from_bytes(_canonical_bytes(value))


@pytest.mark.parametrize(
    ("field", "invalid", "message"),
    [
        ("manifest_type", "manifest", "manifest_type"),
        ("schema_version", 2, "schema_version"),
        ("source_type", "file", "source_type"),
        ("status", "failed", "status"),
        ("source_name", "unknown", "source"),
        ("source_date", "2026-9-1", "source_date"),
        ("batch_id", "A" * 64, "batch_id"),
        ("completion_run_id", "run_BAD", "completion_run_id"),
        ("completed_at", "2026-09-01T12:30:45Z", "completed_at"),
    ],
)
def test_parser_rejects_unknown_constants_and_noncanonical_identifiers(
    field: str, invalid: object, message: str
) -> None:
    value = _canonical_value()
    value[field] = invalid

    with pytest.raises((TypeError, ValueError), match=message):
        ApiLogicalRunManifest.from_bytes(_canonical_bytes(value))


@pytest.mark.parametrize(
    ("parameter", "invalid"),
    [
        ("page-size", "025"),
        ("page-size", "0"),
        ("page-size", "101"),
        ("resource-path", "/v1/not-fx"),
    ],
)
def test_parser_rejects_noncanonical_or_mismatched_identity_parameters(
    parameter: str, invalid: str
) -> None:
    value = _canonical_value()
    value["identity_parameters"][parameter] = invalid

    with pytest.raises(ValueError, match="identity parameters|page-size"):
        ApiLogicalRunManifest.from_bytes(_canonical_bytes(value))


def test_parser_rederives_batch_id_and_every_page_key() -> None:
    value = _canonical_value()
    value["batch_id"] = "c" * 64
    with pytest.raises(ValueError, match="batch_id"):
        ApiLogicalRunManifest.from_bytes(_canonical_bytes(value))

    value = _canonical_value()
    value["pages"][0]["object_key"] += ".other"
    with pytest.raises(ValueError, match="object_key"):
        ApiLogicalRunManifest.from_bytes(_canonical_bytes(value))

    value = _canonical_value()
    value["pages"][0]["page_manifest_key"] += ".other"
    with pytest.raises(ValueError, match="page_manifest_key"):
        ApiLogicalRunManifest.from_bytes(_canonical_bytes(value))


@pytest.mark.parametrize(
    "field",
    ["payload_checksum_sha256", "page_manifest_checksum_sha256"],
)
def test_parser_rejects_malformed_checksums(field: str) -> None:
    value = _canonical_value()
    value["pages"][0][field] = "A" * 64

    with pytest.raises(ValueError, match=field):
        ApiLogicalRunManifest.from_bytes(_canonical_bytes(value))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda raw: b"\xef\xbb\xbf" + raw,
        lambda raw: raw + b"\n",
        lambda raw: raw + b"{}",
        lambda raw: raw.replace(b"{", b"{\xff", 1),
        lambda raw: json.dumps(json.loads(raw), indent=2, sort_keys=True).encode(),
        lambda raw: raw.replace(
            b'"manifest_type":',
            b'"manifest_type":"duplicate","manifest_type":',
            1,
        ),
        lambda raw: raw.replace(b'"page-size":', b'"page-size":"1","page-size":', 1),
        lambda raw: raw.replace(b'"page_number":', b'"page_number":9,"page_number":', 1),
    ],
)
def test_parser_rejects_noncanonical_bom_trailing_and_duplicate_json(
    mutation: Callable[[bytes], bytes],
) -> None:
    with pytest.raises(ValueError):
        ApiLogicalRunManifest.from_bytes(mutation(_manifest().to_bytes()))


def test_empty_one_page_and_full_plus_final_page_counts_are_valid() -> None:
    empty = _manifest(record_count=0)
    full_and_final = _manifest(page_size=2, record_count=3)

    assert empty.page_count == 1
    assert empty.pages[0].record_count == 0
    assert [page.record_count for page in full_and_final.pages] == [2, 1]
    assert ApiLogicalRunManifest.from_bytes(empty.to_bytes()) == empty
    assert ApiLogicalRunManifest.from_bytes(full_and_final.to_bytes()) == full_and_final


@pytest.mark.parametrize(
    "mutate",
    [
        lambda pages: (replace(pages[0], page_number=2), pages[1]),
        lambda pages: (pages[1], pages[0]),
        lambda pages: (pages[0], replace(pages[1], page_number=1)),
        lambda pages: (replace(pages[0], record_count=1), pages[1]),
    ],
)
def test_page_order_gaps_duplicates_and_nonfinal_counts_are_rejected(
    mutate: Callable[[tuple[ApiLogicalRunPage, ...]], tuple[ApiLogicalRunPage, ...]],
) -> None:
    manifest = _manifest(page_size=2, record_count=3)

    with pytest.raises(ValueError, match="page|record_count"):
        replace(manifest, pages=mutate(manifest.pages))


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("page_count", 2),
        ("record_count", 3),
    ],
)
def test_page_and_record_count_relationships_are_rejected(field: str, invalid: int) -> None:
    manifest = _manifest(record_count=2)

    with pytest.raises(ValueError, match=field):
        if field == "page_count":
            replace(manifest, page_count=invalid)
        else:
            replace(manifest, record_count=invalid)


def test_page_9999_and_maximum_record_count_fit_the_canonical_bound() -> None:
    manifest = _manifest(
        source_name="shipping-statuses",
        page_size=100,
        record_count=999_900,
        maximum_width=True,
    )
    serialized = manifest.to_bytes()

    assert manifest.page_count == 9_999
    assert manifest.pages[-1].page_number == 9_999
    assert len(serialized) == 6_098_762
    assert len(serialized) < API_LOGICAL_RUN_MANIFEST_MAX_BYTES
    assert ApiLogicalRunManifest.from_bytes(serialized) == manifest


def test_page_10000_record_overflow_and_payload_length_overflow_are_rejected() -> None:
    with pytest.raises(ValueError, match="page_number"):
        replace(_manifest().pages[0], page_number=10_000)
    with pytest.raises(ValueError, match="record_count"):
        replace(_manifest(), record_count=999_901)
    with pytest.raises(ValueError, match="payload_content_length"):
        replace(_manifest().pages[0], payload_content_length=2**63)


def test_parser_rejects_documents_over_the_separate_eight_mib_limit() -> None:
    oversized = b"{" + b" " * API_LOGICAL_RUN_MANIFEST_MAX_BYTES + b"}"

    with pytest.raises(ValueError, match="8388608"):
        ApiLogicalRunManifest.from_bytes(oversized)
