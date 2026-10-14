"""Allow-listed API source and strict page-envelope contracts."""

from datetime import date
from typing import Any

import pytest
from batch_ingestion.api.sources import (
    PageValidationError,
    api_page_identity,
    get_source_descriptor,
    page_payload_name,
    validate_page_envelope,
)

from batch_ingestion.identifiers import SourceType, derive_batch_id, raw_object_key

SOURCE_DATE = date(2026, 9, 1)


def _fx_item(**overrides: object) -> dict[str, object]:
    item: dict[str, object] = {
        "base_currency": "USD",
        "quote_currency": "EUR",
        "rate": "0.854635",
        "effective_date": "2026-09-01",
    }
    item.update(overrides)
    return item


def _shipping_item(**overrides: object) -> dict[str, object]:
    item: dict[str, object] = {
        "shipment_id": "SHP-20260901-0001",
        "order_id": 20_260_901_001,
        "status": "in_transit",
        "updated_at": "2026-09-01T00:00:00Z",
    }
    item.update(overrides)
    return item


def _envelope(source: str, items: list[object], **overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "source": source,
        "source_date": "2026-09-01",
        "page": 1,
        "page_size": 2,
        "total_pages": 1,
        "total_records": len(items),
        "next_page": None,
        "items": items,
    }
    value.update(overrides)
    return value


def test_only_documented_sources_are_allow_listed() -> None:
    fx = get_source_descriptor("fx-rates")
    shipping = get_source_descriptor("shipping-statuses")

    assert (fx.name, fx.path) == ("fx-rates", "/v1/fx-rates")
    assert (shipping.name, shipping.path) == (
        "shipping-statuses",
        "/v1/shipping-statuses",
    )
    with pytest.raises(ValueError, match="unsupported API source"):
        get_source_descriptor("https://example.com/arbitrary")


def test_api_identity_and_page_keys_are_deterministic_and_page_is_not_identity() -> None:
    descriptor = get_source_descriptor("fx-rates")

    identity = api_page_identity(descriptor, SOURCE_DATE, page_size=25)
    repeated = api_page_identity(descriptor, SOURCE_DATE, page_size=25)

    assert identity == repeated
    assert identity.source_type is SourceType.API
    assert identity.parameters == (("page-size", "25"), ("resource-path", "/v1/fx-rates"))
    assert derive_batch_id(identity) == derive_batch_id(repeated)
    assert page_payload_name(1, max_pages=9_999) == "page-0001.json"
    assert page_payload_name(9_999, max_pages=9_999) == "page-9999.json"
    assert raw_object_key(identity, page_payload_name(1, max_pages=9_999)).endswith(
        "/page-0001.json"
    )

    with pytest.raises(ValueError, match="page must be between 1 and 9999"):
        page_payload_name(10_000, max_pages=9_999)


@pytest.mark.parametrize(
    ("source", "item"),
    [("fx-rates", _fx_item()), ("shipping-statuses", _shipping_item())],
)
def test_valid_page_envelopes_are_typed_and_preserve_items(
    source: str,
    item: dict[str, object],
) -> None:
    descriptor = get_source_descriptor(source)

    page = validate_page_envelope(
        _envelope(source, [item]),
        descriptor=descriptor,
        source_date=SOURCE_DATE,
        requested_page=1,
        page_size=2,
        max_pages=100,
    )

    assert page.source == source
    assert page.source_date == SOURCE_DATE
    assert page.page == 1
    assert page.total_records == 1
    assert page.items == (item,)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(extra="unknown"),
        lambda value: value.pop("items"),
        lambda value: value.update(page=True),
        lambda value: value.update(total_records=-1),
        lambda value: value.update(source="shipping-statuses"),
        lambda value: value.update(source_date="2026-9-1"),
        lambda value: value.update(page_size=3),
        lambda value: value.update(total_pages=101),
        lambda value: value.update(next_page=1),
    ],
)
def test_envelope_fields_types_identity_and_bounds_are_strict(
    mutation: Any,
) -> None:
    value = _envelope("fx-rates", [_fx_item()])
    mutation(value)

    with pytest.raises(PageValidationError):
        validate_page_envelope(
            value,
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            requested_page=1,
            page_size=2,
            max_pages=100,
        )


@pytest.mark.parametrize(
    "item",
    [
        _fx_item(extra="unknown"),
        _fx_item(base_currency="EUR"),
        _fx_item(quote_currency="CAD"),
        _fx_item(rate=0.85),
        _fx_item(rate="0.85"),
        _fx_item(effective_date="2026-09-02"),
    ],
)
def test_fx_item_schema_is_strict(item: dict[str, object]) -> None:
    with pytest.raises(PageValidationError):
        validate_page_envelope(
            _envelope("fx-rates", [item]),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            requested_page=1,
            page_size=2,
            max_pages=100,
        )


@pytest.mark.parametrize(
    "item",
    [
        _shipping_item(extra="unknown"),
        _shipping_item(shipment_id="bad"),
        _shipping_item(order_id=True),
        _shipping_item(status="lost"),
        _shipping_item(updated_at="2026-09-01T00:00:00"),
        _shipping_item(updated_at="2026-09-02T00:00:00Z"),
    ],
)
def test_shipping_item_schema_is_strict(item: dict[str, object]) -> None:
    with pytest.raises(PageValidationError):
        validate_page_envelope(
            _envelope("shipping-statuses", [item]),
            descriptor=get_source_descriptor("shipping-statuses"),
            source_date=SOURCE_DATE,
            requested_page=1,
            page_size=2,
            max_pages=100,
        )


def test_page_counts_next_page_and_later_page_totals_must_be_consistent() -> None:
    first = validate_page_envelope(
        _envelope(
            "fx-rates",
            [_fx_item(quote_currency="AUD"), _fx_item(quote_currency="EUR")],
            total_pages=2,
            total_records=3,
            next_page=2,
        ),
        descriptor=get_source_descriptor("fx-rates"),
        source_date=SOURCE_DATE,
        requested_page=1,
        page_size=2,
        max_pages=100,
    )
    assert first.next_page == 2

    second_value = _envelope(
        "fx-rates",
        [_fx_item(quote_currency="GBP")],
        page=2,
        total_pages=2,
        total_records=3,
    )
    second = validate_page_envelope(
        second_value,
        descriptor=get_source_descriptor("fx-rates"),
        source_date=SOURCE_DATE,
        requested_page=2,
        page_size=2,
        max_pages=100,
        expected_total_pages=first.total_pages,
        expected_total_records=first.total_records,
    )
    assert second.next_page is None

    for field, bad_value in (("total_pages", 3), ("total_records", 4), ("next_page", 3)):
        inconsistent = dict(second_value)
        inconsistent[field] = bad_value
        with pytest.raises(PageValidationError):
            validate_page_envelope(
                inconsistent,
                descriptor=get_source_descriptor("fx-rates"),
                source_date=SOURCE_DATE,
                requested_page=2,
                page_size=2,
                max_pages=100,
                expected_total_pages=2,
                expected_total_records=3,
            )
