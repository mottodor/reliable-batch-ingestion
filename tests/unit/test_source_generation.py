"""Pure deterministic source-record contracts for BI-040 slice 1."""

import random
from collections.abc import Callable
from dataclasses import fields
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from mock_api.data import (  # pyright: ignore[reportMissingImports] - pi-lens new-module cache
    FxRateRecord,
    ShippingStatusRecord,
    generate_fx_rates,
    generate_shipping_statuses,
)

from batch_ingestion.source_records import (  # pyright: ignore[reportMissingImports] - pi-lens cache
    OrderRecord,
    SupplierPriceRecord,
    generate_order_records,
    generate_supplier_price_records,
)
from batch_ingestion.source_seed import (  # pyright: ignore[reportMissingImports] - pi-lens cache
    DEFAULT_SOURCE_SEED,
    MAX_SOURCE_SEED,
    SourceSeedError,
    parse_source_seed,
    validate_source_seed,
)

SOURCE_DATE = date(2026, 9, 1)


def _all_records(
    seed: int,
) -> tuple[
    tuple[FxRateRecord, ...],
    tuple[ShippingStatusRecord, ...],
    tuple[OrderRecord, ...],
    tuple[SupplierPriceRecord, ...],
]:
    return (
        generate_fx_rates(seed, SOURCE_DATE),
        generate_shipping_statuses(seed, SOURCE_DATE),
        generate_order_records(seed, SOURCE_DATE),
        generate_supplier_price_records(seed, SOURCE_DATE),
    )


@pytest.mark.parametrize(
    "raw_seed",
    ["", " ", "-1", "+1", "1.0", "1_000", "１２", str(MAX_SOURCE_SEED + 1)],
)
def test_invalid_source_seed_text_is_rejected(raw_seed: str) -> None:
    with pytest.raises(SourceSeedError, match="SOURCE_SEED"):
        parse_source_seed(raw_seed)


@pytest.mark.parametrize("seed", [-1, MAX_SOURCE_SEED + 1])
def test_out_of_range_integer_seed_is_rejected(seed: int) -> None:
    with pytest.raises(SourceSeedError, match="SOURCE_SEED"):
        validate_source_seed(seed)


def test_boolean_seed_is_not_accepted_as_an_integer() -> None:
    with pytest.raises(TypeError, match="SOURCE_SEED"):
        validate_source_seed(True)


def test_default_source_seed_has_one_canonical_integer_value() -> None:
    assert DEFAULT_SOURCE_SEED == 20260901
    assert parse_source_seed("20260901") == DEFAULT_SOURCE_SEED
    assert validate_source_seed(DEFAULT_SOURCE_SEED) == DEFAULT_SOURCE_SEED


def test_same_seed_produces_equal_records_without_consuming_global_random_state() -> None:
    random.seed(918273)
    random_state = random.getstate()

    first = _all_records(DEFAULT_SOURCE_SEED)
    second = _all_records(DEFAULT_SOURCE_SEED)

    assert first == second
    assert random.getstate() == random_state


def test_different_seed_changes_each_source_without_changing_record_types() -> None:
    first = _all_records(DEFAULT_SOURCE_SEED)
    second = _all_records(DEFAULT_SOURCE_SEED + 1)

    for first_source, second_source in zip(first, second, strict=True):
        assert first_source != second_source
        assert tuple(type(record) for record in first_source) == tuple(
            type(record) for record in second_source
        )


def test_default_seed_has_stable_reference_values() -> None:
    fx_rates, shipping_statuses, orders, supplier_prices = _all_records(DEFAULT_SOURCE_SEED)

    assert fx_rates[0] == FxRateRecord("USD", "AUD", "1.453321", SOURCE_DATE)
    assert shipping_statuses[0] == ShippingStatusRecord(
        "SHP-20260901-0001",
        20260901001,
        "in_transit",
        datetime(2026, 9, 1, tzinfo=UTC),
    )
    assert orders[0] == OrderRecord(
        20260901001,
        11891,
        "cancelled",
        "returned",
        "GBP",
        Decimal("421.10"),
        datetime(2026, 9, 1, tzinfo=UTC),
    )
    assert supplier_prices[0] == SupplierPriceRecord(
        "SUP-001",
        "SKU-0001",
        23251,
        "GBP",
        SOURCE_DATE,
    )


def test_record_schemas_are_explicit_and_stable() -> None:
    assert tuple(field.name for field in fields(FxRateRecord)) == (
        "base_currency",
        "quote_currency",
        "rate",
        "effective_date",
    )
    assert tuple(field.name for field in fields(ShippingStatusRecord)) == (
        "shipment_id",
        "order_id",
        "status",
        "updated_at",
    )
    assert tuple(field.name for field in fields(OrderRecord)) == (
        "order_id",
        "customer_id",
        "order_status",
        "shipping_status",
        "currency",
        "total_amount",
        "updated_at",
    )
    assert tuple(field.name for field in fields(SupplierPriceRecord)) == (
        "supplier_id",
        "product_id",
        "price_minor_units",
        "currency",
        "effective_date",
    )


def test_fx_rates_have_stable_order_and_fixed_source_date() -> None:
    records = generate_fx_rates(DEFAULT_SOURCE_SEED, SOURCE_DATE)

    assert len(records) == 4
    assert tuple(record.quote_currency for record in records) == tuple(
        sorted(record.quote_currency for record in records)
    )
    assert {record.base_currency for record in records} == {"USD"}
    assert {record.effective_date for record in records} == {SOURCE_DATE}
    assert all(
        record.rate.count(".") == 1 and len(record.rate.rsplit(".", 1)[1]) == 6
        for record in records
    )


def test_shipping_statuses_are_sorted_and_have_fixed_utc_timestamps() -> None:
    records = generate_shipping_statuses(DEFAULT_SOURCE_SEED, SOURCE_DATE)

    assert len(records) == 12
    assert tuple(record.shipment_id for record in records) == tuple(
        sorted(record.shipment_id for record in records)
    )
    assert tuple(record.updated_at for record in records) == tuple(
        sorted(record.updated_at for record in records)
    )
    assert len({record.updated_at for record in records}) == len(records)
    assert all(record.updated_at.tzinfo is UTC for record in records)
    assert all(record.updated_at.date() == SOURCE_DATE for record in records)


def test_orders_match_the_planned_schema_and_watermark_contract() -> None:
    records = generate_order_records(DEFAULT_SOURCE_SEED, SOURCE_DATE)

    assert len(records) == 24
    assert tuple(record.order_id for record in records) == tuple(
        sorted(record.order_id for record in records)
    )
    assert tuple(record.updated_at for record in records) == tuple(
        sorted(record.updated_at for record in records)
    )
    assert len({record.updated_at for record in records}) == len(records)
    assert all(record.updated_at.tzinfo is UTC for record in records)
    assert all(record.total_amount.as_tuple().exponent == -2 for record in records)
    assert all(record.total_amount >= 0 for record in records)


def test_supplier_prices_are_sorted_and_use_positive_integer_minor_units() -> None:
    records = generate_supplier_price_records(DEFAULT_SOURCE_SEED, SOURCE_DATE)

    assert len(records) == 15
    assert records == tuple(
        sorted(
            records,
            key=lambda record: (
                record.supplier_id,
                record.product_id,
                record.effective_date,
            ),
        )
    )
    assert all(record.price_minor_units > 0 for record in records)
    assert all(type(record.price_minor_units) is int for record in records)
    assert {record.effective_date for record in records} == {SOURCE_DATE}


@pytest.mark.parametrize(
    "generator",
    [
        generate_fx_rates,
        generate_shipping_statuses,
        generate_order_records,
        generate_supplier_price_records,
    ],
)
def test_generators_require_a_date_not_a_datetime(
    generator: Callable[[int, date], object],
) -> None:
    timestamp = datetime(2026, 9, 1, tzinfo=UTC)

    with pytest.raises(TypeError, match="source_date"):
        generator(DEFAULT_SOURCE_SEED, timestamp)
