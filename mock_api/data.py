"""Typed deterministic records served by the future local mock API."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal

from batch_ingestion.source_seed import (  # pyright: ignore[reportMissingImports] - pi-lens cache
    derive_seeded_int,
    deterministic_order_id,
    validate_source_date,
    validate_source_seed,
)

FxCurrency = Literal["AUD", "EUR", "GBP", "JPY"]
ApiShippingStatus = Literal["delivered", "in_transit", "label_created", "returned"]

_QUOTE_CURRENCIES: tuple[FxCurrency, ...] = ("AUD", "EUR", "GBP", "JPY")
_RATE_BASE_MICRO_UNITS: dict[FxCurrency, int] = {
    "AUD": 1_450_000,
    "EUR": 850_000,
    "GBP": 750_000,
    "JPY": 140_000_000,
}
_SHIPPING_STATUSES: tuple[ApiShippingStatus, ...] = (
    "delivered",
    "in_transit",
    "label_created",
    "returned",
)
_SHIPPING_RECORD_COUNT = 12


@dataclass(frozen=True, slots=True)
class FxRateRecord:
    """One daily USD foreign-exchange rate represented without binary floats."""

    base_currency: Literal["USD"]
    quote_currency: FxCurrency
    rate: str
    effective_date: date


@dataclass(frozen=True, slots=True)
class ShippingStatusRecord:
    """One deterministic shipping-status update for a generated order."""

    shipment_id: str
    order_id: int
    status: ApiShippingStatus
    updated_at: datetime


def generate_fx_rates(seed: int, source_date: date) -> tuple[FxRateRecord, ...]:
    """Generate quote-currency-sorted daily FX records."""
    validated_seed = validate_source_seed(seed)
    validated_date = validate_source_date(source_date)
    records: list[FxRateRecord] = []

    for ordinal, quote_currency in enumerate(_QUOTE_CURRENCIES):
        jitter = (
            derive_seeded_int(
                validated_seed,
                namespace="api-fx-rates",
                source_date=validated_date,
                ordinal=ordinal,
                field="rate-jitter",
                modulo=20_001,
            )
            - 10_000
        )
        rate_micro_units = _RATE_BASE_MICRO_UNITS[quote_currency] + jitter
        rate = f"{rate_micro_units // 1_000_000}.{rate_micro_units % 1_000_000:06d}"
        records.append(
            FxRateRecord(
                base_currency="USD",
                quote_currency=quote_currency,
                rate=rate,
                effective_date=validated_date,
            )
        )

    return tuple(records)


def generate_shipping_statuses(
    seed: int,
    source_date: date,
) -> tuple[ShippingStatusRecord, ...]:
    """Generate shipment-ID-sorted updates with unique fixed UTC timestamps."""
    validated_seed = validate_source_seed(seed)
    validated_date = validate_source_date(source_date)
    start = datetime.combine(validated_date, time.min, tzinfo=UTC)
    records: list[ShippingStatusRecord] = []

    for ordinal in range(_SHIPPING_RECORD_COUNT):
        status = _SHIPPING_STATUSES[
            derive_seeded_int(
                validated_seed,
                namespace="api-shipping-statuses",
                source_date=validated_date,
                ordinal=ordinal,
                field="status",
                modulo=len(_SHIPPING_STATUSES),
            )
        ]
        records.append(
            ShippingStatusRecord(
                shipment_id=f"SHP-{validated_date:%Y%m%d}-{ordinal + 1:04d}",
                order_id=deterministic_order_id(validated_seed, ordinal),
                status=status,
                updated_at=start + timedelta(minutes=37 * ordinal),
            )
        )

    return tuple(records)
