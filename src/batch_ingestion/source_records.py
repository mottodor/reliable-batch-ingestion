"""Typed deterministic OmniRetail order and supplier source records."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Literal

from .source_seed import (  # pyright: ignore[reportMissingImports] - pi-lens new-module cache
    derive_seeded_int,
    deterministic_order_id,
    validate_source_date,
    validate_source_seed,
)

OrderStatus = Literal["cancelled", "confirmed", "fulfilled", "pending"]
ShippingStatus = Literal["delivered", "in_transit", "label_created", "returned"]
Currency = Literal["EUR", "GBP", "USD"]

_ORDER_STATUSES: tuple[OrderStatus, ...] = ("cancelled", "confirmed", "fulfilled", "pending")
_SHIPPING_STATUSES: tuple[ShippingStatus, ...] = (
    "delivered",
    "in_transit",
    "label_created",
    "returned",
)
_CURRENCIES: tuple[Currency, ...] = ("EUR", "GBP", "USD")
_SUPPLIERS = ("SUP-001", "SUP-002", "SUP-003")
_ORDER_COUNT = 24
_SUPPLIER_PRICE_COUNT = 15


@dataclass(frozen=True, slots=True)
class OrderRecord:
    """One generated row matching the planned PostgreSQL orders schema."""

    order_id: int
    customer_id: int
    order_status: OrderStatus
    shipping_status: ShippingStatus
    currency: Currency
    total_amount: Decimal
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class SupplierPriceRecord:
    """One generated row matching the supplier-price fixture schema."""

    supplier_id: str
    product_id: str
    price_minor_units: int
    currency: Currency
    effective_date: date


def generate_order_records(seed: int, source_date: date) -> tuple[OrderRecord, ...]:
    """Generate stable order rows with unique ascending UTC watermarks."""
    validated_seed = validate_source_seed(seed)
    validated_date = validate_source_date(source_date)
    start = datetime.combine(validated_date, time.min, tzinfo=UTC)
    records: list[OrderRecord] = []

    for ordinal in range(_ORDER_COUNT):
        customer_id = 10_000 + derive_seeded_int(
            validated_seed,
            namespace="postgres-orders",
            source_date=validated_date,
            ordinal=ordinal,
            field="customer-id",
            modulo=2_000,
        )
        order_status = _ORDER_STATUSES[
            derive_seeded_int(
                validated_seed,
                namespace="postgres-orders",
                source_date=validated_date,
                ordinal=ordinal,
                field="order-status",
                modulo=len(_ORDER_STATUSES),
            )
        ]
        shipping_status = _SHIPPING_STATUSES[
            derive_seeded_int(
                validated_seed,
                namespace="postgres-orders",
                source_date=validated_date,
                ordinal=ordinal,
                field="shipping-status",
                modulo=len(_SHIPPING_STATUSES),
            )
        ]
        currency = _CURRENCIES[
            derive_seeded_int(
                validated_seed,
                namespace="postgres-orders",
                source_date=validated_date,
                ordinal=ordinal,
                field="currency",
                modulo=len(_CURRENCIES),
            )
        ]
        amount_minor_units = 500 + derive_seeded_int(
            validated_seed,
            namespace="postgres-orders",
            source_date=validated_date,
            ordinal=ordinal,
            field="total-amount",
            modulo=100_000,
        )
        records.append(
            OrderRecord(
                order_id=deterministic_order_id(validated_seed, ordinal),
                customer_id=customer_id,
                order_status=order_status,
                shipping_status=shipping_status,
                currency=currency,
                total_amount=Decimal(amount_minor_units).scaleb(-2),
                updated_at=start + timedelta(minutes=37 * ordinal),
            )
        )

    return tuple(records)


def generate_supplier_price_records(
    seed: int,
    source_date: date,
) -> tuple[SupplierPriceRecord, ...]:
    """Generate stable supplier prices sorted by their documented logical key."""
    validated_seed = validate_source_seed(seed)
    validated_date = validate_source_date(source_date)
    records: list[SupplierPriceRecord] = []

    for ordinal in range(_SUPPLIER_PRICE_COUNT):
        supplier_id = _SUPPLIERS[ordinal % len(_SUPPLIERS)]
        currency = _CURRENCIES[
            derive_seeded_int(
                validated_seed,
                namespace="supplier-prices",
                source_date=validated_date,
                ordinal=ordinal,
                field="currency",
                modulo=len(_CURRENCIES),
            )
        ]
        price_minor_units = 100 + derive_seeded_int(
            validated_seed,
            namespace="supplier-prices",
            source_date=validated_date,
            ordinal=ordinal,
            field="price-minor-units",
            modulo=50_000,
        )
        records.append(
            SupplierPriceRecord(
                supplier_id=supplier_id,
                product_id=f"SKU-{ordinal + 1:04d}",
                price_minor_units=price_minor_units,
                currency=currency,
                effective_date=validated_date,
            )
        )

    return tuple(
        sorted(
            records,
            key=lambda record: (
                record.supplier_id,
                record.product_id,
                record.effective_date,
            ),
        )
    )
