"""Strict Arrow schema, writer options, and order-row value contract."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final

import pyarrow as pa

ORDERS_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_status", pa.string(), nullable=False),
        pa.field("shipping_status", pa.string(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("total_amount", pa.decimal128(12, 2), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

_ORDER_STATUSES = frozenset({"cancelled", "confirmed", "fulfilled", "pending"})
_SHIPPING_STATUSES = frozenset({"delivered", "in_transit", "label_created", "returned"})
_CURRENCIES = frozenset({"EUR", "GBP", "USD"})
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


@dataclass(frozen=True, slots=True)
class ParquetWriterOptions:
    """Versioned options that make later one-chunk serialization explicit."""

    version: str = "2.6"
    data_page_version: str = "2.0"
    compression: str = "NONE"
    use_dictionary: bool = False
    write_statistics: bool = False

    def as_kwargs(self) -> dict[str, str | bool]:
        return {
            "version": self.version,
            "data_page_version": self.data_page_version,
            "compression": self.compression,
            "use_dictionary": self.use_dictionary,
            "write_statistics": self.write_statistics,
        }


PARQUET_WRITER_OPTIONS: Final = ParquetWriterOptions()


@dataclass(frozen=True, slots=True)
class SnapshotOrderRow:
    """One exact source row accepted by the versioned Parquet contract."""

    order_id: int
    customer_id: int
    order_status: str
    shipping_status: str
    currency: str
    total_amount: Decimal
    updated_at: datetime

    def __post_init__(self) -> None:
        _validate_int64(self.order_id, field="order_id")
        _validate_int64(self.customer_id, field="customer_id")
        _validate_allowed(self.order_status, _ORDER_STATUSES, field="order_status")
        _validate_allowed(self.shipping_status, _SHIPPING_STATUSES, field="shipping_status")
        _validate_allowed(self.currency, _CURRENCIES, field="currency")
        _validate_amount(self.total_amount)
        _validate_updated_at(self.updated_at)

    @classmethod
    def from_sequence(cls, values: Sequence[object]) -> SnapshotOrderRow:
        """Validate a seven-column database result without coercing any value."""
        if len(values) != len(ORDERS_ARROW_SCHEMA):
            raise ValueError("order row must contain exactly seven columns")
        return cls(
            order_id=values[0],  # type: ignore[arg-type]
            customer_id=values[1],  # type: ignore[arg-type]
            order_status=values[2],  # type: ignore[arg-type]
            shipping_status=values[3],  # type: ignore[arg-type]
            currency=values[4],  # type: ignore[arg-type]
            total_amount=values[5],  # type: ignore[arg-type]
            updated_at=values[6],  # type: ignore[arg-type]
        )

    def as_tuple(self) -> tuple[int, int, str, str, str, Decimal, datetime]:
        """Return fields in the exact database and Arrow column order."""
        return (
            self.order_id,
            self.customer_id,
            self.order_status,
            self.shipping_status,
            self.currency,
            self.total_amount,
            self.updated_at,
        )


def _validate_int64(value: int, *, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not _INT64_MIN <= value <= _INT64_MAX
    ):
        raise TypeError(f"{field} must be a signed 64-bit integer")


def _validate_allowed(value: str, allowed: frozenset[str], *, field: str) -> None:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{field} violates the orders source contract")


def _validate_amount(value: Decimal) -> None:
    if not isinstance(value, Decimal):
        raise TypeError("total_amount must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError("total_amount must be finite and non-negative")
    decimal_tuple = value.as_tuple()
    if decimal_tuple.exponent != -2 or len(decimal_tuple.digits) > 12:
        raise ValueError("total_amount must have exact decimal(12, 2) scale and precision")


def _validate_updated_at(value: datetime) -> None:
    if not isinstance(value, datetime):
        raise TypeError("updated_at must be a datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("updated_at must be timezone-aware UTC")
