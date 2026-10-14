"""Allow-listed REST source descriptors and strict page-envelope validation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation

from batch_ingestion.identifiers import BatchIdentity, SourceType

_ENVELOPE_FIELDS = {
    "source",
    "source_date",
    "page",
    "page_size",
    "total_pages",
    "total_records",
    "next_page",
    "items",
}
_FX_FIELDS = {"base_currency", "quote_currency", "rate", "effective_date"}
_SHIPPING_FIELDS = {"shipment_id", "order_id", "status", "updated_at"}
_FX_QUOTES = {"AUD", "EUR", "GBP", "JPY"}
_SHIPPING_STATUSES = {"delivered", "in_transit", "label_created", "returned"}
_RATE_PATTERN = re.compile(r"[0-9]+\.[0-9]{6}", flags=re.ASCII)
_SHIPMENT_PATTERN = re.compile(r"SHP-([0-9]{8})-([0-9]{4})", flags=re.ASCII)


class PageValidationError(ValueError):
    """Raised after a preserved page violates its documented source contract."""


@dataclass(frozen=True, slots=True)
class ApiSourceDescriptor:
    """One production allow-listed REST source."""

    name: str
    path: str


@dataclass(frozen=True, slots=True)
class ApiPage:
    """Validated page metadata and source records without raw-body reserialization."""

    source: str
    source_date: date
    page: int
    page_size: int
    total_pages: int
    total_records: int
    next_page: int | None
    items: tuple[dict[str, object], ...]


_SOURCES = {
    "fx-rates": ApiSourceDescriptor(name="fx-rates", path="/v1/fx-rates"),
    "shipping-statuses": ApiSourceDescriptor(
        name="shipping-statuses",
        path="/v1/shipping-statuses",
    ),
}


def get_source_descriptor(source_name: str) -> ApiSourceDescriptor:
    """Resolve one fixed source name; arbitrary paths and URLs are forbidden."""
    try:
        return _SOURCES[source_name]
    except (KeyError, TypeError) as error:
        raise ValueError(f"unsupported API source: {source_name!r}") from error


def api_page_identity(
    descriptor: ApiSourceDescriptor,
    source_date: date,
    *,
    page_size: int,
) -> BatchIdentity:
    """Create logical identity; page number deliberately remains a payload detail."""
    _positive_int(page_size, field="page_size")
    return BatchIdentity.create(
        source_name=descriptor.name,
        source_type=SourceType.API,
        source_date=source_date,
        parameters={"resource-path": descriptor.path, "page-size": str(page_size)},
    )


def page_payload_name(page: int, *, max_pages: int) -> str:
    """Return the fixed four-digit page payload name within configured bounds."""
    _positive_int(max_pages, field="max_pages")
    if max_pages > 9_999:
        raise ValueError("max_pages must not exceed 9999")
    if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= max_pages:
        raise ValueError(f"page must be between 1 and {max_pages}")
    return f"page-{page:04d}.json"


def validate_page_envelope(
    value: object,
    *,
    descriptor: ApiSourceDescriptor,
    source_date: date,
    requested_page: int,
    page_size: int,
    max_pages: int,
    expected_total_pages: int | None = None,
    expected_total_records: int | None = None,
) -> ApiPage:
    """Validate envelope identity, pagination invariants, and source item schema."""
    if not isinstance(value, Mapping):
        raise PageValidationError("page envelope must be an object")
    if set(value) != _ENVELOPE_FIELDS:
        raise PageValidationError("page envelope fields do not match the contract")
    _positive_int(requested_page, field="requested_page", error_type=PageValidationError)
    _positive_int(page_size, field="page_size", error_type=PageValidationError)
    _positive_int(max_pages, field="max_pages", error_type=PageValidationError)

    source = _string(value["source"], field="source")
    source_date_text = _string(value["source_date"], field="source_date")
    page = _positive_int(value["page"], field="page", error_type=PageValidationError)
    response_page_size = _positive_int(
        value["page_size"], field="page_size", error_type=PageValidationError
    )
    total_pages = _positive_int(
        value["total_pages"], field="total_pages", error_type=PageValidationError
    )
    total_records = _non_negative_int(value["total_records"], field="total_records")
    next_page = value["next_page"]
    items_value = value["items"]

    parsed_source_date = _canonical_date(source_date_text, field="source_date")
    if source != descriptor.name or parsed_source_date != source_date:
        raise PageValidationError("response source identity does not match the request")
    if page != requested_page or response_page_size != page_size:
        raise PageValidationError("response page identity does not match the request")
    if total_pages > max_pages:
        raise PageValidationError("total_pages exceeds configured maximum")
    calculated_pages = max(1, (total_records + page_size - 1) // page_size)
    if total_pages != calculated_pages or page > total_pages:
        raise PageValidationError("page counts are inconsistent")
    if expected_total_pages is not None and total_pages != expected_total_pages:
        raise PageValidationError("total_pages changed after page 1")
    if expected_total_records is not None and total_records != expected_total_records:
        raise PageValidationError("total_records changed after page 1")

    expected_next = page + 1 if page < total_pages else None
    if isinstance(next_page, bool) or next_page != expected_next:
        raise PageValidationError("next_page does not describe strict sequential traversal")
    if not isinstance(items_value, list):
        raise PageValidationError("items must be an array")
    expected_item_count = min(page_size, max(0, total_records - (page - 1) * page_size))
    if len(items_value) != expected_item_count:
        raise PageValidationError("item count is inconsistent with pagination metadata")

    validated_items: list[dict[str, object]] = []
    for item in items_value:
        if not isinstance(item, Mapping):
            raise PageValidationError("every item must be an object")
        copied = dict(item)
        if descriptor.name == "fx-rates":
            _validate_fx_item(copied, source_date)
        elif descriptor.name == "shipping-statuses":
            _validate_shipping_item(copied, source_date)
        else:
            raise PageValidationError("source descriptor is not allow-listed")
        validated_items.append(copied)

    return ApiPage(
        source=source,
        source_date=parsed_source_date,
        page=page,
        page_size=response_page_size,
        total_pages=total_pages,
        total_records=total_records,
        next_page=expected_next,
        items=tuple(validated_items),
    )


def _validate_fx_item(item: dict[str, object], source_date: date) -> None:
    if set(item) != _FX_FIELDS:
        raise PageValidationError("FX item fields do not match the contract")
    base = _string(item["base_currency"], field="base_currency")
    quote = _string(item["quote_currency"], field="quote_currency")
    rate = _string(item["rate"], field="rate")
    effective_date = _string(item["effective_date"], field="effective_date")
    if base != "USD" or quote not in _FX_QUOTES:
        raise PageValidationError("FX currencies are invalid")
    if _RATE_PATTERN.fullmatch(rate) is None:
        raise PageValidationError("FX rate must have exactly six fractional digits")
    try:
        if Decimal(rate) <= 0:
            raise PageValidationError("FX rate must be positive")
    except InvalidOperation as error:
        raise PageValidationError("FX rate is invalid") from error
    if _canonical_date(effective_date, field="effective_date") != source_date:
        raise PageValidationError("FX effective_date must match source_date")


def _validate_shipping_item(item: dict[str, object], source_date: date) -> None:
    if set(item) != _SHIPPING_FIELDS:
        raise PageValidationError("shipping item fields do not match the contract")
    shipment_id = _string(item["shipment_id"], field="shipment_id")
    order_id = _positive_int(item["order_id"], field="order_id", error_type=PageValidationError)
    status = _string(item["status"], field="status")
    updated_at = _string(item["updated_at"], field="updated_at")
    match = _SHIPMENT_PATTERN.fullmatch(shipment_id)
    expected_date = source_date.strftime("%Y%m%d")
    if match is None or match.group(1) != expected_date or match.group(2) == "0000":
        raise PageValidationError("shipment_id is invalid for source_date")
    if order_id <= 0 or status not in _SHIPPING_STATUSES:
        raise PageValidationError("shipping order_id or status is invalid")
    if not updated_at.endswith("Z"):
        raise PageValidationError("updated_at must use canonical UTC Z form")
    try:
        parsed = datetime.fromisoformat(updated_at.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise PageValidationError("updated_at is not an ISO timestamp") from error
    if parsed.tzinfo != UTC or parsed.date() != source_date:
        raise PageValidationError("updated_at must be UTC on source_date")


def _string(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise PageValidationError(f"{field} must be a non-empty string")
    return value


def _canonical_date(value: str, *, field: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise PageValidationError(f"{field} must be a canonical ISO date") from error
    if parsed.isoformat() != value:
        raise PageValidationError(f"{field} must be a canonical ISO date")
    return parsed


def _positive_int(
    value: object,
    *,
    field: str,
    error_type: type[ValueError] = ValueError,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise error_type(f"{field} must be a positive integer")
    return value


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PageValidationError(f"{field} must be a non-negative integer")
    return value
