"""Pure deterministic HTTP contract and bounded fault state for the mock API."""

import json
import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .data import generate_fx_rates, generate_shipping_statuses

MAX_PAGE_SIZE = 100
DEFAULT_MAX_FAULT_KEYS = 128
MALFORMED_JSON_BODY = b'{"error":"malformed-page","items":[\n'
_SCENARIOS = {
    "happy",
    "rate_limit_once",
    "permanent_500",
    "malformed_json",
    "partial_pages",
}
_RESOURCE_PATHS = {
    "/v1/fx-rates": "fx-rates",
    "/v1/shipping-statuses": "shipping-statuses",
}
_SCENARIO_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_ASCII_INTEGER_PATTERN = re.compile(r"[0-9]+", flags=re.ASCII)


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """Complete deterministic response before transport-added HTTP headers."""

    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


class FaultDecision(Enum):
    """Outcome of one rate-limit-once state transition."""

    FIRST = "first"
    SEEN = "seen"
    CAPACITY = "capacity"


class FaultStateStore:
    """Thread-safe bounded state for isolated rate-limit-once scenario keys."""

    def __init__(self, max_keys: int = DEFAULT_MAX_FAULT_KEYS) -> None:
        if isinstance(max_keys, bool) or not isinstance(max_keys, int) or max_keys <= 0:
            raise ValueError("max_keys must be a positive integer")
        self._max_keys = max_keys
        self._counts: dict[str, int] = {}
        self._lock = Lock()

    @property
    def key_count(self) -> int:
        """Return the current retained-key count under the store lock."""
        with self._lock:
            return len(self._counts)

    def consume_rate_limit_once(self, key: str) -> FaultDecision:
        """Record a request without evicting keys that have already consumed their fault."""
        with self._lock:
            if key in self._counts:
                self._counts[key] += 1
                return FaultDecision.SEEN
            if len(self._counts) >= self._max_keys:
                return FaultDecision.CAPACITY
            self._counts[key] = 1
            return FaultDecision.FIRST

    def reset(self, key: str) -> bool:
        """Remove one scenario key and report whether it existed."""
        with self._lock:
            return self._counts.pop(key, None) is not None

    def reset_all(self) -> int:
        """Clear every scenario key and return the number removed."""
        with self._lock:
            removed = len(self._counts)
            self._counts.clear()
            return removed


def canonical_json(value: object) -> bytes:
    """Serialize one response body with stable UTF-8 bytes and a final newline."""
    return (
        json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")


def _response(
    status: int,
    body: bytes,
    *,
    extra_headers: tuple[tuple[str, str], ...] = (),
) -> HttpResponse:
    headers = (
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(body))),
        ("Cache-Control", "no-store"),
        *extra_headers,
    )
    return HttpResponse(status=status, headers=headers, body=body)


def _json_response(
    status: int,
    value: object,
    *,
    extra_headers: tuple[tuple[str, str], ...] = (),
) -> HttpResponse:
    return _response(status, canonical_json(value), extra_headers=extra_headers)


def _error_response(
    status: int,
    code: str,
    message: str,
    *,
    extra_headers: tuple[tuple[str, str], ...] = (),
) -> HttpResponse:
    return _json_response(
        status,
        {"error": {"code": code, "message": message}},
        extra_headers=extra_headers,
    )


def _parse_query(raw_query: str) -> dict[str, str] | None:
    try:
        parsed = parse_qs(raw_query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return None
    if any(len(values) != 1 for values in parsed.values()):
        return None
    return {name: values[0] for name, values in parsed.items()}


def _positive_integer(value: str, *, maximum: int | None = None) -> int | None:
    if _ASCII_INTEGER_PATTERN.fullmatch(value) is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    if parsed <= 0 or (maximum is not None and parsed > maximum):
        return None
    return parsed


def _parse_source_date(value: str) -> date | None:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _valid_scenario_key(value: str) -> bool:
    return _SCENARIO_KEY_PATTERN.fullmatch(value) is not None


def _invalid_query(message: str) -> HttpResponse:
    return _error_response(400, "invalid_query", message)


def _handle_health(method: str, raw_query: str) -> HttpResponse:
    if method != "GET":
        return _error_response(
            405,
            "method_not_allowed",
            "health supports GET only",
            extra_headers=(("Allow", "GET"),),
        )
    if raw_query:
        return _invalid_query("health does not accept query parameters")
    return _json_response(200, {"status": "ok"})


def _handle_reset(
    method: str,
    raw_query: str,
    fault_state: FaultStateStore,
) -> HttpResponse:
    if method != "POST":
        return _error_response(
            405,
            "method_not_allowed",
            "fault reset supports POST only",
            extra_headers=(("Allow", "POST"),),
        )
    query = _parse_query(raw_query)
    if query is None or set(query) - {"scenario_key"}:
        return _invalid_query("reset accepts at most one scenario_key")
    scenario_key = query.get("scenario_key")
    if scenario_key is None:
        return _json_response(200, {"keys_removed": fault_state.reset_all(), "reset": "all"})
    if not _valid_scenario_key(scenario_key):
        return _invalid_query("scenario_key has an invalid format")
    fault_state.reset(scenario_key)
    return _json_response(200, {"reset": "one", "scenario_key": scenario_key})


def _serialize_items(source: str, source_seed: int, source_date: date) -> list[dict[str, Any]]:
    if source == "fx-rates":
        return [
            {
                "base_currency": record.base_currency,
                "quote_currency": record.quote_currency,
                "rate": record.rate,
                "effective_date": record.effective_date.isoformat(),
            }
            for record in generate_fx_rates(source_seed, source_date)
        ]
    return [
        {
            "shipment_id": record.shipment_id,
            "order_id": record.order_id,
            "status": record.status,
            "updated_at": record.updated_at.isoformat().replace("+00:00", "Z"),
        }
        for record in generate_shipping_statuses(source_seed, source_date)
    ]


def _validate_resource_query(
    raw_query: str,
) -> tuple[dict[str, str], date, int, int, str, int | None] | HttpResponse:
    query = _parse_query(raw_query)
    if query is None:
        return _invalid_query("query parameters must occur exactly once")
    base_keys = {"source_date", "page", "page_size"}
    if not base_keys.issubset(query):
        return _invalid_query("source_date, page, and page_size are required")

    scenario = query.get("scenario", "happy")
    if scenario not in _SCENARIOS:
        return _invalid_query("scenario is not supported")
    expected_keys = set(base_keys)
    if "scenario" in query:
        expected_keys.add("scenario")
    if scenario == "rate_limit_once":
        expected_keys.add("scenario_key")
    elif scenario in {"malformed_json", "partial_pages"}:
        expected_keys.add("fault_page")
    if set(query) != expected_keys:
        return _invalid_query("query parameters do not match the selected scenario")

    source_date = _parse_source_date(query["source_date"])
    page = _positive_integer(query["page"])
    page_size = _positive_integer(query["page_size"], maximum=MAX_PAGE_SIZE)
    if source_date is None or page is None or page_size is None:
        return _invalid_query("source_date, page, or page_size is invalid")

    scenario_key = query.get("scenario_key")
    if scenario_key is not None and not _valid_scenario_key(scenario_key):
        return _invalid_query("scenario_key has an invalid format")

    fault_page: int | None = None
    if "fault_page" in query:
        fault_page = _positive_integer(query["fault_page"])
        if fault_page is None or (scenario == "partial_pages" and fault_page < 2):
            return _invalid_query("fault_page is invalid for the selected scenario")

    return query, source_date, page, page_size, scenario, fault_page


def _handle_resource(
    method: str,
    path: str,
    raw_query: str,
    source_seed: int,
    fault_state: FaultStateStore,
) -> HttpResponse:
    if method != "GET":
        return _error_response(
            405,
            "method_not_allowed",
            "resources support GET only",
            extra_headers=(("Allow", "GET"),),
        )

    validated = _validate_resource_query(raw_query)
    if isinstance(validated, HttpResponse):
        return validated
    query, source_date, page, page_size, scenario, fault_page = validated
    source = _RESOURCE_PATHS[path]
    items = _serialize_items(source, source_seed, source_date)
    total_records = len(items)
    total_pages = (total_records + page_size - 1) // page_size
    if page > total_pages:
        return _error_response(404, "page_not_found", "requested page exceeds total_pages")
    if fault_page is not None and fault_page > total_pages:
        return _invalid_query("fault_page exceeds total_pages")

    if scenario == "rate_limit_once":
        decision = fault_state.consume_rate_limit_once(query["scenario_key"])
        if decision is FaultDecision.CAPACITY:
            return _error_response(
                503,
                "fault_state_capacity",
                "fault scenario key capacity is exhausted",
            )
        if decision is FaultDecision.FIRST:
            return _error_response(
                429,
                "rate_limited",
                "injected one-time rate limit",
                extra_headers=(("Retry-After", "1"),),
            )
    elif scenario == "permanent_500":
        return _error_response(500, "permanent_failure", "injected permanent failure")
    elif scenario == "malformed_json" and page == fault_page:
        return _response(200, MALFORMED_JSON_BODY)
    elif scenario == "partial_pages" and page == fault_page:
        return _error_response(500, "partial_run_failure", "injected partial-run failure")

    start = (page - 1) * page_size
    page_items = items[start : start + page_size]
    next_page = page + 1 if page < total_pages else None
    return _json_response(
        200,
        {
            "source": source,
            "source_date": source_date.isoformat(),
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "total_records": total_records,
            "next_page": next_page,
            "items": page_items,
        },
    )


def handle_request(
    *,
    method: str,
    target: str,
    source_seed: int,
    fault_state: FaultStateStore,
) -> HttpResponse:
    """Return one response without binding a socket or performing external I/O."""
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc or parsed.fragment:
        return _error_response(400, "invalid_target", "request target must be an origin form")
    if parsed.path == "/health":
        return _handle_health(method, parsed.query)
    if parsed.path == "/__control/reset":
        return _handle_reset(method, parsed.query, fault_state)
    if parsed.path not in _RESOURCE_PATHS:
        return _error_response(404, "not_found", "route does not exist")
    return _handle_resource(
        method,
        parsed.path,
        parsed.query,
        source_seed,
        fault_state,
    )
