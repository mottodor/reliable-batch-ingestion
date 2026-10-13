"""Pure request/response and fault-state contracts for the local mock API."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Any, cast

import pytest
from mock_api.app import (  # pyright: ignore[reportMissingImports] - test-first module
    MALFORMED_JSON_BODY,
    MAX_PAGE_SIZE,
    FaultDecision,
    FaultStateStore,
    HttpResponse,
    canonical_json,
    handle_request,
)

from batch_ingestion.source_seed import DEFAULT_SOURCE_SEED

SOURCE_DATE = date(2026, 9, 1)
SOURCE_DATE_QUERY = "source_date=2026-09-01"


def _request(
    target: str,
    *,
    method: str = "GET",
    store: FaultStateStore | None = None,
) -> HttpResponse:
    return handle_request(
        method=method,
        target=target,
        source_seed=DEFAULT_SOURCE_SEED,
        fault_state=store or FaultStateStore(),
    )


def _json(response: HttpResponse) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(response.body))


def test_fx_pages_are_canonical_byte_stable_and_bounded() -> None:
    target = f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2"

    first = _request(target)
    second = _request(target)

    assert first == second
    assert first.status == 200
    assert first.headers == (
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(first.body))),
        ("Cache-Control", "no-store"),
    )
    assert first.body == canonical_json(_json(first))
    assert first.body == (
        b'{"items":[{"base_currency":"USD","effective_date":"2026-09-01",'
        b'"quote_currency":"AUD","rate":"1.453321"},{"base_currency":"USD",'
        b'"effective_date":"2026-09-01","quote_currency":"EUR",'
        b'"rate":"0.854635"}],"next_page":2,"page":1,"page_size":2,'
        b'"source":"fx-rates","source_date":"2026-09-01","total_pages":2,'
        b'"total_records":4}\n'
    )


def test_shipping_status_pages_have_stable_order_and_terminal_next_page() -> None:
    response = _request(f"/v1/shipping-statuses?{SOURCE_DATE_QUERY}&page=3&page_size=5")

    assert response.status == 200
    payload = _json(response)
    assert isinstance(payload, dict)
    assert payload["source"] == "shipping-statuses"
    assert payload["page"] == 3
    assert payload["page_size"] == 5
    assert payload["total_pages"] == 3
    assert payload["total_records"] == 12
    assert payload["next_page"] is None
    items = payload["items"]
    assert isinstance(items, list)
    assert [item["shipment_id"] for item in items] == [
        "SHP-20260901-0011",
        "SHP-20260901-0012",
    ]
    assert all(item["updated_at"].endswith("Z") for item in items)


@pytest.mark.parametrize(
    ("target", "status", "code"),
    [
        ("/v1/unknown?source_date=2026-09-01&page=1&page_size=2", 404, "not_found"),
        ("/v1/fx-rates?page=1&page_size=2", 400, "invalid_query"),
        ("/v1/fx-rates?source_date=2026-9-1&page=1&page_size=2", 400, "invalid_query"),
        (f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=0&page_size=2", 400, "invalid_query"),
        (f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=0", 400, "invalid_query"),
        (
            f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size={MAX_PAGE_SIZE + 1}",
            400,
            "invalid_query",
        ),
        (f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=3&page_size=2", 404, "page_not_found"),
        (
            f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page=2&page_size=2",
            400,
            "invalid_query",
        ),
        (f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2&extra=x", 400, "invalid_query"),
    ],
)
def test_invalid_routes_and_queries_are_deterministic_permanent_errors(
    target: str,
    status: int,
    code: str,
) -> None:
    first = _request(target)
    second = _request(target)

    assert first == second
    assert first.status == status
    assert _json(first) == {"error": {"code": code, "message": _json(first)["error"]["message"]}}


def test_unsupported_method_is_a_deterministic_405() -> None:
    response = _request(
        f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2",
        method="POST",
    )

    assert response.status == 405
    assert dict(response.headers)["Allow"] == "GET"
    assert _json(response)["error"]["code"] == "method_not_allowed"


def test_rate_limit_once_is_isolated_and_health_does_not_advance_it() -> None:
    store = FaultStateStore(max_keys=4)
    target = (
        f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2"
        "&scenario=rate_limit_once&scenario_key=client-a"
    )

    health = _request("/health", store=store)
    first = _request(target, store=store)
    second = _request(target, store=store)
    other_key = _request(target.replace("client-a", "client-b"), store=store)

    assert health.status == 200
    assert first.status == 429
    assert dict(first.headers)["Retry-After"] == "1"
    assert second.status == 200
    assert other_key.status == 429
    assert store.key_count == 2


def test_rate_limit_counter_can_reset_one_key_or_all_keys() -> None:
    store = FaultStateStore(max_keys=4)
    first_target = (
        f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2"
        "&scenario=rate_limit_once&scenario_key=reset-one"
    )
    second_target = first_target.replace("reset-one", "reset-all")
    assert _request(first_target, store=store).status == 429
    assert _request(second_target, store=store).status == 429

    reset_one = _request("/__control/reset?scenario_key=reset-one", method="POST", store=store)
    assert reset_one.status == 200
    assert _json(reset_one) == {"reset": "one", "scenario_key": "reset-one"}
    assert _request(first_target, store=store).status == 429
    assert _request(second_target, store=store).status == 200

    reset_all = _request("/__control/reset", method="POST", store=store)
    assert reset_all.status == 200
    assert _json(reset_all) == {"keys_removed": 2, "reset": "all"}
    assert store.key_count == 0
    assert _request(second_target, store=store).status == 429


def test_fault_store_rejects_new_keys_at_capacity_without_evicting_existing_keys() -> None:
    store = FaultStateStore(max_keys=2)

    def target(key: str) -> str:
        return (
            f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2"
            f"&scenario=rate_limit_once&scenario_key={key}"
        )

    assert _request(target("one"), store=store).status == 429
    assert _request(target("two"), store=store).status == 429
    at_capacity = _request(target("three"), store=store)

    assert at_capacity.status == 503
    assert _json(at_capacity)["error"]["code"] == "fault_state_capacity"
    assert store.key_count == 2
    assert _request(target("one"), store=store).status == 200

    assert store.reset("two") is True
    assert _request(target("three"), store=store).status == 429
    assert store.key_count == 2


def test_rate_limit_transition_is_thread_safe() -> None:
    store = FaultStateStore(max_keys=2)

    with ThreadPoolExecutor(max_workers=16) as executor:
        decisions = tuple(executor.map(store.consume_rate_limit_once, ["shared"] * 64))

    assert decisions.count(FaultDecision.FIRST) == 1
    assert decisions.count(FaultDecision.SEEN) == 63
    assert decisions.count(FaultDecision.CAPACITY) == 0
    assert store.key_count == 1


def test_permanent_500_is_repeatable_and_stateless() -> None:
    store = FaultStateStore()
    target = f"/v1/shipping-statuses?{SOURCE_DATE_QUERY}&page=1&page_size=5&scenario=permanent_500"

    first = _request(target, store=store)
    second = _request(target, store=store)

    assert first == second
    assert first.status == 500
    assert _json(first)["error"]["code"] == "permanent_failure"
    assert store.key_count == 0


def test_malformed_json_affects_only_the_selected_page_with_exact_bytes() -> None:
    store = FaultStateStore()
    prefix = f"/v1/shipping-statuses?{SOURCE_DATE_QUERY}&page_size=5"
    selected = _request(f"{prefix}&page=2&scenario=malformed_json&fault_page=2", store=store)
    other = _request(f"{prefix}&page=1&scenario=malformed_json&fault_page=2", store=store)

    assert selected.status == 200
    assert dict(selected.headers)["Content-Type"] == "application/json; charset=utf-8"
    assert selected.body == MALFORMED_JSON_BODY
    with pytest.raises(json.JSONDecodeError):
        json.loads(selected.body)
    assert other.status == 200
    assert store.key_count == 0


def test_partial_pages_succeeds_before_repeatable_selected_failure() -> None:
    store = FaultStateStore()
    prefix = f"/v1/shipping-statuses?{SOURCE_DATE_QUERY}&page_size=5"
    first = _request(f"{prefix}&page=1&scenario=partial_pages&fault_page=2", store=store)
    failed = _request(f"{prefix}&page=2&scenario=partial_pages&fault_page=2", store=store)
    repeated = _request(f"{prefix}&page=2&scenario=partial_pages&fault_page=2", store=store)

    assert first.status == 200
    assert failed == repeated
    assert failed.status == 500
    assert _json(failed)["error"]["code"] == "partial_run_failure"
    assert store.key_count == 0


@pytest.mark.parametrize(
    "target",
    [
        f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2&scenario=unknown",
        (f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2&scenario=rate_limit_once"),
        (
            f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2"
            "&scenario=rate_limit_once&scenario_key=invalid%20key"
        ),
        (f"/v1/fx-rates?{SOURCE_DATE_QUERY}&page=1&page_size=2&scenario=malformed_json"),
        (
            f"/v1/shipping-statuses?{SOURCE_DATE_QUERY}&page=1&page_size=5"
            "&scenario=partial_pages&fault_page=1"
        ),
    ],
)
def test_invalid_fault_selection_is_a_permanent_400(target: str) -> None:
    response = _request(target)

    assert response.status == 400
    assert _json(response)["error"]["code"] == "invalid_query"
