"""Live bounded HTTP checks for the Compose-hosted deterministic mock API."""

import os
import shlex
import subprocess
from collections.abc import Iterator, Mapping
from pathlib import Path

import httpx
import pytest
from mock_api.app import MALFORMED_JSON_BODY, canonical_json

from batch_ingestion.config import AppConfig

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
COMMAND_TIMEOUT_SECONDS = 90
QueryValue = str | int | float | bool | None


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name:
            raise ValueError(f"invalid environment line for {name or '<unknown>'}")
        values[name] = value
    return values


@pytest.fixture(scope="module")
def env_path() -> Path:
    configured_path = Path(os.environ.get("BI020_ENV_FILE", ".env"))
    return configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path


@pytest.fixture(scope="module")
def environment(env_path: Path) -> Mapping[str, str]:
    return _load_dotenv(env_path)


@pytest.fixture(scope="module")
def app_config(environment: Mapping[str, str]) -> AppConfig:
    return AppConfig.from_env(environment)


@pytest.fixture(scope="module")
def client(app_config: AppConfig) -> Iterator[httpx.Client]:
    timeout = httpx.Timeout(
        connect=app_config.http.connect_timeout_seconds,
        read=app_config.http.read_timeout_seconds,
        write=app_config.http.read_timeout_seconds,
        pool=app_config.http.connect_timeout_seconds,
    )
    with httpx.Client(
        base_url=app_config.http.base_url,
        timeout=timeout,
        trust_env=False,
    ) as client:
        yield client


def _reset(client: httpx.Client, scenario_key: str | None = None) -> httpx.Response:
    parameters = {} if scenario_key is None else {"scenario_key": scenario_key}
    return client.post("/__control/reset", params=parameters)


def test_health_and_happy_resources_are_bounded_and_byte_stable(client: httpx.Client) -> None:
    health = client.get("/health")
    assert health.status_code == 200
    assert health.content == canonical_json({"status": "ok"})

    resources = {"fx-rates": 4, "shipping-statuses": 12}
    for resource, expected_records in resources.items():
        bodies: list[bytes] = []
        page = 1
        seen_records = 0
        while True:
            response = client.get(
                f"/v1/{resource}",
                params={"source_date": "2026-09-01", "page": page, "page_size": 3},
            )
            assert response.status_code == 200
            payload = response.json()
            assert response.content == canonical_json(payload)
            assert payload["source"] == resource
            assert payload["source_date"] == "2026-09-01"
            assert payload["page"] == page
            assert payload["total_records"] == expected_records
            bodies.append(response.content)
            seen_records += len(payload["items"])
            next_page = payload["next_page"]
            if next_page is None:
                break
            assert next_page == page + 1
            page = next_page

        assert seen_records == expected_records
        repeated = client.get(
            f"/v1/{resource}",
            params={"source_date": "2026-09-01", "page": 1, "page_size": 3},
        )
        assert repeated.content == bodies[0]


def test_live_rate_limit_once_is_isolated_resettable_and_has_retry_after(
    client: httpx.Client,
) -> None:
    first_key = "integration-rate-limit-a"
    second_key = "integration-rate-limit-b"
    assert _reset(client).status_code == 200
    parameters: dict[str, QueryValue] = {
        "source_date": "2026-09-01",
        "page": 1,
        "page_size": 2,
        "scenario": "rate_limit_once",
        "scenario_key": first_key,
    }

    first = client.get("/v1/fx-rates", params=parameters)
    second = client.get("/v1/fx-rates", params=parameters)
    other = client.get(
        "/v1/fx-rates",
        params={**parameters, "scenario_key": second_key},
    )

    assert first.status_code == 429
    assert first.headers["Retry-After"] == "1"
    assert second.status_code == 200
    assert other.status_code == 429

    reset = _reset(client, first_key)
    assert reset.status_code == 200
    assert client.get("/v1/fx-rates", params=parameters).status_code == 429
    assert _reset(client).status_code == 200


def test_live_permanent_malformed_partial_and_4xx_faults_are_repeatable(
    client: httpx.Client,
) -> None:
    base_parameters: dict[str, str | int] = {
        "source_date": "2026-09-01",
        "page": 1,
        "page_size": 5,
    }

    permanent = client.get(
        "/v1/shipping-statuses",
        params={**base_parameters, "scenario": "permanent_500"},
    )
    permanent_repeat = client.get(
        "/v1/shipping-statuses",
        params={**base_parameters, "scenario": "permanent_500"},
    )
    assert permanent.status_code == 500
    assert permanent.content == permanent_repeat.content

    malformed_parameters: dict[str, QueryValue] = {
        **base_parameters,
        "page": 2,
        "scenario": "malformed_json",
        "fault_page": 2,
    }
    malformed = client.get("/v1/shipping-statuses", params=malformed_parameters)
    assert malformed.status_code == 200
    assert malformed.headers["Content-Type"] == "application/json; charset=utf-8"
    assert malformed.content == MALFORMED_JSON_BODY

    partial_parameters: dict[str, QueryValue] = {
        **base_parameters,
        "scenario": "partial_pages",
        "fault_page": 2,
    }
    before_failure = client.get("/v1/shipping-statuses", params=partial_parameters)
    failed = client.get(
        "/v1/shipping-statuses",
        params={**partial_parameters, "page": 2},
    )
    failed_repeat = client.get(
        "/v1/shipping-statuses",
        params={**partial_parameters, "page": 2},
    )
    assert before_failure.status_code == 200
    assert failed.status_code == 500
    assert failed.content == failed_repeat.content

    invalid = client.get(
        "/v1/fx-rates",
        params={"source_date": "2026-09-01", "page": 0, "page_size": 2},
    )
    missing = client.get("/v1/does-not-exist")
    assert invalid.status_code == 400
    assert missing.status_code == 404


def test_same_seed_page_bytes_survive_a_bounded_mock_service_restart(
    client: httpx.Client,
    env_path: Path,
    app_config: AppConfig,
) -> None:
    parameters: dict[str, QueryValue] = {
        "source_date": "2026-09-01",
        "page": 1,
        "page_size": 3,
    }
    before = client.get("/v1/fx-rates", params=parameters)
    assert before.status_code == 200

    compose_command = shlex.split(os.environ.get("COMPOSE", "docker compose"))
    project_name = os.environ.get("COMPOSE_PROJECT_NAME", "reliable-batch-ingestion")
    base_command = [
        *compose_command,
        "--project-name",
        project_name,
        "--env-file",
        str(env_path),
    ]
    client.close()
    stopped = subprocess.run(
        [*base_command, "stop", "mock-api"],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    assert stopped.returncode == 0, stopped.stderr
    started = subprocess.run(
        [
            *base_command,
            "up",
            "--detach",
            "--wait",
            "--wait-timeout",
            "60",
            "mock-api",
        ],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    assert started.returncode == 0, started.stderr

    timeout = httpx.Timeout(connect=2, read=5, write=5, pool=2)
    with httpx.Client(
        base_url=app_config.http.base_url,
        timeout=timeout,
        trust_env=False,
    ) as restarted_client:
        after = restarted_client.get("/v1/fx-rates", params=parameters)
    assert after.status_code == 200
    assert after.content == before.content
