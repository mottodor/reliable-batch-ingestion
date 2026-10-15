"""Focused live evidence for the four BI-080 public demonstration targets."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Iterator, Mapping
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest
from mypy_boto3_s3 import S3Client

from batch_ingestion.config import AppConfig
from batch_ingestion.storage import create_s3_client

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name or name in values:
            raise ValueError("invalid demo integration environment")
        values[name] = value
    return values


def _delete_bucket_objects(client: S3Client, bucket: str) -> None:
    continuation_token: str | None = None
    for _ in range(100):
        arguments: dict[str, object] = {"Bucket": bucket}
        if continuation_token is not None:
            arguments["ContinuationToken"] = continuation_token
        page = client.list_objects_v2(**arguments)  # type: ignore[arg-type]
        for item in page.get("Contents", []):
            key = item.get("Key")
            if key is not None:
                client.delete_object(Bucket=bucket, Key=key)
        if not page.get("IsTruncated"):
            return
        continuation_token = page.get("NextContinuationToken")
        if not continuation_token:
            raise AssertionError("truncated test-bucket listing omitted its token")
    raise AssertionError("test-bucket cleanup exceeded its page bound")


def _json_lines(output: str) -> tuple[Mapping[str, object], ...]:
    parsed: list[Mapping[str, object]] = []
    for line in output.splitlines():
        if not line.startswith("{"):
            continue
        value: object = json.loads(line)
        assert isinstance(value, dict)
        parsed.append(value)
    return tuple(parsed)


@pytest.fixture
def isolated_demo_environment(tmp_path: Path) -> Iterator[tuple[Path, str, str, AppConfig]]:
    base_path = Path(os.environ.get("BI080_ENV_FILE", ".env"))
    if not base_path.is_absolute():
        base_path = REPOSITORY_ROOT / base_path
    values = _load_dotenv(base_path)
    token = uuid4().hex[:12]
    bucket = f"bi080-{token}"
    project = f"bi080-it-{token}"
    values["S3_BUCKET"] = bucket
    values["SNAPSHOT_CHUNK_ROWS"] = "10"
    env_path = tmp_path / ".env"
    env_path.write_text(
        "".join(f"{name}={value}\n" for name, value in values.items()),
        encoding="utf-8",
    )
    env_path.chmod(0o600)
    config = AppConfig.from_env(values)
    client = create_s3_client(config.storage)
    with closing(client):
        client.create_bucket(Bucket=bucket)
    try:
        yield env_path, project, bucket, config
    finally:
        cleanup_client = create_s3_client(config.storage)
        with closing(cleanup_client):
            _delete_bucket_objects(cleanup_client, bucket)
            cleanup_client.delete_bucket(Bucket=bucket)
        shutil.rmtree(REPOSITORY_ROOT / "artifacts" / "demo" / project, ignore_errors=True)


def test_public_demo_targets_create_replay_retry_reject_and_inspect_exact_inventory(
    isolated_demo_environment: tuple[Path, str, str, AppConfig],
) -> None:
    env_path, project, _, config = isolated_demo_environment
    targets = (
        "demo-happy-path",
        "demo-duplicate",
        "demo-api-retry",
        "demo-bad-file",
        "demo-inspect",
    )
    outputs: dict[str, tuple[Mapping[str, object], ...]] = {}
    for target in targets:
        result = subprocess.run(
            (
                "make",
                target,
                f"COMPOSE_ENV_FILE={env_path}",
                f"COMPOSE_PROJECT_NAME={project}",
                "DEMO_TIMEOUT_SECONDS=60",
            ),
            cwd=REPOSITORY_ROOT,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=75,
            check=False,
        )
        assert result.returncode == 0, (target, result.stdout, result.stderr)
        events = _json_lines(result.stdout)
        summaries = [event for event in events if event.get("event") == "demo_summary"]
        assert len(summaries) == 1
        assert summaries[0]["passed"] is True
        outputs[target] = events
        emitted = result.stdout + result.stderr
        assert config.storage.access_key not in emitted
        assert config.storage.secret_key not in emitted
        assert config.postgres.password not in emitted
        assert "SUP-" not in emitted

    duplicate_summary = next(
        event for event in outputs["demo-duplicate"] if event.get("event") == "demo_summary"
    )
    assert duplicate_summary["object_counts"] == {
        "archive": 1,
        "landing": 0,
        "manifest": 5,
        "raw": 5,
        "rejected": 0,
    }
    retry_events = [
        event for event in outputs["demo-api-retry"] if event.get("event") == "api_retry"
    ]
    assert len(retry_events) == 1
    assert retry_events[0]["http_status"] == 429
    file_events = [
        event for event in outputs["demo-bad-file"] if event.get("event") == "file_ingestion"
    ]
    assert {event["rejection_code"] for event in file_events} >= {
        "schema_mismatch",
        "malformed_file",
    }
    inspect_summary = next(
        event for event in outputs["demo-inspect"] if event.get("event") == "demo_summary"
    )
    assert inspect_summary["object_counts"] == {
        "archive": 1,
        "landing": 0,
        "manifest": 8,
        "raw": 8,
        "rejected": 2,
    }


@pytest.mark.parametrize(
    "scenario",
    (
        "api-rate-limit",
        "api-permanent-500",
        "api-malformed-json",
        "api-after-page",
        "file-same-name",
        "file-renamed-content",
        "file-schema-invalid",
        "file-during-move",
        "postgres-connection-drop",
        "s3-unavailable",
        "payload-before-manifest",
    ),
)
def test_allow_listed_recovery_scenario_proves_intermediate_and_final_state(
    isolated_demo_environment: tuple[Path, str, str, AppConfig],
    scenario: str,
) -> None:
    env_path, project, _, config = isolated_demo_environment
    result = subprocess.run(
        (
            "make",
            "demo-recovery",
            f"SCENARIO={scenario}",
            f"COMPOSE_ENV_FILE={env_path}",
            f"COMPOSE_PROJECT_NAME={project}",
            "DEMO_TIMEOUT_SECONDS=60",
        ),
        cwd=REPOSITORY_ROOT,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=75,
        check=False,
    )

    assert result.returncode == 0, (scenario, result.stdout, result.stderr)
    events = _json_lines(result.stdout)
    summaries = [event for event in events if event.get("event") == "demo_summary"]
    assert len(summaries) == 1
    assert summaries[0]["scenario"] == scenario
    assert summaries[0]["passed"] is True
    assert summaries[0]["attempted_exit_class"] == "expected_failure"
    assert summaries[0]["recovery_exit_class"] in {"succeeded", "expected_failure"}
    assert any(event.get("event") == "demo_state" for event in events)
    assert any(
        event.get("event") == "demo_state" and event.get("checkpoint") == "final"
        for event in events
    )
    emitted = result.stdout + result.stderr
    assert config.storage.access_key not in emitted
    assert config.storage.secret_key not in emitted
    assert config.postgres.password not in emitted
    assert "SUP-" not in emitted

    if scenario in {
        "api-after-page",
        "file-during-move",
        "postgres-connection-drop",
        "payload-before-manifest",
    }:
        replay = subprocess.run(
            (
                "make",
                "demo-recovery",
                f"SCENARIO={scenario}",
                f"COMPOSE_ENV_FILE={env_path}",
                f"COMPOSE_PROJECT_NAME={project}",
                "DEMO_TIMEOUT_SECONDS=60",
            ),
            cwd=REPOSITORY_ROOT,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=75,
            check=False,
        )
        assert replay.returncode == 0, (scenario, replay.stdout, replay.stderr)
        replay_summaries = [
            event for event in _json_lines(replay.stdout) if event.get("event") == "demo_summary"
        ]
        assert len(replay_summaries) == 1
        assert replay_summaries[0]["passed"] is True
        assert replay_summaries[0]["recovery_exit_class"] == "succeeded"
        replay_output = replay.stdout + replay.stderr
        assert config.storage.access_key not in replay_output
        assert config.storage.secret_key not in replay_output
        assert config.postgres.password not in replay_output
        assert "SUP-" not in replay_output
