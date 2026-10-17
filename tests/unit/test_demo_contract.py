"""Contracts for the BI-080 slice-one demo tooling."""

from __future__ import annotations

import importlib
import io
import json
import os
import stat
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
import scripts.demo_faults
import scripts.demo_worker
import scripts.init_local_env as init_local_env_module
from scripts.demo import (
    RECOVERY_SCENARIOS,
    ArtifactRelativePath,
    DemoErrorCode,
    DemoEventCode,
    DemoExitClass,
    DemoObjectCounts,
    DemoState,
    DemoSummary,
    ExpectedObjectState,
    ProductionDemoAdapter,
    RecoveryScenario,
    SupervisedResult,
    run_supervised,
)
from scripts.demo import (
    main as demo_main,
)
from scripts.demo_faults import DeleteBoundaryClient, ManifestBoundaryStorage
from scripts.demo_inventory import (
    DemoBatchRecord,
    DemoInventoryError,
    DemoInventoryVerifier,
)
from scripts.init_local_env import InitializationError, initialize_local_environment

from batch_ingestion.api.logical_manifest import (
    API_LOGICAL_RUN_MANIFEST_STATUS,
    API_LOGICAL_RUN_MANIFEST_TYPE,
    ApiLogicalRunManifest,
    ApiLogicalRunPage,
    api_logical_run_manifest_key,
)
from batch_ingestion.identifiers import (
    BatchIdentity,
    SourceType,
    derive_batch_id,
    manifest_key_for,
    raw_object_key,
)
from batch_ingestion.manifests import Manifest, ManifestStatus

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE_PATH = REPOSITORY_ROOT / "Makefile"
README_PATH = REPOSITORY_ROOT / "README.md"
EXAMPLE_ENV_PATH = REPOSITORY_ROOT / ".env.example"

SECRET_VARIABLES = (
    "POSTGRES_PASSWORD",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "GARAGE_RPC_SECRET",
)


def _assignments(content: str) -> dict[str, str]:
    return {
        name: value
        for line in content.splitlines()
        if line and not line.startswith("#")
        for name, separator, value in [line.partition("=")]
        if separator
    }


def _replace_setting(content: str, name: str, value: str) -> str:
    prefix = f"{name}="
    lines = content.splitlines(keepends=True)
    matches = [index for index, line in enumerate(lines) if line.startswith(prefix)]
    assert len(matches) == 1
    index = matches[0]
    newline = "\n" if lines[index].endswith("\n") else ""
    lines[index] = f"{name}={value}{newline}"
    return "".join(lines)


def _initialized_environment(content: str) -> tuple[str, dict[str, str]]:
    generated = {
        "POSTGRES_PASSWORD": "generated-postgres-sentinel",
        "S3_ACCESS_KEY": "generated-access-sentinel",
        "S3_SECRET_KEY": "generated-storage-sentinel",
        "GARAGE_RPC_SECRET": "0123456789abcdef" * 4,
    }
    initialized = content
    placeholders = {
        "POSTGRES_PASSWORD": "change-me",
        "S3_ACCESS_KEY": "change-me",
        "S3_SECRET_KEY": "change-me",
        "GARAGE_RPC_SECRET": "change-me-64-character-hex-value",
    }
    for name, placeholder in placeholders.items():
        initialized = _replace_setting(initialized, name, generated[name])
        assert placeholder not in _assignments(initialized)[name]
    return initialized, generated


def test_make_exposes_exact_demo_surface_with_preflight_and_forwarding() -> None:
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")

    targets = (
        "init-local-env",
        "demo-happy-path",
        "demo-duplicate",
        "demo-api-retry",
        "demo-bad-file",
        "demo-recovery",
        "demo-inspect",
    )
    phony_line = next(line for line in makefile.splitlines() if line.startswith(".PHONY:"))
    for target in targets:
        assert target in phony_line
        assert f"{target}:" in makefile

    assert "DEMO_TIMEOUT_SECONDS ?=" in makefile
    assert 'scripts/init_local_env.py "$(COMPOSE_ENV_FILE)"' in makefile
    for target in targets[1:]:
        declaration = next(line for line in makefile.splitlines() if line.startswith(f"{target}:"))
        assert "check-local-env" in declaration
    assert '--scenario "$(SCENARIO)"' in makefile
    assert '--timeout-seconds "$(DEMO_TIMEOUT_SECONDS)"' in makefile
    assert '--env-file "$(COMPOSE_ENV_FILE)"' in makefile
    assert '--project-name "$(COMPOSE_PROJECT_NAME)"' in makefile


def test_make_and_readme_describe_the_exact_pure_preflight_surface() -> None:
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")
    readme = README_PATH.read_text(encoding="utf-8")

    assert (
        "init-local-env: ## Initialize placeholders and validate complete local configuration"
        in makefile
    )
    assert (
        "check-local-env: ## Validate complete application and Compose-only local settings"
        in makefile
    )
    assert "PostgreSQL, S3, HTTP, file, snapshot, and logging settings" in readme
    assert "Compose-only ports, Garage RPC secret, and recognized placeholders" in readme
    assert "does not contact services or prove that credentials are accepted" in readme
    assert "SOURCE_SEED" in readme


def test_initializer_creates_owner_only_file_and_prints_no_values(tmp_path: Path) -> None:
    target = tmp_path / ".env"
    messages: list[str] = []

    result = initialize_local_environment(
        example_path=EXAMPLE_ENV_PATH,
        target_path=target,
        is_tracked=lambda _: False,
        output=messages.append,
    )

    content = target.read_text(encoding="utf-8")
    values = _assignments(content)
    assert result.changed is True
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert all("change-me" not in values[name] for name in SECRET_VARIABLES)
    assert len(values["GARAGE_RPC_SECRET"]) == 64
    assert set(values["GARAGE_RPC_SECRET"]) <= set("0123456789abcdef")
    emitted = "\n".join(messages)
    assert all(values[name] not in emitted for name in SECRET_VARIABLES)
    assert "PASS" in emitted


def test_initializer_replaces_only_recognized_placeholders_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / ".env"
    target.write_bytes(EXAMPLE_ENV_PATH.read_bytes())
    preflight_paths: list[Path] = []

    def full_preflight(path: Path) -> None:
        init_local_env_module._default_preflight(path)
        preflight_paths.append(path)

    first = initialize_local_environment(
        example_path=EXAMPLE_ENV_PATH,
        target_path=target,
        is_tracked=lambda _: False,
        preflight=full_preflight,
        output=lambda _: None,
    )
    initialized = target.read_bytes()

    def reject_regeneration() -> dict[str, str]:
        pytest.fail("valid rerun must not regenerate credentials")

    monkeypatch.setattr(init_local_env_module, "_generated_values", reject_regeneration)
    second = initialize_local_environment(
        example_path=EXAMPLE_ENV_PATH,
        target_path=target,
        is_tracked=lambda _: False,
        preflight=full_preflight,
        output=lambda _: None,
    )

    assert first.changed is True
    assert second.changed is False
    assert target.read_bytes() == initialized
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert len(preflight_paths) == 2
    assert preflight_paths[0].parent == target.parent
    assert preflight_paths[0] != target
    assert preflight_paths[1] == target
    example = _assignments(EXAMPLE_ENV_PATH.read_text(encoding="utf-8"))
    actual = _assignments(initialized.decode())
    for name in set(example) - set(SECRET_VARIABLES):
        assert actual[name] == example[name]


def test_initializer_refuses_symlink_tracked_duplicate_and_unknown_placeholder(
    tmp_path: Path,
) -> None:
    regular = tmp_path / "regular.env"
    regular.write_bytes(EXAMPLE_ENV_PATH.read_bytes())
    symlink = tmp_path / "linked.env"
    symlink.symlink_to(regular)

    with pytest.raises(InitializationError, match="symlink"):
        initialize_local_environment(
            example_path=EXAMPLE_ENV_PATH,
            target_path=symlink,
            is_tracked=lambda _: False,
            output=lambda _: None,
        )
    with pytest.raises(InitializationError, match="tracked"):
        initialize_local_environment(
            example_path=EXAMPLE_ENV_PATH,
            target_path=regular,
            is_tracked=lambda _: True,
            output=lambda _: None,
        )

    duplicate = tmp_path / "duplicate.env"
    duplicate.write_text(
        EXAMPLE_ENV_PATH.read_text(encoding="utf-8") + "\nS3_SECRET_KEY=change-me\n",
        encoding="utf-8",
    )
    with pytest.raises(InitializationError, match="duplicate variable: S3_SECRET_KEY"):
        initialize_local_environment(
            example_path=EXAMPLE_ENV_PATH,
            target_path=duplicate,
            is_tracked=lambda _: False,
            output=lambda _: None,
        )

    unknown = tmp_path / "unknown.env"
    unknown.write_text(
        EXAMPLE_ENV_PATH.read_text(encoding="utf-8") + "\nEXTRA_TOKEN=change-me\n",
        encoding="utf-8",
    )
    with pytest.raises(InitializationError, match="unrecognized placeholder"):
        initialize_local_environment(
            example_path=EXAMPLE_ENV_PATH,
            target_path=unknown,
            is_tracked=lambda _: False,
            output=lambda _: None,
        )


def test_initializer_refuses_partial_rotation_of_non_placeholder_credentials(
    tmp_path: Path,
) -> None:
    target = tmp_path / ".env"
    target.write_text(
        EXAMPLE_ENV_PATH.read_text(encoding="utf-8").replace(
            "POSTGRES_PASSWORD=change-me", "POSTGRES_PASSWORD=existing-private-value"
        ),
        encoding="utf-8",
    )

    with pytest.raises(InitializationError, match="partially initialized") as error:
        initialize_local_environment(
            example_path=EXAMPLE_ENV_PATH,
            target_path=target,
            is_tracked=lambda _: False,
            output=lambda _: None,
        )

    assert "existing-private-value" not in str(error.value)
    assert target.read_text(encoding="utf-8").startswith("# Copy to .env")


def test_initializer_hands_initialized_values_to_existing_preflight(tmp_path: Path) -> None:
    target = tmp_path / ".env"
    observed: list[Path] = []

    def preflight(path: Path) -> None:
        values = _assignments(path.read_text(encoding="utf-8"))
        assert all("change-me" not in values[name] for name in SECRET_VARIABLES)
        observed.append(path)

    initialize_local_environment(
        example_path=EXAMPLE_ENV_PATH,
        target_path=target,
        is_tracked=lambda _: False,
        preflight=preflight,
        output=lambda _: None,
    )

    assert len(observed) == 1
    assert observed[0].parent == target.parent
    assert target.is_file()


def test_initializer_does_not_publish_invalid_full_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    example = tmp_path / ".env.example"
    target = tmp_path / ".env"
    invalid_content = _replace_setting(
        EXAMPLE_ENV_PATH.read_text(encoding="utf-8"),
        "HTTP_MAX_ATTEMPTS",
        "invalid-http-bound-sentinel",
    )
    example.write_text(invalid_content, encoding="utf-8")
    generated = {
        "POSTGRES_PASSWORD": "generated-postgres-sentinel",
        "S3_ACCESS_KEY": "generated-access-sentinel",
        "S3_SECRET_KEY": "generated-storage-sentinel",
        "GARAGE_RPC_SECRET": "0123456789abcdef" * 4,
    }
    monkeypatch.setattr(init_local_env_module, "_generated_values", lambda: generated)
    messages: list[str] = []

    with pytest.raises(InitializationError, match="HTTP_MAX_ATTEMPTS") as error:
        initialize_local_environment(
            example_path=example,
            target_path=target,
            is_tracked=lambda _: False,
            output=messages.append,
        )

    failure = str(error.value)
    assert not target.exists()
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []
    assert messages == []
    assert "invalid-http-bound-sentinel" not in failure
    for value in generated.values():
        assert value not in failure


def test_initializer_preserves_placeholder_target_when_full_preflight_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / ".env"
    original = _replace_setting(
        EXAMPLE_ENV_PATH.read_text(encoding="utf-8"),
        "FILE_MAX_RECORDS",
        "0",
    ).encode()
    target.write_bytes(original)
    generated = {
        "POSTGRES_PASSWORD": "generated-postgres-sentinel",
        "S3_ACCESS_KEY": "generated-access-sentinel",
        "S3_SECRET_KEY": "generated-storage-sentinel",
        "GARAGE_RPC_SECRET": "0123456789abcdef" * 4,
    }
    monkeypatch.setattr(init_local_env_module, "_generated_values", lambda: generated)
    messages: list[str] = []

    with pytest.raises(InitializationError, match="FILE_MAX_RECORDS") as error:
        initialize_local_environment(
            example_path=EXAMPLE_ENV_PATH,
            target_path=target,
            is_tracked=lambda _: False,
            output=messages.append,
        )

    failure = str(error.value)
    assert target.read_bytes() == original
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []
    assert messages == []
    for value in generated.values():
        assert value not in failure


def test_initializer_preserves_initialized_target_when_full_preflight_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / ".env"
    initialized, generated = _initialized_environment(
        _replace_setting(
            EXAMPLE_ENV_PATH.read_text(encoding="utf-8"),
            "SNAPSHOT_MAX_CHUNKS",
            "10000",
        )
    )
    original = initialized.encode()
    target.write_bytes(original)
    messages: list[str] = []

    def reject_regeneration() -> dict[str, str]:
        pytest.fail("initialized target must not regenerate credentials")

    monkeypatch.setattr(init_local_env_module, "_generated_values", reject_regeneration)
    with pytest.raises(InitializationError, match="SNAPSHOT_MAX_CHUNKS") as error:
        initialize_local_environment(
            example_path=EXAMPLE_ENV_PATH,
            target_path=target,
            is_tracked=lambda _: False,
            output=messages.append,
        )

    failure = str(error.value)
    assert target.read_bytes() == original
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []
    assert messages == []
    for value in generated.values():
        assert value not in failure


def test_demo_contracts_are_allow_listed_canonical_and_path_safe() -> None:
    assert tuple(item.value for item in RecoveryScenario) == RECOVERY_SCENARIOS
    assert len(RECOVERY_SCENARIOS) == 11
    assert RecoveryScenario.PAYLOAD_BEFORE_MANIFEST.value in RECOVERY_SCENARIOS
    assert DemoEventCode.STATE.value == "demo_state"
    assert DemoEventCode.SUMMARY.value == "demo_summary"
    with pytest.raises(ValueError, match="relative"):
        ArtifactRelativePath("/tmp/private/report.ndjson")
    with pytest.raises(ValueError, match="parent"):
        ArtifactRelativePath("artifacts/demo/../private.ndjson")

    state = DemoState(
        scenario="payload-before-manifest",
        checkpoint="intermediate",
        expected_state=ExpectedObjectState.PAYLOAD_ONLY,
        object_key="raw/api/fx-rates/example.json",
    )
    parsed = json.loads(state.to_json())
    assert parsed == {
        "checkpoint": "intermediate",
        "event": "demo_state",
        "expected_state": "payload_only",
        "object_key": "raw/api/fx-rates/example.json",
        "scenario": "payload-before-manifest",
    }
    assert state.to_json() == json.dumps(parsed, separators=(",", ":"), sort_keys=True)


def test_demo_summary_is_secret_free_and_deterministic() -> None:
    summary = DemoSummary(
        scenario="api-permanent-500",
        passed=False,
        duration_seconds=1.25,
        attempted_exit_class=DemoExitClass.EXPECTED_FAILURE,
        recovery_exit_class=None,
        object_counts=DemoObjectCounts(raw=1, manifest=0, landing=0, archive=0, rejected=0),
        report_path=ArtifactRelativePath("artifacts/demo/api-permanent-500/report.ndjson"),
        error_code=DemoErrorCode.NOT_IMPLEMENTED,
    )

    serialized = summary.to_json()
    assert serialized == json.dumps(summary.to_dict(), separators=(",", ":"), sort_keys=True)
    assert json.loads(serialized)["event"] == "demo_summary"
    forbidden_fields = {
        "password",
        "secret",
        "authorization",
        "dsn",
        "payload",
        "exception",
        "absolute_path",
    }
    assert forbidden_fields.isdisjoint(summary.to_dict())


def test_demo_cli_rejects_unknown_scenario_and_invalid_timeout_before_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[object] = []

    def record_execution(request: object) -> int:
        calls.append(request)
        return 0

    monkeypatch.setattr("scripts.demo.execute_demo", record_execution)

    unknown_result = demo_main(
        [
            "recovery",
            "--scenario",
            "arbitrary.module:callable",
            "--timeout-seconds",
            "10",
        ]
    )
    timeout_result = demo_main(["happy-path", "--timeout-seconds", "0"])
    project_result = demo_main(["happy-path", "--project-name", "../private"])

    assert unknown_result == 2
    assert timeout_result == 2
    assert project_result == 2
    assert calls == []
    captured = capsys.readouterr()
    assert "payload-before-manifest" in captured.err
    assert "positive" in captured.err
    assert "project name" in captured.err


class _FakeProcess:
    def __init__(self, outcomes: list[object], *, returncode: int = 0) -> None:
        self.pid = 4321
        self.returncode: int | None = returncode
        self._outcomes = outcomes
        self.communicate_timeouts: list[float | None] = []

    def communicate(
        self,
        input: str | None = None,
        timeout: float | None = None,
    ) -> tuple[str, str]:
        del input
        self.communicate_timeouts.append(timeout)
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, tuple)
        return outcome


@pytest.mark.parametrize(
    ("returncode", "expected_codes", "expected_class"),
    [
        (0, frozenset({0}), DemoExitClass.SUCCEEDED),
        (3, frozenset({3}), DemoExitClass.EXPECTED_FAILURE),
        (3, frozenset({0}), DemoExitClass.UNEXPECTED_FAILURE),
    ],
)
def test_supervisor_classifies_expected_and_unexpected_exits(
    returncode: int,
    expected_codes: frozenset[int],
    expected_class: DemoExitClass,
) -> None:
    process = _FakeProcess([("out", "err")], returncode=returncode)
    starts: list[tuple[object, dict[str, Any]]] = []

    def start(command: tuple[str, ...], **kwargs: Any) -> _FakeProcess:
        starts.append((command, kwargs))
        return process

    result = run_supervised(
        ("worker", "arg"),
        timeout_seconds=2.0,
        expected_exit_codes=expected_codes,
        popen_factory=start,
    )

    assert isinstance(result, SupervisedResult)
    assert result.exit_class is expected_class
    assert result.stdout == "out"
    assert result.stderr == "err"
    assert starts[0][1]["start_new_session"] is True
    assert process.communicate_timeouts == [2.0]


def test_supervisor_terminates_then_reaps_timed_out_process_group() -> None:
    timeout = subprocess.TimeoutExpired(("worker",), timeout=0.1, output="partial", stderr="wait")
    process = _FakeProcess([timeout, ("done", "stopped")], returncode=-15)
    signals: list[tuple[int, int]] = []

    result = run_supervised(
        ("worker",),
        timeout_seconds=0.1,
        expected_exit_codes=frozenset({0}),
        termination_grace_seconds=0.2,
        popen_factory=lambda command, **kwargs: process,
        signal_process_group=lambda pid, signal_number: signals.append((pid, signal_number)),
    )

    assert result.exit_class is DemoExitClass.TIMED_OUT
    assert result.returncode == -15
    assert signals == [(4321, 15)]
    assert process.communicate_timeouts == [0.1, 0.2]


def test_supervisor_escalates_to_kill_and_reaps_after_termination_grace() -> None:
    initial_timeout = subprocess.TimeoutExpired(("worker",), timeout=0.1)
    grace_timeout = subprocess.TimeoutExpired(("worker",), timeout=0.2)
    process = _FakeProcess(
        [initial_timeout, grace_timeout, ("", "killed")],
        returncode=-9,
    )
    signals: list[tuple[int, int]] = []

    result = run_supervised(
        ("worker",),
        timeout_seconds=0.1,
        termination_grace_seconds=0.2,
        popen_factory=lambda command, **kwargs: process,
        signal_process_group=lambda pid, signal_number: signals.append((pid, signal_number)),
    )

    assert result.exit_class is DemoExitClass.TIMED_OUT
    assert result.returncode == -9
    assert signals == [(4321, 15), (4321, 9)]
    assert process.communicate_timeouts == [0.1, 0.2, 0.2]


def test_supervisor_rejects_non_positive_timeout_before_start() -> None:
    started = False

    def start(command: tuple[str, ...], **kwargs: Any) -> _FakeProcess:
        nonlocal started
        started = True
        return _FakeProcess([("", "")])

    with pytest.raises(ValueError, match="positive"):
        run_supervised(("worker",), timeout_seconds=0, popen_factory=start)

    assert started is False


def test_demo_modules_have_no_import_time_file_or_process_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    before = set(tmp_path.iterdir())

    importlib.reload(sys.modules["scripts.demo"])
    importlib.reload(scripts.demo_faults)
    importlib.reload(scripts.demo_worker)
    importlib.reload(sys.modules["scripts.init_local_env"])

    assert set(tmp_path.iterdir()) == before
    assert os.environ.get("DEMO_FAULT") is None


class _Body(io.BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.closed_by_client = False

    def close(self) -> None:
        self.closed_by_client = True
        super().close()


class _InventoryClient:
    def __init__(self, objects: dict[str, bytes], *, misleading_etag: str = '"not-a-sha"') -> None:
        self.objects = objects
        self.misleading_etag = misleading_etag
        self.bodies: list[_Body] = []

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        prefix = str(kwargs["Prefix"])
        keys = sorted(key for key in self.objects if key.startswith(prefix))
        return {
            "IsTruncated": False,
            "Contents": [
                {"Key": key, "Size": len(self.objects[key]), "ETag": self.misleading_etag}
                for key in keys
            ],
        }

    def get_object(self, **kwargs: object) -> dict[str, object]:
        key = str(kwargs["Key"])
        body = _Body(self.objects[key])
        self.bodies.append(body)
        return {
            "Body": body,
            "ContentLength": len(self.objects[key]),
            "ETag": self.misleading_etag,
        }


def _api_record_and_objects() -> tuple[DemoBatchRecord, dict[str, bytes]]:
    identity = BatchIdentity.create(
        source_name="fx-rates",
        source_type=SourceType.API,
        source_date=date(2026, 9, 1),
        parameters={"page-size": "25", "resource-path": "/v1/fx-rates"},
    )
    batch_id = derive_batch_id(identity)
    raw_key = raw_object_key(identity, "page-0001.json")
    manifest_key = manifest_key_for(raw_key)
    payload = b'{"bounded":"evidence"}'
    manifest = Manifest(
        source_name="fx-rates",
        source_type=SourceType.API,
        source_date=date(2026, 9, 1),
        batch_id=batch_id,
        run_id="run_80808080808080808080808080808080",
        object_key=raw_key,
        original_file_name=None,
        content_type="application/json",
        content_length=len(payload),
        checksum_sha256=sha256(payload).hexdigest(),
        schema_version=1,
        extracted_at=datetime(2026, 9, 1, 12, tzinfo=UTC),
        status=ManifestStatus.SUCCEEDED,
        attempt=1,
    )
    record = DemoBatchRecord(
        scenario="happy-path",
        source_name="fx-rates",
        source_type=SourceType.API,
        source_date="2026-09-01",
        batch_id=batch_id,
        payload_keys=(raw_key,),
        expected_status=ManifestStatus.SUCCEEDED,
    )
    manifest_bytes = manifest.to_json().encode()
    aggregate = ApiLogicalRunManifest(
        manifest_type=API_LOGICAL_RUN_MANIFEST_TYPE,
        schema_version=1,
        source_name=identity.source_name,
        source_type=identity.source_type,
        source_date=identity.source_date,
        batch_id=batch_id,
        identity_parameters=dict(identity.parameters),
        completion_run_id="run_80808080808080808080808080808080",
        completed_at=datetime(2026, 9, 1, 12, 1, tzinfo=UTC),
        status=API_LOGICAL_RUN_MANIFEST_STATUS,
        page_count=1,
        record_count=1,
        pages=(
            ApiLogicalRunPage(
                page_number=1,
                object_key=raw_key,
                page_manifest_key=manifest_key,
                payload_content_length=len(payload),
                payload_checksum_sha256=sha256(payload).hexdigest(),
                page_manifest_checksum_sha256=sha256(manifest_bytes).hexdigest(),
                record_count=1,
            ),
        ),
    )
    return record, {
        raw_key: payload,
        manifest_key: manifest_bytes,
        api_logical_run_manifest_key(identity): aggregate.to_bytes(),
    }


def test_production_adapter_delegates_to_all_three_existing_cli_paths(tmp_path: Path) -> None:
    calls: list[tuple[tuple[str, ...], frozenset[int]]] = []

    def record(
        command: tuple[str, ...],
        *,
        timeout_seconds: float,
        expected_exit_codes: frozenset[int],
        cwd: Path | None,
        env: object,
    ) -> SupervisedResult:
        assert timeout_seconds == 12.0
        assert cwd == tmp_path
        assert env == {"SAFE": "value"}
        calls.append((command, expected_exit_codes))
        return SupervisedResult(DemoExitClass.SUCCEEDED, 0, "{}\n", "")

    adapter = ProductionDemoAdapter(
        repository_root=tmp_path,
        environ={"SAFE": "value"},
        remaining_seconds=lambda: 12.0,
        supervisor=record,
    )
    adapter.run_api(source="fx-rates", source_date="2026-09-01")
    adapter.run_file(
        source="supplier-prices",
        source_date="2026-09-01",
        path=Path("fixture.csv"),
        expected_exit_codes=frozenset({4}),
    )
    adapter.run_snapshot(source="orders", source_date="2026-09-01")

    assert [call[0][1:3] for call in calls] == [
        ("-m", "batch_ingestion.cli"),
        ("-m", "batch_ingestion.files.cli"),
        ("-m", "batch_ingestion.postgres_snapshot.cli"),
    ]
    assert calls[0][1] == frozenset({0})
    assert calls[1][1] == frozenset({4})
    assert calls[2][1] == frozenset({0})


def test_inventory_verifier_streams_exact_payload_and_ignores_etag() -> None:
    record, objects = _api_record_and_objects()
    client = _InventoryClient(objects, misleading_etag='"definitely-not-the-checksum"')

    evidence = DemoInventoryVerifier(
        client=client,
        bucket="demo-bucket",
        maximum_object_bytes=1_000_000,
        chunk_size=5,
    ).verify((record,))

    assert set(evidence.objects) == set(objects)
    assert evidence.raw_count == 1
    assert evidence.manifest_count == 2
    assert (
        evidence.objects[record.payload_keys[0]].checksum_sha256
        == sha256(objects[record.payload_keys[0]]).hexdigest()
    )
    assert all(body.closed_by_client for body in client.bodies)


def test_inventory_verifier_requires_and_validates_api_logical_run_manifest() -> None:
    record, objects = _api_record_and_objects()
    aggregate_key = next(key for key in objects if key.endswith("logical-run.manifest.json"))

    missing = dict(objects)
    del missing[aggregate_key]
    with pytest.raises(DemoInventoryError, match="exact inventory"):
        DemoInventoryVerifier(
            client=_InventoryClient(missing),
            bucket="demo-bucket",
            maximum_object_bytes=1_000_000,
        ).verify((record,))

    aggregate = ApiLogicalRunManifest.from_bytes(objects[aggregate_key])
    conflicting_page = replace(
        aggregate.pages[0],
        page_manifest_checksum_sha256="f" * 64,
    )
    conflicting = replace(aggregate, pages=(conflicting_page,)).to_bytes()
    with pytest.raises(DemoInventoryError, match="logical-run manifest evidence mismatch"):
        DemoInventoryVerifier(
            client=_InventoryClient({**objects, aggregate_key: conflicting}),
            bucket="demo-bucket",
            maximum_object_bytes=1_000_000,
        ).verify((record,))


def test_inventory_verifier_rejects_unexpected_key_and_manifest_byte_change() -> None:
    record, objects = _api_record_and_objects()
    unexpected = record.payload_keys[0].rsplit("/", 1)[0] + "/page-0002.json"
    objects_with_extra = {**objects, unexpected: b"unexpected"}

    with pytest.raises(DemoInventoryError, match="exact inventory"):
        DemoInventoryVerifier(
            client=_InventoryClient(objects_with_extra),
            bucket="demo-bucket",
            maximum_object_bytes=1_000_000,
        ).verify((record,))

    verifier = DemoInventoryVerifier(
        client=_InventoryClient(objects),
        bucket="demo-bucket",
        maximum_object_bytes=1_000_000,
    )
    before = verifier.verify((record,))
    changed = dict(objects)
    manifest_key = manifest_key_for(record.payload_keys[0])
    page_manifest = Manifest.from_json(changed[manifest_key].decode())
    changed[manifest_key] = (
        replace(
            page_manifest,
            run_id="run_81818181818181818181818181818181",
        )
        .to_json()
        .encode()
    )
    aggregate_key = next(key for key in changed if key.endswith("logical-run.manifest.json"))
    aggregate = ApiLogicalRunManifest.from_bytes(changed[aggregate_key])
    changed_page = replace(
        aggregate.pages[0],
        page_manifest_checksum_sha256=sha256(changed[manifest_key]).hexdigest(),
    )
    changed[aggregate_key] = replace(aggregate, pages=(changed_page,)).to_bytes()
    after = DemoInventoryVerifier(
        client=_InventoryClient(changed),
        bucket="demo-bucket",
        maximum_object_bytes=1_000_000,
    ).verify((record,))

    with pytest.raises(DemoInventoryError, match="inventory changed"):
        before.require_unchanged(after)


class _BoundaryCrash(RuntimeError):
    pass


class _BoundaryStorage:
    def __init__(self) -> None:
        self.writes: list[str] = []

    def reconcile_payload_manifest(self, *args: object, **kwargs: object) -> object:
        return (args, kwargs)

    def write_immutable(self, key: str, stream: object, **kwargs: object) -> str:
        self.writes.append(key)
        return key


def _boundary_keys() -> tuple[str, str]:
    identity = BatchIdentity.create(
        source_name="fx-rates",
        source_type=SourceType.API,
        source_date=date(2026, 9, 5),
        parameters={"page-size": "2"},
    )
    payload_key = raw_object_key(identity, "page-0001.json")
    return payload_key, manifest_key_for(payload_key)


@pytest.mark.parametrize(
    ("after_manifest", "expected_writes"),
    [(False, 1), (True, 2)],
)
def test_manifest_failpoints_require_verified_raw_and_fire_once_at_selected_boundary(
    after_manifest: bool,
    expected_writes: int,
) -> None:
    storage = _BoundaryStorage()
    crashes: list[tuple[str, str]] = []

    def crash(boundary: str, key: str) -> None:
        crashes.append((boundary, key))
        raise _BoundaryCrash

    wrapped = ManifestBoundaryStorage(
        storage,
        after_manifest=after_manifest,
        crash=crash,
    )
    payload_key, manifest_key = _boundary_keys()
    wrapped.write_immutable("manifests/unrelated.json", io.BytesIO(), content_length=0)
    wrapped.write_immutable(payload_key, io.BytesIO(b"raw"), content_length=3)
    with pytest.raises(_BoundaryCrash):
        wrapped.write_immutable(manifest_key, io.BytesIO(b"manifest"), content_length=8)

    assert crashes == [
        (
            "api_page_complete" if after_manifest else "payload_before_manifest",
            payload_key,
        )
    ]
    assert len(storage.writes) == expected_writes + 1  # includes unrelated manifest


class _BoundaryClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def copy_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(("copy", str(kwargs["Key"])))
        return {}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(("get", str(kwargs["Key"])))
        return {}

    def delete_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(("delete", str(kwargs["Key"])))
        return {}


def test_file_delete_failpoint_requires_selected_copy_and_destination_read() -> None:
    client = _BoundaryClient()
    crashes: list[tuple[str, str]] = []

    def crash(boundary: str, key: str) -> None:
        crashes.append((boundary, key))
        raise _BoundaryCrash

    wrapped = DeleteBoundaryClient(
        client,
        source_key="landing/file/source.csv",
        destination_key="archive/file/payload.csv",
        crash=crash,
    )
    wrapped.delete_object(Key="landing/file/unrelated.csv")
    wrapped.copy_object(
        Key="archive/file/payload.csv",
        CopySource={"Key": "landing/file/source.csv"},
    )
    wrapped.get_object(Key="archive/file/payload.csv")
    with pytest.raises(_BoundaryCrash):
        wrapped.delete_object(Key="landing/file/source.csv")

    assert crashes == [
        (
            "file_destination_verified_before_landing_delete",
            "landing/file/source.csv",
        )
    ]
    assert ("delete", "landing/file/source.csv") not in client.calls


def test_demo_batch_record_is_strict_round_trip_and_path_safe() -> None:
    record, _ = _api_record_and_objects()
    assert DemoBatchRecord.from_dict(record.to_dict()) == record

    invalid = record.to_dict()
    invalid["payload_keys"] = ["../private"]
    with pytest.raises(ValueError, match="safe relative"):
        DemoBatchRecord.from_dict(invalid)
