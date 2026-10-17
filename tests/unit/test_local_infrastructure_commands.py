"""Contract tests for bounded local-infrastructure lifecycle commands."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PREFLIGHT_SCRIPT = REPOSITORY_ROOT / "scripts" / "check_local_env.py"
MAKEFILE_PATH = REPOSITORY_ROOT / "Makefile"
EXAMPLE_ENV_PATH = REPOSITORY_ROOT / ".env.example"

POSTGRES_PASSWORD = "postgres-sentinel-secret"
S3_ACCESS_KEY = "access-sentinel-secret"
S3_SECRET_KEY = "storage-sentinel-secret"
GARAGE_RPC_SECRET = "0123456789abcdef" * 4
SECRET_SENTINELS = (
    POSTGRES_PASSWORD,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    GARAGE_RPC_SECRET,
)
VALID_LOCAL_ENV = (
    EXAMPLE_ENV_PATH.read_text(encoding="utf-8")
    .replace("POSTGRES_PASSWORD=change-me", f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}")
    .replace("S3_ACCESS_KEY=change-me", f"S3_ACCESS_KEY={S3_ACCESS_KEY}")
    .replace("S3_SECRET_KEY=change-me", f"S3_SECRET_KEY={S3_SECRET_KEY}")
    .replace(
        "GARAGE_RPC_SECRET=change-me-64-character-hex-value",
        f"GARAGE_RPC_SECRET={GARAGE_RPC_SECRET}",
    )
)


def _run_preflight(env_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(PREFLIGHT_SCRIPT), str(env_path)],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _replace_setting(content: str, name: str, value: str) -> str:
    lines = content.splitlines(keepends=True)
    prefix = f"{name}="
    matches = [index for index, line in enumerate(lines) if line.startswith(prefix)]
    assert len(matches) == 1
    index = matches[0]
    newline = "\n" if lines[index].endswith("\n") else ""
    lines[index] = f"{name}={value}{newline}"
    return "".join(lines)


def _omit_setting(content: str, name: str) -> str:
    prefix = f"{name}="
    lines = [line for line in content.splitlines(keepends=True) if not line.startswith(prefix)]
    assert len(lines) == len(content.splitlines(keepends=True)) - 1
    return "".join(lines)


def _assert_redacted_failure(
    result: subprocess.CompletedProcess[str],
    *,
    expected_name: str,
    forbidden_values: tuple[str, ...] = SECRET_SENTINELS,
) -> None:
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("Local environment preflight: FAIL\n")
    assert expected_name in result.stderr
    for value in forbidden_values:
        assert value not in output


def test_preflight_accepts_complete_local_configuration(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(VALID_LOCAL_ENV, encoding="utf-8")

    result = _run_preflight(env_path)

    assert result.returncode == 0
    assert result.stdout.strip() == f"Local environment preflight: PASS ({env_path})"
    assert result.stderr == ""
    for value in SECRET_SENTINELS:
        assert value not in result.stdout


@pytest.mark.parametrize(
    "name",
    [
        "POSTGRES_HOST",
        "S3_REGION",
        "HTTP_MAX_ATTEMPTS",
        "FILE_MAX_BYTES",
        "SNAPSHOT_CHUNK_ROWS",
        "LOG_LEVEL",
    ],
)
def test_preflight_rejects_omitted_application_setting(
    tmp_path: Path,
    name: str,
) -> None:
    env_path = tmp_path / f"missing-{name.lower()}.env"
    env_path.write_text(_omit_setting(VALID_LOCAL_ENV, name), encoding="utf-8")

    result = _run_preflight(env_path)

    _assert_redacted_failure(result, expected_name=name)


def test_preflight_rejects_missing_s3_retry_budget(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        _omit_setting(VALID_LOCAL_ENV, "S3_TOTAL_MAX_ATTEMPTS"),
        encoding="utf-8",
    )

    result = _run_preflight(env_path)

    _assert_redacted_failure(result, expected_name="S3_TOTAL_MAX_ATTEMPTS")


@pytest.mark.parametrize("name", ["S3_PORT", "GARAGE_ADMIN_PORT", "MOCK_API_PORT"])
@pytest.mark.parametrize("value", [None, "0", "invalid-port", "65536"])
def test_preflight_rejects_missing_or_invalid_local_port(
    tmp_path: Path,
    name: str,
    value: str | None,
) -> None:
    env_path = tmp_path / f"local-port-{name.lower()}-{value}.env"
    content = (
        _omit_setting(VALID_LOCAL_ENV, name)
        if value is None
        else _replace_setting(VALID_LOCAL_ENV, name, value)
    )
    env_path.write_text(content, encoding="utf-8")

    result = _run_preflight(env_path)

    forbidden_values = SECRET_SENTINELS if value is None else (*SECRET_SENTINELS, value)
    _assert_redacted_failure(
        result,
        expected_name=name,
        forbidden_values=forbidden_values,
    )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("S3_TOTAL_MAX_ATTEMPTS", "0"),
        ("S3_CONNECT_TIMEOUT_SECONDS", "nan"),
        ("S3_READ_TIMEOUT_SECONDS", "-1"),
        ("POSTGRES_CONNECT_TIMEOUT_SECONDS", "inf"),
        ("HTTP_MAX_ATTEMPTS", "0"),
        ("FILE_MAX_RECORDS", "invalid-bound"),
        ("SNAPSHOT_MAX_ELAPSED_SECONDS", "nan"),
    ],
)
def test_preflight_rejects_invalid_application_bounds(
    tmp_path: Path,
    name: str,
    value: str,
) -> None:
    env_path = tmp_path / f"invalid-{name.lower()}.env"
    env_path.write_text(_replace_setting(VALID_LOCAL_ENV, name, value), encoding="utf-8")

    result = _run_preflight(env_path)

    _assert_redacted_failure(
        result,
        expected_name=name,
        forbidden_values=(*SECRET_SENTINELS, value),
    )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("S3_ADDRESSING_STYLE", "invalid-addressing"),
        ("LOG_LEVEL", "verbose"),
        ("HTTP_PAGE_SIZE", "101"),
        ("HTTP_MAX_PAGES", "10000"),
        ("SNAPSHOT_MAX_CHUNKS", "10000"),
    ],
)
def test_preflight_rejects_invalid_enumerations_and_protocol_maxima(
    tmp_path: Path,
    name: str,
    value: str,
) -> None:
    env_path = tmp_path / f"invalid-{name.lower()}.env"
    env_path.write_text(_replace_setting(VALID_LOCAL_ENV, name, value), encoding="utf-8")

    result = _run_preflight(env_path)

    _assert_redacted_failure(
        result,
        expected_name=name,
        forbidden_values=(*SECRET_SENTINELS, value),
    )


@pytest.mark.parametrize("name", ["S3_ENDPOINT_URL", "MOCK_API_BASE_URL"])
def test_preflight_rejects_credential_bearing_urls_without_disclosure(
    tmp_path: Path,
    name: str,
) -> None:
    credential_bearing_url = "http://url-user:url-sentinel-secret@localhost:3900"
    env_path = tmp_path / f"credential-url-{name.lower()}.env"
    env_path.write_text(
        _replace_setting(VALID_LOCAL_ENV, name, credential_bearing_url),
        encoding="utf-8",
    )

    result = _run_preflight(env_path)

    _assert_redacted_failure(
        result,
        expected_name=name,
        forbidden_values=(*SECRET_SENTINELS, credential_bearing_url, "url-sentinel-secret"),
    )


@pytest.mark.parametrize(
    ("overrides", "expected_name"),
    [
        (
            {"FILE_MAX_BYTES": "1024", "FILE_SPOOL_THRESHOLD_BYTES": "1025"},
            "FILE_SPOOL_THRESHOLD_BYTES",
        ),
        (
            {
                "SNAPSHOT_MAX_CHUNK_BYTES": "1024",
                "SNAPSHOT_SPOOL_THRESHOLD_BYTES": "1025",
            },
            "SNAPSHOT_SPOOL_THRESHOLD_BYTES",
        ),
    ],
)
def test_preflight_rejects_application_cross_field_violations(
    tmp_path: Path,
    overrides: dict[str, str],
    expected_name: str,
) -> None:
    content = VALID_LOCAL_ENV
    for name, value in overrides.items():
        content = _replace_setting(content, name, value)
    env_path = tmp_path / f"cross-field-{expected_name.lower()}.env"
    env_path.write_text(content, encoding="utf-8")

    result = _run_preflight(env_path)

    _assert_redacted_failure(result, expected_name=expected_name)


@pytest.mark.parametrize(
    ("name", "placeholder"),
    [
        ("POSTGRES_PASSWORD", "change-me"),
        ("S3_ACCESS_KEY", "change-me"),
        ("S3_SECRET_KEY", "change-me"),
        ("GARAGE_RPC_SECRET", "change-me-64-character-hex-value"),
    ],
)
def test_preflight_rejects_placeholders_without_printing_values(
    tmp_path: Path,
    name: str,
    placeholder: str,
) -> None:
    env_path = tmp_path / f"placeholder-{name.lower()}.env"
    env_path.write_text(
        _replace_setting(VALID_LOCAL_ENV, name, placeholder),
        encoding="utf-8",
    )

    result = _run_preflight(env_path)

    _assert_redacted_failure(
        result,
        expected_name=name,
        forbidden_values=(*SECRET_SENTINELS, placeholder),
    )
    assert result.stderr.count(name) == 1


def test_preflight_rejects_invalid_rpc_secret(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        _replace_setting(VALID_LOCAL_ENV, "GARAGE_RPC_SECRET", "not-hex"),
        encoding="utf-8",
    )

    result = _run_preflight(env_path)

    _assert_redacted_failure(
        result,
        expected_name="GARAGE_RPC_SECRET",
        forbidden_values=(*SECRET_SENTINELS, "not-hex"),
    )


def test_make_targets_use_preflight_and_bounded_compose_wait() -> None:
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")

    assert "COMPOSE_PROJECT_NAME ?= reliable-batch-ingestion" in makefile
    assert "COMPOSE_WAIT_TIMEOUT_SECONDS ?= 60" in makefile
    assert "check-local-env:" in makefile
    assert "up: check-local-env" in makefile
    assert "--project-name $(COMPOSE_PROJECT_NAME)" in makefile
    assert "--detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT_SECONDS)" in makefile
    assert "seed:" in makefile
    assert "$(MAKE) up" in makefile
    assert "integration: seed" in makefile
    assert "BI020_MARKER_MODE=create" in makefile
    assert "BI020_MARKER_MODE=verify" in makefile
    assert "scripts/check_infrastructure_recovery.py" in makefile


def _write_compose_recorder(tmp_path: Path) -> Path:
    recorder = tmp_path / "record_compose.py"
    recorder.write_text(
        """\
import os
from pathlib import Path
import sys

Path(os.environ["COMPOSE_RECORD_PATH"]).write_text("\\n".join(sys.argv[1:]) + "\\n")
""",
        encoding="utf-8",
    )
    return recorder


def _run_reset_target(
    tmp_path: Path,
    target: str,
    *,
    confirm: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    env_path = tmp_path / ".env"
    env_path.write_text(VALID_LOCAL_ENV, encoding="utf-8")
    record_path = tmp_path / "compose-arguments.txt"
    recorder = _write_compose_recorder(tmp_path)
    environment = os.environ.copy()
    environment["COMPOSE_RECORD_PATH"] = str(record_path)
    command = [
        "make",
        target,
        f"COMPOSE={sys.executable} {recorder}",
        f"COMPOSE_ENV_FILE={env_path}",
        "COMPOSE_PROJECT_NAME=bi020-reset-unit-test",
    ]
    if confirm is not None:
        command.append(f"CONFIRM={confirm}")
    result = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    return result, record_path


def test_reset_warns_fails_and_never_invokes_compose(tmp_path: Path) -> None:
    result, record_path = _run_reset_target(tmp_path, "reset")

    assert result.returncode != 0
    assert "destructive" in result.stderr.lower()
    assert not record_path.exists()


def test_destructive_reset_rejects_missing_or_wrong_confirmation(tmp_path: Path) -> None:
    missing_result, missing_record = _run_reset_target(tmp_path / "missing", "reset-destructive")
    wrong_result, wrong_record = _run_reset_target(
        tmp_path / "wrong",
        "reset-destructive",
        confirm="wrong",
    )

    assert missing_result.returncode != 0
    assert wrong_result.returncode != 0
    assert "CONFIRM=destroy-local-data" in missing_result.stderr
    assert "CONFIRM=destroy-local-data" in wrong_result.stderr
    assert not missing_record.exists()
    assert not wrong_record.exists()


def test_confirmed_destructive_reset_is_project_scoped(tmp_path: Path) -> None:
    result, record_path = _run_reset_target(
        tmp_path,
        "reset-destructive",
        confirm="destroy-local-data",
    )

    assert result.returncode == 0
    assert record_path.read_text(encoding="utf-8").splitlines() == [
        "--project-name",
        "bi020-reset-unit-test",
        "--env-file",
        str(tmp_path / ".env"),
        "down",
        "--volumes",
        "--remove-orphans",
    ]
