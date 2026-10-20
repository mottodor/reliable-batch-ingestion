"""Contract tests for bounded local-infrastructure lifecycle commands."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from scripts.check_local_env import main as preflight_main
from scripts.check_local_env import read_env, validate_environment

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


@pytest.fixture(scope="module")
def valid_local_values(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    env_path = tmp_path_factory.mktemp("direct-preflight") / ".env"
    env_path.write_text(VALID_LOCAL_ENV, encoding="utf-8")

    values, errors = read_env(env_path)

    assert errors == []
    return values


def _assert_direct_validation_failure(
    values: dict[str, str],
    *,
    expected_name: str,
    forbidden_values: tuple[str, ...] = SECRET_SENTINELS,
) -> None:
    errors = validate_environment(values)
    diagnostics = "\n".join(errors)

    assert errors
    assert expected_name in diagnostics
    for value in forbidden_values:
        assert value not in diagnostics


def test_read_env_parses_complete_local_configuration(
    valid_local_values: dict[str, str],
) -> None:
    assert valid_local_values["POSTGRES_PASSWORD"] == POSTGRES_PASSWORD
    assert valid_local_values["S3_ACCESS_KEY"] == S3_ACCESS_KEY
    assert valid_local_values["S3_SECRET_KEY"] == S3_SECRET_KEY
    assert valid_local_values["GARAGE_RPC_SECRET"] == GARAGE_RPC_SECRET


def test_read_env_reports_missing_path_without_values(tmp_path: Path) -> None:
    env_path = tmp_path / "missing.env"

    values, errors = read_env(env_path)

    assert values == {}
    assert errors == [f"configuration file is missing: {env_path}"]


def test_read_env_redacts_malformed_and_duplicate_assignments(tmp_path: Path) -> None:
    malformed_value = "malformed-value-sentinel"
    first_secret = "first-secret-sentinel"
    second_secret = "second-secret-sentinel"
    env_path = tmp_path / ".env"
    env_path.write_text(
        f"{malformed_value}\nS3_SECRET_KEY={first_secret}\nS3_SECRET_KEY={second_secret}\n",
        encoding="utf-8",
    )

    values, errors = read_env(env_path)
    diagnostics = "\n".join(errors)

    assert values == {"S3_SECRET_KEY": first_secret}
    assert errors == [
        "invalid assignment at line 1",
        "duplicate variable: S3_SECRET_KEY",
    ]
    for forbidden_value in (malformed_value, first_secret, second_secret):
        assert forbidden_value not in diagnostics


def test_preflight_main_accepts_complete_local_configuration_directly(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(VALID_LOCAL_ENV, encoding="utf-8")

    exit_code = preflight_main([str(env_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == f"Local environment preflight: PASS ({env_path})\n"
    assert captured.err == ""
    for value in SECRET_SENTINELS:
        assert value not in captured.out


def test_preflight_main_rejects_invalid_configuration_directly(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    invalid_secret = "invalid-rpc-secret-sentinel"
    env_path = tmp_path / ".env"
    env_path.write_text(
        _replace_setting(VALID_LOCAL_ENV, "GARAGE_RPC_SECRET", invalid_secret),
        encoding="utf-8",
    )

    exit_code = preflight_main([str(env_path)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert captured.err.startswith("Local environment preflight: FAIL\n")
    assert "GARAGE_RPC_SECRET" in captured.err
    for value in (*SECRET_SENTINELS, invalid_secret):
        assert value not in captured.err


def test_preflight_main_rejects_extra_arguments_directly(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = preflight_main(["first.env", "second.env"])
    captured = capsys.readouterr()

    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "usage: check_local_env.py [ENV_FILE]\n"


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
def test_validate_environment_rejects_omitted_application_setting_directly(
    valid_local_values: dict[str, str],
    name: str,
) -> None:
    values = valid_local_values.copy()
    del values[name]

    _assert_direct_validation_failure(values, expected_name=name)


def test_validate_environment_rejects_missing_s3_retry_budget_directly(
    valid_local_values: dict[str, str],
) -> None:
    values = valid_local_values.copy()
    del values["S3_TOTAL_MAX_ATTEMPTS"]

    _assert_direct_validation_failure(
        values,
        expected_name="S3_TOTAL_MAX_ATTEMPTS",
    )


@pytest.mark.parametrize("name", ["S3_PORT", "GARAGE_ADMIN_PORT", "MOCK_API_PORT"])
@pytest.mark.parametrize("value", [None, "0", "invalid-port", "65536"])
def test_validate_environment_rejects_invalid_local_port_directly(
    valid_local_values: dict[str, str],
    name: str,
    value: str | None,
) -> None:
    values = valid_local_values.copy()
    if value is None:
        del values[name]
    else:
        values[name] = value

    forbidden_values = SECRET_SENTINELS if value is None else (*SECRET_SENTINELS, value)
    _assert_direct_validation_failure(
        values,
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
        ("S3_ADDRESSING_STYLE", "invalid-addressing"),
        ("LOG_LEVEL", "verbose"),
        ("HTTP_PAGE_SIZE", "101"),
        ("HTTP_MAX_PAGES", "10000"),
        ("SNAPSHOT_MAX_CHUNKS", "10000"),
    ],
)
def test_validate_environment_rejects_invalid_application_value_directly(
    valid_local_values: dict[str, str],
    name: str,
    value: str,
) -> None:
    values = valid_local_values.copy()
    values[name] = value

    _assert_direct_validation_failure(
        values,
        expected_name=name,
        forbidden_values=(*SECRET_SENTINELS, value),
    )


@pytest.mark.parametrize("name", ["S3_ENDPOINT_URL", "MOCK_API_BASE_URL"])
def test_validate_environment_rejects_credential_url_directly(
    valid_local_values: dict[str, str],
    name: str,
) -> None:
    credential_bearing_url = "http://url-user:url-sentinel-secret@localhost:3900"
    values = valid_local_values.copy()
    values[name] = credential_bearing_url

    _assert_direct_validation_failure(
        values,
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
def test_validate_environment_rejects_cross_field_violation_directly(
    valid_local_values: dict[str, str],
    overrides: dict[str, str],
    expected_name: str,
) -> None:
    values = valid_local_values.copy()
    values.update(overrides)

    _assert_direct_validation_failure(values, expected_name=expected_name)


@pytest.mark.parametrize(
    ("name", "placeholder"),
    [
        ("POSTGRES_PASSWORD", "change-me"),
        ("S3_ACCESS_KEY", "change-me"),
        ("S3_SECRET_KEY", "change-me"),
        ("GARAGE_RPC_SECRET", "change-me-64-character-hex-value"),
    ],
)
def test_validate_environment_rejects_placeholder_directly(
    valid_local_values: dict[str, str],
    name: str,
    placeholder: str,
) -> None:
    values = valid_local_values.copy()
    values[name] = placeholder

    errors = validate_environment(values)
    diagnostics = "\n".join(errors)

    assert errors
    assert diagnostics.count(name) == 1
    for value in (*SECRET_SENTINELS, placeholder):
        assert value not in diagnostics


def test_validate_environment_rejects_invalid_rpc_secret_directly(
    valid_local_values: dict[str, str],
) -> None:
    invalid_secret = "not-hex"
    values = valid_local_values.copy()
    values["GARAGE_RPC_SECRET"] = invalid_secret

    _assert_direct_validation_failure(
        values,
        expected_name="GARAGE_RPC_SECRET",
        forbidden_values=(*SECRET_SENTINELS, invalid_secret),
    )


def test_preflight_script_accepts_complete_local_configuration_smoke(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(VALID_LOCAL_ENV, encoding="utf-8")

    result = _run_preflight(env_path)

    assert result.returncode == 0
    assert result.stdout == f"Local environment preflight: PASS ({env_path})\n"
    assert result.stderr == ""
    for value in SECRET_SENTINELS:
        assert value not in result.stdout


def test_preflight_script_rejects_invalid_configuration_smoke(tmp_path: Path) -> None:
    invalid_secret = "invalid-rpc-secret-sentinel"
    env_path = tmp_path / ".env"
    env_path.write_text(
        _replace_setting(VALID_LOCAL_ENV, "GARAGE_RPC_SECRET", invalid_secret),
        encoding="utf-8",
    )

    result = _run_preflight(env_path)

    _assert_redacted_failure(
        result,
        expected_name="GARAGE_RPC_SECRET",
        forbidden_values=(*SECRET_SENTINELS, invalid_secret),
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
