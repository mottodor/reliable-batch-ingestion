"""Contract tests for bounded local-infrastructure lifecycle commands."""

import os
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PREFLIGHT_SCRIPT = REPOSITORY_ROOT / "scripts" / "check_local_env.py"
MAKEFILE_PATH = REPOSITORY_ROOT / "Makefile"

VALID_INFRASTRUCTURE_ENV = """\
POSTGRES_PORT=5432
POSTGRES_DB=batch_lab
POSTGRES_USER=batch_lab
POSTGRES_PASSWORD=local-password
S3_PORT=3900
S3_BUCKET=batch-lab
S3_ACCESS_KEY=local-access-key
S3_SECRET_KEY=local-secret-key
GARAGE_ADMIN_PORT=3903
GARAGE_RPC_SECRET=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
"""


def _run_preflight(env_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(PREFLIGHT_SCRIPT), str(env_path)],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_preflight_accepts_complete_local_configuration(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(VALID_INFRASTRUCTURE_ENV, encoding="utf-8")

    result = _run_preflight(env_path)

    assert result.returncode == 0
    assert result.stdout.strip() == f"Local environment preflight: PASS ({env_path})"
    assert result.stderr == ""


def test_preflight_rejects_placeholders_without_printing_values(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        VALID_INFRASTRUCTURE_ENV.replace("local-secret-key", "change-me"),
        encoding="utf-8",
    )

    result = _run_preflight(env_path)

    assert result.returncode == 1
    assert "S3_SECRET_KEY" in result.stderr
    assert "change-me" not in result.stderr


def test_preflight_rejects_invalid_rpc_secret(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        VALID_INFRASTRUCTURE_ENV.replace(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "not-hex",
        ),
        encoding="utf-8",
    )

    result = _run_preflight(env_path)

    assert result.returncode == 1
    assert "GARAGE_RPC_SECRET" in result.stderr
    assert "not-hex" not in result.stderr


def test_make_targets_use_preflight_and_bounded_compose_wait() -> None:
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")

    assert "COMPOSE_PROJECT_NAME ?= reliable-batch-ingestion" in makefile
    assert "COMPOSE_WAIT_TIMEOUT_SECONDS ?= 60" in makefile
    assert "check-local-env:" in makefile
    assert "up: check-local-env" in makefile
    assert "--project-name $(COMPOSE_PROJECT_NAME)" in makefile
    assert "--detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT_SECONDS)" in makefile
    assert "seed: up" in makefile
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
    env_path.write_text(VALID_INFRASTRUCTURE_ENV, encoding="utf-8")
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
