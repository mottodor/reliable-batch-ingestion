"""Contract tests for bounded local-infrastructure lifecycle commands."""

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

    assert "COMPOSE_WAIT_TIMEOUT_SECONDS ?= 60" in makefile
    assert "check-local-env:" in makefile
    assert "up: check-local-env" in makefile
    assert "--detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT_SECONDS)" in makefile
    assert "seed: up" in makefile
    assert "integration: seed" in makefile
    assert "BI020_SENTINEL_MODE=create" in makefile
    assert "BI020_SENTINEL_MODE=verify" in makefile
