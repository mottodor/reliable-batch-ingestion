#!/usr/bin/env python3
"""Exercise BI-020 persistence and destructive recovery in an isolated project."""

import argparse
import os
import shlex
import socket
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

from check_local_env import read_env, validate_environment

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ISOLATED_PROJECT_NAME = "reliable-batch-ingestion-bi020-reset-test"
COMMAND_TIMEOUT_SECONDS = 180
VOLUME_SUFFIXES = ("postgres_data", "garage_metadata", "garage_data")


class RecoveryCheckError(RuntimeError):
    """Raised when isolated persistence or reset behavior violates its contract."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("env_file", type=Path)
    parser.add_argument(
        "--protected-project-name",
        default="reliable-batch-ingestion",
    )
    return parser


def _run(
    command: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
    expected_success: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=None if environment is None else dict(environment),
        check=False,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)

    succeeded = result.returncode == 0
    if succeeded != expected_success:
        expectation = "succeed" if expected_success else "fail closed"
        raise RecoveryCheckError(
            f"command did not {expectation}: {shlex.join(command)} (exit {result.returncode})"
        )
    return result


def _available_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            sockets.append(listener)
        return [int(listener.getsockname()[1]) for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


def _write_isolated_env(base_values: Mapping[str, str], path: Path) -> None:
    postgres_port, s3_port, admin_port = _available_ports(3)
    values = dict(base_values)
    values.update(
        {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": str(postgres_port),
            "S3_ENDPOINT_URL": f"http://localhost:{s3_port}",
            "S3_PORT": str(s3_port),
            "GARAGE_ADMIN_PORT": str(admin_port),
        }
    )
    serialized = "".join(f"{name}={value}\n" for name, value in sorted(values.items()))
    path.write_text(serialized, encoding="utf-8")
    path.chmod(0o600)


def _volume_names(project_name: str) -> list[str]:
    return [f"{project_name}_{suffix}" for suffix in VOLUME_SUFFIXES]


def _volume_fingerprint(project_name: str, docker_command: Sequence[str]) -> dict[str, str | None]:
    fingerprints: dict[str, str | None] = {}
    for volume_name in _volume_names(project_name):
        result = subprocess.run(
            [*docker_command, "volume", "inspect", volume_name],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
        fingerprints[volume_name] = result.stdout if result.returncode == 0 else None
    return fingerprints


def _assert_volumes(project_name: str, docker_command: Sequence[str], *, present: bool) -> None:
    fingerprints = _volume_fingerprint(project_name, docker_command)
    unexpected = [
        name for name, fingerprint in fingerprints.items() if (fingerprint is not None) != present
    ]
    if unexpected:
        expected_state = "present" if present else "absent"
        raise RecoveryCheckError(
            f"expected isolated volumes to be {expected_state}: {', '.join(unexpected)}"
        )


def _make_command(
    target: str,
    env_file: Path,
    compose_value: str,
    *,
    confirm: str | None = None,
) -> list[str]:
    command = [
        "make",
        target,
        f"COMPOSE={compose_value}",
        f"COMPOSE_ENV_FILE={env_file}",
        f"COMPOSE_PROJECT_NAME={ISOLATED_PROJECT_NAME}",
    ]
    if confirm is not None:
        command.append(f"CONFIRM={confirm}")
    return command


def _probe_command(uv_command: Sequence[str], env_file: Path) -> list[str]:
    return [
        *uv_command,
        "run",
        "pytest",
        "tests/integration/test_infrastructure.py",
        "-m",
        "integration",
    ]


def _probe_environment(env_file: Path, mode: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment["BI020_ENV_FILE"] = str(env_file)
    environment["BI020_MARKER_MODE"] = mode
    return environment


def _direct_cleanup(
    compose_command: Sequence[str],
    env_file: Path,
) -> None:
    _run(
        [
            *compose_command,
            "--project-name",
            ISOLATED_PROJECT_NAME,
            "--env-file",
            str(env_file),
            "down",
            "--volumes",
            "--remove-orphans",
        ]
    )


def _exercise(
    env_file: Path,
    protected_project_name: str,
) -> None:
    if protected_project_name == ISOLATED_PROJECT_NAME:
        raise RecoveryCheckError("isolated project must differ from the protected project")

    compose_value = os.environ.get("COMPOSE", "docker compose")
    compose_command = shlex.split(compose_value)
    docker_command = shlex.split(os.environ.get("DOCKER", "docker"))
    uv_command = shlex.split(os.environ.get("UV", "uv"))
    protected_before = _volume_fingerprint(protected_project_name, docker_command)
    probe_command = _probe_command(uv_command, env_file)

    _direct_cleanup(compose_command, env_file)
    try:
        _run(_make_command("up", env_file, compose_value))
        _assert_volumes(ISOLATED_PROJECT_NAME, docker_command, present=True)
        _run(
            probe_command,
            environment=_probe_environment(env_file, "create"),
        )

        _run(_make_command("down", env_file, compose_value))
        _assert_volumes(ISOLATED_PROJECT_NAME, docker_command, present=True)
        _run(_make_command("up", env_file, compose_value))
        _run(
            probe_command,
            environment=_probe_environment(env_file, "verify"),
        )

        _run(
            _make_command("reset", env_file, compose_value),
            expected_success=False,
        )
        _run(
            _make_command("reset-destructive", env_file, compose_value),
            expected_success=False,
        )
        _run(
            _make_command(
                "reset-destructive",
                env_file,
                compose_value,
                confirm="wrong",
            ),
            expected_success=False,
        )
        _run(
            probe_command,
            environment=_probe_environment(env_file, "verify"),
        )

        _run(
            _make_command(
                "reset-destructive",
                env_file,
                compose_value,
                confirm="destroy-local-data",
            )
        )
        _assert_volumes(ISOLATED_PROJECT_NAME, docker_command, present=False)

        _run(_make_command("up", env_file, compose_value))
        _run(
            probe_command,
            environment=_probe_environment(env_file, "absent"),
        )
    finally:
        _direct_cleanup(compose_command, env_file)

    protected_after = _volume_fingerprint(protected_project_name, docker_command)
    if protected_after != protected_before:
        raise RecoveryCheckError("protected Compose project volumes changed")


def main(argv: Sequence[str] | None = None) -> int:
    """Create isolated configuration and run persistence/reset assertions."""
    arguments = _parser().parse_args(argv)
    values, parse_errors = read_env(arguments.env_file)
    errors = [*parse_errors, *validate_environment(values)]
    if errors:
        print("Recovery check configuration: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    try:
        with tempfile.TemporaryDirectory(prefix="bi020-recovery-") as temporary_directory:
            isolated_env = Path(temporary_directory) / ".env"
            _write_isolated_env(values, isolated_env)
            _exercise(isolated_env, arguments.protected_project_name)
    except (RecoveryCheckError, subprocess.TimeoutExpired) as error:
        print(f"Infrastructure recovery check: FAIL: {error}", file=sys.stderr)
        return 1

    print("Infrastructure recovery check: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
