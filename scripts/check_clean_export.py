#!/usr/bin/env python3
"""Run the BI-080 demonstration walkthrough from an isolated clean export."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROTECTED_PROJECT = "reliable-batch-ingestion"
DEFAULT_COMMAND_TIMEOUT_SECONDS = 300.0
DEFAULT_TOTAL_TIMEOUT_SECONDS = 1_800.0
CLEANUP_TIMEOUT_SECONDS = 120.0
VOLUME_SUFFIXES = ("postgres_data", "garage_metadata", "garage_data")
SECRET_VARIABLES = (
    "POSTGRES_PASSWORD",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "GARAGE_RPC_SECRET",
)
ISOLATED_ENVIRONMENT_FIELDS = frozenset(
    {
        "POSTGRES_PORT",
        "S3_ENDPOINT_URL",
        "S3_PORT",
        "S3_BUCKET",
        "GARAGE_ADMIN_PORT",
        "MOCK_API_PORT",
        "MOCK_API_BASE_URL",
    }
)
_FORBIDDEN_EXPORT_PARTS = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "artifacts",
    }
)
_PROJECT_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{2,62}")
_PAYLOAD_VALUE_PATTERN = re.compile(r"SUP-[A-Za-z0-9_-]+")
_EXPECTED_INSPECT_COUNTS = {
    "archive": 1,
    "landing": 0,
    "manifest": 6,
    "raw": 6,
    "rejected": 2,
}


class CleanExportError(RuntimeError):
    """A clean-export contract failure safe to report without subprocess output."""


@dataclass(frozen=True, slots=True)
class WalkthroughCommand:
    """One bounded command in the isolated walkthrough."""

    name: str
    argv: tuple[str, ...]
    timeout_seconds: float
    phase: str


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Captured bounded-command result."""

    returncode: int
    stdout: str
    stderr: str
    elapsed_seconds: float


@dataclass(frozen=True, slots=True)
class CleanExportSummary:
    """Secret-free timing and cleanup evidence for one walkthrough."""

    project_name: str
    setup_seconds: float
    happy_path_seconds: float
    walkthrough_seconds: float
    cleanup_verified: bool

    def to_json(self) -> str:
        return json.dumps(
            {
                "cleanup_verified": self.cleanup_verified,
                "event": "clean_export_summary",
                "happy_path_seconds": round(self.happy_path_seconds, 6),
                "passed": True,
                "project_name": self.project_name,
                "setup_seconds": round(self.setup_seconds, 6),
                "walkthrough_seconds": round(self.walkthrough_seconds, 6),
            },
            separators=(",", ":"),
            sort_keys=True,
        )


class _Deadline:
    def __init__(self, timeout_seconds: float) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("total timeout must be positive and finite")
        self._expires_at = time.monotonic() + timeout_seconds

    def command_timeout(self, maximum: float) -> float:
        remaining = self._expires_at - time.monotonic()
        if remaining <= 0:
            raise CleanExportError("clean-export total deadline exhausted")
        return min(maximum, remaining)


def parse_export_paths(raw_paths: bytes) -> tuple[Path, ...]:
    """Validate the NUL-delimited Git inventory used as the export boundary."""
    parsed: list[Path] = []
    for raw_path in raw_paths.split(b"\0"):
        if not raw_path:
            continue
        try:
            value = raw_path.decode("utf-8")
        except UnicodeDecodeError as error:
            raise CleanExportError("repository path is not valid UTF-8") from error
        pure = PurePosixPath(value)
        if (
            pure.is_absolute()
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
        ):
            raise CleanExportError("export inventory contains an unsafe path")
        if pure.name in {".env", ".env.local"} or any(
            part in _FORBIDDEN_EXPORT_PARTS for part in pure.parts
        ):
            raise CleanExportError("export inventory contains ignored local state")
        parsed.append(Path(*pure.parts))
    if not parsed:
        raise CleanExportError("Git returned an empty export inventory")
    if len(parsed) != len(set(parsed)):
        raise CleanExportError("Git returned duplicate export paths")
    return tuple(sorted(parsed, key=lambda path: path.as_posix()))


def copy_export_files(
    source_root: Path,
    destination_root: Path,
    paths: Sequence[Path],
) -> None:
    """Copy only validated regular files and preserve executable metadata."""
    destination_root.mkdir(parents=True, exist_ok=False)
    for relative_path in paths:
        source = source_root / relative_path
        destination = destination_root / relative_path
        if source.is_symlink():
            raise CleanExportError("clean export refuses repository symlinks")
        if not source.is_file():
            raise CleanExportError("clean export inventory entry is not a regular file")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination, follow_symlinks=False)


def _read_assignments(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise CleanExportError("isolated environment could not be read") from error
    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name or name in values:
            raise CleanExportError(
                f"isolated environment has an invalid assignment at line {line_number}"
            )
        values[name] = value
    return values


def rewrite_isolated_environment(path: Path, overrides: Mapping[str, str]) -> None:
    """Atomically override only test-owned bucket and loopback endpoint fields."""
    if set(overrides) != ISOLATED_ENVIRONMENT_FIELDS:
        raise CleanExportError("isolated environment override does not match the allow-list")
    if path.is_symlink() or not path.is_file():
        raise CleanExportError("isolated environment must be a regular non-symlink file")
    values = _read_assignments(path)
    missing = ISOLATED_ENVIRONMENT_FIELDS - values.keys()
    if missing:
        raise CleanExportError("isolated environment is missing an allow-listed field")
    if any("\n" in value or "\r" in value for value in overrides.values()):
        raise CleanExportError("isolated environment override contains a line break")

    output_lines: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines(keepends=True):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            output_lines.append(raw_line)
            continue
        name, separator, _ = stripped.partition("=")
        if separator and name in overrides:
            ending = "\n" if raw_line.endswith("\n") else ""
            output_lines.append(f"{name}={overrides[name]}{ending}")
        else:
            output_lines.append(raw_line)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".clean-export-env-",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write("".join(output_lines))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
        path.chmod(0o600)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def walkthrough_commands(
    *,
    env_file: Path,
    project_name: str,
    command_timeout_seconds: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
) -> tuple[WalkthroughCommand, ...]:
    """Build the fixed clean-export sequence without executing it."""
    if _PROJECT_PATTERN.fullmatch(project_name) is None:
        raise ValueError("invalid isolated Compose project name")
    if not math.isfinite(command_timeout_seconds) or command_timeout_seconds <= 0:
        raise ValueError("command timeout must be positive and finite")
    env_argument = f"COMPOSE_ENV_FILE={env_file.as_posix()}"
    project_argument = f"COMPOSE_PROJECT_NAME={project_name}"
    demo_timeout = "DEMO_TIMEOUT_SECONDS=180"

    def make(name: str, target: str, *, phase: str = "walkthrough") -> WalkthroughCommand:
        return WalkthroughCommand(
            name,
            ("make", target, env_argument, project_argument, demo_timeout),
            command_timeout_seconds,
            phase,
        )

    return (
        WalkthroughCommand(
            "locked-setup",
            ("uv", "sync", "--locked", "--all-groups"),
            command_timeout_seconds,
            "setup",
        ),
        WalkthroughCommand(
            "doctor",
            ("./scripts/doctor.sh",),
            command_timeout_seconds,
            "setup",
        ),
        WalkthroughCommand(
            "compose-config",
            (
                "docker",
                "compose",
                "--project-name",
                project_name,
                "--env-file",
                env_file.as_posix(),
                "config",
                "--quiet",
            ),
            command_timeout_seconds,
            "setup",
        ),
        make("up", "up", phase="setup"),
        WalkthroughCommand(
            "seed",
            (
                "make",
                "seed",
                env_argument,
                project_argument,
                "SOURCE_SEED=20260901",
            ),
            command_timeout_seconds,
            "setup",
        ),
        make("demo-happy-path", "demo-happy-path"),
        make("demo-duplicate", "demo-duplicate"),
        make("demo-api-retry", "demo-api-retry"),
        make("demo-bad-file", "demo-bad-file"),
        make("offline-tests", "test"),
        make("demo-inspect", "demo-inspect"),
    )


def redact_output(output: str, secrets_to_hide: Sequence[str]) -> str:
    """Redact generated credentials and recognizable fixture row values."""
    redacted = output
    for value in sorted((value for value in secrets_to_hide if value), key=len, reverse=True):
        redacted = redacted.replace(value, "[REDACTED]")
    return _PAYLOAD_VALUE_PATTERN.sub("[REDACTED]", redacted)


def _contains_sensitive_output(output: str, credentials: Sequence[str]) -> bool:
    return any(value and value in output for value in credentials) or bool(
        _PAYLOAD_VALUE_PATTERN.search(output)
    )


def _run_command(
    argv: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float,
    check: bool = True,
) -> CommandResult:
    started_at = time.monotonic()
    try:
        completed = subprocess.run(
            tuple(argv),
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise CleanExportError("bounded clean-export command timed out") from error
    result = CommandResult(
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        elapsed_seconds=time.monotonic() - started_at,
    )
    if check and result.returncode != 0:
        raise CleanExportError("clean-export command failed")
    return result


def _git_export_paths(repository_root: Path) -> tuple[Path, ...]:
    try:
        result = subprocess.run(
            (
                "git",
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ),
            cwd=repository_root,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=30,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise CleanExportError("Git export inventory timed out") from error
    if result.returncode != 0:
        raise CleanExportError("Git export inventory failed")
    return parse_export_paths(result.stdout)


def _available_ports(count: int) -> tuple[int, ...]:
    listeners: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            listeners.append(listener)
        return tuple(int(listener.getsockname()[1]) for listener in listeners)
    finally:
        for listener in listeners:
            listener.close()


def _isolated_overrides(bucket: str) -> dict[str, str]:
    postgres_port, s3_port, admin_port, mock_api_port = _available_ports(4)
    return {
        "POSTGRES_PORT": str(postgres_port),
        "S3_ENDPOINT_URL": f"http://localhost:{s3_port}",
        "S3_PORT": str(s3_port),
        "S3_BUCKET": bucket,
        "GARAGE_ADMIN_PORT": str(admin_port),
        "MOCK_API_PORT": str(mock_api_port),
        "MOCK_API_BASE_URL": f"http://localhost:{mock_api_port}",
    }


def _volume_fingerprint(project_name: str, *, cwd: Path) -> dict[str, str | None]:
    fingerprint: dict[str, str | None] = {}
    for suffix in VOLUME_SUFFIXES:
        volume_name = f"{project_name}_{suffix}"
        result = _run_command(
            ("docker", "volume", "inspect", volume_name),
            cwd=cwd,
            timeout_seconds=30,
            check=False,
        )
        fingerprint[volume_name] = result.stdout if result.returncode == 0 else None
    return fingerprint


def _assert_project_resources_absent(project_name: str, *, cwd: Path) -> None:
    label = f"label=com.docker.compose.project={project_name}"
    queries = (
        ("docker", "ps", "--all", "--quiet", "--filter", label),
        ("docker", "network", "ls", "--quiet", "--filter", label),
        ("docker", "volume", "ls", "--quiet", "--filter", label),
    )
    for query in queries:
        result = _run_command(query, cwd=cwd, timeout_seconds=30)
        if result.stdout.strip():
            raise CleanExportError("isolated Compose resources remain after cleanup")


def _require_demo_summary(result: CommandResult, *, command_name: str) -> None:
    summaries: list[Mapping[str, object]] = []
    for line in result.stdout.splitlines():
        if not line.startswith("{"):
            continue
        try:
            value: object = json.loads(line)
        except json.JSONDecodeError as error:
            raise CleanExportError("demo emitted malformed JSON evidence") from error
        if isinstance(value, dict) and value.get("event") == "demo_summary":
            summaries.append(value)
    if len(summaries) != 1 or summaries[0].get("passed") is not True:
        raise CleanExportError("demo did not emit one passing summary")
    if (
        command_name == "demo-inspect"
        and summaries[0].get("object_counts") != _EXPECTED_INSPECT_COUNTS
    ):
        raise CleanExportError("final demo inventory counts differ from the clean-export contract")


def _scan_demo_artifacts(export_root: Path, project_name: str, credentials: Sequence[str]) -> None:
    artifact_root = export_root / "artifacts" / "demo" / project_name
    if not artifact_root.is_dir():
        raise CleanExportError("demo artifact directory is missing")
    for path in sorted(artifact_root.rglob("*")):
        if not path.is_file() or path.name not in {"report.ndjson", "state.json"}:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as error:
            raise CleanExportError("demo artifact is not UTF-8 text") from error
        if _contains_sensitive_output(content, credentials):
            raise CleanExportError("demo artifact contains credential or payload values")


def _cleanup_isolated_project(export_root: Path, env_file: Path, project_name: str) -> None:
    make_down = (
        "make",
        "down",
        f"COMPOSE_ENV_FILE={env_file.as_posix()}",
        f"COMPOSE_PROJECT_NAME={project_name}",
    )
    down = _run_command(
        make_down,
        cwd=export_root,
        timeout_seconds=CLEANUP_TIMEOUT_SECONDS,
        check=False,
    )
    reset = _run_command(
        (
            "python3",
            "scripts/reset_local_infrastructure.py",
            "--confirm",
            "destroy-local-data",
            "--compose",
            "docker compose",
            "--env-file",
            env_file.as_posix(),
            "--project-name",
            project_name,
        ),
        cwd=export_root,
        timeout_seconds=CLEANUP_TIMEOUT_SECONDS,
        check=False,
    )
    if down.returncode != 0 or reset.returncode != 0:
        raise CleanExportError("isolated Compose cleanup command failed")
    _assert_project_resources_absent(project_name, cwd=export_root)


def run_clean_export(
    *,
    repository_root: Path = REPOSITORY_ROOT,
    protected_project_name: str = DEFAULT_PROTECTED_PROJECT,
    command_timeout_seconds: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
    total_timeout_seconds: float = DEFAULT_TOTAL_TIMEOUT_SECONDS,
) -> CleanExportSummary:
    """Run setup, public demos, inspection, and exact isolated cleanup."""
    if _PROJECT_PATTERN.fullmatch(protected_project_name) is None:
        raise CleanExportError("protected Compose project name is invalid")
    if not math.isfinite(command_timeout_seconds) or command_timeout_seconds <= 0:
        raise CleanExportError("command timeout must be positive and finite")
    deadline = _Deadline(total_timeout_seconds)
    started_at = time.monotonic()
    unique_suffix = uuid4().hex[:12]
    project_name = f"bi080-clean-{unique_suffix}"
    bucket = f"bi080-clean-{unique_suffix}"
    if project_name == protected_project_name:
        raise CleanExportError("isolated and protected Compose projects must differ")

    paths = _git_export_paths(repository_root)
    protected_before = _volume_fingerprint(protected_project_name, cwd=repository_root)
    summary: CleanExportSummary | None = None
    primary_error: BaseException | None = None
    cleanup_error: BaseException | None = None

    with tempfile.TemporaryDirectory(prefix="bi080-clean-export-") as temporary_directory:
        export_root = Path(temporary_directory) / "repository"
        env_file = Path(".env")
        absolute_env_file = export_root / env_file
        copy_export_files(repository_root, export_root, paths)
        if absolute_env_file.exists() or (export_root / ".git").exists():
            raise CleanExportError("clean export contains local configuration or Git metadata")

        try:
            initialize = _run_command(
                ("python3", "scripts/init_local_env.py", env_file.as_posix()),
                cwd=export_root,
                timeout_seconds=deadline.command_timeout(command_timeout_seconds),
            )
            if "PASS" not in initialize.stdout:
                raise CleanExportError("local environment initialization did not report success")
            rewrite_isolated_environment(absolute_env_file, _isolated_overrides(bucket))
            assignments = _read_assignments(absolute_env_file)
            credentials = tuple(assignments.get(name, "") for name in SECRET_VARIABLES)
            if not all(credentials):
                raise CleanExportError("initialized environment omitted a credential field")

            setup_seconds = initialize.elapsed_seconds
            happy_path_seconds = 0.0
            for command in walkthrough_commands(
                env_file=env_file,
                project_name=project_name,
                command_timeout_seconds=command_timeout_seconds,
            ):
                result = _run_command(
                    command.argv,
                    cwd=export_root,
                    timeout_seconds=deadline.command_timeout(command.timeout_seconds),
                )
                combined = result.stdout + result.stderr
                if _contains_sensitive_output(combined, credentials):
                    raise CleanExportError("command output contains credential or payload values")
                if command.name.startswith("demo-"):
                    _require_demo_summary(result, command_name=command.name)
                if command.phase == "setup":
                    setup_seconds += result.elapsed_seconds
                if command.name == "demo-happy-path":
                    happy_path_seconds = result.elapsed_seconds
                    if happy_path_seconds > 180:
                        raise CleanExportError("warm happy path exceeded the three-minute budget")
                print(
                    f"OK   {command.name} ({result.elapsed_seconds:.2f}s)",
                    file=sys.stderr,
                )
            _scan_demo_artifacts(export_root, project_name, credentials)
            summary = CleanExportSummary(
                project_name=project_name,
                setup_seconds=setup_seconds,
                happy_path_seconds=happy_path_seconds,
                walkthrough_seconds=time.monotonic() - started_at,
                cleanup_verified=True,
            )
        except BaseException as error:
            primary_error = error
        finally:
            if absolute_env_file.is_file():
                try:
                    _cleanup_isolated_project(export_root, env_file, project_name)
                except BaseException as error:
                    cleanup_error = error

    try:
        protected_after = _volume_fingerprint(protected_project_name, cwd=repository_root)
        if protected_after != protected_before:
            raise CleanExportError("protected Compose project volume fingerprints changed")
    except BaseException as error:
        cleanup_error = cleanup_error or error

    if primary_error is not None:
        if cleanup_error is not None:
            raise CleanExportError(
                "walkthrough and isolated cleanup both failed"
            ) from primary_error
        if isinstance(primary_error, CleanExportError):
            raise primary_error
        raise CleanExportError("clean-export walkthrough failed") from primary_error
    if cleanup_error is not None:
        if isinstance(cleanup_error, CleanExportError):
            raise cleanup_error
        raise CleanExportError("isolated cleanup failed") from cleanup_error
    if summary is None:
        raise CleanExportError("clean-export walkthrough produced no summary")
    return summary


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--protected-project-name",
        default=DEFAULT_PROTECTED_PROJECT,
    )
    parser.add_argument(
        "--command-timeout-seconds",
        type=_positive_float,
        default=DEFAULT_COMMAND_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--total-timeout-seconds",
        type=_positive_float,
        default=DEFAULT_TOTAL_TIMEOUT_SECONDS,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the clean-export acceptance walkthrough."""
    arguments = _parser().parse_args(argv)
    try:
        summary = run_clean_export(
            protected_project_name=arguments.protected_project_name,
            command_timeout_seconds=arguments.command_timeout_seconds,
            total_timeout_seconds=arguments.total_timeout_seconds,
        )
    except CleanExportError as error:
        print(f"Clean-export walkthrough: FAIL - {error}", file=sys.stderr)
        return 1
    print(summary.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
