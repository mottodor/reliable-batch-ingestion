#!/usr/bin/env python3
"""Allow-listed, bounded parent skeleton for BI-080 demonstrations."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
import signal
import subprocess
import sys
import time
from abc import abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Protocol, TextIO, cast

import httpx
from botocore.exceptions import BotoCoreError, ClientError

from batch_ingestion.api.client import open_http_client
from batch_ingestion.api.service import ApiIngestionError, ingest_api_date
from batch_ingestion.api.sources import (
    api_page_identity,
    get_source_descriptor,
    page_payload_name,
)
from batch_ingestion.config import AppConfig, ConfigurationError
from batch_ingestion.identifiers import (
    SourceType,
    derive_batch_id,
    manifest_key_for,
    raw_object_key,
)
from batch_ingestion.manifests import Manifest, ManifestStatus
from batch_ingestion.postgres_snapshot.contracts import SnapshotDescriptor
from batch_ingestion.storage import S3Storage, StorageError, create_s3_client
from scripts.demo_faults import EXPECTED_CRASH_EXIT
from scripts.demo_inventory import (
    DemoBatchRecord,
    DemoInventoryError,
    DemoInventoryEvidence,
    DemoInventoryVerifier,
    DemoS3Client,
)


class RecoveryScenario(StrEnum):
    """Every allow-listed mandatory or deliberate recovery exercise."""

    API_RATE_LIMIT = "api-rate-limit"
    API_PERMANENT_500 = "api-permanent-500"
    API_MALFORMED_JSON = "api-malformed-json"
    API_AFTER_PAGE = "api-after-page"
    FILE_SAME_NAME = "file-same-name"
    FILE_RENAMED_CONTENT = "file-renamed-content"
    FILE_SCHEMA_INVALID = "file-schema-invalid"
    FILE_DURING_MOVE = "file-during-move"
    POSTGRES_CONNECTION_DROP = "postgres-connection-drop"
    S3_UNAVAILABLE = "s3-unavailable"
    PAYLOAD_BEFORE_MANIFEST = "payload-before-manifest"


class PublicDemoScenario(StrEnum):
    """Public demos and the supporting read-only inspector."""

    HAPPY_PATH = "happy-path"
    DUPLICATE = "duplicate"
    API_RETRY = "api-retry"
    BAD_FILE = "bad-file"
    INSPECT = "inspect"


RECOVERY_SCENARIOS: tuple[str, ...] = tuple(item.value for item in RecoveryScenario)
PUBLIC_SCENARIOS: tuple[str, ...] = tuple(item.value for item in PublicDemoScenario)
ALL_SCENARIOS = frozenset((*RECOVERY_SCENARIOS, *PUBLIC_SCENARIOS))


class DemoEventCode(StrEnum):
    """Closed event names emitted by demo orchestration."""

    STATE = "demo_state"
    SUMMARY = "demo_summary"


class DemoErrorCode(StrEnum):
    """Stable orchestration failures that contain no backend exception text."""

    INVALID_ARGUMENT = "invalid_argument"
    UNKNOWN_SCENARIO = "unknown_scenario"
    SUBPROCESS_TIMEOUT = "subprocess_timeout"
    UNEXPECTED_EXIT = "unexpected_exit"
    CONFIGURATION_ERROR = "configuration_error"
    STATE_MISSING = "state_missing"
    VERIFICATION_FAILED = "verification_failed"
    ARTIFACT_ERROR = "artifact_error"
    NOT_IMPLEMENTED = "not_implemented"


class DemoExitClass(StrEnum):
    """Bounded subprocess classifications used by the parent report."""

    NOT_RUN = "not_run"
    SUCCEEDED = "succeeded"
    EXPECTED_FAILURE = "expected_failure"
    UNEXPECTED_FAILURE = "unexpected_failure"
    TIMED_OUT = "timed_out"


class ExpectedObjectState(StrEnum):
    """Allow-listed exact states asserted by later demo slices."""

    ABSENT = "absent"
    PAYLOAD_ONLY = "payload_only"
    COMPLETE = "complete"
    SOURCE_ONLY = "source_only"
    BOTH_IDENTICAL = "both_identical"
    DESTINATION_ONLY = "destination_only"
    REJECTED = "rejected"


class ArtifactRelativePath(str):
    """Validated repository-relative report path safe for structured output."""

    def __new__(cls, value: str) -> ArtifactRelativePath:
        if not isinstance(value, str) or not value:
            raise ValueError("artifact path must be a non-empty relative path")
        path = PurePosixPath(value)
        if path.is_absolute():
            raise ValueError("artifact path must be relative")
        if ".." in path.parts:
            raise ValueError("artifact path must not contain parent traversal")
        if path.parts[:2] != ("artifacts", "demo"):
            raise ValueError("artifact path must be below artifacts/demo")
        return str.__new__(cls, path.as_posix())


def _validate_scenario(value: str) -> None:
    if value not in ALL_SCENARIOS:
        raise ValueError("scenario must be allow-listed")


def _validate_relative_key(value: str) -> None:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ValueError("object_key must be a safe relative key")


def _validate_non_negative_integer(value: int, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class DemoObjectCounts:
    """Counts by fixed disposition, preventing arbitrary report fields."""

    raw: int
    manifest: int
    landing: int
    archive: int
    rejected: int

    def __post_init__(self) -> None:
        for name, value in self.to_dict().items():
            _validate_non_negative_integer(value, field=name)

    def to_dict(self) -> dict[str, int]:
        return {
            "raw": self.raw,
            "manifest": self.manifest,
            "landing": self.landing,
            "archive": self.archive,
            "rejected": self.rejected,
        }


@dataclass(frozen=True, slots=True)
class DemoState:
    """One secret-free assertion over a deterministic object key."""

    scenario: str
    checkpoint: str
    expected_state: ExpectedObjectState
    object_key: str

    def __post_init__(self) -> None:
        _validate_scenario(self.scenario)
        if not self.checkpoint or not self.checkpoint.replace("-", "_").isidentifier():
            raise ValueError("checkpoint must be an identifier")
        if not isinstance(self.expected_state, ExpectedObjectState):
            raise TypeError("expected_state must be an ExpectedObjectState")
        _validate_relative_key(self.object_key)

    def to_dict(self) -> dict[str, object]:
        return {
            "event": DemoEventCode.STATE.value,
            "scenario": self.scenario,
            "checkpoint": self.checkpoint,
            "expected_state": self.expected_state.value,
            "object_key": self.object_key,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True, slots=True)
class DemoSummary:
    """Terminal orchestration report with a deliberately closed field set."""

    scenario: str
    passed: bool
    duration_seconds: float
    attempted_exit_class: DemoExitClass
    recovery_exit_class: DemoExitClass | None
    object_counts: DemoObjectCounts
    report_path: ArtifactRelativePath
    error_code: DemoErrorCode | None = None

    def __post_init__(self) -> None:
        _validate_scenario(self.scenario)
        if not isinstance(self.passed, bool):
            raise TypeError("passed must be a boolean")
        if (
            isinstance(self.duration_seconds, bool)
            or not isinstance(self.duration_seconds, (int, float))
            or not math.isfinite(self.duration_seconds)
            or self.duration_seconds < 0
        ):
            raise ValueError("duration_seconds must be finite and non-negative")
        if not isinstance(self.attempted_exit_class, DemoExitClass):
            raise TypeError("attempted_exit_class must be a DemoExitClass")
        if self.recovery_exit_class is not None and not isinstance(
            self.recovery_exit_class, DemoExitClass
        ):
            raise TypeError("recovery_exit_class must be a DemoExitClass or None")
        if not isinstance(self.object_counts, DemoObjectCounts):
            raise TypeError("object_counts must be DemoObjectCounts")
        if not isinstance(self.report_path, ArtifactRelativePath):
            raise TypeError("report_path must be an ArtifactRelativePath")
        if self.passed and self.error_code is not None:
            raise ValueError("a passing summary must not have an error_code")
        if not self.passed and not isinstance(self.error_code, DemoErrorCode):
            raise ValueError("a failing summary must have an error_code")

    def to_dict(self) -> dict[str, object]:
        return {
            "event": DemoEventCode.SUMMARY.value,
            "scenario": self.scenario,
            "passed": self.passed,
            "duration_seconds": self.duration_seconds,
            "attempted_exit_class": self.attempted_exit_class.value,
            "recovery_exit_class": (
                self.recovery_exit_class.value if self.recovery_exit_class is not None else None
            ),
            "object_counts": self.object_counts.to_dict(),
            "report_path": str(self.report_path),
            "error_code": self.error_code.value if self.error_code is not None else None,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)


class _Process(Protocol):
    pid: int
    returncode: int | None

    @abstractmethod
    def communicate(
        self,
        input: str | None = None,
        timeout: float | None = None,
    ) -> tuple[str, str]:
        """Return captured text after the process exits or the timeout expires."""


PopenFactory = Callable[..., _Process]
SignalProcessGroup = Callable[[int, int], None]


@dataclass(frozen=True, slots=True)
class SupervisedResult:
    """Captured bounded child outcome; command arguments are intentionally omitted."""

    exit_class: DemoExitClass
    returncode: int
    stdout: str
    stderr: str


def _default_popen(
    command: tuple[str, ...],
    *,
    cwd: Path | None,
    env: Mapping[str, str] | None,
    stdin: int,
    stdout: int,
    stderr: int,
    text: bool,
    start_new_session: bool,
) -> _Process:
    return cast(
        _Process,
        subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            text=text,
            start_new_session=start_new_session,
        ),
    )


def _default_signal_process_group(pid: int, signal_number: int) -> None:
    os.killpg(pid, signal_number)


def _signal_if_running(
    process: _Process,
    signal_number: int,
    signal_process_group: SignalProcessGroup,
) -> None:
    with contextlib.suppress(ProcessLookupError):
        signal_process_group(process.pid, signal_number)


def run_supervised(
    command: Sequence[str],
    *,
    timeout_seconds: float,
    expected_exit_codes: frozenset[int] = frozenset({0}),
    termination_grace_seconds: float = 1.0,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    popen_factory: PopenFactory = _default_popen,
    signal_process_group: SignalProcessGroup = _default_signal_process_group,
) -> SupervisedResult:
    """Run one child in its own process group and reap it within explicit deadlines."""
    if (
        isinstance(timeout_seconds, bool)
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise ValueError("timeout_seconds must be positive and finite")
    if (
        isinstance(termination_grace_seconds, bool)
        or not math.isfinite(termination_grace_seconds)
        or termination_grace_seconds <= 0
    ):
        raise ValueError("termination_grace_seconds must be positive and finite")
    normalized_command = tuple(command)
    if not normalized_command or any(
        not isinstance(part, str) or not part for part in normalized_command
    ):
        raise ValueError("command must contain non-empty string arguments")
    if not expected_exit_codes or any(
        isinstance(code, bool) or not isinstance(code, int) for code in expected_exit_codes
    ):
        raise ValueError("expected_exit_codes must contain integer exit codes")

    process = popen_factory(
        normalized_command,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        _signal_if_running(process, signal.SIGTERM, signal_process_group)
        try:
            stdout, stderr = process.communicate(timeout=termination_grace_seconds)
        except subprocess.TimeoutExpired:
            _signal_if_running(process, signal.SIGKILL, signal_process_group)
            try:
                stdout, stderr = process.communicate(timeout=termination_grace_seconds)
            except subprocess.TimeoutExpired as error:
                raise RuntimeError("timed-out child could not be reaped") from error
        returncode = process.returncode
        if returncode is None:
            raise RuntimeError("timed-out child has no return code after cleanup") from None
        return SupervisedResult(DemoExitClass.TIMED_OUT, returncode, stdout, stderr)

    returncode = process.returncode
    if returncode is None:
        raise RuntimeError("completed child has no return code")
    if returncode == 0 and returncode in expected_exit_codes:
        exit_class = DemoExitClass.SUCCEEDED
    elif returncode in expected_exit_codes:
        exit_class = DemoExitClass.EXPECTED_FAILURE
    else:
        exit_class = DemoExitClass.UNEXPECTED_FAILURE
    return SupervisedResult(exit_class, returncode, stdout, stderr)


Supervisor = Callable[..., SupervisedResult]


class ProductionDemoAdapter:
    """Bounded subprocess adapter over the three existing production CLIs."""

    def __init__(
        self,
        *,
        repository_root: Path,
        environ: Mapping[str, str],
        remaining_seconds: Callable[[], float],
        supervisor: Supervisor = run_supervised,
    ) -> None:
        self._repository_root = repository_root
        self._environ = dict(environ)
        self._remaining_seconds = remaining_seconds
        self._supervisor = supervisor

    def run_api(self, *, source: str, source_date: str) -> SupervisedResult:
        return self._run(
            (
                sys.executable,
                "-m",
                "batch_ingestion.cli",
                "run",
                "--source",
                source,
                "--date",
                source_date,
            ),
            expected_exit_codes=frozenset({0}),
        )

    def run_file(
        self,
        *,
        source: str,
        source_date: str,
        path: Path,
        expected_exit_codes: frozenset[int] = frozenset({0}),
    ) -> SupervisedResult:
        return self._run(
            (
                sys.executable,
                "-m",
                "batch_ingestion.files.cli",
                "run",
                "--source",
                source,
                "--date",
                source_date,
                "--path",
                str(path),
            ),
            expected_exit_codes=expected_exit_codes,
        )

    def run_snapshot(self, *, source: str, source_date: str) -> SupervisedResult:
        return self._run(
            (
                sys.executable,
                "-m",
                "batch_ingestion.postgres_snapshot.cli",
                "run",
                "--source",
                source,
                "--date",
                source_date,
            ),
            expected_exit_codes=frozenset({0}),
        )

    def resume_snapshot(self, record: DemoBatchRecord) -> SupervisedResult:
        arguments = [
            sys.executable,
            "-m",
            "batch_ingestion.postgres_snapshot.cli",
            "resume",
            "--source",
            record.source_name,
            "--date",
            record.source_date,
        ]
        if record.expected_record_count == 0:
            arguments.append("--empty")
        else:
            assert record.upper_updated_at is not None
            assert record.upper_order_id is not None
            assert record.expected_record_count is not None
            arguments.extend(
                (
                    "--upper-updated-at",
                    record.upper_updated_at,
                    "--upper-order-id",
                    str(record.upper_order_id),
                    "--expected-record-count",
                    str(record.expected_record_count),
                )
            )
        return self._run(tuple(arguments), expected_exit_codes=frozenset({0}))

    def resume_file(self, *, landing_key: str) -> SupervisedResult:
        return self._run(
            (
                sys.executable,
                "-m",
                "batch_ingestion.files.cli",
                "resume",
                "--landing-key",
                landing_key,
            ),
            expected_exit_codes=frozenset({0}),
        )

    def run_worker(
        self,
        *,
        scenario: str,
        expected_exit_codes: frozenset[int],
        environment_overrides: Mapping[str, str] | None = None,
    ) -> SupervisedResult:
        environment = dict(self._environ)
        if environment_overrides is not None:
            environment.update(environment_overrides)
        return self._supervisor(
            (
                sys.executable,
                str(self._repository_root / "scripts" / "demo_worker.py"),
                "--scenario",
                scenario,
            ),
            timeout_seconds=self._remaining_seconds(),
            expected_exit_codes=expected_exit_codes,
            cwd=self._repository_root,
            env=environment,
        )

    def run_api_with_overrides(
        self,
        *,
        source: str,
        source_date: str,
        environment_overrides: Mapping[str, str],
        expected_exit_codes: frozenset[int],
    ) -> SupervisedResult:
        environment = dict(self._environ)
        environment.update(environment_overrides)
        return self._supervisor(
            (
                sys.executable,
                "-m",
                "batch_ingestion.cli",
                "run",
                "--source",
                source,
                "--date",
                source_date,
            ),
            timeout_seconds=self._remaining_seconds(),
            expected_exit_codes=expected_exit_codes,
            cwd=self._repository_root,
            env=environment,
        )

    def run_api_retry(
        self,
        *,
        source: str,
        source_date: str,
        scenario_key: str,
    ) -> SupervisedResult:
        return self._run(
            (
                sys.executable,
                str(self._repository_root / "scripts" / "demo.py"),
                "_api-retry-worker",
                "--source",
                source,
                "--date",
                source_date,
                "--scenario-key",
                scenario_key,
            ),
            expected_exit_codes=frozenset({0}),
        )

    def _run(
        self,
        command: tuple[str, ...],
        *,
        expected_exit_codes: frozenset[int],
    ) -> SupervisedResult:
        return self._supervisor(
            command,
            timeout_seconds=self._remaining_seconds(),
            expected_exit_codes=expected_exit_codes,
            cwd=self._repository_root,
            env=self._environ,
        )


@dataclass(frozen=True, slots=True)
class DemoRequest:
    """Validated CLI request created before clients, artifacts, or children exist."""

    scenario: str
    timeout_seconds: float
    env_file: Path
    project_name: str


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timeout must be a positive number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("timeout must be positive and finite")
    return parsed


def _project_name(value: str) -> str:
    if not value or not value.replace("-", "_").isidentifier():
        raise argparse.ArgumentTypeError("project name must be a path-safe identifier")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="demo.py", exit_on_error=False)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--timeout-seconds", type=_positive_float, default=210.0)
        subparser.add_argument("--env-file", default=".env")
        subparser.add_argument(
            "--project-name",
            type=_project_name,
            default="reliable-batch-ingestion",
        )

    for command in PUBLIC_SCENARIOS:
        subparser = subparsers.add_parser(command, exit_on_error=False)
        add_common(subparser)
    recovery = subparsers.add_parser("recovery", exit_on_error=False)
    add_common(recovery)
    recovery.add_argument("--scenario", required=True, choices=RECOVERY_SCENARIOS)
    return parser


_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_SOURCE_DATE = "2026-09-01"
_API_RETRY_SOURCE_DATE = "2026-09-02"
_STATE_SCHEMA_VERSION = 1
_RUN_SUMMARY_FIELDS = frozenset(
    {
        "records_read",
        "records_accepted",
        "records_rejected",
        "pages_fetched",
        "bytes_written",
        "retry_count",
        "duplicate_count",
        "duration_seconds",
        "source_date",
        "batch_id",
        "run_id",
    }
)


class DemoExecutionError(RuntimeError):
    """Stable parent failure without backend or secret-bearing text."""

    def __init__(
        self,
        code: DemoErrorCode,
        *,
        exit_class: DemoExitClass = DemoExitClass.UNEXPECTED_FAILURE,
    ) -> None:
        super().__init__(code.value)
        self.code = code
        self.exit_class = exit_class


class _Deadline:
    def __init__(self, timeout_seconds: float) -> None:
        self._deadline = time.monotonic() + timeout_seconds

    def remaining(self) -> float:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise DemoExecutionError(
                DemoErrorCode.SUBPROCESS_TIMEOUT,
                exit_class=DemoExitClass.TIMED_OUT,
            )
        return remaining


class _ArtifactReport:
    def __init__(self, *, absolute_path: Path, relative_path: ArtifactRelativePath) -> None:
        self.absolute_path = absolute_path
        self.relative_path = relative_path

    @classmethod
    def create(cls, *, project_name: str, scenario: str) -> _ArtifactReport:
        if not project_name or not project_name.replace("-", "_").isidentifier():
            raise DemoExecutionError(DemoErrorCode.INVALID_ARGUMENT)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        relative = ArtifactRelativePath(
            f"artifacts/demo/{project_name}/{scenario}/{timestamp}-{os.getpid()}/report.ndjson"
        )
        absolute = _REPOSITORY_ROOT / str(relative)
        try:
            absolute.parent.mkdir(parents=True, exist_ok=False)
            absolute.touch(mode=0o600)
        except OSError as error:
            raise DemoExecutionError(DemoErrorCode.ARTIFACT_ERROR) from error
        return cls(absolute_path=absolute, relative_path=relative)

    def emit(self, value: Mapping[str, object]) -> None:
        serialized = json.dumps(dict(value), separators=(",", ":"), sort_keys=True)
        try:
            with self.absolute_path.open("a", encoding="utf-8") as stream:
                stream.write(serialized + "\n")
        except OSError as error:
            raise DemoExecutionError(DemoErrorCode.ARTIFACT_ERROR) from error
        print(serialized)


@dataclass(frozen=True, slots=True)
class _ScenarioOutcome:
    records: tuple[DemoBatchRecord, ...]
    evidence: DemoInventoryEvidence
    attempted_exit_class: DemoExitClass = DemoExitClass.SUCCEEDED
    recovery_exit_class: DemoExitClass | None = None


def _load_environment(path: Path) -> dict[str, str]:
    selected = path if path.is_absolute() else _REPOSITORY_ROOT / path
    values: dict[str, str] = {}
    try:
        lines = selected.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise DemoExecutionError(DemoErrorCode.CONFIGURATION_ERROR) from error
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name or name in values:
            raise DemoExecutionError(DemoErrorCode.CONFIGURATION_ERROR)
        values[name] = value
    environment = dict(os.environ)
    environment.update(values)
    environment.pop("DEMO_FAULT", None)
    return environment


def _state_path(project_name: str) -> Path:
    return _REPOSITORY_ROOT / "artifacts" / "demo" / project_name / "state.json"


def _load_state(project_name: str) -> tuple[DemoBatchRecord, ...]:
    path = _state_path(project_name)
    if not path.exists():
        return ()
    try:
        parsed: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED) from error
    if not isinstance(parsed, dict) or set(parsed) != {"schema_version", "records"}:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    if parsed["schema_version"] != _STATE_SCHEMA_VERSION:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    raw_records = parsed["records"]
    if not isinstance(raw_records, list) or not all(isinstance(item, dict) for item in raw_records):
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    try:
        return tuple(
            DemoBatchRecord.from_dict(cast(Mapping[str, object], item)) for item in raw_records
        )
    except (TypeError, ValueError) as error:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED) from error


def _merge_records(
    existing: Sequence[DemoBatchRecord],
    added: Sequence[DemoBatchRecord],
) -> tuple[DemoBatchRecord, ...]:
    merged: dict[tuple[str, str, str, str], DemoBatchRecord] = {}
    for record in (*existing, *added):
        key = (
            record.source_type.value,
            record.source_name,
            record.source_date,
            record.batch_id,
        )
        prior = merged.get(key)
        if prior is None:
            merged[key] = record
            continue
        comparable_prior = prior.to_dict()
        comparable_record = record.to_dict()
        comparable_prior.pop("scenario")
        comparable_record.pop("scenario")
        comparable_prior.pop("landing_prefixes")
        comparable_record.pop("landing_prefixes")
        if comparable_prior != comparable_record:
            raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        merged[key] = DemoBatchRecord(
            scenario=prior.scenario,
            source_name=prior.source_name,
            source_type=prior.source_type,
            source_date=prior.source_date,
            batch_id=prior.batch_id,
            payload_keys=prior.payload_keys,
            expected_status=prior.expected_status,
            disposition_key=prior.disposition_key,
            landing_prefixes=tuple(
                sorted(set((*prior.landing_prefixes, *record.landing_prefixes)))
            ),
            expected_record_count=prior.expected_record_count,
            upper_updated_at=prior.upper_updated_at,
            upper_order_id=prior.upper_order_id,
            chunk_rows=prior.chunk_rows,
        )
    return tuple(merged[key] for key in sorted(merged))


def _write_state(project_name: str, records: Sequence[DemoBatchRecord]) -> None:
    path = _state_path(project_name)
    temporary = path.with_suffix(".tmp")
    serialized = json.dumps(
        {
            "schema_version": _STATE_SCHEMA_VERSION,
            "records": [record.to_dict() for record in records],
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(serialized + "\n", encoding="utf-8")
        temporary.chmod(0o600)
        temporary.replace(path)
    except OSError as error:
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise DemoExecutionError(DemoErrorCode.ARTIFACT_ERROR) from error


def _parse_events(
    result: SupervisedResult,
    report: _ArtifactReport,
) -> tuple[dict[str, object], ...]:
    events: list[dict[str, object]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            value: object = json.loads(line)
        except json.JSONDecodeError as error:
            raise DemoExecutionError(DemoErrorCode.UNEXPECTED_EXIT) from error
        if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
            raise DemoExecutionError(DemoErrorCode.UNEXPECTED_EXIT)
        event = cast(dict[str, object], value)
        report.emit(event)
        events.append(event)
    if not events:
        raise DemoExecutionError(DemoErrorCode.UNEXPECTED_EXIT)
    return tuple(events)


def _require_exit(
    result: SupervisedResult,
    report: _ArtifactReport,
    *,
    expected_class: DemoExitClass = DemoExitClass.SUCCEEDED,
) -> tuple[dict[str, object], ...]:
    if result.exit_class is DemoExitClass.TIMED_OUT:
        raise DemoExecutionError(
            DemoErrorCode.SUBPROCESS_TIMEOUT,
            exit_class=DemoExitClass.TIMED_OUT,
        )
    if result.exit_class is not expected_class:
        raise DemoExecutionError(
            DemoErrorCode.UNEXPECTED_EXIT,
            exit_class=result.exit_class,
        )
    return _parse_events(result, report)


def _event(events: Sequence[Mapping[str, object]], name: str) -> Mapping[str, object]:
    matches = [event for event in events if event.get("event") == name]
    if len(matches) != 1:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    return matches[0]


def _summary(events: Sequence[Mapping[str, object]]) -> Mapping[str, object]:
    matches = [event for event in events if _RUN_SUMMARY_FIELDS.issubset(event)]
    if len(matches) != 1:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    return matches[0]


def _string(value: Mapping[str, object], field: str) -> str:
    selected = value.get(field)
    if not isinstance(selected, str):
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    return selected


def _integer(value: Mapping[str, object], field: str) -> int:
    selected = value.get(field)
    if isinstance(selected, bool) or not isinstance(selected, int) or selected < 0:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    return selected


def _optional_string(value: Mapping[str, object], field: str) -> str | None:
    selected = value.get(field)
    if selected is not None and not isinstance(selected, str):
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    return selected


def _api_record(
    *,
    scenario: str,
    config: AppConfig,
    source_name: str,
    source_date: str,
    events: Sequence[Mapping[str, object]],
) -> DemoBatchRecord:
    summary = _summary(events)
    pages = _integer(summary, "pages_fetched")
    if pages <= 0:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    descriptor = get_source_descriptor(source_name)
    parsed_date = date.fromisoformat(source_date)
    identity = api_page_identity(descriptor, parsed_date, page_size=config.http.page_size)
    batch_id = derive_batch_id(identity)
    if _string(summary, "batch_id") != batch_id:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    payload_keys = tuple(
        raw_object_key(
            identity,
            page_payload_name(page, max_pages=config.http.max_pages),
        )
        for page in range(1, pages + 1)
    )
    return DemoBatchRecord(
        scenario=scenario,
        source_name=source_name,
        source_type=SourceType.API,
        source_date=source_date,
        batch_id=batch_id,
        payload_keys=payload_keys,
        expected_status=ManifestStatus.SUCCEEDED,
    )


def _file_record(
    *,
    scenario: str,
    events: Sequence[Mapping[str, object]],
    expected_outcome: str,
    expected_reason: str | None = None,
) -> DemoBatchRecord:
    event = _event(events, "file_ingestion")
    if event.get("outcome") != expected_outcome or event.get("rejection_code") != expected_reason:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    landing_key = _string(event, "landing_key")
    return DemoBatchRecord(
        scenario=scenario,
        source_name=_string(event, "source_name"),
        source_type=SourceType.FILE,
        source_date=_string(event, "source_date"),
        batch_id=_string(event, "batch_id"),
        payload_keys=(_string(event, "raw_key"),),
        expected_status=(
            ManifestStatus.SUCCEEDED if expected_outcome == "accepted" else ManifestStatus.REJECTED
        ),
        disposition_key=_string(event, "disposition_key"),
        landing_prefixes=(landing_key.rsplit("/", 1)[0] + "/",),
    )


def _snapshot_record(
    *,
    scenario: str,
    config: AppConfig,
    events: Sequence[Mapping[str, object]],
) -> DemoBatchRecord:
    started = _event(events, "snapshot_started")
    expected_count = _integer(started, "expected_record_count")
    upper_order_value = started.get("upper_order_id")
    if upper_order_value is not None and (
        isinstance(upper_order_value, bool) or not isinstance(upper_order_value, int)
    ):
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    descriptor = SnapshotDescriptor.from_resume_arguments(
        source_name=_string(started, "source_name"),
        source_date=date.fromisoformat(_string(started, "source_date")),
        upper_updated_at=_optional_string(started, "upper_updated_at"),
        upper_order_id=upper_order_value,
        expected_record_count=expected_count,
        empty=started.get("empty") is True,
    )
    chunk_rows = _integer(started, "chunk_rows")
    if chunk_rows != config.snapshot.chunk_rows:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    payload_keys = tuple(
        descriptor.raw_key(chunk_ordinal=ordinal, chunk_rows=chunk_rows)
        for ordinal in range(1, descriptor.expected_chunk_count(chunk_rows=chunk_rows) + 1)
    )
    return DemoBatchRecord(
        scenario=scenario,
        source_name=descriptor.source_name,
        source_type=SourceType.POSTGRES_SNAPSHOT,
        source_date=descriptor.source_date.isoformat(),
        batch_id=descriptor.batch_id(chunk_rows=chunk_rows),
        payload_keys=payload_keys,
        expected_status=ManifestStatus.SUCCEEDED,
        expected_record_count=descriptor.expected_record_count,
        upper_updated_at=descriptor.canonical_upper_updated_at,
        upper_order_id=descriptor.upper_order_id,
        chunk_rows=chunk_rows,
    )


def _emit_states(
    report: _ArtifactReport,
    *,
    scenario: str,
    records: Sequence[DemoBatchRecord],
) -> None:
    for record in records:
        expected = (
            ExpectedObjectState.REJECTED
            if record.expected_status is ManifestStatus.REJECTED
            else ExpectedObjectState.COMPLETE
        )
        for payload_key in record.payload_keys:
            report.emit(
                DemoState(
                    scenario=scenario,
                    checkpoint="final",
                    expected_state=expected,
                    object_key=payload_key,
                ).to_dict()
            )
        if record.disposition_key is not None:
            report.emit(
                DemoState(
                    scenario=scenario,
                    checkpoint="disposition",
                    expected_state=ExpectedObjectState.DESTINATION_ONLY,
                    object_key=record.disposition_key,
                ).to_dict()
            )


def _require_zero_write_duplicate(
    events: Sequence[Mapping[str, object]],
    *,
    minimum_duplicates: int,
) -> None:
    summary = _summary(events)
    if _integer(summary, "bytes_written") != 0:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    if _integer(summary, "duplicate_count") < minimum_duplicates:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)


def _run_happy_commands(
    *,
    adapter: ProductionDemoAdapter,
    config: AppConfig,
    report: _ArtifactReport,
) -> tuple[DemoBatchRecord, ...]:
    command_started = time.monotonic()
    api_events = _require_exit(
        adapter.run_api(source="fx-rates", source_date=_DEFAULT_SOURCE_DATE), report
    )
    valid_path = (
        _REPOSITORY_ROOT
        / "fixtures/supplier-prices/same-name-delivery-a/supplier-prices-2026-09-01.csv"
    )
    file_events = _require_exit(
        adapter.run_file(
            source="supplier-prices",
            source_date=_DEFAULT_SOURCE_DATE,
            path=valid_path,
        ),
        report,
    )
    snapshot_events = _require_exit(
        adapter.run_snapshot(source="orders", source_date=_DEFAULT_SOURCE_DATE), report
    )
    if time.monotonic() - command_started > 180:
        raise DemoExecutionError(DemoErrorCode.SUBPROCESS_TIMEOUT)
    return (
        _api_record(
            scenario=PublicDemoScenario.HAPPY_PATH.value,
            config=config,
            source_name="fx-rates",
            source_date=_DEFAULT_SOURCE_DATE,
            events=api_events,
        ),
        _file_record(
            scenario=PublicDemoScenario.HAPPY_PATH.value,
            events=file_events,
            expected_outcome="accepted",
        ),
        _snapshot_record(
            scenario=PublicDemoScenario.HAPPY_PATH.value,
            config=config,
            events=snapshot_events,
        ),
    )


def _run_public_scenario(
    *,
    request: DemoRequest,
    config: AppConfig,
    adapter: ProductionDemoAdapter,
    verifier: DemoInventoryVerifier,
    report: _ArtifactReport,
) -> _ScenarioOutcome:
    existing = _load_state(request.project_name)
    if request.scenario == PublicDemoScenario.HAPPY_PATH.value:
        added = _run_happy_commands(adapter=adapter, config=config, report=report)
        records = _merge_records(existing, added)
        evidence = verifier.verify(records)
        _write_state(request.project_name, records)
        _emit_states(report, scenario=request.scenario, records=added)
        return _ScenarioOutcome(records, evidence)

    if request.scenario == PublicDemoScenario.DUPLICATE.value:
        baseline_records = _run_happy_commands(adapter=adapter, config=config, report=report)
        records = _merge_records(existing, baseline_records)
        before = verifier.verify(records)
        api_record = next(
            record for record in baseline_records if record.source_type is SourceType.API
        )
        snapshot_record = next(
            record
            for record in baseline_records
            if record.source_type is SourceType.POSTGRES_SNAPSHOT
        )
        api_events = _require_exit(
            adapter.run_api(source=api_record.source_name, source_date=api_record.source_date),
            report,
        )
        _require_zero_write_duplicate(api_events, minimum_duplicates=len(api_record.payload_keys))
        snapshot_events = _require_exit(adapter.resume_snapshot(snapshot_record), report)
        _require_zero_write_duplicate(
            snapshot_events,
            minimum_duplicates=len(snapshot_record.payload_keys),
        )
        same_name_path = (
            _REPOSITORY_ROOT
            / "fixtures/supplier-prices/same-name-delivery-b/supplier-prices-2026-09-01.csv"
        )
        same_name_events = _require_exit(
            adapter.run_file(
                source="supplier-prices",
                source_date=_DEFAULT_SOURCE_DATE,
                path=same_name_path,
            ),
            report,
        )
        if _event(same_name_events, "file_ingestion").get("duplicate_class") != (
            "same_name_redelivery"
        ):
            raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        _require_zero_write_duplicate(same_name_events, minimum_duplicates=1)
        renamed_path = (
            _REPOSITORY_ROOT / "fixtures/supplier-prices/renamed-content/"
            "supplier-prices-renamed-2026-09-01.csv"
        )
        renamed_events = _require_exit(
            adapter.run_file(
                source="supplier-prices",
                source_date=_DEFAULT_SOURCE_DATE,
                path=renamed_path,
            ),
            report,
        )
        if _event(renamed_events, "file_ingestion").get("duplicate_class") != "renamed_content":
            raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        _require_zero_write_duplicate(renamed_events, minimum_duplicates=1)
        duplicate_records = (
            _file_record(
                scenario=PublicDemoScenario.DUPLICATE.value,
                events=same_name_events,
                expected_outcome="accepted",
            ),
            _file_record(
                scenario=PublicDemoScenario.DUPLICATE.value,
                events=renamed_events,
                expected_outcome="accepted",
            ),
        )
        records = _merge_records(records, duplicate_records)
        after = verifier.verify(records)
        before.require_unchanged(after)
        _write_state(request.project_name, records)
        _emit_states(report, scenario=request.scenario, records=baseline_records)
        return _ScenarioOutcome(records, after)

    if request.scenario == PublicDemoScenario.API_RETRY.value:
        retry_events = _require_exit(
            adapter.run_api_retry(
                source="fx-rates",
                source_date=_API_RETRY_SOURCE_DATE,
                scenario_key="bi080-api-retry-20260902",
            ),
            report,
        )
        retry_event = _event(retry_events, "api_retry")
        if (
            retry_event.get("http_status") != 429
            or retry_event.get("delay_seconds") != 1.0
            or _integer(_summary(retry_events), "retry_count") != 1
        ):
            raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        added = (
            _api_record(
                scenario=PublicDemoScenario.API_RETRY.value,
                config=config,
                source_name="fx-rates",
                source_date=_API_RETRY_SOURCE_DATE,
                events=retry_events,
            ),
        )
        records = _merge_records(existing, added)
        evidence = verifier.verify(records)
        _write_state(request.project_name, records)
        _emit_states(report, scenario=request.scenario, records=added)
        return _ScenarioOutcome(records, evidence)

    if request.scenario == PublicDemoScenario.BAD_FILE.value:
        input_directory = report.absolute_path.parent / "inputs"
        input_directory.mkdir(mode=0o700)
        invalid_path = input_directory / "supplier-prices-schema-2026-09-01.csv"
        malformed_path = input_directory / "supplier-prices-malformed-2026-09-01.csv"
        corrected_path = input_directory / "supplier-prices-corrected-2026-09-01.csv"
        invalid_fixture = _REPOSITORY_ROOT / "fixtures/supplier-prices/canonical/schema_invalid.csv"
        invalid_path.write_bytes(invalid_fixture.read_bytes())
        malformed_path.write_bytes(
            b"supplier_id,product_id,price_minor_units,currency,effective_date\n\xff\n"
        )
        corrected_path.write_bytes(
            (_REPOSITORY_ROOT / "fixtures/supplier-prices/canonical/valid.csv").read_bytes()
        )
        invalid_events = _require_exit(
            adapter.run_file(
                source="supplier-prices",
                source_date=_DEFAULT_SOURCE_DATE,
                path=invalid_path,
                expected_exit_codes=frozenset({4}),
            ),
            report,
            expected_class=DemoExitClass.EXPECTED_FAILURE,
        )
        malformed_events = _require_exit(
            adapter.run_file(
                source="supplier-prices",
                source_date=_DEFAULT_SOURCE_DATE,
                path=malformed_path,
                expected_exit_codes=frozenset({4}),
            ),
            report,
            expected_class=DemoExitClass.EXPECTED_FAILURE,
        )
        corrected_events = _require_exit(
            adapter.run_file(
                source="supplier-prices",
                source_date=_DEFAULT_SOURCE_DATE,
                path=corrected_path,
            ),
            report,
        )
        added = (
            _file_record(
                scenario=PublicDemoScenario.BAD_FILE.value,
                events=invalid_events,
                expected_outcome="rejected",
                expected_reason="schema_mismatch",
            ),
            _file_record(
                scenario=PublicDemoScenario.BAD_FILE.value,
                events=malformed_events,
                expected_outcome="rejected",
                expected_reason="malformed_file",
            ),
            _file_record(
                scenario=PublicDemoScenario.BAD_FILE.value,
                events=corrected_events,
                expected_outcome="accepted",
            ),
        )
        records = _merge_records(existing, added)
        evidence = verifier.verify(records)
        _write_state(request.project_name, records)
        _emit_states(report, scenario=request.scenario, records=added)
        return _ScenarioOutcome(records, evidence)

    if request.scenario == PublicDemoScenario.INSPECT.value:
        if not existing:
            raise DemoExecutionError(DemoErrorCode.STATE_MISSING)
        evidence = verifier.verify(existing)
        _emit_states(report, scenario=request.scenario, records=existing)
        return _ScenarioOutcome(existing, evidence)

    raise DemoExecutionError(DemoErrorCode.NOT_IMPLEMENTED)


def _counts(evidence: DemoInventoryEvidence | None) -> DemoObjectCounts:
    if evidence is None:
        return DemoObjectCounts(raw=0, manifest=0, landing=0, archive=0, rejected=0)
    return DemoObjectCounts(
        raw=evidence.raw_count,
        manifest=evidence.manifest_count,
        landing=evidence.landing_count,
        archive=evidence.archive_count,
        rejected=evidence.rejected_count,
    )


class _ReadableBody(Protocol):
    def read(self, amount: int | None = None) -> bytes:
        return b""

    def close(self) -> None:
        return None


def _object_bytes(client: DemoS3Client, *, bucket: str, key: str, maximum: int) -> bytes:
    response = client.get_object(Bucket=bucket, Key=key)
    raw_body = response.get("Body")
    if raw_body is None:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    body = cast(_ReadableBody, raw_body)
    try:
        value = body.read(maximum + 1)
    finally:
        body.close()
    if not isinstance(value, bytes) or len(value) > maximum:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    return value


def _key_exists(client: DemoS3Client, *, bucket: str, key: str) -> bool:
    response = client.list_objects_v2(Bucket=bucket, Prefix=key)
    contents = response.get("Contents", [])
    if not isinstance(contents, list):
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
    keys = {
        item.get("Key")
        for item in contents
        if isinstance(item, dict) and isinstance(item.get("Key"), str)
    }
    return key in keys


def _require_pair_state(
    client: DemoS3Client,
    *,
    bucket: str,
    payload_key: str,
    expected: ExpectedObjectState,
) -> None:
    payload_exists = _key_exists(client, bucket=bucket, key=payload_key)
    manifest_exists = _key_exists(client, bucket=bucket, key=manifest_key_for(payload_key))
    actual = (
        ExpectedObjectState.COMPLETE
        if payload_exists and manifest_exists
        else ExpectedObjectState.PAYLOAD_ONLY
        if payload_exists
        else ExpectedObjectState.ABSENT
        if not manifest_exists
        else None
    )
    if actual is not expected:
        raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)


def _emit_state(
    report: _ArtifactReport,
    *,
    scenario: str,
    checkpoint: str,
    state: ExpectedObjectState,
    key: str,
) -> None:
    report.emit(
        DemoState(
            scenario=scenario,
            checkpoint=checkpoint,
            expected_state=state,
            object_key=key,
        ).to_dict()
    )


def _api_payload_key(config: AppConfig, *, source_date: str, page: int) -> str:
    descriptor = get_source_descriptor("fx-rates")
    identity = api_page_identity(
        descriptor,
        date.fromisoformat(source_date),
        page_size=config.http.page_size,
    )
    return raw_object_key(
        identity,
        page_payload_name(page, max_pages=config.http.max_pages),
    )


def _failed_api_record(
    *,
    scenario: str,
    config: AppConfig,
    source_date: str,
) -> DemoBatchRecord:
    descriptor = get_source_descriptor("fx-rates")
    identity = api_page_identity(
        descriptor,
        date.fromisoformat(source_date),
        page_size=config.http.page_size,
    )
    return DemoBatchRecord(
        scenario=scenario,
        source_name="fx-rates",
        source_type=SourceType.API,
        source_date=source_date,
        batch_id=derive_batch_id(identity),
        payload_keys=(_api_payload_key(config, source_date=source_date, page=1),),
        expected_status=ManifestStatus.FAILED,
    )


def _worker_events(
    result: SupervisedResult,
    report: _ArtifactReport,
    *,
    expected_class: DemoExitClass = DemoExitClass.EXPECTED_FAILURE,
) -> tuple[dict[str, object], ...]:
    return _require_exit(result, report, expected_class=expected_class)


def _public_recovery(
    public_scenario: PublicDemoScenario,
    *,
    request: DemoRequest,
    config: AppConfig,
    adapter: ProductionDemoAdapter,
    verifier: DemoInventoryVerifier,
    report: _ArtifactReport,
) -> _ScenarioOutcome:
    outcome = _run_public_scenario(
        request=DemoRequest(
            scenario=public_scenario.value,
            timeout_seconds=request.timeout_seconds,
            env_file=request.env_file,
            project_name=request.project_name,
        ),
        config=config,
        adapter=adapter,
        verifier=verifier,
        report=report,
    )
    _emit_states(report, scenario=request.scenario, records=outcome.records)
    return _ScenarioOutcome(
        outcome.records,
        outcome.evidence,
        DemoExitClass.EXPECTED_FAILURE,
        DemoExitClass.SUCCEEDED,
    )


def _run_recovery_scenario(
    *,
    request: DemoRequest,
    config: AppConfig,
    environment: Mapping[str, str],
    deadline: _Deadline,
    adapter: ProductionDemoAdapter,
    verifier: DemoInventoryVerifier,
    client: DemoS3Client,
    report: _ArtifactReport,
) -> _ScenarioOutcome:
    scenario = request.scenario
    if scenario == RecoveryScenario.API_RATE_LIMIT.value:
        return _public_recovery(
            PublicDemoScenario.API_RETRY,
            request=request,
            config=config,
            adapter=adapter,
            verifier=verifier,
            report=report,
        )
    if scenario in {
        RecoveryScenario.FILE_SAME_NAME.value,
        RecoveryScenario.FILE_RENAMED_CONTENT.value,
    }:
        return _public_recovery(
            PublicDemoScenario.DUPLICATE,
            request=request,
            config=config,
            adapter=adapter,
            verifier=verifier,
            report=report,
        )
    if scenario == RecoveryScenario.FILE_SCHEMA_INVALID.value:
        return _public_recovery(
            PublicDemoScenario.BAD_FILE,
            request=request,
            config=config,
            adapter=adapter,
            verifier=verifier,
            report=report,
        )

    worker_overrides: dict[str, str] = {}
    if scenario in {
        RecoveryScenario.API_AFTER_PAGE.value,
        RecoveryScenario.PAYLOAD_BEFORE_MANIFEST.value,
    }:
        worker_overrides["HTTP_PAGE_SIZE"] = "2"
    if scenario == RecoveryScenario.POSTGRES_CONNECTION_DROP.value:
        worker_overrides["SNAPSHOT_CHUNK_ROWS"] = "5"
    worker_environment = dict(environment)
    worker_environment.update(worker_overrides)
    worker_config = AppConfig.from_env(worker_environment)

    if scenario == RecoveryScenario.S3_UNAVAILABLE.value:
        existing = _load_state(request.project_name)
        before = verifier.verify(existing) if existing else None
        source_date = "2026-09-07"
        failed = adapter.run_api_with_overrides(
            source="fx-rates",
            source_date=source_date,
            environment_overrides={
                "S3_ENDPOINT_URL": "http://127.0.0.1:1",
                "S3_TOTAL_MAX_ATTEMPTS": "1",
                "S3_CONNECT_TIMEOUT_SECONDS": "0.1",
                "S3_READ_TIMEOUT_SECONDS": "0.1",
                "NO_PROXY": "127.0.0.1,localhost",
                "no_proxy": "127.0.0.1,localhost",
            },
            expected_exit_codes=frozenset({4}),
        )
        attempted_events = _worker_events(failed, report)
        api_failures = [
            event
            for event in attempted_events
            if event.get("event") == "api_ingestion"
            and event.get("status") == "failed"
            and event.get("error_code") == "storage_error"
        ]
        summaries = [event for event in attempted_events if event.get("event") == "run_summary"]
        if (
            len(api_failures) != 1
            or len(summaries) != 1
            or attempted_events[-2:] != (api_failures[0], summaries[0])
            or api_failures[0].get("batch_id") != summaries[0].get("batch_id")
            or api_failures[0].get("run_id") != summaries[0].get("run_id")
        ):
            raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        key = _api_payload_key(config, source_date=source_date, page=1)
        _require_pair_state(
            client,
            bucket=config.storage.bucket,
            payload_key=key,
            expected=ExpectedObjectState.ABSENT,
        )
        _emit_state(
            report,
            scenario=scenario,
            checkpoint="unavailable",
            state=ExpectedObjectState.ABSENT,
            key=key,
        )
        if before is not None:
            before.require_unchanged(verifier.verify(existing))
        recovered = adapter.run_api(source="fx-rates", source_date=source_date)
        events = _require_exit(recovered, report)
        added = (
            _api_record(
                scenario=scenario,
                config=config,
                source_name="fx-rates",
                source_date=source_date,
                events=events,
            ),
        )
        records = _merge_records(existing, added)
        evidence = verifier.verify(records)
        _write_state(request.project_name, records)
        _emit_states(report, scenario=scenario, records=added)
        return _ScenarioOutcome(
            records,
            evidence,
            failed.exit_class,
            recovered.exit_class,
        )

    expected_codes = (
        frozenset({4})
        if scenario
        in {
            RecoveryScenario.API_PERMANENT_500.value,
            RecoveryScenario.API_MALFORMED_JSON.value,
        }
        else frozenset({0, EXPECTED_CRASH_EXIT})
    )
    attempted = adapter.run_worker(
        scenario=scenario,
        expected_exit_codes=expected_codes,
        environment_overrides=worker_overrides,
    )
    attempted_events = _worker_events(
        attempted,
        report,
        expected_class=(
            DemoExitClass.SUCCEEDED
            if attempted.exit_class is DemoExitClass.SUCCEEDED
            else DemoExitClass.EXPECTED_FAILURE
        ),
    )
    existing = _load_state(request.project_name)

    if scenario == RecoveryScenario.API_PERMANENT_500.value:
        source_date = "2026-09-03"
        first_key = _api_payload_key(worker_config, source_date=source_date, page=1)
        _require_pair_state(
            client,
            bucket=config.storage.bucket,
            payload_key=first_key,
            expected=ExpectedObjectState.ABSENT,
        )
        _emit_state(
            report,
            scenario=scenario,
            checkpoint="attempt_budget_exhausted",
            state=ExpectedObjectState.ABSENT,
            key=first_key,
        )
        recovered = adapter.run_api(source="fx-rates", source_date=source_date)
        events = _require_exit(recovered, report)
        added = (
            _api_record(
                scenario=scenario,
                config=config,
                source_name="fx-rates",
                source_date=source_date,
                events=events,
            ),
        )
    elif scenario == RecoveryScenario.API_MALFORMED_JSON.value:
        source_date = "2026-09-04"
        record = _failed_api_record(
            scenario=scenario,
            config=worker_config,
            source_date=source_date,
        )
        key = record.payload_keys[0]
        _require_pair_state(
            client,
            bucket=config.storage.bucket,
            payload_key=key,
            expected=ExpectedObjectState.COMPLETE,
        )
        manifest_before = _object_bytes(
            client,
            bucket=config.storage.bucket,
            key=manifest_key_for(key),
            maximum=1_000_000,
        )
        if Manifest.from_json(manifest_before.decode("utf-8")).status is not ManifestStatus.FAILED:
            raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        payload_before = hashlib.sha256(
            _object_bytes(
                client,
                bucket=config.storage.bucket,
                key=key,
                maximum=config.http.max_response_bytes,
            )
        ).hexdigest()
        _emit_state(
            report,
            scenario=scenario,
            checkpoint="terminal_failed",
            state=ExpectedObjectState.COMPLETE,
            key=key,
        )
        recovered = adapter.run_worker(
            scenario=scenario,
            expected_exit_codes=frozenset({4}),
        )
        _worker_events(recovered, report)
        if (
            manifest_before
            != _object_bytes(
                client,
                bucket=config.storage.bucket,
                key=manifest_key_for(key),
                maximum=1_000_000,
            )
            or payload_before
            != hashlib.sha256(
                _object_bytes(
                    client,
                    bucket=config.storage.bucket,
                    key=key,
                    maximum=config.http.max_response_bytes,
                )
            ).hexdigest()
        ):
            raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        added = (record,)
    elif scenario in {
        RecoveryScenario.API_AFTER_PAGE.value,
        RecoveryScenario.PAYLOAD_BEFORE_MANIFEST.value,
    }:
        source_date = (
            "2026-09-05" if scenario == RecoveryScenario.API_AFTER_PAGE.value else "2026-09-06"
        )
        first_key = _api_payload_key(worker_config, source_date=source_date, page=1)
        intermediate = (
            ExpectedObjectState.COMPLETE
            if scenario == RecoveryScenario.API_AFTER_PAGE.value
            else ExpectedObjectState.PAYLOAD_ONLY
        )
        second_key = _api_payload_key(worker_config, source_date=source_date, page=2)
        if attempted.exit_class is DemoExitClass.SUCCEEDED:
            intermediate = ExpectedObjectState.COMPLETE
            _require_pair_state(
                client,
                bucket=config.storage.bucket,
                payload_key=second_key,
                expected=ExpectedObjectState.COMPLETE,
            )
        else:
            _require_pair_state(
                client,
                bucket=config.storage.bucket,
                payload_key=second_key,
                expected=ExpectedObjectState.ABSENT,
            )
        _require_pair_state(
            client,
            bucket=config.storage.bucket,
            payload_key=first_key,
            expected=intermediate,
        )
        _emit_state(
            report,
            scenario=scenario,
            checkpoint=(
                "already_recovered"
                if attempted.exit_class is DemoExitClass.SUCCEEDED
                else "interrupted"
            ),
            state=intermediate,
            key=first_key,
        )
        recovery_adapter = ProductionDemoAdapter(
            repository_root=_REPOSITORY_ROOT,
            environ=worker_environment,
            remaining_seconds=deadline.remaining,
        )
        recovered = recovery_adapter.run_api(source="fx-rates", source_date=source_date)
        events = _require_exit(recovered, report)
        added = (
            _api_record(
                scenario=scenario,
                config=worker_config,
                source_name="fx-rates",
                source_date=source_date,
                events=events,
            ),
        )
    elif scenario == RecoveryScenario.FILE_DURING_MOVE.value:
        descriptor_event = _event(attempted_events, "demo_worker_descriptor")
        landing_key = _string(descriptor_event, "landing_key")
        raw_key = _string(descriptor_event, "raw_key")
        disposition_key = _string(descriptor_event, "disposition_key")
        if attempted.exit_class is DemoExitClass.SUCCEEDED:
            if _key_exists(
                client,
                bucket=config.storage.bucket,
                key=landing_key,
            ) or not all(
                _key_exists(client, bucket=config.storage.bucket, key=key)
                for key in (raw_key, disposition_key, manifest_key_for(raw_key))
            ):
                raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
            _emit_state(
                report,
                scenario=scenario,
                checkpoint="already_recovered",
                state=ExpectedObjectState.DESTINATION_ONLY,
                key=disposition_key,
            )
        else:
            if not all(
                _key_exists(client, bucket=config.storage.bucket, key=key)
                for key in (landing_key, raw_key, disposition_key)
            ) or _key_exists(
                client,
                bucket=config.storage.bucket,
                key=manifest_key_for(raw_key),
            ):
                raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
            landing = _object_bytes(
                client,
                bucket=config.storage.bucket,
                key=landing_key,
                maximum=config.file.max_file_bytes,
            )
            destination = _object_bytes(
                client,
                bucket=config.storage.bucket,
                key=disposition_key,
                maximum=config.file.max_file_bytes,
            )
            if landing != destination:
                raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
            _emit_state(
                report,
                scenario=scenario,
                checkpoint="copy_verified",
                state=ExpectedObjectState.BOTH_IDENTICAL,
                key=landing_key,
            )
        recovered = adapter.resume_file(landing_key=landing_key)
        events = _require_exit(recovered, report)
        added = (
            _file_record(
                scenario=scenario,
                events=events,
                expected_outcome="accepted",
            ),
        )
    elif scenario == RecoveryScenario.POSTGRES_CONNECTION_DROP.value:
        partial = _snapshot_record(
            scenario=scenario,
            config=worker_config,
            events=attempted_events,
        )
        first_key = partial.payload_keys[0]
        _require_pair_state(
            client,
            bucket=config.storage.bucket,
            payload_key=first_key,
            expected=ExpectedObjectState.COMPLETE,
        )
        if len(partial.payload_keys) < 2:
            raise DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        later_state = (
            ExpectedObjectState.COMPLETE
            if _key_exists(
                client,
                bucket=config.storage.bucket,
                key=partial.payload_keys[1],
            )
            else ExpectedObjectState.ABSENT
        )
        _require_pair_state(
            client,
            bucket=config.storage.bucket,
            payload_key=partial.payload_keys[1],
            expected=later_state,
        )
        _emit_state(
            report,
            scenario=scenario,
            checkpoint=(
                "already_recovered"
                if later_state is ExpectedObjectState.COMPLETE
                else "first_chunk_complete"
            ),
            state=ExpectedObjectState.COMPLETE,
            key=first_key,
        )
        recovery_adapter = ProductionDemoAdapter(
            repository_root=_REPOSITORY_ROOT,
            environ=worker_environment,
            remaining_seconds=deadline.remaining,
        )
        recovered = recovery_adapter.resume_snapshot(partial)
        events = _require_exit(recovered, report)
        added = (
            _snapshot_record(
                scenario=scenario,
                config=worker_config,
                events=events,
            ),
        )
    else:
        raise DemoExecutionError(DemoErrorCode.NOT_IMPLEMENTED)

    records = _merge_records(existing, added)
    evidence = verifier.verify(records)
    _write_state(request.project_name, records)
    _emit_states(report, scenario=scenario, records=added)
    return _ScenarioOutcome(
        records,
        evidence,
        attempted.exit_class,
        recovered.exit_class,
    )


def execute_demo(request: DemoRequest) -> int:
    """Execute one bounded public demonstration or recovery exercise."""
    started = time.monotonic()
    report = _ArtifactReport.create(
        project_name=request.project_name,
        scenario=request.scenario,
    )
    evidence: DemoInventoryEvidence | None = None
    attempted_exit_class = DemoExitClass.NOT_RUN
    recovery_exit_class: DemoExitClass | None = None
    try:
        environment = _load_environment(request.env_file)
        config = AppConfig.from_env(environment)
        deadline = _Deadline(request.timeout_seconds)
        adapter = ProductionDemoAdapter(
            repository_root=_REPOSITORY_ROOT,
            environ=environment,
            remaining_seconds=deadline.remaining,
        )
        maximum_object_bytes = max(
            config.http.max_response_bytes,
            config.file.max_file_bytes,
            config.snapshot.max_chunk_bytes,
        )
        client = create_s3_client(config.storage)
        with contextlib.closing(client):
            verifier = DemoInventoryVerifier(
                client=cast(DemoS3Client, client),
                bucket=config.storage.bucket,
                maximum_object_bytes=maximum_object_bytes,
            )
            if request.scenario in RECOVERY_SCENARIOS:
                outcome = _run_recovery_scenario(
                    request=request,
                    config=config,
                    environment=environment,
                    deadline=deadline,
                    adapter=adapter,
                    verifier=verifier,
                    client=cast(DemoS3Client, client),
                    report=report,
                )
            else:
                outcome = _run_public_scenario(
                    request=request,
                    config=config,
                    adapter=adapter,
                    verifier=verifier,
                    report=report,
                )
            evidence = outcome.evidence
            attempted_exit_class = outcome.attempted_exit_class
            recovery_exit_class = outcome.recovery_exit_class
        summary = DemoSummary(
            scenario=request.scenario,
            passed=True,
            duration_seconds=round(time.monotonic() - started, 6),
            attempted_exit_class=attempted_exit_class,
            recovery_exit_class=recovery_exit_class,
            object_counts=_counts(evidence),
            report_path=report.relative_path,
        )
        report.emit(summary.to_dict())
        return 0
    except ConfigurationError:
        error = DemoExecutionError(DemoErrorCode.CONFIGURATION_ERROR)
    except (DemoInventoryError, ValueError) as cause:
        error = DemoExecutionError(DemoErrorCode.VERIFICATION_FAILED)
        error.__cause__ = cause
    except (BotoCoreError, ClientError, StorageError, httpx.HTTPError, OSError) as cause:
        error = DemoExecutionError(DemoErrorCode.UNEXPECTED_EXIT)
        error.__cause__ = cause
    except DemoExecutionError as cause:
        error = cause

    summary = DemoSummary(
        scenario=request.scenario,
        passed=False,
        duration_seconds=round(time.monotonic() - started, 6),
        attempted_exit_class=error.exit_class,
        recovery_exit_class=None,
        object_counts=_counts(evidence),
        report_path=report.relative_path,
        error_code=error.code,
    )
    report.emit(summary.to_dict())
    return 4


def _run_api_retry_worker(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="demo.py _api-retry-worker", exit_on_error=False)
    parser.add_argument("--source", choices=("fx-rates",), required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--scenario-key", choices=("bi080-api-retry-20260902",), required=True)
    try:
        arguments = parser.parse_args(list(argv))
        source_date = date.fromisoformat(str(arguments.date))
        if source_date.isoformat() != arguments.date:
            return 2
        config = AppConfig.from_env()
        descriptor = get_source_descriptor(str(arguments.source))
        with (
            contextlib.closing(create_s3_client(config.storage)) as s3_client,
            open_http_client(config.http) as http_client,
        ):
            reset = http_client.post(
                "/__control/reset",
                params={"scenario_key": str(arguments.scenario_key)},
            )
            if reset.status_code != 200:
                return 5
            storage = S3Storage(client=s3_client, bucket=config.storage.bucket)
            ingest_api_date(
                client=http_client,
                storage=storage,
                http_config=config.http,
                descriptor=descriptor,
                source_date=source_date,
                query_parameter_provider=lambda _: {
                    "scenario": "rate_limit_once",
                    "scenario_key": str(arguments.scenario_key),
                },
                event_sink=lambda event: print(
                    json.dumps(event, separators=(",", ":"), sort_keys=True)
                ),
            )
    except (argparse.ArgumentError, argparse.ArgumentTypeError, ConfigurationError, ValueError):
        return 2
    except ApiIngestionError:
        return 4
    except (BotoCoreError, ClientError, StorageError, httpx.HTTPError, OSError):
        return 5
    return 0


def main(argv: Sequence[str] | None = None, *, stderr: TextIO | None = None) -> int:
    """Validate an allow-listed request before creating clients, artifacts, or children."""
    selected_argv = list(sys.argv[1:] if argv is None else argv)
    if selected_argv[:1] == ["_api-retry-worker"]:
        return _run_api_retry_worker(selected_argv[1:])
    error_stream = sys.stderr if stderr is None else stderr
    parser = _build_parser()
    try:
        arguments = parser.parse_args(selected_argv)
    except (argparse.ArgumentError, argparse.ArgumentTypeError) as error:
        print(f"demo arguments: {error}", file=error_stream)
        if "--scenario" in str(error):
            print(
                f"accepted recovery scenarios: {', '.join(RECOVERY_SCENARIOS)}",
                file=error_stream,
            )
        return 2
    except SystemExit as error:
        return error.code if isinstance(error.code, int) else 1

    command = str(arguments.command)
    scenario = str(arguments.scenario) if command == "recovery" else command
    request = DemoRequest(
        scenario=scenario,
        timeout_seconds=cast(float, arguments.timeout_seconds),
        env_file=Path(str(arguments.env_file)),
        project_name=str(arguments.project_name),
    )
    return execute_demo(request)


if __name__ == "__main__":
    raise SystemExit(main())
