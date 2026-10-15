"""Strict JSON-emitting command boundary for bounded PostgreSQL snapshots."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import date
from enum import IntEnum
from typing import Never, TextIO, cast

from botocore.exceptions import BotoCoreError, ClientError

from batch_ingestion.config import AppConfig, ConfigurationError
from batch_ingestion.storage import S3Storage, StorageError, create_s3_client

from .contracts import SnapshotDescriptor, is_supported_source
from .events import SnapshotHandledError
from .service import ingest_order_snapshot

_CANONICAL_NON_NEGATIVE_INTEGER = re.compile(r"0|[1-9]\d*")
_CANONICAL_SIGNED_INTEGER = re.compile(r"0|-?[1-9]\d*")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


class CliExitCode(IntEnum):
    """Stable process exit codes shared by ingestion command boundaries."""

    SUCCESS = 0
    USAGE = 2
    CONFIGURATION = 3
    INGESTION_FAILED = 4
    RUNTIME_ERROR = 5


class CliUsageError(ValueError):
    """Raised when arguments cannot safely enter the snapshot contract."""


@dataclass(frozen=True, slots=True)
class RunCommand:
    source: str
    source_date: date


@dataclass(frozen=True, slots=True)
class ResumeCommand:
    descriptor: SnapshotDescriptor


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise CliUsageError(message)


def _canonical_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("date must use canonical YYYY-MM-DD form") from error
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("date must use canonical YYYY-MM-DD form")
    return parsed


def _non_negative_integer(value: str) -> int:
    if _CANONICAL_NON_NEGATIVE_INTEGER.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("value must be a canonical non-negative integer")
    try:
        return int(value)
    except ValueError as error:  # defensive: the canonical regex already excludes this path
        raise argparse.ArgumentTypeError(
            "value must be a canonical non-negative integer"
        ) from error


def _signed_int64(value: str) -> int:
    if _CANONICAL_SIGNED_INTEGER.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("value must be a canonical signed integer")
    try:
        parsed = int(value)
    except ValueError as error:  # defensive: the canonical regex already excludes this path
        raise argparse.ArgumentTypeError("value must be a canonical signed integer") from error
    if not _INT64_MIN <= parsed <= _INT64_MAX:
        raise argparse.ArgumentTypeError("value must fit a signed 64-bit integer")
    return parsed


def _add_source_and_date(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source", required=True)
    parser.add_argument("--date", dest="source_date", type=_canonical_date, required=True)


def build_parser() -> argparse.ArgumentParser:
    """Build strict run/resume forms without configuration or I/O."""
    parser = _ArgumentParser(prog="batch-ingestion-postgres")
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run")
    _add_source_and_date(run_parser)

    resume_parser = commands.add_parser("resume")
    _add_source_and_date(resume_parser)
    resume_parser.add_argument("--upper-updated-at")
    resume_parser.add_argument("--upper-order-id", type=_signed_int64)
    resume_parser.add_argument("--expected-record-count", type=_non_negative_integer)
    resume_parser.add_argument("--empty", action="store_true")
    return parser


def parse_command(argv: Sequence[str]) -> RunCommand | ResumeCommand:
    """Parse and validate the complete logical command before loading configuration."""
    namespace = build_parser().parse_args(argv)
    source = cast(str, namespace.source)
    source_date = cast(date, namespace.source_date)
    if not is_supported_source(source):
        raise CliUsageError("source must be one supported PostgreSQL snapshot source")

    if namespace.command == "run":
        return RunCommand(source=source, source_date=source_date)
    if namespace.command == "resume":
        try:
            descriptor = SnapshotDescriptor.from_resume_arguments(
                source_name=source,
                source_date=source_date,
                upper_updated_at=cast(str | None, namespace.upper_updated_at),
                upper_order_id=cast(int | None, namespace.upper_order_id),
                expected_record_count=cast(int | None, namespace.expected_record_count),
                empty=cast(bool, namespace.empty),
            )
        except (TypeError, ValueError) as error:
            raise CliUsageError("resume descriptor is not canonical or complete") from error
        return ResumeCommand(descriptor=descriptor)
    raise CliUsageError("a command is required")


def load_config(environ: Mapping[str, str] | None) -> AppConfig:
    """Load configuration explicitly so imports remain side-effect free."""
    return AppConfig.from_env(environ)


def _emit(payload: dict[str, object], *, output: TextIO) -> None:
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True), file=output, flush=True)


def _emit_error(*, code: str, message: str, output: TextIO) -> None:
    _emit(
        {
            "event": "cli_error",
            "error_code": code,
            "message": message,
        },
        output=output,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    output: TextIO | None = None,
) -> int:
    """Run or resume one bounded snapshot and map outcomes to stable exits."""
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    destination = sys.stdout if output is None else output

    try:
        command = parse_command(arguments)
    except CliUsageError as error:
        _emit_error(code="invalid_arguments", message=str(error), output=destination)
        return CliExitCode.USAGE

    try:
        config = load_config(environ)
    except ConfigurationError as error:
        _emit_error(code="configuration_error", message=str(error), output=destination)
        return CliExitCode.CONFIGURATION

    if isinstance(command, ResumeCommand) and (
        command.descriptor.expected_record_count > config.snapshot.max_records
        or command.descriptor.expected_chunk_count(chunk_rows=config.snapshot.chunk_rows)
        > config.snapshot.max_chunks
    ):
        _emit_error(
            code="invalid_arguments",
            message="resume descriptor exceeds configured snapshot bounds",
            output=destination,
        )
        return CliExitCode.USAGE

    try:
        client = create_s3_client(config.storage)
    except (BotoCoreError, ClientError, OSError, RuntimeError):
        _emit_error(
            code="runtime_error",
            message="PostgreSQL snapshot runtime setup failed",
            output=destination,
        )
        return CliExitCode.RUNTIME_ERROR

    failed_event_emitted = False

    def event_sink(event: dict[str, object]) -> None:
        nonlocal failed_event_emitted
        if event.get("event") == "snapshot_failed":
            failed_event_emitted = True
        _emit(event, output=destination)

    try:
        with closing(client):
            storage = S3Storage(client=client, bucket=config.storage.bucket)
            resume_descriptor = command.descriptor if isinstance(command, ResumeCommand) else None
            try:
                ingest_order_snapshot(
                    storage=storage,
                    postgres_config=config.postgres,
                    snapshot_config=config.snapshot,
                    source_name=(
                        command.descriptor.source_name
                        if isinstance(command, ResumeCommand)
                        else command.source
                    ),
                    source_date=(
                        command.descriptor.source_date
                        if isinstance(command, ResumeCommand)
                        else command.source_date
                    ),
                    resume_descriptor=resume_descriptor,
                    event_sink=event_sink,
                )
            except SnapshotHandledError as error:
                if not failed_event_emitted:
                    _emit_error(
                        code=error.code.value,
                        message="PostgreSQL snapshot ingestion did not complete",
                        output=destination,
                    )
                return CliExitCode.INGESTION_FAILED
    except (BotoCoreError, ClientError, OSError, RuntimeError, StorageError):
        _emit_error(
            code="runtime_error",
            message="PostgreSQL snapshot runtime or cleanup failed",
            output=destination,
        )
        return CliExitCode.RUNTIME_ERROR

    return CliExitCode.SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())
