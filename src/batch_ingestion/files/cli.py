"""Explicit JSON-emitting command boundary for supplier-file ingestion."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass
from datetime import date
from enum import IntEnum
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, Never, TextIO, cast

from botocore.exceptions import BotoCoreError, ClientError

from batch_ingestion.config import AppConfig, ConfigurationError, FileConfig
from batch_ingestion.storage import StorageError, storage_session

from .contracts import DeliveryDescriptor, parse_landing_key
from .events import FileOutcome
from .service import FileIngestionError, FileStorage, ingest_file_delivery

_CHECKSUM_LENGTH = 64
_LOCAL_READ_CHUNK_SIZE = 64 * 1024


class CliExitCode(IntEnum):
    """Stable process exit codes shared by ingestion command boundaries."""

    SUCCESS = 0
    USAGE = 2
    CONFIGURATION = 3
    INGESTION_FAILED = 4
    RUNTIME_ERROR = 5


class CliUsageError(ValueError):
    """Raised when file intake cannot safely enter the canonical contract."""


@dataclass(frozen=True, slots=True)
class RunCommand:
    source: str
    source_date: date
    path: Path
    expected_sha256: str | None


@dataclass(frozen=True, slots=True)
class ResumeCommand:
    delivery: DeliveryDescriptor


@dataclass(frozen=True, slots=True)
class _AdmittedLocalFile:
    delivery: DeliveryDescriptor
    stream: BinaryIO
    content_length: int


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


def _expected_checksum(value: str) -> str:
    if len(value) != _CHECKSUM_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise argparse.ArgumentTypeError(
            "expected checksum must be 64 lowercase hexadecimal characters"
        )
    return value


def _landing_delivery(value: str) -> DeliveryDescriptor:
    try:
        return parse_landing_key(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("landing key must be one canonical BI-060 key") from error


def build_parser() -> argparse.ArgumentParser:
    """Build strict run/resume forms without configuration or I/O."""
    parser = _ArgumentParser(prog="batch-ingestion-file")
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run")
    run_parser.add_argument("--source", required=True)
    run_parser.add_argument("--date", dest="source_date", type=_canonical_date, required=True)
    run_parser.add_argument("--path", required=True)
    run_parser.add_argument("--expected-sha256", type=_expected_checksum)

    resume_parser = commands.add_parser("resume")
    resume_parser.add_argument("--landing-key", type=_landing_delivery, required=True)
    return parser


def parse_command(argv: Sequence[str]) -> RunCommand | ResumeCommand:
    """Parse command syntax and reconstruct resume identity before client creation."""
    namespace = build_parser().parse_args(argv)
    if namespace.command == "run":
        return RunCommand(
            source=cast(str, namespace.source),
            source_date=cast(date, namespace.source_date),
            path=Path(cast(str, namespace.path)),
            expected_sha256=cast(str | None, namespace.expected_sha256),
        )
    if namespace.command == "resume":
        return ResumeCommand(delivery=cast(DeliveryDescriptor, namespace.landing_key))
    raise CliUsageError("a command is required")


def load_config(environ: Mapping[str, str] | None) -> AppConfig:
    """Load configuration explicitly so imports remain side-effect free."""
    return AppConfig.from_env(environ)


@contextmanager
def _admit_local_file(command: RunCommand, config: FileConfig) -> Iterator[_AdmittedLocalFile]:
    """Open and checksum one bounded regular file without exposing its path."""
    try:
        with command.path.open("rb") as stream:
            mode = os.fstat(stream.fileno()).st_mode
            if not stat.S_ISREG(mode) or not stream.seekable():
                raise CliUsageError("path must identify one seekable regular file")

            digest = sha256()
            content_length = 0
            while chunk := stream.read(_LOCAL_READ_CHUNK_SIZE):
                content_length += len(chunk)
                if content_length > config.max_file_bytes:
                    raise CliUsageError("local file exceeds FILE_MAX_BYTES")
                digest.update(chunk)
            stream.seek(0)

            try:
                delivery = DeliveryDescriptor.create(
                    source_name=command.source,
                    source_date=command.source_date,
                    original_file_name=command.path.name,
                    actual_sha256=digest.hexdigest(),
                    expected_sha256=command.expected_sha256,
                )
            except (TypeError, ValueError) as error:
                raise CliUsageError("local file admission values are not canonical") from error
            yield _AdmittedLocalFile(delivery, stream, content_length)
    except CliUsageError:
        raise
    except OSError as error:
        raise CliUsageError("local file is unavailable or unreadable") from error


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
    """Stage or resume one delivery and map every handled outcome to a stable exit."""
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

    admission = (
        _admit_local_file(command, config.file)
        if isinstance(command, RunCommand)
        else nullcontext(None)
    )
    try:
        with admission as local_file:
            delivery = command.delivery if isinstance(command, ResumeCommand) else None
            if local_file is not None:
                delivery = local_file.delivery
            assert delivery is not None

            def storage_session_provider() -> AbstractContextManager[FileStorage]:
                return storage_session(config.storage)

            try:
                result = ingest_file_delivery(
                    storage_session_provider=storage_session_provider,
                    config=config.file,
                    delivery=delivery,
                    landing_stream=local_file.stream if local_file is not None else None,
                    landing_content_length=(
                        local_file.content_length if local_file is not None else None
                    ),
                    event_sink=lambda event: _emit(event, output=destination),
                )
            except FileIngestionError:
                return CliExitCode.INGESTION_FAILED
            except (BotoCoreError, ClientError, OSError, StorageError):
                _emit_error(
                    code="runtime_error",
                    message="File ingestion runtime setup or cleanup failed",
                    output=destination,
                )
                return CliExitCode.RUNTIME_ERROR

            if result.event.outcome is FileOutcome.REJECTED:
                return CliExitCode.INGESTION_FAILED
            return CliExitCode.SUCCESS
    except CliUsageError as error:
        _emit_error(code="invalid_arguments", message=str(error), output=destination)
        return CliExitCode.USAGE


if __name__ == "__main__":
    raise SystemExit(main())
