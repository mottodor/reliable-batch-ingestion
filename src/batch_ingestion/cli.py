"""Explicit JSON-emitting command boundary for REST API ingestion."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date
from enum import IntEnum
from typing import Never, TextIO, cast

import httpx
from botocore.exceptions import BotoCoreError

from batch_ingestion.api.client import open_http_client
from batch_ingestion.api.service import (
    ApiIngestionError,
    ApiStorage,
    ingest_api_backfill,
    ingest_api_date,
    validate_backfill_range,
)
from batch_ingestion.api.sources import get_source_descriptor
from batch_ingestion.config import AppConfig, ConfigurationError
from batch_ingestion.storage import StorageError, storage_session

_SOURCE_CHOICES = ("fx-rates", "shipping-statuses")


class CliExitCode(IntEnum):
    """Stable process exit codes for the ingestion command boundary."""

    SUCCESS = 0
    USAGE = 2
    CONFIGURATION = 3
    INGESTION_FAILED = 4
    RUNTIME_ERROR = 5


class CliUsageError(ValueError):
    """Raised for command arguments that cannot safely start ingestion."""


@dataclass(frozen=True, slots=True)
class RunCommand:
    source: str
    source_date: date


@dataclass(frozen=True, slots=True)
class BackfillCommand:
    source: str
    from_date: date
    to_date: date


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


def build_parser() -> argparse.ArgumentParser:
    """Build the two-form parser without reading configuration or opening clients."""
    parser = _ArgumentParser(prog="batch-ingestion-api")
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run")
    run_parser.add_argument("--source", choices=_SOURCE_CHOICES, required=True)
    run_parser.add_argument("--date", dest="source_date", type=_canonical_date, required=True)

    backfill_parser = commands.add_parser("backfill")
    backfill_parser.add_argument("--source", choices=_SOURCE_CHOICES, required=True)
    backfill_parser.add_argument("--from", dest="from_date", type=_canonical_date, required=True)
    backfill_parser.add_argument("--to", dest="to_date", type=_canonical_date, required=True)
    return parser


def parse_command(argv: Sequence[str]) -> RunCommand | BackfillCommand:
    """Parse strict allow-listed command arguments and reject reversed ranges."""
    namespace = build_parser().parse_args(argv)
    source = cast(str, namespace.source)
    if namespace.command == "run":
        return RunCommand(source=source, source_date=cast(date, namespace.source_date))
    if namespace.command == "backfill":
        from_date = cast(date, namespace.from_date)
        to_date = cast(date, namespace.to_date)
        try:
            validate_backfill_range(from_date=from_date, to_date=to_date, max_days=10**9)
        except ValueError as error:
            raise CliUsageError(str(error)) from error
        return BackfillCommand(source=source, from_date=from_date, to_date=to_date)
    raise CliUsageError("a command is required")


def load_config(environ: Mapping[str, str] | None) -> AppConfig:
    """Load configuration explicitly so tests can prove pre-client validation."""
    return AppConfig.from_env(environ)


def _emit(payload: dict[str, object], *, output: TextIO) -> None:
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True), file=output, flush=True)


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    output: TextIO | None = None,
) -> int:
    """Run one command, always mapping handled outcomes to stable exit codes."""
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    destination = sys.stdout if output is None else output

    try:
        command = parse_command(arguments)
    except CliUsageError as error:
        _emit(
            {
                "event": "cli_error",
                "error_code": "invalid_arguments",
                "message": str(error),
            },
            output=destination,
        )
        return CliExitCode.USAGE

    try:
        config = load_config(environ)
    except ConfigurationError as error:
        _emit(
            {
                "event": "cli_error",
                "error_code": "configuration_error",
                "message": str(error),
            },
            output=destination,
        )
        return CliExitCode.CONFIGURATION

    if isinstance(command, BackfillCommand):
        try:
            day_count = validate_backfill_range(
                from_date=command.from_date,
                to_date=command.to_date,
                max_days=config.http.max_backfill_days,
            )
        except ValueError as error:
            _emit(
                {
                    "event": "cli_error",
                    "error_code": "invalid_arguments",
                    "message": str(error),
                },
                output=destination,
            )
            return CliExitCode.USAGE
    else:
        day_count = 1

    descriptor = get_source_descriptor(command.source)

    def event_sink(event: dict[str, object]) -> None:
        _emit(event, output=destination)

    def storage_session_provider() -> AbstractContextManager[ApiStorage]:
        return storage_session(config.storage)

    try:
        with open_http_client(config.http) as http_client:
            if isinstance(command, RunCommand):
                ingest_api_date(
                    client=http_client,
                    storage_session_provider=storage_session_provider,
                    http_config=config.http,
                    descriptor=descriptor,
                    source_date=command.source_date,
                    event_sink=event_sink,
                )
            else:
                ingest_api_backfill(
                    client=http_client,
                    storage_session_provider=storage_session_provider,
                    http_config=config.http,
                    descriptor=descriptor,
                    from_date=command.from_date,
                    to_date=command.to_date,
                    event_sink=event_sink,
                )
                _emit(
                    {
                        "event": "api_backfill",
                        "status": "succeeded",
                        "source": command.source,
                        "from_date": command.from_date.isoformat(),
                        "to_date": command.to_date.isoformat(),
                        "dates_completed": day_count,
                    },
                    output=destination,
                )
    except ApiIngestionError:
        return CliExitCode.INGESTION_FAILED
    except (BotoCoreError, httpx.HTTPError, OSError, StorageError):
        _emit(
            {
                "event": "cli_error",
                "error_code": "runtime_error",
                "message": "API ingestion runtime setup or cleanup failed",
            },
            output=destination,
        )
        return CliExitCode.RUNTIME_ERROR

    return CliExitCode.SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())
