#!/usr/bin/env python3
"""Private BI-080 worker for deliberate, allow-listed recovery boundaries."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, cast

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from batch_ingestion.api.client import open_http_client
from batch_ingestion.api.service import ApiIngestionError, ingest_api_date
from batch_ingestion.api.sources import get_source_descriptor
from batch_ingestion.config import AppConfig, ConfigurationError, PostgresConfig
from batch_ingestion.files.cli import RunCommand, _admit_local_file
from batch_ingestion.files.service import FileIngestionStateError, ingest_file_delivery
from batch_ingestion.postgres_snapshot.events import SnapshotErrorCode, SnapshotHandledError
from batch_ingestion.postgres_snapshot.reader import (
    OrderSnapshotRead,
    create_postgres_connection,
    open_order_snapshot,
)
from batch_ingestion.postgres_snapshot.service import ingest_order_snapshot
from batch_ingestion.storage import S3Storage, StorageError, create_s3_client
from scripts.demo_faults import DeleteBoundaryClient, ManifestBoundaryStorage

_WORKER_SCENARIOS = (
    "api-permanent-500",
    "api-malformed-json",
    "api-after-page",
    "file-during-move",
    "postgres-connection-drop",
    "payload-before-manifest",
)
_API_DATES = {
    "api-permanent-500": date(2026, 9, 3),
    "api-malformed-json": date(2026, 9, 4),
    "api-after-page": date(2026, 9, 5),
    "payload-before-manifest": date(2026, 9, 6),
}
_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _emit(event: Mapping[str, object]) -> None:
    print(json.dumps(dict(event), separators=(",", ":"), sort_keys=True), flush=True)


def _run_api(scenario: str, config: AppConfig) -> int:
    source_date = _API_DATES[scenario]
    _emit(
        {
            "event": "demo_worker_descriptor",
            "scenario": scenario,
            "source_name": "fx-rates",
            "source_date": source_date.isoformat(),
        }
    )
    raw_client = create_s3_client(config.storage)
    with contextlib.closing(raw_client), open_http_client(config.http) as http_client:
        base_storage = S3Storage(client=raw_client, bucket=config.storage.bucket)
        storage: Any = base_storage
        parameters = None
        if scenario == "api-permanent-500":

            def permanent_500(_: int) -> dict[str, str]:
                return {"scenario": "permanent_500"}

            parameters = permanent_500
        elif scenario == "api-malformed-json":

            def malformed_json(_: int) -> dict[str, str]:
                return {"scenario": "malformed_json", "fault_page": "1"}

            parameters = malformed_json
        elif scenario == "api-after-page":
            storage = ManifestBoundaryStorage(base_storage, after_manifest=True)
        elif scenario == "payload-before-manifest":
            storage = ManifestBoundaryStorage(base_storage, after_manifest=False)
        try:
            ingest_api_date(
                client=http_client,
                storage=storage,
                http_config=config.http,
                descriptor=get_source_descriptor("fx-rates"),
                source_date=source_date,
                query_parameter_provider=parameters,
                event_sink=_emit,
            )
        except ApiIngestionError:
            return 4
    return 0


def _run_file_move(config: AppConfig) -> int:
    path = (
        _REPOSITORY_ROOT
        / "fixtures/supplier-prices/same-name-delivery-a/supplier-prices-2026-09-01.csv"
    )
    digest = hashlib.sha256()
    with path.open("rb") as fixture_stream:
        while chunk := fixture_stream.read(64 * 1024):
            digest.update(chunk)
    with _admit_local_file(
        RunCommand(
            source="supplier-prices",
            source_date=date(2026, 9, 1),
            path=path,
            expected_sha256=digest.hexdigest(),
        ),
        config.file,
    ) as local_file:
        delivery = local_file.delivery
        _emit(
            {
                "event": "demo_worker_descriptor",
                "scenario": "file-during-move",
                "source_name": delivery.source_name,
                "source_date": delivery.source_date.isoformat(),
                "batch_id": delivery.batch_id,
                "landing_key": delivery.landing_key,
                "raw_key": delivery.raw_key,
                "manifest_key": delivery.manifest_key,
                "disposition_key": delivery.archive_key,
            }
        )
        raw_client = create_s3_client(config.storage)
        proxy = DeleteBoundaryClient(
            raw_client,
            source_key=delivery.landing_key,
            destination_key=delivery.archive_key,
        )
        with contextlib.closing(raw_client):
            storage = S3Storage(client=cast(Any, proxy), bucket=config.storage.bucket)
            local_file.stream.seek(0)
            storage.write_immutable(
                delivery.landing_key,
                local_file.stream,
                content_length=local_file.content_length,
                checksum_sha256=delivery.actual_sha256,
                content_type=delivery.content_type,
            )
            ingest_file_delivery(
                storage=storage,
                config=config.file,
                delivery=delivery,
                event_sink=_emit,
            )
    return 0


def _terminate_backend(config: PostgresConfig, backend_pid: int) -> None:
    with create_postgres_connection(config) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))
            if cursor.fetchone() != (True,):
                raise RuntimeError("backend termination was not acknowledged")


class _CapturingOpener:
    def __init__(self) -> None:
        self.backend_pid: int | None = None

    @contextmanager
    def __call__(self, **kwargs: object) -> Iterator[OrderSnapshotRead]:
        with open_order_snapshot(**kwargs) as snapshot:  # type: ignore[arg-type]
            self.backend_pid = snapshot.cursor.connection.info.backend_pid
            yield snapshot


def _run_postgres_drop(config: AppConfig) -> int:
    opener = _CapturingOpener()
    terminated = False
    client = create_s3_client(config.storage)

    def event_sink(event: dict[str, object]) -> None:
        nonlocal terminated
        _emit(event)
        if event.get("event") == "snapshot_chunk_completed" and not terminated:
            if opener.backend_pid is None:
                raise RuntimeError("snapshot backend PID is unavailable")
            _terminate_backend(config.postgres, opener.backend_pid)
            terminated = True

    try:
        ingest_order_snapshot(
            storage=S3Storage(client=client, bucket=config.storage.bucket),
            postgres_config=config.postgres,
            snapshot_config=config.snapshot,
            source_name="orders",
            source_date=date(2026, 9, 1),
            snapshot_opener=opener,
            event_sink=event_sink,
        )
    except SnapshotHandledError as error:
        if terminated and error.code is SnapshotErrorCode.POSTGRES_CONNECTION_FAILED:
            return 86
        return 4
    finally:
        client.close()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="demo_worker.py", exit_on_error=False)
    parser.add_argument("--scenario", choices=_WORKER_SCENARIOS, required=True)
    try:
        arguments = parser.parse_args(sys.argv[1:] if argv is None else list(argv))
        config = AppConfig.from_env()
        scenario = str(arguments.scenario)
        if scenario in _API_DATES:
            return _run_api(scenario, config)
        if scenario == "file-during-move":
            return _run_file_move(config)
        if scenario == "postgres-connection-drop":
            return _run_postgres_drop(config)
    except (argparse.ArgumentError, argparse.ArgumentTypeError, ConfigurationError, ValueError):
        return 2
    except (
        BotoCoreError,
        ClientError,
        FileIngestionStateError,
        StorageError,
        OSError,
        psycopg.Error,
        RuntimeError,
    ):
        return 5
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
