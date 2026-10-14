"""File-ingestion CLI admission, staging, output, exits, and cleanup."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import BinaryIO

import pytest

from batch_ingestion.config import (
    AppConfig,
    FileConfig,
    HttpConfig,
    PostgresConfig,
    SnapshotConfig,
    StorageConfig,
)
from batch_ingestion.files import DeliveryDescriptor, FileOutcome
from batch_ingestion.storage import ImmutableWriteResult, ImmutableWriteStatus, StoredObject


def _config(*, maximum_bytes: int = 1024 * 1024) -> AppConfig:
    return AppConfig(
        postgres=PostgresConfig("localhost", 5432, "db", "user", "password"),
        storage=StorageConfig(
            endpoint_url="http://s3.invalid",
            region="test",
            bucket="bucket",
            access_key="access",
            secret_key="secret",
        ),
        http=HttpConfig(
            base_url="http://api.invalid",
            connect_timeout_seconds=1.0,
            read_timeout_seconds=1.0,
            max_attempts=2,
            backoff_base_seconds=0.1,
            backoff_max_seconds=0.2,
            max_elapsed_seconds=1.0,
            page_size=2,
            max_pages=10,
            max_response_bytes=1024,
            max_backfill_days=31,
        ),
        file=FileConfig(
            max_file_bytes=maximum_bytes,
            spool_threshold_bytes=min(512, maximum_bytes),
            max_records=100,
            parquet_batch_rows=10,
            max_diagnostic_details=10,
        ),
        snapshot=SnapshotConfig(
            max_elapsed_seconds=10.0,
            chunk_rows=10,
            max_chunks=10,
            max_chunk_bytes=1024,
            spool_threshold_bytes=512,
        ),
        log_level="INFO",
    )


class _ClosableS3Client:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@dataclass
class _RecordingStorage:
    staged_key: str | None = None
    staged_body: bytes | None = None
    staged_stream: BinaryIO | None = None

    def write_immutable(
        self,
        key: str,
        stream: BinaryIO,
        *,
        content_length: int,
        checksum_sha256: str,
        content_type: str,
    ) -> ImmutableWriteResult:
        body = stream.read()
        stream.seek(0)
        assert len(body) == content_length
        assert sha256(body).hexdigest() == checksum_sha256
        self.staged_key = key
        self.staged_body = body
        self.staged_stream = stream
        stored = StoredObject(
            key=key,
            content_length=content_length,
            checksum_sha256=checksum_sha256,
            content_type=content_type,
            metadata={"checksum-sha256": checksum_sha256},
        )
        return ImmutableWriteResult(ImmutableWriteStatus.CREATED, stored)


def _json_lines(captured: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in captured.splitlines() if line]


def _patch_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    config: AppConfig | None = None,
) -> tuple[_RecordingStorage, _ClosableS3Client, list[DeliveryDescriptor]]:
    from batch_ingestion.files import cli

    storage = _RecordingStorage()
    client = _ClosableS3Client()
    deliveries: list[DeliveryDescriptor] = []
    monkeypatch.setattr(cli, "load_config", lambda environ: config or _config())
    monkeypatch.setattr(cli, "create_s3_client", lambda storage_config: client)
    monkeypatch.setattr(cli, "S3Storage", lambda **kwargs: storage)

    def ingest(**kwargs: object) -> object:
        delivery = kwargs["delivery"]
        assert isinstance(delivery, DeliveryDescriptor)
        deliveries.append(delivery)
        sink = kwargs["event_sink"]
        assert callable(sink)
        outcome = FileOutcome.ACCEPTED
        if delivery.source_name == "unsupported":
            outcome = FileOutcome.REJECTED
        sink(
            {
                "event": "file_ingestion",
                "outcome": outcome.value,
                "duplicate_class": "new_delivery",
            }
        )
        sink(
            {
                "event": "run_summary",
                "source_date": delivery.source_date.isoformat(),
                "batch_id": delivery.batch_id,
                "run_id": "run_" + "1" * 32,
            }
        )
        return SimpleNamespace(event=SimpleNamespace(outcome=outcome))

    monkeypatch.setattr(cli, "ingest_file_delivery", ingest)
    return storage, client, deliveries


def test_run_stages_exact_local_bytes_emits_ndjson_and_closes_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.files.cli import CliExitCode, main

    payload = b"supplier_id,product_id,price_minor_units,currency,effective_date\n"
    source = tmp_path / "supplier-prices-2026-09-01.csv"
    source.write_bytes(payload)
    storage, client, deliveries = _patch_runtime(monkeypatch)

    exit_code = main(
        [
            "run",
            "--source",
            "supplier-prices",
            "--date",
            "2026-09-01",
            "--path",
            str(source),
            "--expected-sha256",
            sha256(payload).hexdigest(),
        ],
        environ={},
    )

    assert exit_code == CliExitCode.SUCCESS
    assert len(deliveries) == 1
    delivery = deliveries[0]
    assert storage.staged_key == delivery.landing_key
    assert storage.staged_body == payload
    assert storage.staged_stream is not None and storage.staged_stream.closed
    assert client.closed
    assert [line["event"] for line in _json_lines(capsys.readouterr().out)] == [
        "file_ingestion",
        "run_summary",
    ]


def test_resume_strictly_parses_landing_key_without_opening_a_local_file(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.files.cli import CliExitCode, main

    delivery = DeliveryDescriptor.create(
        source_name="supplier-prices",
        source_date=date(2026, 9, 1),
        original_file_name="supplier-prices-2026-09-01.parquet",
        actual_sha256="a" * 64,
    )
    storage, client, deliveries = _patch_runtime(monkeypatch)

    exit_code = main(["resume", "--landing-key", delivery.landing_key], environ={})

    assert exit_code == CliExitCode.SUCCESS
    assert deliveries == [delivery]
    assert storage.staged_key is None
    assert client.closed
    assert _json_lines(capsys.readouterr().out)[0]["event"] == "file_ingestion"


@pytest.mark.parametrize(
    "argv",
    [
        ["run", "--source", "supplier-prices", "--date", "2026-9-1", "--path", "x"],
        [
            "run",
            "--source",
            "supplier-prices",
            "--date",
            "2026-09-01",
            "--path",
            "x",
            "--expected-sha256",
            "A" * 64,
        ],
        ["resume", "--landing-key", "landing/not-a-bi060-key"],
    ],
)
def test_invalid_arguments_fail_before_configuration_or_client_creation(
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.files import cli

    called = False

    def unexpected_config(environ: object) -> AppConfig:
        nonlocal called
        called = True
        raise AssertionError("configuration must not be loaded")

    monkeypatch.setattr(cli, "load_config", unexpected_config)

    assert cli.main(argv, environ={}) == cli.CliExitCode.USAGE
    assert not called
    assert _json_lines(capsys.readouterr().out)[0]["error_code"] == "invalid_arguments"


@pytest.mark.parametrize("kind", ["missing", "directory", "oversized", "unsafe_name"])
def test_run_admission_failure_creates_no_client_or_s3_state(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.files import cli

    path = tmp_path / "supplier-prices-2026-09-01.csv"
    config = _config(maximum_bytes=4)
    if kind == "directory":
        path.mkdir()
    elif kind == "oversized":
        path.write_bytes(b"12345")
    elif kind == "unsafe_name":
        path = tmp_path / "Supplier Prices.csv"
        path.write_bytes(b"1")
    created = False
    monkeypatch.setattr(cli, "load_config", lambda environ: config)

    def unexpected_client(storage_config: StorageConfig) -> object:
        nonlocal created
        created = True
        raise AssertionError("client must not be created")

    monkeypatch.setattr(cli, "create_s3_client", unexpected_client)

    exit_code = cli.main(
        [
            "run",
            "--source",
            "supplier-prices",
            "--date",
            "2026-09-01",
            "--path",
            str(path),
        ],
        environ={},
    )

    assert exit_code == cli.CliExitCode.USAGE
    assert not created
    output = capsys.readouterr().out
    assert str(tmp_path) not in output
    assert _json_lines(output)[0]["error_code"] == "invalid_arguments"


def test_rejected_delivery_returns_handled_exit_and_still_closes_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from batch_ingestion.files.cli import CliExitCode, main

    source = tmp_path / "supplier-prices-2026-09-01.csv"
    source.write_bytes(b"payload")
    _, client, _ = _patch_runtime(monkeypatch)

    exit_code = main(
        [
            "run",
            "--source",
            "unsupported",
            "--date",
            "2026-09-01",
            "--path",
            str(source),
        ],
        environ={},
    )

    assert exit_code == CliExitCode.INGESTION_FAILED
    assert client.closed


def test_configuration_and_runtime_failures_have_stable_secret_free_exits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.config import ConfigurationError
    from batch_ingestion.files import cli

    source = tmp_path / "supplier-prices-2026-09-01.csv"
    source.write_bytes(b"payload")
    argv = [
        "run",
        "--source",
        "supplier-prices",
        "--date",
        "2026-09-01",
        "--path",
        str(source),
    ]
    secret = "do-not-print-me"

    def invalid_config(environ: object) -> AppConfig:
        raise ConfigurationError("configuration is invalid")

    monkeypatch.setattr(cli, "load_config", invalid_config)
    assert cli.main(argv, environ={"S3_SECRET_KEY": secret}) == cli.CliExitCode.CONFIGURATION
    assert secret not in capsys.readouterr().out

    monkeypatch.setattr(cli, "load_config", lambda environ: _config())

    def failed_client(storage_config: StorageConfig) -> object:
        raise OSError(secret)

    monkeypatch.setattr(cli, "create_s3_client", failed_client)
    assert cli.main(argv, environ={}) == cli.CliExitCode.RUNTIME_ERROR
    output = capsys.readouterr().out
    assert secret not in output
    assert _json_lines(output)[0]["error_code"] == "runtime_error"


def test_make_exposes_file_ingestion_command() -> None:
    makefile = (Path(__file__).resolve().parents[2] / "Makefile").read_text(encoding="utf-8")

    assert "ingest-file: check-local-env" in makefile
    assert "python -m batch_ingestion.files.cli $(ARGS)" in makefile


def test_recoverable_ingestion_failure_is_structured_and_closes_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.files import cli
    from batch_ingestion.files.service import FileIngestionStateError

    source = tmp_path / "supplier-prices-2026-09-01.csv"
    source.write_bytes(b"payload")
    _, client, _ = _patch_runtime(monkeypatch)

    def fail(**kwargs: object) -> object:
        raise FileIngestionStateError("sensitive backend detail")

    monkeypatch.setattr(cli, "ingest_file_delivery", fail)
    exit_code = cli.main(
        [
            "run",
            "--source",
            "supplier-prices",
            "--date",
            "2026-09-01",
            "--path",
            str(source),
        ],
        environ={},
    )

    assert exit_code == cli.CliExitCode.INGESTION_FAILED
    assert client.closed
    output = capsys.readouterr().out
    assert "sensitive backend detail" not in output
    assert _json_lines(output)[0] == {
        "event": "cli_error",
        "error_code": "ingestion_failed",
        "message": "File ingestion did not reach a terminal disposition",
    }
