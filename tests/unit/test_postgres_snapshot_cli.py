"""Strict PostgreSQL snapshot CLI parsing, output, exits, and cleanup."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from batch_ingestion.config import (
    AppConfig,
    FileConfig,
    HttpConfig,
    PostgresConfig,
    SnapshotConfig,
    StorageConfig,
)
from batch_ingestion.postgres_snapshot import (
    SnapshotDescriptor,
    SnapshotErrorCode,
    SnapshotHandledError,
)


def _config() -> AppConfig:
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
            max_file_bytes=1024,
            spool_threshold_bytes=512,
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
    def __init__(self, *, close_error: Exception | None = None) -> None:
        self.closed = False
        self.close_error = close_error

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def _json_lines(captured: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in captured.splitlines() if line]


def _patch_success_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_ClosableS3Client, list[dict[str, object]]]:
    from batch_ingestion.postgres_snapshot import cli

    client = _ClosableS3Client()
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(cli, "load_config", lambda environ: _config())
    monkeypatch.setattr(cli, "create_s3_client", lambda config: client)
    monkeypatch.setattr(cli, "S3Storage", lambda **kwargs: SimpleNamespace())

    def ingest(**kwargs: object) -> object:
        calls.append(dict(kwargs))
        source_date = kwargs["source_date"]
        assert isinstance(source_date, date)
        descriptor = kwargs["resume_descriptor"]
        if descriptor is None:
            descriptor = SnapshotDescriptor.create(
                source_name="orders",
                source_date=source_date,
                upper_updated_at=datetime(2026, 9, 1, 14, 11, tzinfo=UTC),
                upper_order_id=20260901024,
                expected_record_count=24,
            )
        assert isinstance(descriptor, SnapshotDescriptor)
        sink = kwargs["event_sink"]
        assert callable(sink)
        sink(
            {
                "event": "snapshot_started",
                "source_name": descriptor.source_name,
                "source_date": descriptor.source_date.isoformat(),
                **descriptor.resume_fields(),
            }
        )
        sink(
            {
                "event": "run_summary",
                "source_date": descriptor.source_date.isoformat(),
                "batch_id": "batch_" + "1" * 64,
                "run_id": "run_" + "1" * 32,
            }
        )
        return SimpleNamespace(descriptor=descriptor)

    monkeypatch.setattr(cli, "ingest_order_snapshot", ingest)
    return client, calls


def test_run_emits_ndjson_delegates_configs_and_closes_s3_client(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.postgres_snapshot.cli import CliExitCode, main

    client, calls = _patch_success_runtime(monkeypatch)

    exit_code = main(
        ["run", "--source", "orders", "--date", "2026-09-01"],
        environ={},
    )

    assert exit_code == CliExitCode.SUCCESS
    assert client.closed
    assert len(calls) == 1
    assert calls[0]["postgres_config"] == _config().postgres
    assert calls[0]["snapshot_config"] == _config().snapshot
    assert calls[0]["source_name"] == "orders"
    assert calls[0]["source_date"] == date(2026, 9, 1)
    assert calls[0]["resume_descriptor"] is None
    assert [line["event"] for line in _json_lines(capsys.readouterr().out)] == [
        "snapshot_started",
        "run_summary",
    ]


@pytest.mark.parametrize(
    ("argv", "expected_descriptor"),
    [
        (
            [
                "resume",
                "--source",
                "orders",
                "--date",
                "2026-09-01",
                "--upper-updated-at",
                "2026-09-01T14:11:00.000000Z",
                "--upper-order-id",
                "20260901024",
                "--expected-record-count",
                "24",
            ],
            SnapshotDescriptor.create(
                source_name="orders",
                source_date=date(2026, 9, 1),
                upper_updated_at=datetime(2026, 9, 1, 14, 11, tzinfo=UTC),
                upper_order_id=20260901024,
                expected_record_count=24,
            ),
        ),
        (
            [
                "resume",
                "--source",
                "orders",
                "--date",
                "2026-09-02",
                "--empty",
            ],
            SnapshotDescriptor.empty(source_name="orders", source_date=date(2026, 9, 2)),
        ),
    ],
)
def test_resume_reconstructs_exact_descriptor_before_opening_clients(
    argv: list[str],
    expected_descriptor: SnapshotDescriptor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from batch_ingestion.postgres_snapshot.cli import CliExitCode, main

    client, calls = _patch_success_runtime(monkeypatch)

    assert main(argv, environ={}) == CliExitCode.SUCCESS
    assert calls[0]["resume_descriptor"] == expected_descriptor
    assert client.closed


@pytest.mark.parametrize(
    "argv",
    [
        ["run", "--source", "other", "--date", "2026-09-01"],
        ["run", "--source", "orders", "--date", "2026-9-1"],
        ["run", "--source", "orders", "--date", "2026-09-01", "--empty"],
        ["resume", "--source", "orders", "--date", "2026-09-01"],
        [
            "resume",
            "--source",
            "orders",
            "--date",
            "2026-09-01",
            "--empty",
            "--expected-record-count",
            "1",
        ],
        [
            "resume",
            "--source",
            "orders",
            "--date",
            "2026-09-01",
            "--upper-updated-at",
            "2026-09-01T14:11:00Z",
            "--upper-order-id",
            "20260901024",
            "--expected-record-count",
            "24",
        ],
        [
            "resume",
            "--source",
            "orders",
            "--date",
            "2026-09-01",
            "--upper-updated-at",
            "2026-09-01T14:11:00.000000Z",
            "--upper-order-id",
            "20260901024",
        ],
        [
            "resume",
            "--source",
            "orders",
            "--date",
            "2026-09-01",
            "--upper-updated-at",
            "2026-09-02T00:00:00.000000Z",
            "--upper-order-id",
            "20260901024",
            "--expected-record-count",
            "24",
        ],
        [
            "resume",
            "--source",
            "orders",
            "--date",
            "2026-09-01",
            "--upper-updated-at",
            "2026-09-01T14:11:00.000000Z",
            "--upper-order-id",
            "-0",
            "--expected-record-count",
            "24",
        ],
    ],
)
def test_invalid_arguments_fail_before_configuration_or_client_creation(
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.postgres_snapshot import cli

    called = False

    def unexpected_config(environ: object) -> AppConfig:
        nonlocal called
        called = True
        raise AssertionError("configuration must not be loaded")

    monkeypatch.setattr(cli, "load_config", unexpected_config)

    assert cli.main(argv, environ={}) == cli.CliExitCode.USAGE
    assert not called
    assert _json_lines(capsys.readouterr().out)[0]["error_code"] == "invalid_arguments"


def test_resume_over_configured_bound_fails_before_client_creation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.postgres_snapshot import cli

    created = False
    monkeypatch.setattr(cli, "load_config", lambda environ: _config())

    def unexpected_client(config: StorageConfig) -> object:
        nonlocal created
        created = True
        raise AssertionError("client must not be created")

    monkeypatch.setattr(cli, "create_s3_client", unexpected_client)

    exit_code = cli.main(
        [
            "resume",
            "--source",
            "orders",
            "--date",
            "2026-09-01",
            "--upper-updated-at",
            "2026-09-01T14:11:00.000000Z",
            "--upper-order-id",
            "20260901024",
            "--expected-record-count",
            "101",
        ],
        environ={},
    )

    assert exit_code == cli.CliExitCode.USAGE
    assert not created
    assert _json_lines(capsys.readouterr().out)[0]["error_code"] == "invalid_arguments"


def test_configuration_failure_has_stable_exit_before_client_creation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.config import ConfigurationError
    from batch_ingestion.postgres_snapshot import cli

    created = False

    def invalid_config(environ: object) -> AppConfig:
        raise ConfigurationError("invalid configuration")

    def unexpected_client(config: StorageConfig) -> object:
        nonlocal created
        created = True
        raise AssertionError("client must not be created")

    monkeypatch.setattr(cli, "load_config", invalid_config)
    monkeypatch.setattr(cli, "create_s3_client", unexpected_client)

    assert (
        cli.main(
            ["run", "--source", "orders", "--date", "2026-09-01"],
            environ={},
        )
        == cli.CliExitCode.CONFIGURATION
    )
    assert not created
    assert _json_lines(capsys.readouterr().out)[0]["error_code"] == "configuration_error"


def test_handled_ingestion_failure_is_secret_free_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.postgres_snapshot import cli

    client, _ = _patch_success_runtime(monkeypatch)

    def fail(**kwargs: object) -> object:
        raise SnapshotHandledError(SnapshotErrorCode.POSTGRES_CONNECTION_FAILED)

    monkeypatch.setattr(cli, "ingest_order_snapshot", fail)

    exit_code = cli.main(
        ["run", "--source", "orders", "--date", "2026-09-01"],
        environ={"POSTGRES_PASSWORD": "do-not-print-me"},
    )

    assert exit_code == cli.CliExitCode.INGESTION_FAILED
    assert client.closed
    output = capsys.readouterr().out
    assert "do-not-print-me" not in output
    assert _json_lines(output)[0] == {
        "event": "cli_error",
        "error_code": "postgres_connection_failed",
        "message": "PostgreSQL snapshot ingestion did not complete",
    }


@pytest.mark.parametrize("failure_point", ["setup", "ingest", "cleanup"])
def test_unexpected_runtime_failures_have_stable_exit_and_cleanup(
    failure_point: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion.postgres_snapshot import cli

    secret = "sensitive-runtime-detail"
    client = _ClosableS3Client(close_error=OSError(secret) if failure_point == "cleanup" else None)
    monkeypatch.setattr(cli, "load_config", lambda environ: _config())

    if failure_point == "setup":
        monkeypatch.setattr(
            cli,
            "create_s3_client",
            lambda config: (_ for _ in ()).throw(OSError(secret)),
        )
    else:
        monkeypatch.setattr(cli, "create_s3_client", lambda config: client)
        monkeypatch.setattr(cli, "S3Storage", lambda **kwargs: SimpleNamespace())
        if failure_point == "ingest":
            monkeypatch.setattr(
                cli,
                "ingest_order_snapshot",
                lambda **kwargs: (_ for _ in ()).throw(RuntimeError(secret)),
            )
        else:
            monkeypatch.setattr(
                cli,
                "ingest_order_snapshot",
                lambda **kwargs: SimpleNamespace(),
            )

    exit_code = cli.main(
        ["run", "--source", "orders", "--date", "2026-09-01"],
        environ={},
    )

    assert exit_code == cli.CliExitCode.RUNTIME_ERROR
    if failure_point != "setup":
        assert client.closed
    output = capsys.readouterr().out
    assert secret not in output
    assert _json_lines(output)[-1]["error_code"] == "runtime_error"


def test_make_exposes_postgres_snapshot_command() -> None:
    makefile = (Path(__file__).resolve().parents[2] / "Makefile").read_text(encoding="utf-8")

    assert "ingest-postgres: check-local-env" in makefile
    assert "python -m batch_ingestion.postgres_snapshot.cli $(ARGS)" in makefile
