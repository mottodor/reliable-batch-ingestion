"""REST ingestion CLI argument, lifecycle, output, and exit-code contracts."""

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import date
from typing import Any

import pytest

from batch_ingestion.api.service import (
    ApiErrorCode,
    ApiIngestionError,
    ApiIngestionResult,
    ApiRunEvent,
    ApiRunStatus,
)
from batch_ingestion.api.sources import ApiSourceDescriptor
from batch_ingestion.cli import CliExitCode, main
from batch_ingestion.config import (
    AppConfig,
    FileConfig,
    HttpConfig,
    PostgresConfig,
    SnapshotConfig,
    StorageConfig,
)
from batch_ingestion.storage import StorageBackendError
from batch_ingestion.summary import RunSummary


def _config(*, max_backfill_days: int = 31) -> AppConfig:
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
            max_backfill_days=max_backfill_days,
        ),
        file=FileConfig(
            max_file_bytes=1_024,
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


def _result(source_date: date) -> ApiIngestionResult:
    batch_id = "1" * 64
    run_id = "run_" + "2" * 32
    summary = RunSummary(
        records_read=1,
        records_accepted=1,
        records_rejected=0,
        pages_fetched=1,
        bytes_written=10,
        retry_count=0,
        duplicate_count=0,
        duration_seconds=0.1,
        source_date=source_date,
        batch_id=batch_id,
        run_id=run_id,
    )
    event = ApiRunEvent(
        status=ApiRunStatus.SUCCEEDED,
        source_date=source_date,
        batch_id=batch_id,
        run_id=run_id,
        page=None,
        error_code=None,
        http_status=None,
        classification=None,
        message="API ingestion succeeded",
    )
    return ApiIngestionResult(
        summary,
        event,
        (),
        (),
        "manifests/api/test/logical-run.manifest.json",
        (),
    )


class _SessionState:
    def __init__(self) -> None:
        self.closed = False


class _Storage:
    pass


def _patch_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    config: AppConfig | None = None,
) -> tuple[list[str], _SessionState]:
    from batch_ingestion import cli

    lifecycle: list[str] = []
    session_state = _SessionState()
    monkeypatch.setattr(cli, "load_config", lambda environ: config or _config())

    @contextmanager
    def open_storage(storage_config: StorageConfig) -> Iterator[object]:
        del storage_config
        lifecycle.append("storage_open")
        try:
            yield _Storage()
        finally:
            session_state.closed = True
            lifecycle.append("storage_close")

    @contextmanager
    def open_client(http_config: HttpConfig) -> Iterator[object]:
        lifecycle.append("http_open")
        try:
            yield object()
        finally:
            lifecycle.append("http_close")

    monkeypatch.setattr(cli, "storage_session", open_storage)
    monkeypatch.setattr(cli, "open_http_client", open_client)
    return lifecycle, session_state


def _json_lines(captured: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in captured.splitlines() if line]


def test_run_command_uses_allow_list_emits_json_and_closes_both_clients(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion import cli

    lifecycle, session_state = _patch_runtime(monkeypatch)
    observed: dict[str, object] = {}

    def ingest_date(**kwargs: Any) -> ApiIngestionResult:
        observed.update(kwargs)
        with kwargs["storage_session_provider"]():
            pass
        source_date = kwargs["source_date"]
        assert isinstance(source_date, date)
        result = _result(source_date)
        kwargs["event_sink"](result.event.to_dict())
        kwargs["event_sink"]({"event": "run_summary", **result.summary.to_dict()})
        return result

    monkeypatch.setattr(cli, "ingest_api_date", ingest_date)

    exit_code = main(["run", "--source", "fx-rates", "--date", "2026-09-01"], environ={})

    assert exit_code == CliExitCode.SUCCESS
    assert observed["source_date"] == date(2026, 9, 1)
    descriptor = observed["descriptor"]
    assert isinstance(descriptor, ApiSourceDescriptor)
    assert descriptor.name == "fx-rates"
    assert lifecycle == ["http_open", "storage_open", "storage_close", "http_close"]
    assert session_state.closed is True
    assert [item["event"] for item in _json_lines(capsys.readouterr().out)] == [
        "api_ingestion",
        "run_summary",
    ]


def test_backfill_command_passes_inclusive_range_to_service(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion import cli

    lifecycle, session_state = _patch_runtime(monkeypatch)
    observed: dict[str, object] = {}

    def ingest_backfill(**kwargs: Any) -> object:
        observed.update(kwargs)
        with kwargs["storage_session_provider"]():
            pass
        return object()

    monkeypatch.setattr(cli, "ingest_api_backfill", ingest_backfill)

    exit_code = main(
        [
            "backfill",
            "--source",
            "shipping-statuses",
            "--from",
            "2026-09-01",
            "--to",
            "2026-09-03",
        ],
        environ={},
    )

    assert exit_code == CliExitCode.SUCCESS
    assert observed["from_date"] == date(2026, 9, 1)
    assert observed["to_date"] == date(2026, 9, 3)
    descriptor = observed["descriptor"]
    assert isinstance(descriptor, ApiSourceDescriptor)
    assert descriptor.name == "shipping-statuses"
    assert lifecycle == ["http_open", "storage_open", "storage_close", "http_close"]
    assert session_state.closed is True
    assert _json_lines(capsys.readouterr().out) == [
        {
            "event": "api_backfill",
            "status": "succeeded",
            "source": "shipping-statuses",
            "from_date": "2026-09-01",
            "to_date": "2026-09-03",
            "dates_completed": 3,
        }
    ]


@pytest.mark.parametrize(
    "argv",
    [
        ["run", "--source", "unknown", "--date", "2026-09-01"],
        ["run", "--source", "fx-rates", "--date", "2026-9-1"],
        [
            "backfill",
            "--source",
            "fx-rates",
            "--from",
            "2026-09-03",
            "--to",
            "2026-09-01",
        ],
    ],
)
def test_invalid_arguments_return_stable_usage_exit_with_structured_output(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(argv, environ={})

    assert exit_code == CliExitCode.USAGE
    output = _json_lines(capsys.readouterr().out)
    assert output[0]["event"] == "cli_error"
    assert output[0]["error_code"] == "invalid_arguments"


def test_maximum_range_is_rejected_before_opening_clients(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    lifecycle, session_state = _patch_runtime(monkeypatch, config=_config(max_backfill_days=2))

    exit_code = main(
        [
            "backfill",
            "--source",
            "fx-rates",
            "--from",
            "2026-09-01",
            "--to",
            "2026-09-03",
        ],
        environ={},
    )

    assert exit_code == CliExitCode.USAGE
    assert lifecycle == []
    assert session_state.closed is False
    assert _json_lines(capsys.readouterr().out)[0]["error_code"] == "invalid_arguments"


def test_configuration_failure_has_stable_exit_and_does_not_print_environment_values(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion import cli
    from batch_ingestion.config import ConfigurationError

    secret = "do-not-print-this-secret"

    def reject(environ: Mapping[str, str] | None) -> AppConfig:
        raise ConfigurationError("configuration is invalid")

    monkeypatch.setattr(cli, "load_config", reject)

    exit_code = main(
        ["run", "--source", "fx-rates", "--date", "2026-09-01"],
        environ={"S3_SECRET_KEY": secret},
    )

    assert exit_code == CliExitCode.CONFIGURATION
    output = capsys.readouterr().out
    assert secret not in output
    assert _json_lines(output)[0]["error_code"] == "configuration_error"


def test_storage_session_setup_failure_uses_source_terminal_pair_and_handled_exit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion import cli

    secret = "do-not-print-runtime-secret"
    monkeypatch.setattr(cli, "load_config", lambda environ: _config())

    @contextmanager
    def fail_to_open(storage_config: StorageConfig) -> Iterator[object]:
        del storage_config
        try:
            raise OSError(secret)
        except OSError as cause:
            raise StorageBackendError("storage backend operation failed") from cause
        yield object()

    monkeypatch.setattr(cli, "storage_session", fail_to_open)

    @contextmanager
    def open_client(http_config: HttpConfig) -> Iterator[object]:
        del http_config
        yield object()

    monkeypatch.setattr(cli, "open_http_client", open_client)

    exit_code = main(
        ["run", "--source", "fx-rates", "--date", "2026-09-01"],
        environ={},
    )

    assert exit_code == CliExitCode.INGESTION_FAILED
    output = capsys.readouterr().out
    assert secret not in output
    records = _json_lines(output)
    assert [record["event"] for record in records] == ["api_ingestion", "run_summary"]
    assert records[0]["error_code"] == "storage_error"


def test_http_runtime_setup_failure_keeps_generic_non_secret_exit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion import cli

    secret = "do-not-print-http-runtime-secret"
    monkeypatch.setattr(cli, "load_config", lambda environ: _config())

    @contextmanager
    def fail_to_open(http_config: HttpConfig) -> Iterator[object]:
        del http_config
        raise OSError(secret)
        yield object()

    monkeypatch.setattr(cli, "open_http_client", fail_to_open)

    exit_code = main(
        ["run", "--source", "fx-rates", "--date", "2026-09-01"],
        environ={},
    )

    assert exit_code == CliExitCode.RUNTIME_ERROR
    output = capsys.readouterr().out
    assert secret not in output
    assert _json_lines(output) == [
        {
            "event": "cli_error",
            "error_code": "runtime_error",
            "message": "API ingestion runtime setup or cleanup failed",
        }
    ]


def test_ingestion_failure_returns_stable_exit_and_closes_clients(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from batch_ingestion import cli

    lifecycle, session_state = _patch_runtime(monkeypatch)

    def fail(**kwargs: Any) -> ApiIngestionResult:
        with kwargs["storage_session_provider"]():
            pass
        source_date = kwargs["source_date"]
        assert isinstance(source_date, date)
        result = _result(source_date)
        failure_event = ApiRunEvent(
            status=ApiRunStatus.FAILED,
            source_date=source_date,
            batch_id=result.summary.batch_id,
            run_id=result.summary.run_id,
            page=1,
            error_code=ApiErrorCode.HTTP_PERMANENT_CLIENT_ERROR,
            http_status=404,
            classification=None,
            message="API request failed permanently",
        )
        kwargs["event_sink"](failure_event.to_dict())
        kwargs["event_sink"]({"event": "run_summary", **result.summary.to_dict()})
        raise ApiIngestionError(
            code=ApiErrorCode.HTTP_PERMANENT_CLIENT_ERROR,
            summary=result.summary,
            event=failure_event,
        )

    monkeypatch.setattr(cli, "ingest_api_date", fail)

    exit_code = main(["run", "--source", "fx-rates", "--date", "2026-09-01"], environ={})

    assert exit_code == CliExitCode.INGESTION_FAILED
    assert lifecycle == ["http_open", "storage_open", "storage_close", "http_close"]
    assert session_state.closed is True
    assert [item["event"] for item in _json_lines(capsys.readouterr().out)] == [
        "api_ingestion",
        "run_summary",
    ]
