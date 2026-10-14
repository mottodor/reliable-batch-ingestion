"""Live Garage coverage for restart-safe supplier-file service outcomes."""

from __future__ import annotations

import io
import json
import os
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from batch_ingestion.config import AppConfig
from batch_ingestion.files import (
    DeliveryDescriptor,
    DuplicateClass,
    FileOutcome,
    RejectionCode,
    ingest_file_delivery,
)
from batch_ingestion.files import cli as file_cli
from batch_ingestion.manifests import ManifestStatus
from batch_ingestion.storage import ReconciliationState, S3Storage, create_s3_client

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name:
            raise ValueError(f"invalid environment line for {name or '<unknown>'}")
        values[name] = value
    return values


@pytest.fixture(scope="module")
def app_config() -> AppConfig:
    configured_path = Path(os.environ.get("BI060_ENV_FILE", ".env"))
    env_path = (
        configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path
    )
    return AppConfig.from_env(_load_dotenv(env_path))


@pytest.fixture(scope="module")
def storage(app_config: AppConfig) -> Iterator[S3Storage]:
    client = create_s3_client(app_config.storage)
    with closing(client):
        yield S3Storage(client=client, bucket=app_config.storage.bucket)


def _delete_keys(app_config: AppConfig, keys: tuple[str, ...]) -> None:
    client = create_s3_client(app_config.storage)
    with closing(client):
        for key in keys:
            client.delete_object(Bucket=app_config.storage.bucket, Key=key)


def _delivery(payload: bytes, source_date: date) -> DeliveryDescriptor:
    return DeliveryDescriptor.create(
        source_name="supplier-prices",
        source_date=source_date,
        original_file_name=f"supplier-prices-{source_date.isoformat()}.csv",
        actual_sha256=sha256(payload).hexdigest(),
    )


def _all_test_keys(delivery: DeliveryDescriptor) -> tuple[str, ...]:
    return (
        delivery.landing_key,
        delivery.raw_key,
        delivery.manifest_key,
        delivery.archive_key,
        *(delivery.rejected_key(code) for code in RejectionCode),
    )


@pytest.mark.parametrize(
    ("source_date", "price", "expected_outcome", "expected_status", "expected_reason"),
    [
        (
            date(2026, 9, 3),
            "1250",
            FileOutcome.ACCEPTED,
            ManifestStatus.SUCCEEDED,
            None,
        ),
        (
            date(2026, 9, 4),
            "0",
            FileOutcome.REJECTED,
            ManifestStatus.REJECTED,
            RejectionCode.BUSINESS_RULE_VIOLATION,
        ),
    ],
)
def test_live_valid_and_rejected_files_preserve_exact_raw_and_disposition_bytes(
    app_config: AppConfig,
    storage: S3Storage,
    source_date: date,
    price: str,
    expected_outcome: FileOutcome,
    expected_status: ManifestStatus,
    expected_reason: RejectionCode | None,
) -> None:
    payload = (
        "supplier_id,product_id,price_minor_units,currency,effective_date\n"
        f"SUP-001,SKU-0001,{price},USD,{source_date.isoformat()}\n"
    ).encode()
    checksum = sha256(payload).hexdigest()
    delivery = _delivery(payload, source_date)
    disposition_key = (
        delivery.archive_key if expected_reason is None else delivery.rejected_key(expected_reason)
    )
    test_keys = _all_test_keys(delivery)
    _delete_keys(app_config, test_keys)
    try:
        storage.write_immutable(
            delivery.landing_key,
            io.BytesIO(payload),
            content_length=len(payload),
            checksum_sha256=checksum,
            content_type=delivery.content_type,
        )

        ticks = iter((1.0, 1.25))
        result = ingest_file_delivery(
            storage=storage,
            config=app_config.file,
            delivery=delivery,
            monotonic=lambda: next(ticks),
            utc_now=lambda: datetime(2026, 9, 5, tzinfo=UTC),
            run_id_factory=lambda: "run_33333333333333333333333333333333",
        )

        assert result.event.outcome is expected_outcome
        assert result.event.rejection_code is expected_reason
        assert result.manifest.status is expected_status
        assert (
            storage.inspect_object(
                delivery.landing_key,
                content_length=len(payload),
                checksum_sha256=checksum,
            )
            is None
        )
        for key in (delivery.raw_key, disposition_key):
            with storage.download_spooled(
                key,
                maximum_bytes=len(payload),
                spool_threshold_bytes=max(1, len(payload) // 2),
                expected_content_length=len(payload),
                expected_checksum_sha256=checksum,
            ) as downloaded:
                assert downloaded.stream.read() == payload

        reconciled = storage.reconcile_payload_manifest(
            delivery.raw_key,
            content_length=len(payload),
            checksum_sha256=checksum,
        )
        assert reconciled.state is ReconciliationState.COMPLETE
        assert reconciled.manifest == result.manifest
    finally:
        _delete_keys(app_config, test_keys)


def _fixture_path(scenario: str, file_format: str) -> Path:
    fixture_root = REPOSITORY_ROOT / "fixtures" / "supplier-prices" / "canonical"
    return fixture_root / f"{scenario}.{file_format}"


def _named_fixture(kind: str, label: str, file_format: str) -> Path:
    return (
        REPOSITORY_ROOT
        / "fixtures"
        / "supplier-prices"
        / kind
        / f"supplier-prices-{label}2026-09-01.{file_format}"
    )


def _parse_output(output: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in output.getvalue().splitlines() if line]


def _cli_run(
    *,
    app_config: AppConfig,
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
) -> tuple[int, list[dict[str, object]]]:
    monkeypatch.setattr(file_cli, "load_config", lambda environ: app_config)
    output = io.StringIO()
    exit_code = file_cli.main(arguments, environ={}, output=output)
    return exit_code, _parse_output(output)


def _path_delivery(
    path: Path,
    *,
    source_name: str = "supplier-prices",
    expected_sha256: str | None = None,
) -> DeliveryDescriptor:
    payload = path.read_bytes()
    return DeliveryDescriptor.create(
        source_name=source_name,
        source_date=date(2026, 9, 1),
        original_file_name=path.name,
        actual_sha256=sha256(payload).hexdigest(),
        expected_sha256=expected_sha256,
    )


@pytest.mark.parametrize("file_format", ["csv", "json", "parquet"])
def test_live_cli_all_formats_and_both_duplicate_classes_preserve_one_canonical_batch(
    app_config: AppConfig,
    storage: S3Storage,
    monkeypatch: pytest.MonkeyPatch,
    file_format: str,
) -> None:
    original_path = _named_fixture("same-name-delivery-a", "", file_format)
    same_name_path = _named_fixture("same-name-delivery-b", "", file_format)
    renamed_path = _named_fixture("renamed-content", "renamed-", file_format)
    payload = original_path.read_bytes()
    assert same_name_path.read_bytes() == renamed_path.read_bytes() == payload
    original = _path_delivery(original_path)
    renamed = _path_delivery(renamed_path)
    test_keys = tuple(dict.fromkeys((*_all_test_keys(original), *_all_test_keys(renamed))))
    _delete_keys(app_config, test_keys)
    try:
        first_exit, first_lines = _cli_run(
            app_config=app_config,
            monkeypatch=monkeypatch,
            arguments=[
                "run",
                "--source",
                "supplier-prices",
                "--date",
                "2026-09-01",
                "--path",
                str(original_path),
            ],
        )
        assert first_exit == file_cli.CliExitCode.SUCCESS
        first_event, first_summary = first_lines
        assert first_event["outcome"] == FileOutcome.ACCEPTED
        assert first_event["duplicate_class"] == DuplicateClass.NEW_DELIVERY
        assert first_summary["records_read"] == 15
        assert first_summary["records_accepted"] == 15
        assert first_summary["records_rejected"] == 0
        assert first_summary["bytes_written"] == len(payload)
        assert first_summary["duplicate_count"] == 0

        reconciliation = storage.reconcile_payload_manifest(
            original.raw_key,
            content_length=len(payload),
            checksum_sha256=original.actual_sha256,
        )
        assert reconciliation.state is ReconciliationState.COMPLETE
        assert reconciliation.manifest is not None
        original_manifest = reconciliation.manifest
        original_run_id = original_manifest.run_id
        with storage.download_spooled(
            original.archive_key,
            maximum_bytes=len(payload),
            spool_threshold_bytes=max(1, len(payload) // 2),
            expected_content_length=len(payload),
            expected_checksum_sha256=original.actual_sha256,
        ) as downloaded:
            assert downloaded.stream.read() == payload

        same_exit, same_lines = _cli_run(
            app_config=app_config,
            monkeypatch=monkeypatch,
            arguments=[
                "run",
                "--source",
                "supplier-prices",
                "--date",
                "2026-09-01",
                "--path",
                str(same_name_path),
            ],
        )
        assert same_exit == file_cli.CliExitCode.SUCCESS
        assert same_lines[0]["duplicate_class"] == DuplicateClass.SAME_NAME_REDELIVERY
        assert same_lines[1]["bytes_written"] == 0
        assert same_lines[1]["duplicate_count"] == 1

        renamed_exit, renamed_lines = _cli_run(
            app_config=app_config,
            monkeypatch=monkeypatch,
            arguments=[
                "run",
                "--source",
                "supplier-prices",
                "--date",
                "2026-09-01",
                "--path",
                str(renamed_path),
            ],
        )
        assert renamed_exit == file_cli.CliExitCode.SUCCESS
        assert renamed_lines[0]["duplicate_class"] == DuplicateClass.RENAMED_CONTENT
        assert renamed_lines[1]["bytes_written"] == 0
        assert renamed_lines[1]["duplicate_count"] == 1

        final = storage.reconcile_payload_manifest(
            original.raw_key,
            content_length=len(payload),
            checksum_sha256=original.actual_sha256,
        )
        assert final.state is ReconciliationState.COMPLETE
        assert final.manifest == original_manifest
        assert final.manifest is not None and final.manifest.run_id == original_run_id
        for landing_key in (original.landing_key, renamed.landing_key):
            assert (
                storage.inspect_object(
                    landing_key,
                    content_length=len(payload),
                    checksum_sha256=original.actual_sha256,
                )
                is None
            )
    finally:
        _delete_keys(app_config, test_keys)


@pytest.mark.parametrize("file_format", ["csv", "json", "parquet"])
@pytest.mark.parametrize(
    ("scenario", "expected_reason"),
    [
        ("empty", RejectionCode.EMPTY_FILE),
        ("schema_invalid", RejectionCode.SCHEMA_MISMATCH),
        ("mixed", RejectionCode.BUSINESS_RULE_VIOLATION),
        ("malformed", RejectionCode.MALFORMED_FILE),
        ("checksum", RejectionCode.CHECKSUM_MISMATCH),
        ("unsupported", RejectionCode.UNSUPPORTED_SOURCE),
    ],
)
def test_live_cli_rejections_retain_exact_bytes_with_reason_and_whole_file_counters(
    app_config: AppConfig,
    storage: S3Storage,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    file_format: str,
    scenario: str,
    expected_reason: RejectionCode,
) -> None:
    target = tmp_path / f"supplier-prices-2026-09-01.{file_format}"
    if scenario in {"empty", "schema_invalid", "mixed"}:
        target.write_bytes(_fixture_path(scenario, file_format).read_bytes())
    elif scenario in {"checksum", "unsupported"}:
        target.write_bytes(_fixture_path("valid", file_format).read_bytes())
    elif file_format == "csv":
        target.write_bytes(b"\xff")
    elif file_format == "json":
        target.write_bytes(b"{")
    else:
        target.write_bytes(b"not-a-parquet-file")

    expected_checksum = "0" * 64 if scenario == "checksum" else None
    source_name = "unsupported" if scenario == "unsupported" else "supplier-prices"
    delivery = _path_delivery(
        target,
        source_name=source_name,
        expected_sha256=expected_checksum,
    )
    payload = target.read_bytes()
    test_keys = _all_test_keys(delivery)
    _delete_keys(app_config, test_keys)
    arguments = [
        "run",
        "--source",
        source_name,
        "--date",
        "2026-09-01",
        "--path",
        str(target),
    ]
    if expected_checksum is not None:
        arguments.extend(("--expected-sha256", expected_checksum))
    try:
        exit_code, lines = _cli_run(
            app_config=app_config,
            monkeypatch=monkeypatch,
            arguments=arguments,
        )

        assert exit_code == file_cli.CliExitCode.INGESTION_FAILED
        event, summary = lines
        assert event["outcome"] == FileOutcome.REJECTED
        assert event["rejection_code"] == expected_reason
        rejected_key = delivery.rejected_key(expected_reason)
        assert event["disposition_key"] == rejected_key
        if scenario == "mixed":
            assert (
                summary["records_read"],
                summary["records_accepted"],
                summary["records_rejected"],
            ) == (3, 0, 3)

        for key in (delivery.raw_key, rejected_key):
            with storage.download_spooled(
                key,
                maximum_bytes=max(1, len(payload)),
                spool_threshold_bytes=max(1, len(payload)),
                expected_content_length=len(payload),
                expected_checksum_sha256=delivery.actual_sha256,
            ) as downloaded:
                assert downloaded.stream.read() == payload
        assert (
            storage.inspect_object(
                delivery.landing_key,
                content_length=len(payload),
                checksum_sha256=delivery.actual_sha256,
            )
            is None
        )
        reconciliation = storage.reconcile_payload_manifest(
            delivery.raw_key,
            content_length=len(payload),
            checksum_sha256=delivery.actual_sha256,
        )
        assert reconciliation.state is ReconciliationState.COMPLETE
        assert reconciliation.manifest is not None
        assert reconciliation.manifest.status is ManifestStatus.REJECTED
        assert (
            storage.inspect_object(
                delivery.archive_key,
                content_length=len(payload),
                checksum_sha256=delivery.actual_sha256,
            )
            is None
        )
        for other_reason in RejectionCode:
            if other_reason is expected_reason:
                continue
            assert (
                storage.inspect_object(
                    delivery.rejected_key(other_reason),
                    content_length=len(payload),
                    checksum_sha256=delivery.actual_sha256,
                )
                is None
            )
    finally:
        _delete_keys(app_config, test_keys)


class _DeleteOnceClient:
    def __init__(self, client: Any) -> None:
        self._client = client
        self.delete_attempts = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def delete_object(self, **kwargs: Any) -> dict[str, object]:
        self.delete_attempts += 1
        if self.delete_attempts == 1:
            raise ClientError(
                {"Error": {"Code": "ServiceUnavailable", "Message": "injected"}},
                "DeleteObject",
            )
        return cast(dict[str, object], self._client.delete_object(**kwargs))


@pytest.mark.parametrize("file_format", ["csv", "json", "parquet"])
def test_live_resume_converges_copy_then_delete_interruption_for_every_format(
    app_config: AppConfig,
    storage: S3Storage,
    monkeypatch: pytest.MonkeyPatch,
    file_format: str,
) -> None:
    path = _named_fixture("same-name-delivery-a", "", file_format)
    payload = path.read_bytes()
    delivery = _path_delivery(path)
    test_keys = _all_test_keys(delivery)
    _delete_keys(app_config, test_keys)
    client = create_s3_client(app_config.storage)
    try:
        storage.write_immutable(
            delivery.landing_key,
            io.BytesIO(payload),
            content_length=len(payload),
            checksum_sha256=delivery.actual_sha256,
            content_type=delivery.content_type,
        )
        proxy = _DeleteOnceClient(client)
        interrupted_storage = S3Storage(
            client=cast(Any, proxy),
            bucket=app_config.storage.bucket,
        )
        with pytest.raises(ClientError, match="ServiceUnavailable"):
            ingest_file_delivery(
                storage=interrupted_storage,
                config=app_config.file,
                delivery=delivery,
            )

        assert proxy.delete_attempts == 1
        for key in (delivery.landing_key, delivery.raw_key, delivery.archive_key):
            with storage.download_spooled(
                key,
                maximum_bytes=len(payload),
                spool_threshold_bytes=max(1, len(payload) // 2),
                expected_content_length=len(payload),
                expected_checksum_sha256=delivery.actual_sha256,
            ) as downloaded:
                assert downloaded.stream.read() == payload
        assert (
            storage.reconcile_payload_manifest(
                delivery.raw_key,
                content_length=len(payload),
                checksum_sha256=delivery.actual_sha256,
            ).state
            is ReconciliationState.PAYLOAD_ONLY
        )

        exit_code, lines = _cli_run(
            app_config=app_config,
            monkeypatch=monkeypatch,
            arguments=["resume", "--landing-key", delivery.landing_key],
        )
        assert exit_code == file_cli.CliExitCode.SUCCESS
        assert lines[0]["outcome"] == FileOutcome.ACCEPTED
        assert (
            storage.inspect_object(
                delivery.landing_key,
                content_length=len(payload),
                checksum_sha256=delivery.actual_sha256,
            )
            is None
        )
        assert (
            storage.reconcile_payload_manifest(
                delivery.raw_key,
                content_length=len(payload),
                checksum_sha256=delivery.actual_sha256,
            ).state
            is ReconciliationState.COMPLETE
        )
    finally:
        client.close()
        _delete_keys(app_config, test_keys)
