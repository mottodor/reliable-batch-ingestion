"""Pure descriptor, schema, event, and metric contracts for BI-070 slice 1."""

import importlib
import json
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from batch_ingestion.config import AppConfig
from batch_ingestion.postgres_snapshot import (
    ORDERS_ARROW_SCHEMA,
    ORDERS_SOURCE_NAME,
    PARQUET_CONTENT_TYPE,
    PARQUET_WRITER_OPTIONS,
    SNAPSHOT_CONTRACT,
    SnapshotChunkStatus,
    SnapshotCounters,
    SnapshotDescriptor,
    SnapshotErrorCode,
    SnapshotEvent,
    SnapshotHandledError,
    SnapshotOrderRow,
    chunk_payload_name,
    is_supported_source,
    parse_canonical_utc_timestamp,
)

SOURCE_DATE = date(2026, 9, 1)
UPPER_UPDATED_AT = datetime(2026, 9, 1, 14, 11, tzinfo=UTC)
UPPER_ORDER_ID = 202_609_010_024
RUN_ID = "run_1234567812344abc9234567890abcdef"


def _descriptor(
    *,
    source_date: date = SOURCE_DATE,
    upper_updated_at: datetime | None = UPPER_UPDATED_AT,
    upper_order_id: int | None = UPPER_ORDER_ID,
    expected_record_count: int = 24,
) -> SnapshotDescriptor:
    return SnapshotDescriptor.create(
        source_name=ORDERS_SOURCE_NAME,
        source_date=source_date,
        upper_updated_at=upper_updated_at,
        upper_order_id=upper_order_id,
        expected_record_count=expected_record_count,
    )


def test_orders_allow_list_and_utc_daily_window_are_fixed() -> None:
    descriptor = _descriptor()

    assert is_supported_source("orders") is True
    assert is_supported_source("customers") is False
    assert descriptor.window_start == datetime(2026, 9, 1, tzinfo=UTC)
    assert descriptor.window_end_exclusive == datetime(2026, 9, 2, tzinfo=UTC)
    assert descriptor.canonical_window_start == "2026-09-01T00:00:00.000000Z"
    assert descriptor.canonical_window_end_exclusive == "2026-09-02T00:00:00.000000Z"

    with pytest.raises(ValueError, match="unsupported snapshot source"):
        SnapshotDescriptor.create(
            source_name="customers",
            source_date=SOURCE_DATE,
            upper_updated_at=UPPER_UPDATED_AT,
            upper_order_id=UPPER_ORDER_ID,
            expected_record_count=24,
        )


def test_non_empty_and_empty_descriptors_have_canonical_resume_fields() -> None:
    non_empty = _descriptor()
    empty = SnapshotDescriptor.empty(source_name="orders", source_date=SOURCE_DATE)

    assert non_empty.is_empty is False
    assert non_empty.canonical_upper_updated_at == "2026-09-01T14:11:00.000000Z"
    assert non_empty.resume_fields() == {
        "empty": False,
        "upper_updated_at": "2026-09-01T14:11:00.000000Z",
        "upper_order_id": UPPER_ORDER_ID,
        "expected_record_count": 24,
    }
    assert empty.is_empty is True
    assert empty.resume_fields() == {
        "empty": True,
        "upper_updated_at": None,
        "upper_order_id": None,
        "expected_record_count": 0,
    }


def test_resume_arguments_require_exact_empty_or_complete_non_empty_descriptor() -> None:
    assert (
        SnapshotDescriptor.from_resume_arguments(
            source_name="orders",
            source_date=SOURCE_DATE,
            upper_updated_at="2026-09-01T14:11:00.000000Z",
            upper_order_id=UPPER_ORDER_ID,
            expected_record_count=24,
            empty=False,
        )
        == _descriptor()
    )
    assert SnapshotDescriptor.from_resume_arguments(
        source_name="orders",
        source_date=SOURCE_DATE,
        upper_updated_at=None,
        upper_order_id=None,
        expected_record_count=None,
        empty=True,
    ) == SnapshotDescriptor.empty(source_name="orders", source_date=SOURCE_DATE)

    incomplete_values = [
        ("2026-09-01T14:11:00.000000Z", None, 24, False),
        (None, UPPER_ORDER_ID, 24, False),
        ("2026-09-01T14:11:00.000000Z", UPPER_ORDER_ID, None, False),
        ("2026-09-01T14:11:00.000000Z", UPPER_ORDER_ID, 24, True),
    ]
    for upper_timestamp, upper_order_id, count, empty in incomplete_values:
        with pytest.raises(ValueError, match="resume descriptor"):
            SnapshotDescriptor.from_resume_arguments(
                source_name="orders",
                source_date=SOURCE_DATE,
                upper_updated_at=upper_timestamp,
                upper_order_id=upper_order_id,
                expected_record_count=count,
                empty=empty,
            )


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-01T14:11:00Z",
        "2026-09-01T14:11:00.000000+00:00",
        "2026-09-01T14:11:00.000000z",
        "2026-09-01 14:11:00.000000Z",
    ],
)
def test_resume_timestamp_must_already_use_canonical_utc_form(value: str) -> None:
    with pytest.raises(ValueError, match="canonical UTC"):
        parse_canonical_utc_timestamp(value)


def test_descriptor_rejects_inconsistent_count_upper_tuple_and_window() -> None:
    invalid = [
        (None, None, 1),
        (UPPER_UPDATED_AT, UPPER_ORDER_ID, 0),
        (UPPER_UPDATED_AT, None, 24),
        (None, UPPER_ORDER_ID, 24),
        (UPPER_UPDATED_AT, True, 24),
        (UPPER_UPDATED_AT.replace(tzinfo=None), UPPER_ORDER_ID, 24),
        (datetime(2026, 9, 2, tzinfo=UTC), UPPER_ORDER_ID, 24),
    ]
    for upper_updated_at, upper_order_id, count in invalid:
        with pytest.raises((TypeError, ValueError)):
            _descriptor(
                upper_updated_at=upper_updated_at,
                upper_order_id=upper_order_id,
                expected_record_count=count,
            )


def test_chunk_count_names_identity_and_keys_are_deterministic() -> None:
    descriptor = _descriptor()
    identity = descriptor.identity(chunk_rows=10)
    same = _descriptor().identity(chunk_rows=10)
    batch_id = descriptor.batch_id(chunk_rows=10)

    assert identity == same
    assert dict(identity.parameters) == {
        "chunk-rows": "10",
        "expected-record-count": "24",
        "snapshot-contract": SNAPSHOT_CONTRACT,
        "upper-order-id": str(UPPER_ORDER_ID),
        "upper-updated-at": "2026-09-01T14:11:00.000000Z",
        "window-end-exclusive": "2026-09-02T00:00:00.000000Z",
        "window-start": "2026-09-01T00:00:00.000000Z",
    }
    assert descriptor.expected_chunk_count(chunk_rows=10) == 3
    assert (
        SnapshotDescriptor.empty(
            source_name="orders", source_date=SOURCE_DATE
        ).expected_chunk_count(chunk_rows=10)
        == 1
    )
    assert chunk_payload_name(1) == "chunk-0001.parquet"
    assert chunk_payload_name(9_999) == "chunk-9999.parquet"
    assert descriptor.raw_key(chunk_ordinal=1, chunk_rows=10) == (
        "raw/postgres_snapshot/orders/source_date=2026-09-01/"
        f"batch_id={batch_id}/chunk-0001.parquet"
    )
    assert descriptor.manifest_key(chunk_ordinal=1, chunk_rows=10) == (
        "manifests/postgres_snapshot/orders/source_date=2026-09-01/"
        f"batch_id={batch_id}/chunk-0001.parquet.manifest.json"
    )

    with pytest.raises(ValueError, match="chunk_ordinal"):
        chunk_payload_name(0)
    with pytest.raises(ValueError, match="chunk_ordinal"):
        chunk_payload_name(10_000)
    with pytest.raises(ValueError, match="chunk_rows"):
        descriptor.identity(chunk_rows=True)
    oversized = _descriptor(expected_record_count=10_000)
    with pytest.raises(ValueError, match="more than 9999 addressable chunks"):
        oversized.expected_chunk_count(chunk_rows=1)
    with pytest.raises(ValueError, match="more than 9999 addressable chunks"):
        oversized.identity(chunk_rows=1)


def test_every_logical_selection_change_changes_the_batch() -> None:
    baseline = _descriptor().batch_id(chunk_rows=10)
    variants = [
        _descriptor(
            source_date=date(2026, 9, 2), upper_updated_at=datetime(2026, 9, 2, tzinfo=UTC)
        ).batch_id(chunk_rows=10),
        _descriptor(upper_updated_at=UPPER_UPDATED_AT + timedelta(microseconds=1)).batch_id(
            chunk_rows=10
        ),
        _descriptor(upper_order_id=UPPER_ORDER_ID + 1).batch_id(chunk_rows=10),
        _descriptor(expected_record_count=25).batch_id(chunk_rows=10),
        _descriptor().batch_id(chunk_rows=11),
        _descriptor().batch_id(chunk_rows=10, snapshot_contract="omni-retail-orders-parquet-v2"),
    ]

    assert all(candidate != baseline for candidate in variants)


def test_orders_arrow_schema_and_writer_options_are_exact() -> None:
    expected = pa.schema(
        [
            pa.field("order_id", pa.int64(), nullable=False),
            pa.field("customer_id", pa.int64(), nullable=False),
            pa.field("order_status", pa.string(), nullable=False),
            pa.field("shipping_status", pa.string(), nullable=False),
            pa.field("currency", pa.string(), nullable=False),
            pa.field("total_amount", pa.decimal128(12, 2), nullable=False),
            pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )

    assert expected == ORDERS_ARROW_SCHEMA
    assert ORDERS_ARROW_SCHEMA.metadata is None
    assert PARQUET_CONTENT_TYPE == "application/vnd.apache.parquet"
    assert PARQUET_WRITER_OPTIONS.as_kwargs() == {
        "version": "2.6",
        "data_page_version": "2.0",
        "compression": "NONE",
        "use_dictionary": False,
        "write_statistics": False,
    }
    empty = pa.Table.from_pylist([], schema=ORDERS_ARROW_SCHEMA)
    assert empty.num_rows == 0
    assert empty.schema == ORDERS_ARROW_SCHEMA


def test_order_row_validation_preserves_exact_values() -> None:
    values = (
        UPPER_ORDER_ID,
        10_123,
        "confirmed",
        "in_transit",
        "USD",
        Decimal("1234567890.12"),
        UPPER_UPDATED_AT,
    )

    row = SnapshotOrderRow.from_sequence(values)

    assert row.as_tuple() == values


@pytest.mark.parametrize(
    ("index", "value"),
    [
        (0, True),
        (0, 2**63),
        (1, None),
        (2, "unknown"),
        (3, "shipping"),
        (4, "usd"),
        (5, Decimal("1.2")),
        (5, Decimal("12345678901.00")),
        (5, Decimal("NaN")),
        (5, -Decimal("1.00")),
        (6, UPPER_UPDATED_AT.replace(tzinfo=None)),
        (6, UPPER_UPDATED_AT.astimezone(timezone(timedelta(hours=1)))),
    ],
)
def test_order_row_rejects_source_contract_violations(index: int, value: object) -> None:
    values: list[object] = [
        UPPER_ORDER_ID,
        10_123,
        "confirmed",
        "in_transit",
        "USD",
        Decimal("123.45"),
        UPPER_UPDATED_AT,
    ]
    values[index] = value

    with pytest.raises((TypeError, ValueError)):
        SnapshotOrderRow.from_sequence(values)


def test_snapshot_events_errors_and_metrics_are_structured_and_secret_free() -> None:
    descriptor = _descriptor()
    batch_id = descriptor.batch_id(chunk_rows=10)
    started = SnapshotEvent.started(
        descriptor=descriptor,
        chunk_rows=10,
        batch_id=batch_id,
        run_id=RUN_ID,
    )
    chunk = SnapshotEvent.chunk_completed(
        descriptor=descriptor,
        chunk_rows=10,
        batch_id=batch_id,
        run_id=RUN_ID,
        chunk_ordinal=1,
        object_key=descriptor.raw_key(chunk_ordinal=1, chunk_rows=10),
        chunk_status=SnapshotChunkStatus.CREATED,
        records=10,
        content_length=2048,
    )
    counters = SnapshotCounters(
        records_read=24,
        records_accepted=24,
        bytes_written=4096,
        duplicate_count=0,
    )
    succeeded = SnapshotEvent.succeeded(
        descriptor=descriptor,
        chunk_rows=10,
        batch_id=batch_id,
        run_id=RUN_ID,
        counters=counters,
    )
    failed = SnapshotEvent.failed(
        descriptor=descriptor,
        chunk_rows=10,
        batch_id=batch_id,
        run_id=RUN_ID,
        error_code=SnapshotErrorCode.POSTGRES_CONNECTION_FAILED,
        counters=SnapshotCounters(records_read=10),
    )

    assert started.to_dict()["event"] == "snapshot_started"
    assert started.to_dict()["upper_updated_at"] == "2026-09-01T14:11:00.000000Z"
    assert chunk.to_dict()["chunk_status"] == "created"
    assert chunk.to_dict()["chunk_ordinal"] == 1
    assert succeeded.to_dict()["records_accepted"] == 24
    assert failed.to_dict()["error_code"] == "postgres_connection_failed"
    serialized = "\n".join(event.to_json() for event in (started, chunk, succeeded, failed))
    assert "postgres-secret" not in serialized
    assert "postgresql://" not in serialized
    assert "SELECT " not in serialized
    assert all(
        json.loads(event.to_json()) == event.to_dict()
        for event in (started, chunk, succeeded, failed)
    )

    error = SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)
    assert str(error) == "source_snapshot_changed"
    assert error.code is SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED

    summary = counters.to_run_summary(
        duration_seconds=1.25,
        source_date=SOURCE_DATE,
        batch_id=batch_id,
        run_id=RUN_ID,
    )
    assert summary.records_rejected == 0
    assert summary.pages_fetched == 0
    assert summary.retry_count == 0
    assert summary.to_dict() == {
        "records_read": 24,
        "records_accepted": 24,
        "records_rejected": 0,
        "pages_fetched": 0,
        "bytes_written": 4096,
        "retry_count": 0,
        "duplicate_count": 0,
        "duration_seconds": 1.25,
        "source_date": "2026-09-01",
        "batch_id": batch_id,
        "run_id": RUN_ID,
    }


def test_postgres_snapshot_documentation_covers_public_contract_and_recovery() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    contract = (repository_root / "docs/ingestion/postgres-snapshot.md").read_text(encoding="utf-8")
    readme = (repository_root / "README.md").read_text(encoding="utf-8")
    descriptor = _descriptor()

    required_contract_text = {
        'make ingest-postgres ARGS="run',
        'make ingest-postgres ARGS="resume',
        descriptor.raw_key(chunk_ordinal=1, chunk_rows=10),
        descriptor.manifest_key(chunk_ordinal=1, chunk_rows=10),
        "POSTGRES_CONNECT_TIMEOUT_SECONDS",
        "POSTGRES_STATEMENT_TIMEOUT_SECONDS",
        "SNAPSHOT_MAX_ELAPSED_SECONDS",
        "SNAPSHOT_CHUNK_ROWS",
        "SNAPSHOT_MAX_CHUNKS",
        "SNAPSHOT_MAX_CHUNK_BYTES",
        "SNAPSHOT_SPOOL_THRESHOLD_BYTES",
        "REPEATABLE READ",
        "fetchmany",
        "payload_only",
        "source_snapshot_changed",
        "replay from chunk 1",
        "not an exactly-once claim",
        "concurrent writers",
    }
    required_contract_text.update(code.value for code in SnapshotErrorCode)

    assert all(value in contract for value in required_contract_text)
    assert "docs/ingestion/postgres-snapshot.md" in readme
    assert "PostgreSQL snapshot failure and recovery exercises" in readme


def test_snapshot_contract_import_does_not_read_configuration_or_open_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import batch_ingestion.postgres_snapshot.contracts as contracts_module

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("import performed runtime I/O or configuration loading")

    monkeypatch.setattr(AppConfig, "from_env", unexpected)
    importlib.reload(contracts_module)
