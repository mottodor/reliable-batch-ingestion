"""Bounded transactional reader contracts for BI-070 slice 2."""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import cast

import psycopg
import pytest
from psycopg import IsolationLevel

from batch_ingestion.config import PostgresConfig, SnapshotConfig
from batch_ingestion.postgres_snapshot import (
    ORDERS_SOURCE_NAME,
    SnapshotDescriptor,
    SnapshotErrorCode,
    SnapshotHandledError,
)
from batch_ingestion.postgres_snapshot.reader import (
    CAPTURE_ORDERS_SQL,
    EMPTY_EXTRACTION_ORDERS_SQL,
    EXTRACT_ORDERS_SQL,
    SET_STATEMENT_TIMEOUT_SQL,
    VALIDATE_ORDERS_SQL,
    create_postgres_connection,
    open_order_snapshot,
)

SOURCE_DATE = date(2026, 9, 1)


def _row(ordinal: int) -> tuple[object, ...]:
    return (
        202_609_010_000 + ordinal,
        10_000 + ordinal,
        "confirmed",
        "in_transit",
        "USD",
        Decimal(f"{ordinal}.00"),
        datetime(2026, 9, 1, 10, ordinal, tzinfo=UTC),
    )


class FakeCursor:
    def __init__(
        self,
        *,
        connection: FakeConnection,
        capture_row: tuple[object, ...] | None = None,
        batches: list[list[tuple[object, ...]]] | None = None,
        fetch_error: BaseException | None = None,
    ) -> None:
        self.connection = connection
        self.capture_row = capture_row
        self.batches = list(batches or [])
        self.fetch_error = fetch_error
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.fetchmany_sizes: list[int] = []
        self.closed = False

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.closed = True
        self.connection.events.append("cursor_exit")

    def execute(
        self,
        query: str,
        params: tuple[object, ...] | None = None,
    ) -> FakeCursor:
        assert self.connection.transaction_active
        self.execute_calls.append((query, params))
        self.connection.events.append("execute")
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.capture_row

    def fetchmany(self, size: int) -> list[tuple[object, ...]]:
        self.fetchmany_sizes.append(size)
        self.connection.events.append("fetchmany")
        if self.fetch_error is not None:
            error, self.fetch_error = self.fetch_error, None
            raise error
        return self.batches.pop(0)


class FakeTransaction(AbstractContextManager[None]):
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> None:
        assert self.connection.read_only is True
        assert self.connection.isolation_level is IsolationLevel.REPEATABLE_READ
        self.connection.transaction_active = True
        self.connection.events.append("transaction_enter")

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.connection.transaction_active = False
        self.connection.events.append("transaction_exit")


class FakeConnection:
    def __init__(
        self,
        *,
        capture_row: tuple[object, ...] | None,
        batches: list[list[tuple[object, ...]]],
        fetch_error: BaseException | None = None,
    ) -> None:
        self.read_only: bool | None = None
        self.isolation_level: IsolationLevel | None = None
        self.transaction_active = False
        self.events: list[str] = []
        self.control_cursor = FakeCursor(connection=self, capture_row=capture_row)
        self.server_cursor = FakeCursor(
            connection=self,
            batches=batches,
            fetch_error=fetch_error,
        )
        self.cursor_names: list[str | None] = []
        self.closed = False

    def transaction(self) -> FakeTransaction:
        self.events.append("transaction_requested")
        return FakeTransaction(self)

    def cursor(self, name: str | None = None) -> FakeCursor:
        assert self.transaction_active
        self.cursor_names.append(name)
        return self.control_cursor if name is None else self.server_cursor

    def close(self) -> None:
        self.closed = True
        self.events.append("connection_close")


def _postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host="postgres",
        port=5432,
        database="batch_lab",
        user="batch_lab",
        password="not-observable",
        connect_timeout_seconds=2.2,
        statement_timeout_seconds=3.25,
    )


def _snapshot_config(*, max_elapsed_seconds: float = 30.0) -> SnapshotConfig:
    return SnapshotConfig(
        max_elapsed_seconds=max_elapsed_seconds,
        chunk_rows=2,
        max_chunks=10,
        max_chunk_bytes=100_000,
        spool_threshold_bytes=1_024,
    )


def _factory(connection: FakeConnection):  # type: ignore[no-untyped-def]
    def connect(config: PostgresConfig):  # type: ignore[no-untyped-def]
        assert config == _postgres_config()
        return cast("psycopg.Connection[tuple[object, ...]]", connection)

    return connect


def test_connection_construction_is_explicit_bounded_and_has_no_hidden_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    sentinel = cast("psycopg.Connection[tuple[object, ...]]", object())

    def fake_connect(**kwargs: object) -> psycopg.Connection[tuple[object, ...]]:
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(psycopg, "connect", fake_connect)

    assert create_postgres_connection(_postgres_config()) is sentinel
    assert captured == {
        "host": "postgres",
        "port": 5432,
        "dbname": "batch_lab",
        "user": "batch_lab",
        "password": "not-observable",
        "connect_timeout": 3,
        "autocommit": True,
    }


def test_reader_uses_one_read_only_repeatable_read_transaction_and_bounded_fetches() -> None:
    rows = [_row(1), _row(2), _row(3)]
    capture = (rows[-1][6], rows[-1][0], len(rows))
    connection = FakeConnection(capture_row=capture, batches=[rows[:2], rows[2:], []])

    with open_order_snapshot(
        postgres_config=_postgres_config(),
        snapshot_config=_snapshot_config(),
        source_name=ORDERS_SOURCE_NAME,
        source_date=SOURCE_DATE,
        connection_factory=_factory(connection),
        monotonic=lambda: 0.0,
    ) as snapshot:
        assert snapshot.descriptor == SnapshotDescriptor.create(
            source_name="orders",
            source_date=SOURCE_DATE,
            upper_updated_at=cast(datetime, rows[-1][6]),
            upper_order_id=cast(int, rows[-1][0]),
            expected_record_count=3,
        )
        chunks = list(snapshot.iter_chunks())

    assert [len(chunk) for chunk in chunks] == [2, 1]
    assert [row.as_tuple() for chunk in chunks for row in chunk] == rows
    assert connection.cursor_names == [None, "bi070_orders_snapshot"]
    assert connection.server_cursor.fetchmany_sizes == [2, 2, 2]
    assert connection.control_cursor.execute_calls == [
        (SET_STATEMENT_TIMEOUT_SQL, ("3250ms",)),
        (
            CAPTURE_ORDERS_SQL,
            (
                datetime(2026, 9, 1, tzinfo=UTC),
                datetime(2026, 9, 2, tzinfo=UTC),
            ),
        ),
    ]
    assert connection.server_cursor.execute_calls == [
        (
            EXTRACT_ORDERS_SQL,
            (
                datetime(2026, 9, 1, tzinfo=UTC),
                datetime(2026, 9, 2, tzinfo=UTC),
                rows[-1][6],
                rows[-1][0],
            ),
        )
    ]
    assert connection.events.index("transaction_enter") < connection.events.index("execute")
    assert connection.events[-2:] == ["transaction_exit", "connection_close"]
    assert connection.control_cursor.closed is True
    assert connection.server_cursor.closed is True
    assert connection.closed is True


def test_query_contract_is_explicit_parameterized_ordered_and_has_no_unbounded_patterns() -> None:
    normalized = " ".join(EXTRACT_ORDERS_SQL.split()).lower()
    selected = normalized.partition("select ")[2].partition(" from ")[0]

    assert [column.strip() for column in selected.split(",")] == [
        "order_id",
        "customer_id",
        "order_status",
        "shipping_status",
        "currency",
        "total_amount",
        "updated_at",
    ]
    assert "updated_at >= %s" in normalized
    assert "updated_at < %s" in normalized
    assert "(updated_at, order_id) <= (%s, %s)" in normalized
    assert normalized.endswith("order by updated_at, order_id")
    assert "select *" not in normalized
    assert " offset " not in normalized
    assert (
        " ".join(SET_STATEMENT_TIMEOUT_SQL.split()).lower()
        == "select set_config('statement_timeout', %s, true)"
    )

    module_source = Path("src/batch_ingestion/postgres_snapshot/reader.py").read_text(
        encoding="utf-8"
    )
    assert ".fetchall(" not in module_source
    assert "OFFSET" not in module_source.upper()


def test_resume_validates_descriptor_before_opening_server_cursor() -> None:
    rows = [_row(1), _row(2), _row(3)]
    descriptor = SnapshotDescriptor.create(
        source_name="orders",
        source_date=SOURCE_DATE,
        upper_updated_at=cast(datetime, rows[-1][6]),
        upper_order_id=cast(int, rows[-1][0]),
        expected_record_count=3,
    )
    changed_capture = (rows[-2][6], rows[-2][0], 2)
    connection = FakeConnection(capture_row=changed_capture, batches=[])

    with (
        pytest.raises(SnapshotHandledError) as error,
        open_order_snapshot(
            postgres_config=_postgres_config(),
            snapshot_config=_snapshot_config(),
            source_name="orders",
            source_date=SOURCE_DATE,
            resume_descriptor=descriptor,
            connection_factory=_factory(connection),
            monotonic=lambda: 0.0,
        ),
    ):
        pytest.fail("drifted descriptor must fail before yielding a session")

    assert error.value.code is SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED
    assert connection.control_cursor.execute_calls[-1][0] == VALIDATE_ORDERS_SQL
    assert connection.cursor_names == [None]
    assert connection.closed is True


def test_empty_capture_yields_one_empty_chunk_through_the_named_cursor() -> None:
    connection = FakeConnection(capture_row=None, batches=[[]])

    with open_order_snapshot(
        postgres_config=_postgres_config(),
        snapshot_config=_snapshot_config(),
        source_name="orders",
        source_date=SOURCE_DATE,
        connection_factory=_factory(connection),
        monotonic=lambda: 0.0,
    ) as snapshot:
        assert snapshot.descriptor.is_empty is True
        assert list(snapshot.iter_chunks()) == [()]

    assert connection.server_cursor.execute_calls == [(EMPTY_EXTRACTION_ORDERS_SQL, None)]
    assert connection.server_cursor.fetchmany_sizes == [2]


def test_reader_detects_final_count_or_boundary_drift() -> None:
    rows = [_row(1), _row(2), _row(3)]
    connection = FakeConnection(
        capture_row=(rows[-1][6], rows[-1][0], 3),
        batches=[rows[:2], []],
    )

    with (
        pytest.raises(SnapshotHandledError) as error,
        open_order_snapshot(
            postgres_config=_postgres_config(),
            snapshot_config=_snapshot_config(),
            source_name="orders",
            source_date=SOURCE_DATE,
            connection_factory=_factory(connection),
            monotonic=lambda: 0.0,
        ) as snapshot,
    ):
        list(snapshot.iter_chunks())

    assert error.value.code is SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED
    assert connection.closed is True


def test_deadline_and_connection_failure_are_bounded_and_cleanup_without_retry() -> None:
    row = _row(1)
    deadline_connection = FakeConnection(capture_row=(row[6], row[0], 1), batches=[[row], []])
    times = iter([0.0, 0.0, 0.0, 2.0])

    with (
        pytest.raises(SnapshotHandledError) as deadline_error,
        open_order_snapshot(
            postgres_config=_postgres_config(),
            snapshot_config=_snapshot_config(max_elapsed_seconds=1.0),
            source_name="orders",
            source_date=SOURCE_DATE,
            connection_factory=_factory(deadline_connection),
            monotonic=lambda: next(times),
        ) as snapshot,
    ):
        list(snapshot.iter_chunks())

    assert deadline_error.value.code is SnapshotErrorCode.DEADLINE_EXCEEDED
    assert deadline_connection.closed is True

    failed_connection = FakeConnection(
        capture_row=(row[6], row[0], 1),
        batches=[],
        fetch_error=psycopg.OperationalError("connection dropped with secret details"),
    )
    with (
        pytest.raises(SnapshotHandledError) as connection_error,
        open_order_snapshot(
            postgres_config=_postgres_config(),
            snapshot_config=_snapshot_config(),
            source_name="orders",
            source_date=SOURCE_DATE,
            connection_factory=_factory(failed_connection),
            monotonic=lambda: 0.0,
        ) as snapshot,
    ):
        list(snapshot.iter_chunks())

    assert connection_error.value.code is SnapshotErrorCode.POSTGRES_CONNECTION_FAILED
    assert str(connection_error.value) == "postgres_connection_failed"
    assert failed_connection.server_cursor.fetchmany_sizes == [2]
    assert failed_connection.closed is True


def test_rows_must_remain_strictly_ordered_and_inside_the_descriptor_bound() -> None:
    first = _row(1)
    duplicate = first
    connection = FakeConnection(
        capture_row=(first[6], first[0], 2),
        batches=[[first, duplicate], []],
    )

    with (
        pytest.raises(SnapshotHandledError) as error,
        open_order_snapshot(
            postgres_config=_postgres_config(),
            snapshot_config=_snapshot_config(),
            source_name="orders",
            source_date=SOURCE_DATE,
            connection_factory=_factory(connection),
            monotonic=lambda: 0.0,
        ) as snapshot,
    ):
        list(snapshot.iter_chunks())

    assert error.value.code is SnapshotErrorCode.SOURCE_CONTRACT_FAILED


def test_resume_excludes_rows_above_the_supplied_upper_tuple() -> None:
    rows = [_row(1), _row(2)]
    descriptor = SnapshotDescriptor.create(
        source_name="orders",
        source_date=SOURCE_DATE,
        upper_updated_at=cast(datetime, rows[-1][6]),
        upper_order_id=cast(int, rows[-1][0]),
        expected_record_count=2,
    )
    connection = FakeConnection(
        capture_row=(rows[-1][6], rows[-1][0], 2),
        batches=[rows, []],
    )

    with open_order_snapshot(
        postgres_config=_postgres_config(),
        snapshot_config=_snapshot_config(),
        source_name="orders",
        source_date=SOURCE_DATE,
        resume_descriptor=descriptor,
        connection_factory=_factory(connection),
        monotonic=lambda: 0.0,
    ) as snapshot:
        assert [row.as_tuple() for chunk in snapshot.iter_chunks() for row in chunk] == rows

    validate_params = connection.control_cursor.execute_calls[-1][1]
    assert validate_params == (
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        rows[-1][6],
        rows[-1][0],
    )
    extract_params = connection.server_cursor.execute_calls[0][1]
    assert extract_params == validate_params
