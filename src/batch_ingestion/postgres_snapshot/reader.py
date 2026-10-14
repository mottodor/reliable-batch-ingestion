"""Read a bounded orders snapshot in one read-only PostgreSQL transaction."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Final, cast

import psycopg
from psycopg import IsolationLevel

from batch_ingestion.config import PostgresConfig, SnapshotConfig

from .contracts import SnapshotDescriptor
from .events import SnapshotErrorCode, SnapshotHandledError
from .schema import SnapshotOrderRow

SET_STATEMENT_TIMEOUT_SQL: Final = "SELECT set_config('statement_timeout', %s, true)"

CAPTURE_ORDERS_SQL: Final = """
    SELECT updated_at, order_id, count(*) OVER () AS expected_record_count
    FROM omni_retail.orders
    WHERE updated_at >= %s
      AND updated_at < %s
    ORDER BY updated_at DESC, order_id DESC
    LIMIT 1
"""

VALIDATE_ORDERS_SQL: Final = """
    SELECT updated_at, order_id, count(*) OVER () AS expected_record_count
    FROM omni_retail.orders
    WHERE updated_at >= %s
      AND updated_at < %s
      AND (updated_at, order_id) <= (%s, %s)
    ORDER BY updated_at DESC, order_id DESC
    LIMIT 1
"""

EXTRACT_ORDERS_SQL: Final = """
    SELECT
        order_id,
        customer_id,
        order_status,
        shipping_status,
        currency,
        total_amount,
        updated_at
    FROM omni_retail.orders
    WHERE updated_at >= %s
      AND updated_at < %s
      AND (updated_at, order_id) <= (%s, %s)
    ORDER BY updated_at, order_id
"""

EMPTY_EXTRACTION_ORDERS_SQL: Final = """
    SELECT
        order_id,
        customer_id,
        order_status,
        shipping_status,
        currency,
        total_amount,
        updated_at
    FROM omni_retail.orders
    WHERE false
    ORDER BY updated_at, order_id
"""

_SERVER_CURSOR_NAME: Final = "bi070_orders_snapshot"
OrderTuple = tuple[object, ...]
ConnectionFactory = Callable[[PostgresConfig], psycopg.Connection[OrderTuple]]
MonotonicClock = Callable[[], float]


def create_postgres_connection(config: PostgresConfig) -> psycopg.Connection[OrderTuple]:
    """Open one explicit autocommit connection; the reader owns its transaction."""
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=max(1, math.ceil(config.connect_timeout_seconds)),
        autocommit=True,
    )


@dataclass(slots=True)
class _Deadline:
    clock: MonotonicClock
    max_elapsed_seconds: float
    started_at: float = field(init=False)

    def __post_init__(self) -> None:
        self.started_at = self.clock()

    def check(self) -> None:
        if self.clock() - self.started_at >= self.max_elapsed_seconds:
            raise SnapshotHandledError(SnapshotErrorCode.DEADLINE_EXCEEDED)


@dataclass(slots=True)
class OrderSnapshotRead:
    """A one-shot bounded chunk iterator tied to its owning transaction context."""

    descriptor: SnapshotDescriptor
    chunk_rows: int
    cursor: psycopg.ServerCursor[OrderTuple]
    deadline: _Deadline
    _iterated: bool = False

    def iter_chunks(self) -> Iterator[tuple[SnapshotOrderRow, ...]]:
        """Fetch, validate, and yield no more than one configured chunk at a time."""
        if self._iterated:
            raise RuntimeError("snapshot chunks may only be iterated once")
        self._iterated = True
        records_read = 0
        previous_key: tuple[datetime, int] | None = None
        yielded_empty_chunk = False

        while True:
            self.deadline.check()
            raw_rows = self.cursor.fetchmany(self.chunk_rows)
            self.deadline.check()
            if len(raw_rows) > self.chunk_rows:
                raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED)
            if not raw_rows:
                if self.descriptor.is_empty and not yielded_empty_chunk:
                    yielded_empty_chunk = True
                    yield ()
                break

            rows = self._validate_rows(raw_rows, previous_key=previous_key)
            previous_key = (rows[-1].updated_at, rows[-1].order_id)
            records_read += len(rows)
            if records_read > self.descriptor.expected_record_count:
                raise SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)
            yield rows

        expected_upper = (
            None
            if self.descriptor.is_empty
            else (self.descriptor.upper_updated_at, self.descriptor.upper_order_id)
        )
        if records_read != self.descriptor.expected_record_count or previous_key != expected_upper:
            raise SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)

    def _validate_rows(
        self,
        raw_rows: Sequence[Sequence[object]],
        *,
        previous_key: tuple[datetime, int] | None,
    ) -> tuple[SnapshotOrderRow, ...]:
        rows: list[SnapshotOrderRow] = []
        prior = previous_key
        for values in raw_rows:
            try:
                row = SnapshotOrderRow.from_sequence(values)
            except (TypeError, ValueError) as error:
                raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED) from error
            key = (row.updated_at, row.order_id)
            upper = (self.descriptor.upper_updated_at, self.descriptor.upper_order_id)
            if (
                prior is not None
                and key <= prior
                or not (
                    self.descriptor.window_start
                    <= row.updated_at
                    < self.descriptor.window_end_exclusive
                )
                or self.descriptor.is_empty
                or key > upper
            ):
                raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED)
            prior = key
            rows.append(row)
        return tuple(rows)


@contextmanager
def open_order_snapshot(
    *,
    postgres_config: PostgresConfig,
    snapshot_config: SnapshotConfig,
    source_name: str,
    source_date: date,
    resume_descriptor: SnapshotDescriptor | None = None,
    connection_factory: ConnectionFactory = create_postgres_connection,
    monotonic: MonotonicClock = time.monotonic,
) -> Iterator[OrderSnapshotRead]:
    """Capture or validate bounds and expose one named-cursor chunk iterator."""
    deadline = _Deadline(monotonic, snapshot_config.max_elapsed_seconds)
    connection: psycopg.Connection[OrderTuple] | None = None
    try:
        deadline.check()
        connection = connection_factory(postgres_config)
        connection.read_only = True
        connection.isolation_level = IsolationLevel.REPEATABLE_READ
        with connection.transaction():
            deadline.check()
            with connection.cursor() as control_cursor:
                timeout_ms = max(1, math.ceil(postgres_config.statement_timeout_seconds * 1000))
                control_cursor.execute(SET_STATEMENT_TIMEOUT_SQL, (f"{timeout_ms}ms",))
                deadline.check()
                descriptor = _capture_or_validate_descriptor(
                    cursor=control_cursor,
                    source_name=source_name,
                    source_date=source_date,
                    resume_descriptor=resume_descriptor,
                )
                deadline.check()

            if (
                descriptor.expected_record_count > snapshot_config.max_records
                or descriptor.expected_chunk_count(chunk_rows=snapshot_config.chunk_rows)
                > snapshot_config.max_chunks
            ):
                raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED)

            with connection.cursor(name=_SERVER_CURSOR_NAME) as server_cursor:
                deadline.check()
                if descriptor.is_empty:
                    server_cursor.execute(EMPTY_EXTRACTION_ORDERS_SQL)
                else:
                    assert descriptor.upper_updated_at is not None
                    assert descriptor.upper_order_id is not None
                    server_cursor.execute(
                        EXTRACT_ORDERS_SQL,
                        (
                            descriptor.window_start,
                            descriptor.window_end_exclusive,
                            descriptor.upper_updated_at,
                            descriptor.upper_order_id,
                        ),
                    )
                deadline.check()
                yield OrderSnapshotRead(
                    descriptor=descriptor,
                    chunk_rows=snapshot_config.chunk_rows,
                    cursor=server_cursor,
                    deadline=deadline,
                )
    except SnapshotHandledError:
        raise
    except psycopg.Error as error:
        raise SnapshotHandledError(SnapshotErrorCode.POSTGRES_CONNECTION_FAILED) from error
    finally:
        if connection is not None:
            connection.close()


def _capture_or_validate_descriptor(
    *,
    cursor: psycopg.Cursor[OrderTuple],
    source_name: str,
    source_date: date,
    resume_descriptor: SnapshotDescriptor | None,
) -> SnapshotDescriptor:
    probe = SnapshotDescriptor.empty(source_name=source_name, source_date=source_date)
    if resume_descriptor is not None:
        if (
            resume_descriptor.source_name != source_name
            or resume_descriptor.source_date != source_date
        ):
            raise SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)
        if resume_descriptor.is_empty:
            return resume_descriptor
        assert resume_descriptor.upper_updated_at is not None
        assert resume_descriptor.upper_order_id is not None
        cursor.execute(
            VALIDATE_ORDERS_SQL,
            (
                probe.window_start,
                probe.window_end_exclusive,
                resume_descriptor.upper_updated_at,
                resume_descriptor.upper_order_id,
            ),
        )
        observed = _descriptor_from_capture(
            source_name=source_name,
            source_date=source_date,
            capture_row=cursor.fetchone(),
        )
        if observed != resume_descriptor:
            raise SnapshotHandledError(SnapshotErrorCode.SOURCE_SNAPSHOT_CHANGED)
        return resume_descriptor

    cursor.execute(CAPTURE_ORDERS_SQL, (probe.window_start, probe.window_end_exclusive))
    return _descriptor_from_capture(
        source_name=source_name,
        source_date=source_date,
        capture_row=cursor.fetchone(),
    )


def _descriptor_from_capture(
    *,
    source_name: str,
    source_date: date,
    capture_row: Sequence[object] | None,
) -> SnapshotDescriptor:
    if capture_row is None:
        return SnapshotDescriptor.empty(source_name=source_name, source_date=source_date)
    if len(capture_row) != 3:
        raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED)
    try:
        return SnapshotDescriptor.create(
            source_name=source_name,
            source_date=source_date,
            upper_updated_at=cast(datetime, capture_row[0]),
            upper_order_id=cast(int, capture_row[1]),
            expected_record_count=cast(int, capture_row[2]),
        )
    except (TypeError, ValueError) as error:
        raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED) from error
