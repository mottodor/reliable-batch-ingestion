"""Serialize exactly one bounded orders chunk to deterministic Parquet bytes."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import SpooledTemporaryFile
from typing import BinaryIO, cast

import pyarrow as pa
import pyarrow.parquet as pq

from batch_ingestion.checksums import sha256_stream
from batch_ingestion.config import SnapshotConfig

from .events import SnapshotErrorCode, SnapshotHandledError
from .schema import ORDERS_ARROW_SCHEMA, PARQUET_WRITER_OPTIONS, SnapshotOrderRow

type OrderRowInput = SnapshotOrderRow | Sequence[object]


@dataclass(frozen=True, slots=True)
class SerializedOrderChunk:
    """Context-owned, rewound Parquet bytes and their verified byte properties."""

    stream: BinaryIO
    record_count: int
    content_length: int
    checksum_sha256: str


@contextmanager
def serialize_order_chunk(
    rows: Sequence[OrderRowInput],
    *,
    snapshot_config: SnapshotConfig,
) -> Iterator[SerializedOrderChunk]:
    """Validate and serialize one configured row chunk, closing its spool on exit."""
    raw_spool = SpooledTemporaryFile(  # noqa: SIM115 - closed in the finally block
        max_size=snapshot_config.spool_threshold_bytes,
        mode="w+b",
    )
    spool = cast(BinaryIO, raw_spool)
    try:
        validated_rows = _validate_chunk_rows(rows, max_rows=snapshot_config.chunk_rows)
        table = _order_table(validated_rows)
        try:
            with pq.ParquetWriter(
                spool,
                ORDERS_ARROW_SCHEMA,
                version=PARQUET_WRITER_OPTIONS.version,
                data_page_version=PARQUET_WRITER_OPTIONS.data_page_version,
                compression=PARQUET_WRITER_OPTIONS.compression,
                use_dictionary=PARQUET_WRITER_OPTIONS.use_dictionary,
                write_statistics=PARQUET_WRITER_OPTIONS.write_statistics,
            ) as writer:
                writer.write_table(table, row_group_size=max(1, table.num_rows))
        except (pa.ArrowException, OSError) as error:
            raise SnapshotHandledError(SnapshotErrorCode.SERIALIZATION_FAILED) from error

        content_length = spool.seek(0, 2)
        if content_length > snapshot_config.max_chunk_bytes:
            raise SnapshotHandledError(SnapshotErrorCode.SERIALIZATION_FAILED)
        spool.seek(0)
        checksum_sha256 = sha256_stream(spool)
        spool.seek(0)
        yield SerializedOrderChunk(
            stream=spool,
            record_count=len(validated_rows),
            content_length=content_length,
            checksum_sha256=checksum_sha256,
        )
    finally:
        spool.close()


def _validate_chunk_rows(
    rows: Sequence[OrderRowInput],
    *,
    max_rows: int,
) -> tuple[SnapshotOrderRow, ...]:
    if len(rows) > max_rows:
        raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED)
    validated: list[SnapshotOrderRow] = []
    for value in rows:
        try:
            row = (
                value
                if isinstance(value, SnapshotOrderRow)
                else SnapshotOrderRow.from_sequence(value)
            )
        except (TypeError, ValueError) as error:
            raise SnapshotHandledError(SnapshotErrorCode.SOURCE_CONTRACT_FAILED) from error
        validated.append(row)
    return tuple(validated)


def _order_table(rows: Sequence[SnapshotOrderRow]) -> pa.Table:
    values = {
        "order_id": [row.order_id for row in rows],
        "customer_id": [row.customer_id for row in rows],
        "order_status": [row.order_status for row in rows],
        "shipping_status": [row.shipping_status for row in rows],
        "currency": [row.currency for row in rows],
        "total_amount": [row.total_amount for row in rows],
        "updated_at": [row.updated_at for row in rows],
    }
    try:
        return pa.Table.from_pydict(values, schema=ORDERS_ARROW_SCHEMA)
    except pa.ArrowException as error:
        raise SnapshotHandledError(SnapshotErrorCode.SERIALIZATION_FAILED) from error
