"""Deterministic one-chunk Parquet serializer contracts for BI-070 slice 2."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from tempfile import SpooledTemporaryFile
from typing import BinaryIO, Protocol, cast

import pyarrow.parquet as pq
import pytest

from batch_ingestion.config import SnapshotConfig
from batch_ingestion.postgres_snapshot import (
    ORDERS_ARROW_SCHEMA,
    SnapshotErrorCode,
    SnapshotHandledError,
)
from batch_ingestion.postgres_snapshot.serializer import serialize_order_chunk


class _RolledStream(Protocol):
    _rolled: bool


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


def _config(
    *,
    chunk_rows: int = 3,
    max_chunk_bytes: int = 100_000,
    spool_threshold_bytes: int = 100_000,
) -> SnapshotConfig:
    return SnapshotConfig(
        max_elapsed_seconds=30.0,
        chunk_rows=chunk_rows,
        max_chunks=10,
        max_chunk_bytes=max_chunk_bytes,
        spool_threshold_bytes=spool_threshold_bytes,
    )


def _serialize_bytes(rows: list[tuple[object, ...]]) -> tuple[bytes, str, int]:
    with serialize_order_chunk(rows, snapshot_config=_config()) as chunk:
        assert chunk.stream.tell() == 0
        payload = chunk.stream.read()
        return payload, chunk.checksum_sha256, chunk.content_length


def test_serializer_writes_one_exact_deterministic_row_group_and_rewinds() -> None:
    rows = [_row(1), _row(2), _row(3)]

    first_payload, first_checksum, first_length = _serialize_bytes(rows)
    second_payload, second_checksum, second_length = _serialize_bytes(rows)

    assert first_payload == second_payload
    assert first_checksum == second_checksum
    assert first_length == second_length == len(first_payload)
    parquet = pq.ParquetFile(BytesIO(first_payload))
    assert parquet.schema_arrow == ORDERS_ARROW_SCHEMA
    assert parquet.num_row_groups == 1
    assert parquet.metadata.num_rows == 3
    assert parquet.metadata.row_group(0).num_rows == 3
    assert parquet.read().to_pylist() == [
        {
            "order_id": row[0],
            "customer_id": row[1],
            "order_status": row[2],
            "shipping_status": row[3],
            "currency": row[4],
            "total_amount": row[5],
            "updated_at": row[6],
        }
        for row in rows
    ]


def test_empty_chunk_is_schema_bearing_and_has_exactly_one_zero_row_group() -> None:
    payload, _, length = _serialize_bytes([])

    parquet = pq.ParquetFile(BytesIO(payload))
    assert length == len(payload) > 0
    assert parquet.schema_arrow == ORDERS_ARROW_SCHEMA
    assert parquet.num_row_groups == 1
    assert parquet.metadata.num_rows == 0
    assert parquet.metadata.row_group(0).num_rows == 0


def test_serializer_rejects_more_than_one_configured_chunk_and_strict_row_violations() -> None:
    with (
        pytest.raises(SnapshotHandledError) as oversized_rows,
        serialize_order_chunk(
            [_row(1), _row(2), _row(3)],
            snapshot_config=_config(chunk_rows=2),
        ),
    ):
        pytest.fail("oversized row chunk must not be yielded")
    assert oversized_rows.value.code is SnapshotErrorCode.SOURCE_CONTRACT_FAILED

    invalid = list(_row(1))
    invalid[5] = Decimal("1.2")
    with (
        pytest.raises(SnapshotHandledError) as invalid_row,
        serialize_order_chunk([tuple(invalid)], snapshot_config=_config()),
    ):
        pytest.fail("invalid row must not be yielded")
    assert invalid_row.value.code is SnapshotErrorCode.SOURCE_CONTRACT_FAILED
    assert str(invalid_row.value) == "source_contract_failed"


def test_serialized_byte_limit_fails_closed_and_closes_the_spool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streams: list[BinaryIO] = []

    def tracking_spool(*, max_size: int, mode: str) -> BinaryIO:
        raw_stream = SpooledTemporaryFile(  # noqa: SIM115 - serializer owns lifecycle
            max_size=max_size,
            mode=mode,
        )
        stream = cast(BinaryIO, raw_stream)
        streams.append(stream)
        return stream

    import batch_ingestion.postgres_snapshot.serializer as serializer_module

    monkeypatch.setattr(serializer_module, "SpooledTemporaryFile", tracking_spool)

    with (
        pytest.raises(SnapshotHandledError) as error,
        serialize_order_chunk(
            [_row(1)],
            snapshot_config=_config(max_chunk_bytes=10, spool_threshold_bytes=10),
        ),
    ):
        pytest.fail("oversized Parquet bytes must not be yielded")

    assert error.value.code is SnapshotErrorCode.SERIALIZATION_FAILED
    assert len(streams) == 1
    assert streams[0].closed is True


def test_spool_may_roll_to_disk_and_is_closed_after_context_exit() -> None:
    with serialize_order_chunk(
        [_row(1), _row(2)],
        snapshot_config=_config(spool_threshold_bytes=1),
    ) as chunk:
        stream = chunk.stream
        assert cast(_RolledStream, stream)._rolled is True
        assert stream.closed is False
        assert stream.tell() == 0

    assert stream.closed is True


def test_serializer_does_not_retain_or_mutate_caller_rows() -> None:
    rows = [_row(1), _row(2)]
    original = list(rows)

    with serialize_order_chunk(rows, snapshot_config=_config()) as chunk:
        assert chunk.record_count == 2

    assert rows == original
