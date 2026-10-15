"""Contracts and bounded I/O primitives for PostgreSQL order snapshots."""

from .contracts import (
    MAX_CHUNK_ORDINAL,
    ORDERS_SOURCE_NAME,
    PARQUET_CONTENT_TYPE,
    SNAPSHOT_CONTRACT,
    SnapshotDescriptor,
    canonical_utc_timestamp,
    chunk_payload_name,
    is_supported_source,
    parse_canonical_utc_timestamp,
)
from .events import (
    SnapshotChunkStatus,
    SnapshotCounters,
    SnapshotErrorCode,
    SnapshotEvent,
    SnapshotEventType,
    SnapshotHandledError,
)
from .reader import OrderSnapshotRead, create_postgres_connection, open_order_snapshot
from .schema import (
    ORDERS_ARROW_SCHEMA,
    PARQUET_WRITER_OPTIONS,
    ParquetWriterOptions,
    SnapshotOrderRow,
)
from .serializer import SerializedOrderChunk, serialize_order_chunk
from .service import SnapshotIngestionResult, SnapshotStorage, ingest_order_snapshot

__all__ = [
    "MAX_CHUNK_ORDINAL",
    "ORDERS_ARROW_SCHEMA",
    "ORDERS_SOURCE_NAME",
    "PARQUET_CONTENT_TYPE",
    "PARQUET_WRITER_OPTIONS",
    "SNAPSHOT_CONTRACT",
    "OrderSnapshotRead",
    "ParquetWriterOptions",
    "SnapshotChunkStatus",
    "SnapshotCounters",
    "SnapshotDescriptor",
    "SnapshotErrorCode",
    "SnapshotEvent",
    "SnapshotEventType",
    "SnapshotHandledError",
    "SnapshotIngestionResult",
    "SnapshotOrderRow",
    "SnapshotStorage",
    "SerializedOrderChunk",
    "canonical_utc_timestamp",
    "chunk_payload_name",
    "create_postgres_connection",
    "ingest_order_snapshot",
    "is_supported_source",
    "open_order_snapshot",
    "parse_canonical_utc_timestamp",
    "serialize_order_chunk",
]
