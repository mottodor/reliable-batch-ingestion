# PostgreSQL snapshot ingestion contract

This document describes the implemented `BI-070` behavior for bounded, restart-safe daily extraction of `omni_retail.orders` into deterministic Parquet chunks. The identifier, manifest, summary, source, and shared S3 rules remain governed by [`core-contracts.md`](../contracts/core-contracts.md), [`source-contracts.md`](../contracts/source-contracts.md), and [`storage-contract.md`](../contracts/storage-contract.md).

The delivery claim is limited to a consistent PostgreSQL transaction while its connection remains alive, deterministic keys, immutable/idempotent chunk writes, and safe sequential replay from the fixed logical bounds. It is not an exactly-once claim. A PostgreSQL MVCC snapshot does not survive a dead connection, and concurrent writers for the same logical snapshot are unsupported.

## Commands and source boundary

Start and seed the local services, then capture one UTC source date:

```bash
make up
make seed SOURCE_SEED=20260901
make ingest-postgres ARGS="run --source orders --date 2026-09-01"
```

The production source allow-list contains only `orders`, statically mapped to `omni_retail.orders`. The date must use canonical `YYYY-MM-DD` form and defines this half-open UTC window:

```text
2026-09-01T00:00:00.000000Z <= updated_at < 2026-09-02T00:00:00.000000Z
```

`run` captures the greatest `(updated_at, order_id)` tuple and exact record count inside that window. The emitted `snapshot_started` event contains the complete non-secret descriptor required after an interruption. For the default seeded data, a non-empty recovery command has this form:

```bash
make ingest-postgres ARGS="resume --source orders --date 2026-09-01 --upper-updated-at 2026-09-01T14:11:00.000000Z --upper-order-id 202609010024 --expected-record-count 24"
```

Use the values from the interrupted run's event rather than assuming the example values. The timestamp must already use six fractional digits and UTC `Z`; all three non-empty descriptor fields are required together.

An empty capture has no upper tuple and still produces one schema-bearing zero-row Parquet chunk. Its recovery command is:

```bash
make ingest-postgres ARGS="resume --source orders --date 2026-09-01 --empty"
```

`--empty` is mutually exclusive with the upper/count fields. It deliberately remains empty if rows are inserted later. A fresh `run`, rather than that resume command, may capture the later rows as a new logical snapshot.

## Transaction, upper watermark, and extraction query

One attempt opens one explicitly bounded Psycopg connection with no application retry. Before the transaction starts, the reader configures it as read-only with `REPEATABLE READ` isolation. A transaction-local statement timeout is installed before source capture/validation and extraction.

For `run`, descriptor capture and extraction share that transaction and PostgreSQL MVCC snapshot. For non-empty `resume`, the new transaction first requires both the record count and greatest tuple at or below the supplied upper bound to equal the descriptor. A mismatch fails as `source_snapshot_changed` before S3 mutation in that attempt. `resume --empty` deliberately selects no rows.

The extraction path has a fixed explicit seven-column list and parameter-bound values:

```text
order_id
customer_id
order_status
shipping_status
currency
total_amount
updated_at
```

Non-empty extraction applies the daily window and inclusive tuple bound, then orders by `updated_at, order_id`. The table and column identifiers are fixed application constants, not command input. The implementation does not use `SELECT *`, offset pagination, `fetchall()`, or a client-side whole-result cursor.

One named server-side cursor calls only `fetchmany(SNAPSHOT_CHUNK_ROWS)`. Each result is validated for strict ascending tuple order, daily-window membership, inclusive upper-bound membership, exact types, and source values. Final row count and final watermark must equal the descriptor. The cursor, transaction, and connection are closed on success and every failure.

Rows inserted or updated above the captured tuple are excluded from the current transaction and from a later resume. Changes at or below the tuple are source drift: ordinary count/boundary differences fail before mutation on resume, while a byte difference found later conflicts with an immutable chunk. The implementation does not claim a durable historical PostgreSQL snapshot under arbitrary compensating edits after the original connection dies.

## Logical identity and deterministic keys

One `postgres_snapshot` `BatchIdentity` contains:

- source name `orders` and the source date;
- `snapshot-contract=omni-retail-orders-parquet-v1`;
- canonical inclusive `window-start` and exclusive `window-end-exclusive`;
- captured `upper-updated-at` and `upper-order-id`, or literal `none` for both;
- `expected-record-count`;
- configured `chunk-rows`.

Chunk size is part of identity because it changes chunk boundaries and bytes. Timeouts, maximum bytes/chunks, spool threshold, physical `run_id`, and S3 endpoint are operational settings and are not identity inputs. Resume therefore requires the original descriptor and the original configured `SNAPSHOT_CHUNK_ROWS`; changing chunk size intentionally selects a different batch.

Payload names are one-based four-digit ordinals from `chunk-0001.parquet` through `chunk-9999.parquet`. The exact expected chunk count is known from the descriptor as `max(1, ceil(expected_record_count / chunk_rows))`; prefix listing is not used as a checkpoint.

For the default seed descriptor shown above and `SNAPSHOT_CHUNK_ROWS=10`, the example identity and first keys are:

```text
batch_id=6f8e10b4229c34f8d479c241648dd0d5703bd1c74f656358fbf2e96165f27aa4
raw/postgres_snapshot/orders/source_date=2026-09-01/batch_id=6f8e10b4229c34f8d479c241648dd0d5703bd1c74f656358fbf2e96165f27aa4/chunk-0001.parquet
manifests/postgres_snapshot/orders/source_date=2026-09-01/batch_id=6f8e10b4229c34f8d479c241648dd0d5703bd1c74f656358fbf2e96165f27aa4/chunk-0001.parquet.manifest.json
```

The committed `.env.example` uses `SNAPSHOT_CHUNK_ROWS=100`, so it intentionally derives another batch ID and one chunk for the 24-row seed. Operators must derive or copy exact keys from the active descriptor/configuration rather than reuse this ten-row example blindly.

## Strict Parquet chunk contract

Every chunk has this exact non-null Arrow schema and field order:

```text
order_id: int64 not null
customer_id: int64 not null
order_status: string not null
shipping_status: string not null
currency: string not null
total_amount: decimal128(12, 2) not null
updated_at: timestamp[us, tz=UTC] not null
```

Rows are not trimmed, normalized, rounded, reordered, or otherwise transformed. Booleans are not integers; timestamps must already be UTC; amounts must have exact decimal scale two and fit precision 12; statuses and currencies must satisfy the seeded orders source contract.

Each successful cursor fetch becomes one Parquet file with one row group. Writer options are fixed: Parquet version `2.6`, data page version `2.0`, compression `NONE`, dictionary encoding disabled, and statistics disabled. The zero-row snapshot also has one schema-bearing row group. No DSN, SQL, extraction timestamp, or application metadata is embedded by the ingestion service.

Only one fetched chunk is serialized at a time. The serializer builds one chunk's Arrow table and writes it to a bounded `SpooledTemporaryFile`; it never builds a whole-snapshot table. Exact byte replay is guaranteed under the committed Python/PyArrow lock and snapshot contract, not across arbitrary library versions. A future serialization change requires a new snapshot-contract version.

## Bounds and resource ownership

Configuration is loaded only by an explicit CLI call. Imports open no PostgreSQL/S3 connection and read no environment.

| Setting | Implemented meaning |
| --- | --- |
| `POSTGRES_CONNECT_TIMEOUT_SECONDS` | Positive PostgreSQL connection timeout; Psycopg receives a bounded whole-second value |
| `POSTGRES_STATEMENT_TIMEOUT_SECONDS` | Positive transaction-local timeout applied before descriptor capture/validation and extraction |
| `SNAPSHOT_MAX_ELAPSED_SECONDS` | Positive monotonic budget checked across database, serialization, reconciliation, and storage work |
| `SNAPSHOT_CHUNK_ROWS` | Positive maximum rows requested by each `fetchmany`; part of logical identity |
| `SNAPSHOT_MAX_CHUNKS` | Positive expected-inventory bound, no greater than `9,999` |
| `SNAPSHOT_MAX_CHUNK_BYTES` | Positive maximum serialized bytes for one Parquet chunk |
| `SNAPSHOT_SPOOL_THRESHOLD_BYTES` | Positive in-memory spool threshold before temporary-disk spill; no greater than the chunk-byte maximum |
| `S3_TOTAL_MAX_ATTEMPTS` | Botocore total attempts for each S3 request, including its initial request |
| `S3_CONNECT_TIMEOUT_SECONDS` | Bounded S3 connect timeout |
| `S3_READ_TIMEOUT_SECONDS` | Bounded S3 read timeout |

The admitted descriptor count cannot exceed `SNAPSHOT_CHUNK_ROWS * SNAPSHOT_MAX_CHUNKS`. The service retains at most one configured row chunk plus its one-chunk Arrow/Parquet serialization overhead and spool. The spool may spill to local disk. It is rewound for checksum/storage and closed before the next database fetch, including on failure.

There is no PostgreSQL retry loop and no snapshot-specific S3 retry loop. A new CLI invocation is a new physical attempt; bounded Botocore retries remain internal to each S3 request.

## Persistence order and manifests

After descriptor capture or validation, the service emits `snapshot_started` before any S3 mutation. It then processes each expected chunk in order:

1. fetch and validate no more than `SNAPSHOT_CHUNK_ROWS` rows;
2. serialize only that chunk and calculate exact byte length and SHA-256 from the spool;
3. reconcile the exact deterministic raw/manifest keys against those bytes;
4. if absent, immutably write and byte-verify raw Parquet; if `payload_only`, verify/reuse it; if complete, strictly verify and reuse both objects;
5. for absent or `payload_only`, write one immutable succeeded manifest only after raw verification;
6. emit `snapshot_chunk_completed` and close the spool before the next fetch;
7. after cursor exhaustion, verify total rows, chunk count, and final watermark before emitting success and the final summary.

Manifest cardinality is one manifest per raw chunk; there is no mutable aggregate manifest or PostgreSQL checkpoint table. Chunk manifests use schema version 1 with:

```text
source_name=orders
source_type=postgres_snapshot
original_file_name=null
content_type=application/vnd.apache.parquet
status=succeeded
attempt=1
```

The manifest points to the canonical raw object and records its exact length/checksum. A complete replay preserves the original manifest, `run_id`, and extraction timestamp. System, source-drift, serialization, and storage failures do not create a terminal failed manifest that could poison recovery; structured output and existing complete or `payload_only` evidence remain inspectable.

## Recovery states

Recovery uses deterministic exact keys and always replays the bounded source from its first row. It does not trust a mutable “last fetched row” checkpoint.

| Observed state | Rerun behavior | Operator action |
| --- | --- | --- |
| `snapshot_started` emitted, no objects | Revalidate descriptor and replay from chunk 1 | Run `resume` with the exact emitted descriptor |
| Earlier chunks complete, next chunk absent | Fetch again from row one, preserve complete chunks, then create missing chunks | Run the same `resume` |
| Current raw payload exists without manifest (`payload_only`) | Verify exact bytes and write only the missing succeeded manifest | Run the same `resume` |
| Every expected chunk is complete | Verify/reuse every pair, write zero raw bytes, count each chunk as a duplicate | Repeating the matching `resume` is idempotent |
| Manifest exists without payload | Fail `reconciliation_failed`; do not fabricate or delete data | Inspect and explicitly repair the exact retained keys before rerun |
| Existing raw bytes or manifest linkage/status conflicts | Fail without overwrite or automatic cleanup | Investigate the deterministic key and identity contract |
| PostgreSQL connection dies after a complete chunk | Preserve prior chunk evidence and report `postgres_connection_failed` | Restore PostgreSQL and run `resume` with the recorded descriptor; replay from chunk 1 |
| Row appears above the captured upper tuple | Exclude it from the original bounded snapshot | Continue with the original `resume`; use a later fresh `run` for another logical capture |
| Count or greatest tuple at/below the bound changes | Fail `source_snapshot_changed` before S3 mutation in that resume attempt | Restore the bounded source state or retain evidence and perform a reviewed new `run`; do not alter the old identity |
| S3 is temporarily unavailable | Stop within bounded S3/elapsed settings; preserve already committed evidence | Restore S3 and run the same `resume` from chunk 1 |
| Process stops after raw write and before manifest | Leave `payload_only` evidence | Run the same `resume`; only the missing manifest is created |

A failure before `snapshot_started` has no reusable captured descriptor and creates no S3 state; after the underlying issue is fixed, use `run` again. A failure after that event must use `resume`, not a fresh bound, to preserve the original logical snapshot.

## Structured output, metrics, errors, and exits

Output is compact newline-delimited JSON. A normal attempt emits `snapshot_started`, one `snapshot_chunk_completed` per reconciled chunk, `snapshot_succeeded`, and `run_summary`. A handled failure after descriptor capture emits `snapshot_failed` and `run_summary`; a handled failure before descriptor capture is represented by a secret-free `cli_error` because no valid batch ID exists yet.

Events may contain the public descriptor, source/date, batch/run IDs, chunk ordinal/key/status, safe counters, and stable error code. They never contain the PostgreSQL DSN/password, SQL exception text, row values, credentials, or query text.

Handled snapshot error codes are:

```text
source_snapshot_changed
postgres_connection_failed
source_contract_failed
serialization_failed
reconciliation_failed
object_conflict
storage_failed
deadline_exceeded
```

Snapshot `RunSummary` fields mean:

| Field | Meaning |
| --- | --- |
| `records_read` | Source rows returned by completed chunk fetches in this physical attempt |
| `records_accepted` | Rows represented by chunks successfully reconciled in this attempt |
| `records_rejected` | Always zero; source/system failures are not business-row quarantine |
| `pages_fetched` | Always zero for PostgreSQL snapshot ingestion |
| `bytes_written` | Newly created canonical raw Parquet bytes only; excludes manifests and reused chunks |
| `retry_count` | Always zero; resume is another physical attempt and Botocore retries are internal |
| `duplicate_count` | Complete pre-existing chunk/manifest pairs reused without mutation |
| `duration_seconds` | Monotonic elapsed time for the physical attempt |
| `source_date`, `batch_id`, `run_id` | Logical-date/batch and physical-attempt trace identifiers |

CLI exits are stable:

| Exit | Meaning |
| ---: | --- |
| `0` | New snapshot or complete replay succeeded |
| `2` | Invalid command, source/date/descriptor, or configured-bound violation before client work |
| `3` | Invalid or missing runtime configuration |
| `4` | Handled source drift, PostgreSQL, serialization, reconciliation, conflict, deadline, or storage failure |
| `5` | Unexpected S3 client setup, runtime, or cleanup failure |

## Failure exercises and recovery commands

| Failure | Expected retained state | Recovery command/action |
| --- | --- | --- |
| PostgreSQL connection interruption after a complete chunk | Earlier raw/manifest pairs remain complete and unchanged; no mutable checkpoint or fabricated failed manifest | Restore PostgreSQL, then run `make ingest-postgres ARGS="resume ..."` with every field from `snapshot_started` |
| S3 temporarily unavailable after a fetch | Any earlier complete chunks remain; the current chunk is absent or `payload_only`; command ends within bounded settings | Restore S3 and run the same exact `resume`; replay begins at chunk 1 |
| Stop after raw upload/before manifest | Exact verified raw Parquet remains as `payload_only`; manifest is absent | Run the same `resume`; it verifies bytes and writes only the missing manifest |
| Source changes at/below the captured upper tuple | Existing object evidence remains unchanged; resume reports `source_snapshot_changed` or an immutable conflict | Restore the exact bounded source for recovery, or retain old evidence and perform a reviewed fresh `run`; never overwrite old keys |
| Row inserted above the upper tuple | Original batch remains bounded to its descriptor and excludes the row | Finish the original `resume`; capture later work with a fresh `run` |
| Idempotent replay | Exact raw/manifest inventory, checksums, original manifests, and original run IDs remain unchanged; `bytes_written=0` | No recovery needed; the matching `resume` is safe and reports one duplicate per chunk |

## Limitations

- Recovery is replay from chunk 1, not continuation after a mutable client checkpoint. This avoids silently skipping an interrupted fetch at the cost of bounded rereads.
- The original MVCC snapshot ends when its PostgreSQL connection dies. Descriptor checks and immutable chunk comparison expose ordinary drift but do not create a durable database snapshot across arbitrary compensating edits.
- One process must own one logical snapshot at a time. Garage has no versioning, and check-then-write does not coordinate concurrent writers.
- Holding a read-only transaction while bounded chunks are written to S3 is acceptable for this local lab, not presented as a production-scale export architecture.
- There is no CDC, incremental cross-batch watermark, generic table exporter, user-provided SQL, checkpoint/lease table, exported-snapshot keeper, aggregate manifest, scheduler, multipart upload, or schema evolution.
- The CLI does not list or download objects. Inspect retained state with a standard S3 client against the configured bucket and exact keys.
- Automatic overwrite or deletion is never conflict recovery. The contract prioritizes immutable evidence and visible failure over silent cleanup.
