# Core deterministic contracts

This document defines the initial version of the identifier, object-key, manifest, and run-summary contracts implemented by `BI-010`. It is the baseline contract: changing the manifest or object-key contract after this point requires an ADR, as recorded in [`docs/adr/README.md`](../adr/README.md).

## Configuration boundary

`AppConfig.from_env()` is the only environment-reading entry point. Importing `batch_ingestion` creates no network, PostgreSQL, or S3 connections and does not read environment values. Tests and callers may pass an explicit `Mapping[str, str]`.

The configuration model validates required values, positive numeric bounds, HTTP(S) URLs, the S3 addressing style, and the log level. BI-060 adds explicit positive `FILE_MAX_BYTES`, `FILE_SPOOL_THRESHOLD_BYTES`, `FILE_MAX_RECORDS`, `FILE_PARQUET_BATCH_ROWS`, and `FILE_MAX_DIAGNOSTIC_DETAILS` bounds; the in-memory spool threshold may not exceed the admitted file-byte maximum. BI-070 slice 1 adds positive `POSTGRES_CONNECT_TIMEOUT_SECONDS`, `POSTGRES_STATEMENT_TIMEOUT_SECONDS`, `SNAPSHOT_MAX_ELAPSED_SECONDS`, `SNAPSHOT_CHUNK_ROWS`, `SNAPSHOT_MAX_CHUNKS`, `SNAPSHOT_MAX_CHUNK_BYTES`, and `SNAPSHOT_SPOOL_THRESHOLD_BYTES` bounds. Snapshot chunks are limited to the four-digit `1..9999` namespace, and the snapshot spool threshold may not exceed the serialized chunk-byte maximum. PostgreSQL passwords and S3 keys are excluded from dataclass representations. Configuration objects have no manifest serialization path.

## Checksums

`sha256_stream()` reads a binary stream in bounded chunks (64 KiB by default) and returns the 64-character lowercase SHA-256 hexadecimal digest. SHA-256 is calculated from payload bytes. An S3 `ETag` is never interpreted as this checksum.

## Logical and physical identifiers

### Canonical logical input

A `BatchIdentity` contains:

- `source_name`;
- `source_type`: `api`, `file`, or `postgres_snapshot`;
- `source_date` as a calendar date;
- source-specific logical selection parameters.

Normalization is deterministic:

1. Text is Unicode NFC-normalized.
2. `source_name` and parameter names are trimmed and case-folded; whitespace and underscores become hyphens. Their remaining alphabet is lowercase ASCII letters, digits, dots, and hyphens.
3. Parameter values are NFC-normalized and trimmed but remain case-sensitive.
4. Parameter entries are sorted by normalized name. Duplicate normalized names and empty values are invalid.
5. The normalized fields are encoded as UTF-8 canonical JSON with sorted object keys and no insignificant whitespace.

A source adapter must include every value that changes the selected logical data in `parameters`, and must not include a physical retry number. Examples include an API resource/filter, a file content checksum, or snapshot lower/upper bounds. Pagination/chunk numbers identify payloads inside a batch, not a different logical batch.

`batch_id` is the lowercase SHA-256 hexadecimal digest of those canonical bytes. Therefore the same normalized logical input has the same `batch_id`; changing a logical selection field changes it.

### Physical attempts

`run_id` has the form `run_<32 lowercase hex characters>` and is generated from UUID4 for every physical attempt. Retries or reruns retain the logical `batch_id` but receive a new `run_id`. No exactly-once claim is implied.

## Object-key layout

Every raw payload key has this exact layout:

```text
raw/{source_type}/{source_name}/source_date={YYYY-MM-DD}/batch_id={batch_id}/{payload_name}
```

`payload_name` is NFC-normalized, trimmed, and restricted to one safe case-sensitive segment containing ASCII letters, digits, dots, underscores, and hyphens. Source adapters use deterministic names such as `page-0001.json`, `chunk-0001.parquet`, or the normalized delivered file name.

The one-to-one manifest key is derived without storage access:

```text
manifests/{source_type}/{source_name}/source_date={YYYY-MM-DD}/batch_id={batch_id}/{payload_name}.manifest.json
```

Identical canonical identity plus identical normalized payload name yields the same raw and manifest keys. The shared storage boundary treats different bytes at an existing deterministic raw key as a conflict rather than overwriting them; see [`storage-contract.md`](storage-contract.md). Concurrent writers remain outside the core contract.

## Manifest schema version 1

Manifest cardinality is exactly one schema-version-1 manifest per immutable raw payload object. Multiple page or snapshot-chunk manifests may share a logical `batch_id`; each manifest retains the `run_id` and positive `attempt` of the physical attempt that completed that payload. The manifest's `object_key` points to that one raw payload, and `manifest_key_for(object_key)` gives its metadata location. The shared `Manifest`, `Manifest.from_json()`, and `manifest_key_for()` contracts never parse or derive batch-level metadata.

[ADR 0003](../adr/0003-persist-rest-logical-run-manifest.md) defines one API-specific exception to payload/manifest cardinality without changing this schema: after all `N` REST page pairs are verified as succeeded, the API service persists one separate immutable `api_logical_run_manifest` at the deterministic logical-batch key. That aggregate has no synthetic raw payload, uses an independent strict schema/version and 8 MiB bound, and checksums the exact raw-page and stored page-manifest bytes. A successful REST batch therefore has `N` raw pages, `N` schema-version-1 page manifests, and one API logical-run manifest. Its exact fields, publication order, and reconciliation behavior are documented in [`api-ingestion.md`](../ingestion/api-ingestion.md).

Every schema field is always present:

| Field | JSON type | Validation and applicability |
| --- | --- | --- |
| `source_name` | string | Already normalized canonical source name. |
| `source_type` | string | `api`, `file`, or `postgres_snapshot`. |
| `source_date` | string | Calendar date in `YYYY-MM-DD` format. |
| `batch_id` | string | 64 lowercase SHA-256 hexadecimal characters. |
| `run_id` | string | `run_` followed by 32 lowercase hexadecimal characters. |
| `object_key` | string | Canonical raw key matching the source, date, and batch fields. |
| `original_file_name` | string or null | Required for `file`; must be null for API and PostgreSQL payloads. |
| `content_type` | string | Normalized, non-empty media type. |
| `content_length` | integer | Non-negative byte count. |
| `checksum_sha256` | string | 64-character lowercase SHA-256 of payload bytes. |
| `schema_version` | integer | Exactly `1`. |
| `extracted_at` | string | Timezone-aware instant normalized to UTC and serialized as `YYYY-MM-DDTHH:MM:SS.ffffffZ`. |
| `status` | string | One lifecycle status listed below. |
| `attempt` | integer | Positive physical-attempt number. |

Parsing rejects missing fields, unknown fields, wrong JSON types, naive timestamps, noncanonical IDs/keys, and invalid source-specific nullability. Serialization uses compact JSON with sorted keys. The fixed schema contains no DSN, password, credential, token, or Authorization-header field; configured secrets must never be copied into manifest values.

### Status lifecycle

Statuses are:

- `created` — metadata has been constructed for the target payload;
- `raw_stored` — payload bytes are durably preserved and checksum-verified;
- `succeeded` — processing accepted the preserved payload;
- `rejected` — input is invalid and its rejection is recorded;
- `duplicate` — the payload is an understood redelivery;
- `failed` — a system or processing failure is recorded.

Allowed state changes are:

```text
created -> raw_stored | rejected | duplicate | failed
raw_stored -> succeeded | rejected | duplicate | failed
```

`succeeded`, `rejected`, `duplicate`, and `failed` are terminal for that manifest. Writing the same state again is an allowed idempotent no-op. Recovery creates a new physical attempt rather than changing a terminal manifest backward.

Processing outcomes map deterministically:

| Outcome | Status |
| --- | --- |
| `preserved` | `raw_stored` |
| `accepted` | `succeeded` |
| `validation_error` | `rejected` |
| `duplicate` | `duplicate` |
| `system_error` | `failed` |

## Structured run summary

`RunSummary` serializes the observable fields required by the product specification:

```text
records_read
records_accepted
records_rejected
pages_fetched
bytes_written
retry_count
duplicate_count
duration_seconds
source_date
batch_id
run_id
```

Counters and duration are finite and non-negative. `source_date`, `batch_id`, and `run_id` use the same formats as the manifest. JSON is compact and key-sorted so a final summary can be emitted directly as a structured log object.

## Source-attempt terminal output

Once a REST date, admitted file delivery, or descriptor-backed PostgreSQL snapshot has stable `source_date`, `batch_id`, and `run_id` values, its source service owns exactly one ordered terminal pair:

```text
source terminal event
run_summary
```

The source terminal events are `api_ingestion`, `file_ingestion`, and `snapshot_succeeded` or `snapshot_failed`. Source-specific retry/start/chunk events may precede the pair. A one-shot guard rejects duplicate terminal emission, and a handled source exception carries the event and summary already emitted so the CLI returns the handled exit without appending `cli_error` or another summary.

Known backend S3 failures during source-owned session setup, exact-key inspection/read, immutable write, file copy/delete, response streaming, or client close use this same terminal path. The event and summary share the stable trace identifiers; the event exposes only a fixed source-level code, and output excludes credentials, DSNs, endpoint/backend exception text, payload values, and source rows. The original backend exception remains available only through exception chaining.

Failure summaries report confirmed work only. Raw `bytes_written` increases only after a newly created canonical raw payload has been byte-verified; duplicate, accepted, page, and record counters increase only at their source-specific verified boundaries. An uncertain write does not fabricate progress. Storage/system failure output does not create a terminal failed file or snapshot manifest, or a failed REST logical-run aggregate; rerun reconciliation uses the retained deterministic evidence.

Failures before stable attempt identity remain outside this claim: examples are CLI usage/configuration errors, file local-admission errors, and PostgreSQL failures before snapshot descriptor capture. Their source-specific documents define the corresponding CLI output and exit.
