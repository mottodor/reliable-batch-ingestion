# Lab 01 — Reliable batch ingestion

## Purpose

An independent learning project for ingesting data from REST APIs, PostgreSQL, and file sources into S3-compatible object storage. The primary subject is not a particular data format, but the operational properties of an ingestion process: repeatability, idempotency, backfill, retries, quarantine, and reproducible recovery after failure.

The lab does not depend on Airflow, Kafka, Debezium, Iceberg, Trino, dbt, ClickHouse, or Superset. Every scenario must run locally from a clean clone.

## Main learning question

> How can a batch ingestion pipeline restart safely, preserve raw data, avoid uncontrolled duplicates, and handle source errors clearly?

## Learning outcomes

After completing the lab, a developer should be able to:

- design the boundary between a source, raw archive, and rejected data;
- ingest a paginated REST API with timeouts and bounded retries;
- handle HTTP 429, 500, malformed responses, and partial failures correctly;
- extract a PostgreSQL snapshot without `SELECT *` or unbounded memory use;
- accept CSV, JSON, and Parquet files;
- validate file names, checksums, schemas, and business constraints;
- distinguish redelivery from a new batch;
- run a backfill for a date range;
- use `batch_id`, `run_id`, source date, and checksum for tracing;
- preserve the original payload before transformation;
- resume safely after a process failure;
- explain delivery semantics without an unjustified exactly-once claim.

## Project boundaries

### In scope

- deterministic source-data generator;
- mock REST API with pagination and fault injection;
- PostgreSQL as a snapshot source;
- generation and intake of CSV, JSON, and Parquet files;
- Python CLI for running ingestion;
- S3-compatible object storage for landing, raw archive, and rejected data;
- manifests with batch metadata;
- unit and integration tests;
- backfill and recovery exercises;
- structured logs.

### Out of scope

- Airflow orchestration;
- streaming delivery and CDC;
- Bronze/Silver/Gold analytical transformations;
- Iceberg catalog and table maintenance;
- dbt models;
- BI and serving layers;
- distributed Spark processing;
- a full production secret manager.

## Business scenario

The project uses a small OmniRetail domain:

1. A REST API supplies daily foreign-exchange rates and shipping statuses.
2. PostgreSQL contains orders changed during a time interval.
3. A supplier delivers a product-price file.
4. Ingestion stores original data and a manifest in S3-compatible object storage.
5. Valid files move to archive; invalid files move to rejected.
6. Repeating the same logical batch does not create another copy of business data.

Small fixtures with tens or hundreds of rows are sufficient; production-scale volume is unnecessary.

## Minimal architecture

```text
                   +----------------+
                   | Mock REST API  |
                   +--------+-------+
                            |
+------------+              |              +----------------+
| PostgreSQL +--------------+------------->| Python CLI     |
+------------+              |              | ingestion      |
                            |              +-------+--------+
+------------+              |                      |
| CSV/JSON/  +--------------+                      v
| Parquet    |                       +------------------------------+
+------------+                       | S3-compatible object storage |
                                     | landing/raw/archive/rejected |
                                     +------------------------------+
```

## Minimal stack

- Python 3.12;
- uv;
- PostgreSQL;
- S3-compatible Garage object storage (`dxflrs/garage:v2.4.1`);
- a small mock API;
- `httpx` for HTTP;
- `boto3` for the S3 API;
- Psycopg 3 for PostgreSQL;
- `pyarrow` only for Parquet;
- pytest;
- Docker Compose.

## Proposed repository structure

```text
reliable-batch-ingestion/
├── src/batch_lab/
│   ├── api/
│   ├── files/
│   ├── postgres_snapshot/
│   ├── manifests/
│   └── cli.py
├── mock_api/
├── postgres/init/
├── fixtures/
├── tests/
│   ├── unit/
│   └── integration/
├── compose.yaml
├── pyproject.toml
├── uv.lock
├── Makefile
├── .env.example
└── README.md
```

## Raw-object contract

Every accepted object must have an immutable raw payload and a metadata manifest containing at least these fields:

```text
source_name
source_type
source_date
batch_id
run_id
object_key
original_file_name
content_type
content_length
checksum_sha256
schema_version
extracted_at
status
attempt
```

Every field name remains present in the manifest schema. A source-specific field that does not apply, such as `original_file_name` for an API page or PostgreSQL snapshot chunk, is nullable and must not be replaced with invented data.

A manifest must not contain secrets, Authorization headers, or connection passwords.

## Core exercises

### Exercise 1 — REST API ingestion

Implement:

- pagination;
- connect and read timeouts;
- bounded exponential backoff;
- `Retry-After` handling for 429;
- no retries for permanent 4xx responses;
- persistence of every raw page;
- a manifest for the complete logical run;
- repeat execution without duplicating successfully persisted pages.

### Exercise 2 — File ingestion

Implement this flow:

```text
landing -> validation -> archive | rejected
```

Validate:

- allowed source;
- file-name format;
- checksum;
- schema compatibility;
- required fields;
- types;
- duplicate file;
- empty file;
- mixed valid and invalid rows.

Choose and document file atomicity explicitly: reject the whole file or split accepted and rejected records.

### Exercise 3 — PostgreSQL snapshot

Implement extraction:

- with an explicit column list;
- through pages or a server-side cursor;
- with a watermark;
- with a consistent upper bound;
- with safe repeat execution;
- without retaining the entire snapshot in memory.

### Exercise 4 — Backfill

The CLI must support a date range:

```bash
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-07"
```

A repeated backfill must produce the same set of raw objects and manifests, or explicitly create a new physical attempt linked to the existing logical batch.

### Exercise 5 — Recovery

Terminate the process deliberately:

- after uploading a payload but before writing its manifest;
- after some API pages;
- while moving a file;
- after reading a PostgreSQL page.

After restart, the pipeline must continue or repeat the operation safely.

## Test strategy

### Unit tests

- deterministic object-key generation;
- checksum calculation;
- retry classification;
- manifest serialization;
- schema validation;
- watermark calculation;
- duplicate detection;
- error-to-status mapping.

### Integration tests

- mock API pagination;
- 429 followed by success;
- permanent 500 until the retry budget is exhausted;
- broken JSON response;
- duplicate file delivery;
- invalid CSV moved to rejected;
- snapshot restart;
- repeated backfill;
- verification of the raw payload stored in S3-compatible object storage.

## Failure scenarios

The following scenarios are mandatory:

1. The API temporarily returns 429.
2. The API permanently returns 500.
3. One response page contains malformed JSON.
4. A file is delivered again under the same name.
5. The same content is delivered under a different name.
6. A file violates its schema.
7. The PostgreSQL connection drops in the middle of a snapshot.
8. S3-compatible object storage is temporarily unavailable.
9. The process fails between payload and manifest writes.

For every scenario, the README must describe the expected data state and recovery command.

## Observable metrics

Even without Prometheus, the pipeline must emit a final structured summary:

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

## Demonstration scenario

```bash
cp .env.example .env
make up
make seed
make demo-happy-path
make demo-duplicate
make demo-api-retry
make demo-bad-file
make test
make down
```

At the end, the user must be able to inspect:

- raw API pages;
- snapshot files;
- an archived supplier file;
- a rejected malformed file;
- manifests;
- no new objects after an idempotent repeat run.

## Definition of Done

The lab is complete when:

- it runs from a clean clone;
- the happy path completes within a few minutes;
- repeat execution creates no uncontrolled duplicates;
- raw payloads remain unchanged;
- retries are bounded by attempt count and time;
- backfill is reproducible;
- invalid input is retained with a clear rejection reason;
- unit and integration tests pass;
- the README contains at least four failure exercises;
- Git contains no secrets;
- `make reset` is explicitly marked destructive.

## Deliberate trade-offs

- Garage (`dxflrs/garage:v2.4.1`) provides local S3-compatible object storage, but the lab does not model complete IAM behavior.
- There is no scheduler; manual execution keeps attention on ingestion semantics.
- Small duplication at the raw layer is acceptable when the deduplication contract documents it explicitly.
- The project does not claim exactly-once delivery; the goal is deterministic keys, idempotent writes, and understandable redelivery.

## Possible extensions

Only after the core version is complete:

- schema evolution;
- streaming multipart upload;
- XLSX edge cases;
- incremental extraction with a composite watermark;
- contract compatibility checks;
- OpenTelemetry spans;
- Airflow as a separate integration exercise.
