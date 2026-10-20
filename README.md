# Reliable Batch Ingestion Lab

Build a small batch pipeline, break it on purpose, and learn how to restart it without losing raw data or creating uncontrolled duplicates.

The lab ingests daily data from a REST API, PostgreSQL, and supplier CSV/JSON/Parquet files into local S3-compatible storage. It deliberately avoids orchestration and analytics tooling so the reliability mechanics stay visible.

> **Main question:** How can a batch ingestion pipeline restart safely, preserve raw data, and make failures understandable?

The core lab and its independent acceptance audit are complete.

## What you will learn

- derive deterministic batch IDs and object keys;
- preserve immutable raw payloads before parsing or validation;
- paginate APIs with bounded timeouts, retries, and `Retry-After` handling;
- extract a PostgreSQL snapshot with a server-side cursor and fixed upper watermark;
- validate and quarantine whole CSV, JSON, and Parquet deliveries;
- distinguish a duplicate delivery from a new logical batch;
- resume after interruptions between payload, manifest, and file-movement steps;
- describe idempotent redelivery honestly without claiming exactly-once delivery.

## Architecture at a glance

```text
REST API ───────┐
PostgreSQL ─────┼──> Python ingestion CLI ──> Garage (S3 API)
CSV/JSON/Parquet┘                            landing / raw / archive / rejected
```

The local stack uses Python 3.12, uv, Docker Compose, PostgreSQL, and Garage `dxflrs/garage:v2.4.1`. Application code talks to object storage only through the standard S3 API.

## Quick start

Prerequisites: Git, Python 3.12, [uv](https://docs.astral.sh/uv/), Docker, and the Docker Compose plugin.

```bash
cp .env.example .env
make setup
make init-local-env
make up
make seed SOURCE_SEED=20260901
make demo-happy-path
make demo-inspect
```

The happy path ingests both API resources, one supplier file, and one bounded PostgreSQL snapshot. Inspection verifies the stored bytes, checksums, manifests, archive/rejected dispositions, and snapshot rows.

Now exercise safe redelivery and two common failures:

```bash
make demo-duplicate
make demo-api-retry
make demo-bad-file
make demo-inspect
```

Stop the services without deleting their data:

```bash
make down
```

For configuration details, command exits, lifecycle semantics, destructive cleanup, and every ingestion command, use the [complete operator guide](docs/guides/operator-guide.md). Run `make help` for the current command list.

## Try the failure exercises

Each recovery demo creates or verifies the expected intermediate state, then uses the normal ingestion path for recovery or safe terminal handling.

| Exercise | Expected retained state | Recovery |
| --- | --- | --- |
| API rate limit | No 429 body is stored; successful page evidence is completed after a bounded retry | `make demo-recovery SCENARIO=api-rate-limit` |
| Permanent API 500 | Earlier page pairs may remain; the failed response and logical-run manifest are absent | `make demo-recovery SCENARIO=api-permanent-500` |
| Malformed API JSON | Exact HTTP 200 bytes and a failed page manifest remain; no logical-run manifest is created | `make demo-recovery SCENARIO=api-malformed-json` validates the terminal action |
| Same-name file redelivery | Canonical raw, disposition, and original manifest bytes remain unchanged | `make demo-recovery SCENARIO=file-same-name` |
| Same content under a new name | The new landing identity converges on the same canonical business-data copy | `make demo-recovery SCENARIO=file-renamed-content` |
| Schema-invalid file | Exact input bytes remain under raw and a reason-bearing rejected key | `make demo-recovery SCENARIO=file-schema-invalid` |
| PostgreSQL connection loss | Completed chunks remain; replay starts from chunk 1 with the captured descriptor | `make demo-recovery SCENARIO=postgres-connection-drop` |
| S3 temporarily unavailable | Confirmed evidence remains unchanged and the attempt reports a bounded storage failure | `make demo-recovery SCENARIO=s3-unavailable` |
| Payload stored, manifest missing | The immutable payload remains in `payload_only`; rerun adds only the missing manifest | `make demo-recovery SCENARIO=payload-before-manifest` |

The detailed trigger, intermediate-state assertion, exit, recovery command, and final state for all eleven supported scenarios are in [the recovery demo guide](docs/ingestion/recovery-demos.md).

## Run the checks

```bash
make test         # Offline unit and contract tests
make verify       # Full repository gate; does not start services
make integration  # Local Compose integration and recovery checks
```

All waits and retries are bounded. Unit tests do not use public network services.

## Important limits

- Raw payloads are immutable after a successful write.
- Supplier files use whole-file acceptance: one invalid required value rejects the complete file.
- S3 movement is recoverable `copy -> verify -> delete`, not an atomic rename.
- Garage has no bucket versioning; concurrent writers for one logical key are unsupported.
- The project claims deterministic keys, idempotent writes, and safe sequential redelivery—not exactly-once delivery.
- Airflow, Kafka, CDC, lakehouse layers, BI tools, and other possible extensions are intentionally outside the core lab.

## Where to go next

| If you want to… | Read… |
| --- | --- |
| See every setup, lifecycle, ingestion, and cleanup command | [Operator guide](docs/guides/operator-guide.md) |
| Understand REST pagination, retries, backfill, and manifests | [REST ingestion](docs/ingestion/api-ingestion.md) |
| Understand file validation, duplicate classes, and quarantine | [File ingestion](docs/ingestion/file-ingestion.md) |
| Understand bounded snapshots and watermark recovery | [PostgreSQL snapshots](docs/ingestion/postgres-snapshot.md) |
| Explore all deterministic failure/recovery demos | [Recovery demos](docs/ingestion/recovery-demos.md) |
| Study IDs, object keys, manifests, and storage invariants | [Core contracts](docs/contracts/core-contracts.md) and [storage contract](docs/contracts/storage-contract.md) |
| Read the complete learning requirements | [Product specification](docs/specification/batch-ingestion.md) |
| Review architectural trade-offs | [Architecture decisions](docs/adr/README.md) |
| Contribute with a coding agent | [Session navigation](docs/START_HERE.md) and [agent rules](AGENTS.md) |

`.env.example` contains safe placeholders only. Never commit the generated `.env`, access keys, or passwords.
