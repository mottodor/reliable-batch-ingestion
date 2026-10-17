# Reliable Batch Ingestion Lab

A learning project for building restart-safe, idempotent batch ingestion from REST APIs, PostgreSQL, and CSV/JSON/Parquet files into S3-compatible object storage.

> Current stage: the core lab and BI-092 through BI-095 audit repairs are implemented and acceptance-tested. BI-096 clean-commit reproducibility attestation is the next bounded audit follow-up.

## Project references

- Product requirements: [`docs/specification/batch-ingestion.md`](docs/specification/batch-ingestion.md)
- Core deterministic contracts: [`docs/contracts/core-contracts.md`](docs/contracts/core-contracts.md)
- Shared S3 storage contract: [`docs/contracts/storage-contract.md`](docs/contracts/storage-contract.md)
- Deterministic source contracts: [`docs/contracts/source-contracts.md`](docs/contracts/source-contracts.md)
- REST API ingestion contract: [`docs/ingestion/api-ingestion.md`](docs/ingestion/api-ingestion.md)
- File ingestion contract: [`docs/ingestion/file-ingestion.md`](docs/ingestion/file-ingestion.md)
- PostgreSQL snapshot contract: [`docs/ingestion/postgres-snapshot.md`](docs/ingestion/postgres-snapshot.md)
- Recovery demos and clean-export walkthrough: [`docs/ingestion/recovery-demos.md`](docs/ingestion/recovery-demos.md)
- Architectural decisions: [`docs/adr/README.md`](docs/adr/README.md)
- Atomic backlog and active task: [`tasks/backlog.yaml`](tasks/backlog.yaml)
- Current state: [`docs/STATE.md`](docs/STATE.md)
- Coding-agent rules: [`AGENTS.md`](AGENTS.md)

The **Possible extensions** section is outside the current scope.

## Fixed stack

- Python 3.12;
- uv;
- PostgreSQL;
- Garage `dxflrs/garage:v2.4.1` as S3-compatible object storage;
- `httpx`, `boto3`, Psycopg 3, and `pyarrow`;
- pytest;
- Docker Compose.

## Environment setup

```bash
cp .env.example .env
make init-local-env  # Replace only recognized placeholders atomically without printing values.
make doctor
make policy
make setup
make verify
```

`make verify` validates `compose.yaml` against the committed placeholders in `.env.example`; it does not start containers. `make init-local-env` replaces only the four recognized credential placeholders, writes an owner-only temporary file, runs the same full preflight before atomic publication, and leaves a valid initialized file byte-for-byte unchanged on rerun. It refuses tracked files, symlinks, unknown placeholders, and partial credential rotation without printing generated values.

Commands that start or inspect services first run the redacting preflight against the ignored `.env`. The preflight constructs the complete application configuration and validates PostgreSQL, S3, HTTP, file, snapshot, and logging settings. It separately validates Compose-only ports, Garage RPC secret, and recognized placeholders. This is pure configuration validation: it does not contact services or prove that credentials are accepted. It also does not validate `SOURCE_SEED`, service readiness, Compose project names, wait budgets, or arbitrary shell variables; those are checked by their owning commands where applicable. Generate a local Garage RPC secret, for example, with `openssl rand -hex 32`; never commit the result.

## Local infrastructure

```bash
make up           # Start PostgreSQL, Garage, and the mock API; fail if unhealthy within 60 seconds.
make seed         # Start services and replace OmniRetail orders with the default deterministic seed.
make integration  # Check bootstrap plus isolated down/up and destructive recovery.
make logs         # Follow service logs until interrupted.
make down         # Stop containers without deleting named volumes.
make reset        # Refuse implicit deletion and print the confirmed command.
```

`make seed` validates `SOURCE_SEED` before starting Compose, applies the ordered SQL files under `postgres/init/`, and transactionally replaces only `omni_retail.orders`. The default is `20260901`; select another valid seed explicitly with `make seed SOURCE_SEED=<integer>`. A repeated seed produces the same 24 explicit-column rows, while invalid seed text fails before Compose or PostgreSQL is invoked. Existing Garage objects, the BI-020 PostgreSQL marker, and unrelated database objects are preserved.

`make integration` verifies the shared storage boundary against Garage, deterministic PostgreSQL reseeding, the mock API happy path and fault scenarios, unchanged infrastructure markers, repeat bootstrap, and down/up plus destructive-recovery checks in the dedicated `reliable-batch-ingestion-bi020-reset-test` Compose project. Storage checks cover immutable replay/conflict, metadata-preserving move recovery, payload-without-manifest reconciliation, and bounded isolated unavailable-endpoint commands for REST, file, and snapshot ingestion. Each unavailable command must emit its source terminal failure event plus final summary, return handled exit `4`, redact sensitive/backend values, and leave exact target state unchanged. Temporary credentials and ports are not persisted, all waits are bounded, isolated resources are removed in cleanup, and the default project's volumes are compared before and after the exercise. The default services remain running afterward with the default source seed restored.

### Mock API

The Compose-hosted API uses `SOURCE_SEED=20260901` by default and publishes the configured `MOCK_API_PORT` on host loopback only. Both resources require `source_date`, `page`, and `page_size`:

```text
GET http://localhost:8080/v1/fx-rates?source_date=2026-09-01&page=1&page_size=2
GET http://localhost:8080/v1/shipping-statuses?source_date=2026-09-01&page=1&page_size=5
```

Selectable scenarios are `happy`, `rate_limit_once`, `permanent_500`, `malformed_json`, and `partial_pages`. `rate_limit_once` also requires a unique `scenario_key`; malformed and partial scenarios require `fault_page`. Reset one transient key or all keys with the local-only control operation:

```text
POST http://localhost:8080/__control/reset?scenario_key=<key>
POST http://localhost:8080/__control/reset
```

The counter store is thread-safe and bounded to 128 keys. Successful page bodies are canonical and byte-stable for the same seed and request; restarting the service clears only transient fault counters. Exact query and fault semantics are documented in [`docs/contracts/source-contracts.md`](docs/contracts/source-contracts.md).

All lifecycle targets are scoped to `COMPOSE_PROJECT_NAME` (default: `reliable-batch-ingestion`). Normal `make down` preserves the three named volumes. Destructive reset is intentionally a separate, irreversible command and requires the exact confirmation token:

```bash
make reset-destructive CONFIRM=destroy-local-data
```

This deletes PostgreSQL data, Garage metadata, and Garage object data for the selected `COMPOSE_PROJECT_NAME`. Missing or incorrect confirmation fails before Compose is invoked.

### Health inspection

After `make up`, inspect the same health commands used by Compose:

```bash
docker compose --project-name reliable-batch-ingestion --env-file .env ps
docker compose --project-name reliable-batch-ingestion --env-file .env exec garage /garage status
docker compose --project-name reliable-batch-ingestion --env-file .env exec postgres \
  sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
python3 -c 'import urllib.request; urllib.request.urlopen("http://localhost:8080/health", timeout=2).read()'
```

## Supplier fixture corpus

The committed `fixtures/` corpus contains valid, empty, schema-invalid, and mixed-row supplier prices in CSV, JSON, and Parquet, plus same-name redeliveries and same-content/different-name files. `fixtures/checksums.json` records each payload's scenario, format, byte length, and SHA-256. It is fixture-generation evidence rather than an ingestion manifest.

```bash
make fixtures SOURCE_SEED=20260901        # Replace the committed corpus deterministically.
make fixtures-check SOURCE_SEED=20260901  # Regenerate twice and compare bytes and semantics.
```

Generation validates the seed before changing the output. The check uses temporary directories and does not modify committed fixtures. Exact Parquet bytes are scoped to Python 3.12, the committed `uv.lock`, and the supported local toolchain; schema and logical rows remain the cross-version contract. Scenario paths and serialization details are documented in [`docs/contracts/source-contracts.md`](docs/contracts/source-contracts.md).

## Shared S3 boundary

Application code constructs the path-style SigV4 client explicitly from `StorageConfig`. Botocore standard retries use the configured total-attempt budget, including the initial request, plus explicit connect/read timeouts. Shared operations stream and verify SHA-256 from object bytes; they never treat S3 `ETag` as a SHA-256 checksum.

`S3Storage` provides immutable write, recoverable `copy -> verify -> delete`, and read-only payload/manifest reconciliation. Known Botocore construction, request, response-stream, and close failures become one redacted `StorageBackendError` with the original error retained only as its chained cause; missing/conflict/verification classifications remain distinct and programming defects are not mislabeled. REST, file, and snapshot services own the S3 session after stable trace identity exists, delay success until clean client close, and convert a backend failure into exactly one source terminal event followed by one final summary with confirmed-only counters. Different content at an existing deterministic key is a conflict, not an overwrite. A move can temporarily leave both source and destination and converges safely when rerun. Concurrent writers for one key remain unsupported because Garage has no versioning and conditional create is not a contract foundation. See [`docs/contracts/storage-contract.md`](docs/contracts/storage-contract.md) for exact outcomes and recovery states.

## REST API ingestion

Start the local services, then run either one canonical source date or an inclusive date range:

```bash
make up
make ingest-api ARGS="run --source fx-rates --date 2026-09-01"
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-07"
```

The production source allow-list is `fx-rates` and `shipping-statuses`. Dates must use strict `YYYY-MM-DD` form. Backfill runs sequentially in ascending order, includes both endpoints, and stops at the first failed date. Each successful date requires one fully verified logical-run manifest in addition to its immutable page/manifest pairs. Rerunning the original range reuses complete evidence and resumes from the failed date without creating uncontrolled duplicates.

Output is newline-delimited structured JSON: handled source/contract outcomes, including S3 session setup/close and backend inspection/read/write failures, emit exactly one `api_ingestion` event followed by one final run summary; a successful backfill emits a completion event. Backend S3 failures use `storage_error`, confirmed-only counters, and exit `4` without an extra `cli_error`; exit `5` remains for unexpected non-storage HTTP/runtime setup or cleanup failures. Other stable exits are `0` success, `2` invalid arguments/range, and `3` invalid configuration. The date service derives stable trace IDs before it acquires its S3 session, and the command reads the ignored `.env` through the existing configuration contract and closes context-owned clients explicitly.

Each HTTP 200 body is streamed as unbuffered transport chunks under configured HTTP operation timeouts, a total monotonic page budget, and a response-size bound. The total budget is checked around every transport receive, so a continuously trickling peer cannot extend body consumption indefinitely; a chunk that first crosses the deadline is discarded with the temporary spool, no incomplete raw page is persisted, and the handled result is `http_retry_exhausted`. Complete in-budget bytes are checksum-verified in raw storage before JSON/schema validation and paired one-to-one with an immutable terminal page manifest. After all required page pairs succeed, the service writes or reuses one separate success-only `api_logical_run_manifest` at `manifests/api/{source}/source_date={date}/batch_id={batch_id}/logical-run.manifest.json`, then verifies it and every referenced exact byte sequence before emitting success. A successful `N`-page batch therefore has `N` raw pages, `N` page manifests, and one aggregate with no synthetic raw payload.

Transient 429/5xx and permanent 4xx bodies are not stored as business pages. A page payload left without its page manifest is repaired by rerunning the same command; complete successful pairs are verified and reused without rewriting their original manifest or `run_id`. Legacy page-only successful batches gain only the missing aggregate during normal replay. Mixed page-manifest `run_id` values are valid, while the aggregate's original `completion_run_id`, `completed_at`, and bytes remain fixed on replay. `bytes_written` still counts raw bytes only, and `duplicate_count` still counts reused page pairs only.

### REST failure and recovery exercises

The mock fault selectors are integration-test controls rather than production CLI flags. After the selected source fault is cleared, use the recovery shown below:

| Failure | Expected object state | Recovery |
| --- | --- | --- |
| `429 -> success` | No 429 body object; final successful page pair(s) and one aggregate after a bounded retry | No action if it succeeds; otherwise rerun the same command after rate limiting clears |
| Permanent 500 | Earlier pages may be complete; the failed non-200 page and aggregate are absent | Clear/fix the source fault and rerun the same `run` or full `backfill` command |
| Permanent 4xx | No raw object for the failed response and no aggregate; the request is not retried | Correct the source/date/configuration and rerun the corrected command |
| Malformed HTTP 200 | Exact malformed bytes and a terminal `failed` page manifest remain; no aggregate exists | Retain the evidence; unchanged rerun re-surfaces it, while corrected bytes require a reviewed new logical identity |
| Stop after some pages | Completed page pairs remain unchanged; the failed non-200 page and aggregate are absent | Rerun the same command; completed pages are reused and the aggregate is published last |
| Stop after payload/before page manifest | Verified raw payload remains in `payload_only` state; no aggregate exists | Rerun the same command; the missing page manifest is created before completion |
| Stop after final page manifest/before aggregate | Every page pair is complete but logical completion is absent | Rerun the same command; only the missing aggregate is added after exact verification |
| Corrupt/conflicting/orphaned aggregate | Completion claim and remaining page evidence stay unchanged; ingestion stops before fetching | Investigate and explicitly repair the exact retained state; no automatic overwrite/delete is allowed |
| S3 temporarily unavailable | Confirmed page evidence remains unchanged; one failed `api_ingestion`/`storage_error` and one summary report confirmed progress, exit `4`; no failed aggregate is created | Restore S3 and rerun the same `run` or full `backfill`; exact-key reconciliation reuses confirmed evidence |

Exact identity/key examples, retry and `Retry-After` rules, bounds, metric meanings, recovery states, handled error codes, and the sequential-writer limitation are documented in [`docs/ingestion/api-ingestion.md`](docs/ingestion/api-ingestion.md).

## File ingestion

Stage and ingest one local supplier file, optionally checking a supplier-provided SHA-256:

```bash
make ingest-file ARGS="run --source supplier-prices --date 2026-09-01 --path /path/to/supplier-prices-2026-09-01.csv"
make ingest-file ARGS="run --source supplier-prices --date 2026-09-01 --path /path/to/supplier-prices-2026-09-01.parquet --expected-sha256 <64-lowercase-hex>"
```

Resume from a deterministic key printed by an earlier structured event or identified while inspecting storage:

```bash
make ingest-file ARGS="resume --landing-key 'landing/file/supplier-prices/source_date=2026-09-01/format=csv/content_sha256=<sha256>/expected_sha256=none/supplier-prices-2026-09-01.csv'"
```

`run` validates the command and bounded local regular file before creating an S3 client. That admission establishes the deterministic delivery descriptor; the restart-safe file service then owns S3 session setup, immutable landing staging, processing, and client close. `resume` enters the same service without a local staging stream, accepts only one canonical BI-060 landing key, and does not need the original local file. Valid files retain exact bytes under canonical raw and archive keys. Invalid files retain exact bytes under canonical raw and reason-bearing rejected keys; parseable mixed files reject the whole delivery and publish no accepted subset.

Every identified attempt emits exactly one `file_ingestion` event followed by one run summary. Backend failures use outcome `failed` with `storage_failed`; conflicting or non-convergent retained states use `storage_state_failed`. Both keep business `rejection_code`/diagnostics separate, report only confirmed counters, create no failed manifest, and return exit `4` without another `cli_error`. Stable exits are `0` for an accepted new delivery or accepted duplicate, `2` for command/local admission errors, `3` for invalid configuration, `4` for retained rejection or handled ingestion/storage-state failure, and `5` for an unexpected non-storage runtime failure. Same-name and renamed identical deliveries preserve one raw payload, final disposition, and original manifest while reporting distinct duplicate classes. Movement is recoverable `copy -> verify -> delete`, not an atomic rename; after interruption rerun `resume` with the same landing key. Concurrent writers for one logical delivery remain unsupported.

### File failure and recovery exercises

| Failure | Expected object state | Recovery |
| --- | --- | --- |
| Schema/business-invalid or mixed file | Exact bytes under raw and one reason-bearing rejected key; `rejected` manifest; no accepted subset | Correct and redeliver the complete file; retain the rejected evidence |
| Expected-checksum mismatch | Exact bytes under raw and `reason=checksum_mismatch`; `rejected` manifest | Confirm the supplier checksum/bytes and redeliver corrected input as a new content identity |
| Same-name redelivery | Same raw/final keys and unchanged original manifest/run ID; `same_name_redelivery` | No action; repeating `run` is idempotent |
| Same content under a new valid name | Different landing key but the same batch/raw/final keys; `renamed_content` | No action; no second business-data copy is created |
| Stop after raw/final disposition but before manifest | Raw evidence remains; disposition may remain; manifest is absent | Run `resume` with the original canonical landing key |
| Copy succeeds but landing deletion fails | Landing and destination temporarily contain identical verified bytes | Run the same `resume`; it retries delete and writes the manifest only after convergence |
| S3 temporarily unavailable or deterministic state conflicts | Existing evidence remains unchanged; `file_ingestion` outcome `failed` plus one summary, exit `4`; no failed manifest or automatic overwrite/cleanup | Restore S3 and rerun; for a conflict, inspect and explicitly repair the exact retained keys first |

Identity/key examples, filename and schema rules, rejection precedence, bounds, metric meanings, duplicate classes, all recovery states, and limitations are documented in [`docs/ingestion/file-ingestion.md`](docs/ingestion/file-ingestion.md).

## PostgreSQL snapshot ingestion

Capture one bounded daily `orders` snapshot after starting and seeding the local services:

```bash
make up
make seed SOURCE_SEED=20260901
make ingest-postgres ARGS="run --source orders --date 2026-09-01"
```

The `snapshot_started` NDJSON event contains the complete descriptor needed if that attempt is interrupted. Resume a non-empty snapshot with exactly those emitted values:

```bash
make ingest-postgres ARGS="resume --source orders --date 2026-09-01 --upper-updated-at 2026-09-01T14:11:00.000000Z --upper-order-id 202609010024 --expected-record-count 24"
```

Resume an emitted empty descriptor with:

```bash
make ingest-postgres ARGS="resume --source orders --date 2026-09-01 --empty"
```

The sole production source is `orders`. `run` captures a count and inclusive `(updated_at, order_id)` upper watermark inside the selected UTC day, then reads seven explicit columns through a named server-side cursor in one read-only `REPEATABLE READ` transaction. Only one configured row chunk and its bounded Parquet serialization are live at a time. Every deterministic raw chunk is verified before its one succeeded manifest is written.

Recovery always revalidates the emitted descriptor and replays from chunk 1. Complete chunks and their original manifests/run IDs are preserved, `payload_only` chunks receive only the missing manifest, and conflicting evidence is never overwritten. After descriptor capture/revalidation establishes `batch_id`, the service emits `snapshot_started` and opens its S3 session. A backend setup/reconciliation/read/write/close failure emits exactly one `snapshot_failed` with `storage_failed` followed by one final summary, reports confirmed-only counters, creates no failed manifest, and returns exit `4` without duplicate CLI output. Stable exits are `0` success, `2` invalid command/descriptor, `3` invalid configuration, `4` handled ingestion failure, and `5` unexpected non-storage runtime/cleanup failure. Concurrent writers remain unsupported, and this is not an exactly-once claim.

### PostgreSQL snapshot failure and recovery exercises

| Failure | Expected object state | Recovery |
| --- | --- | --- |
| PostgreSQL connection interruption after a complete chunk | Earlier raw/manifest pairs remain unchanged; no mutable checkpoint or failed manifest is created | Restore PostgreSQL and `resume` with every descriptor field from `snapshot_started`; replay starts at chunk 1 |
| S3 temporarily unavailable | Existing complete or `payload_only` evidence remains; `snapshot_failed`/`storage_failed` and one summary report confirmed progress, exit `4` | Restore S3 and run the same exact `resume` |
| Stop after raw upload/before manifest | Verified Parquet exists as `payload_only`; its manifest is absent | Run the same `resume`; only the missing manifest is added |
| Source changes at/below the upper tuple | Existing evidence is unchanged; resume fails as source drift or immutable conflict | Restore the bounded source state, or retain old evidence and use a reviewed fresh `run`; never overwrite old keys |
| Row inserted above the upper tuple | The original bounded batch excludes the row | Finish the original `resume`; use a later fresh `run` for another capture |
| Idempotent replay | Exact inventory/checksums and original manifests/run IDs remain unchanged; no raw bytes are written | No action; the matching `resume` is safe |

Exact columns/schema, identity and key examples, transaction/watermark behavior, bounds, metric meanings, error codes, recovery states, and limitations are documented in [`docs/ingestion/postgres-snapshot.md`](docs/ingestion/postgres-snapshot.md).

## Public demonstrations

After explicit environment initialization, healthy services, and deterministic seeding, run the four specification demos:

```bash
make init-local-env
make up
make seed SOURCE_SEED=20260901
make demo-happy-path
make demo-duplicate
make demo-api-retry
make demo-bad-file
make demo-inspect
```

The demos supervise every ingestion subprocess with `DEMO_TIMEOUT_SECONDS` (default `210`), relay the existing ingestion NDJSON, emit exact `demo_state` assertions, and finish with one `demo_summary`. Reports and non-secret descriptors are written below ignored `artifacts/demo/<compose-project>/`; `demo-inspect` reads those descriptors and verifies only calculated S3 prefixes. Inspection streams actual bytes for SHA-256 rather than trusting `ETag`, strictly parses manifests, requires one logical-run aggregate for every successful API batch, cross-checks its page and page-manifest checksums, verifies file dispositions, and validates snapshot Parquet schema, row order, count, and upper tuple. The isolated accepted walkthrough's final exact inventory is 6 raw objects, 8 manifest objects (including two API aggregates), 1 archive object, 2 rejected objects, and 0 landing objects.

`demo-happy-path` uses the existing production REST, file, and PostgreSQL CLIs. `demo-duplicate` verifies unchanged canonical object and manifest bytes across API/snapshot replay plus same-name and renamed-content file delivery. `demo-api-retry` uses a demo-only child adapter to select the mock API's one-time 429 fault while still calling the production ingestion primitive; no fault flag is added to a production CLI. `demo-bad-file` retains canonical-name schema-invalid and malformed UTF-8 files under raw/rejected keys, then safely delivers corrected valid content without deleting rejected evidence.

The demos preserve evidence and never reset a matching batch. They require sequential writers and claim deterministic keys, idempotent writes, safe redelivery, and exact read-only inspection—not exactly-once delivery, atomic S3 rename, or concurrent-writer safety.

Run one allow-listed failure and recovery exercise with:

```bash
make demo-recovery SCENARIO=payload-before-manifest
```

The allow-list covers `api-rate-limit`, `api-permanent-500`, `api-malformed-json`, `api-after-page`, `file-same-name`, `file-renamed-content`, `file-schema-invalid`, `file-during-move`, `postgres-connection-drop`, `s3-unavailable`, and `payload-before-manifest`. The wrapper exits `0` only after the expected underlying failure, exact intermediate state, and normal recovery or terminal action are verified. Abrupt boundaries use private demo workers; production CLIs expose no fault flags.

Each scenario's trigger, expected underlying exit, retained object state, normal command/action, final state, and limitations are documented in [`docs/ingestion/recovery-demos.md`](docs/ingestion/recovery-demos.md). Malformed HTTP 200 content is deliberately terminal under its original identity: unchanged rerun re-surfaces the failed pair, while corrected bytes require a reviewed new logical identity.

To validate the complete flow from repository-owned, non-ignored files in a unique project with bounded setup and exact cleanup:

```bash
uv run python scripts/check_clean_export.py
```

The harness creates an isolated export and owner-only environment, runs locked setup, doctor, Compose rendering, seed, all four public demos, offline tests, and `demo-inspect`, then removes only its unique containers, network, and volumes through exact destructive confirmation. It compares protected default-project volume fingerprints before and after, rejects secrets/payload values in output and reports, and emits a final `clean_export_summary` separating setup, warm happy-path, and total duration. First-time dependency/image acquisition is setup time; only the warm happy path has the validated three-minute budget.

## Available commands

```bash
make help
make doctor
make policy
make setup
make format
make lint
make test
make fixtures
make fixtures-check
make init-local-env
make check-local-env
make ingest-api ARGS="run --source fx-rates --date 2026-09-01"
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-07"
make ingest-file ARGS="run --source supplier-prices --date 2026-09-01 --path /path/to/supplier-prices-2026-09-01.csv"
make ingest-file ARGS="resume --landing-key 'landing/file/...canonical BI-060 key...'"
make ingest-postgres ARGS="run --source orders --date 2026-09-01"
make ingest-postgres ARGS="resume --source orders --date 2026-09-01 --empty"
make demo-happy-path
make demo-duplicate
make demo-api-retry
make demo-bad-file
make demo-recovery SCENARIO=payload-before-manifest
make demo-inspect
uv run python scripts/check_clean_export.py
make up
make down
make logs
make seed
make integration
make reset
make reset-destructive CONFIRM=destroy-local-data
make verify
```

`demo-recovery` accepts only the eleven documented scenario names and rejects missing or unknown values before opening clients or subprocesses.

## Secrets

`.env.example` contains variable names and safe placeholders only. Never commit the real `.env`, Garage keys, or the PostgreSQL password.
