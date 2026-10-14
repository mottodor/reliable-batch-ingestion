# Reliable Batch Ingestion Lab

A learning project for building restart-safe, idempotent batch ingestion from REST APIs, PostgreSQL, and CSV/JSON/Parquet files into S3-compatible object storage.

> Current stage: BI-050 REST ingestion/backfill and BI-060 restart-safe CSV/JSON/Parquet file ingestion are complete; BI-070 bounded PostgreSQL snapshot ingestion is next.

## Project references

- Product requirements: [`docs/batch-ingestion.md`](docs/batch-ingestion.md)
- Core deterministic contracts: [`docs/core-contracts.md`](docs/core-contracts.md)
- Shared S3 storage contract: [`docs/storage-contract.md`](docs/storage-contract.md)
- Deterministic source contracts: [`docs/source-contracts.md`](docs/source-contracts.md)
- REST API ingestion contract: [`docs/api-ingestion.md`](docs/api-ingestion.md)
- File ingestion contract: [`docs/file-ingestion.md`](docs/file-ingestion.md)
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
# Replace every change-me value and set GARAGE_RPC_SECRET to 64 hexadecimal characters.
make doctor
make policy
make setup
make verify
```

`make verify` validates `compose.yaml` against the committed placeholders in `.env.example`; it does not start containers. Commands that start or inspect services first run a redacting preflight against the ignored `.env` and reject placeholders. The preflight also validates `MOCK_API_PORT` and requires positive `S3_TOTAL_MAX_ATTEMPTS`, `S3_CONNECT_TIMEOUT_SECONDS`, and `S3_READ_TIMEOUT_SECONDS` values. Generate a local Garage RPC secret, for example, with `openssl rand -hex 32`; never commit the result.

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

`make integration` verifies the shared storage boundary against Garage, deterministic PostgreSQL reseeding, the mock API happy path and fault scenarios, unchanged infrastructure markers, repeat bootstrap, and down/up plus destructive-recovery checks in the dedicated `reliable-batch-ingestion-bi020-reset-test` Compose project. Storage checks cover immutable replay/conflict, metadata-preserving move recovery, payload-without-manifest reconciliation, and a bounded deterministic unavailable-endpoint scenario. Temporary credentials and ports are not persisted, all waits are bounded, isolated resources are removed in cleanup, and the default project's volumes are compared before and after the exercise. The default services remain running afterward with the default source seed restored.

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

The counter store is thread-safe and bounded to 128 keys. Successful page bodies are canonical and byte-stable for the same seed and request; restarting the service clears only transient fault counters. Exact query and fault semantics are documented in [`docs/source-contracts.md`](docs/source-contracts.md).

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

Generation validates the seed before changing the output. The check uses temporary directories and does not modify committed fixtures. Exact Parquet bytes are scoped to Python 3.12, the committed `uv.lock`, and the supported local toolchain; schema and logical rows remain the cross-version contract. Scenario paths and serialization details are documented in [`docs/source-contracts.md`](docs/source-contracts.md).

## Shared S3 boundary

Application code constructs the path-style SigV4 client explicitly from `StorageConfig`. Botocore standard retries use the configured total-attempt budget, including the initial request, plus explicit connect/read timeouts. Shared operations stream and verify SHA-256 from object bytes; they never treat S3 `ETag` as a SHA-256 checksum.

`S3Storage` provides immutable write, recoverable `copy -> verify -> delete`, and read-only payload/manifest reconciliation. Different content at an existing deterministic key is a conflict, not an overwrite. A move can temporarily leave both source and destination and converges safely when rerun. Concurrent writers for one key remain unsupported because Garage has no versioning and conditional create is not a contract foundation. See [`docs/storage-contract.md`](docs/storage-contract.md) for exact outcomes and recovery states.

## REST API ingestion

Start the local services, then run either one canonical source date or an inclusive date range:

```bash
make up
make ingest-api ARGS="run --source fx-rates --date 2026-09-01"
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-07"
```

The production source allow-list is `fx-rates` and `shipping-statuses`. Dates must use strict `YYYY-MM-DD` form. Backfill runs sequentially in ascending order, includes both endpoints, and stops at the first failed date. Rerunning the original range reuses complete immutable page/manifest pairs and resumes from the failed date without creating uncontrolled duplicates.

Output is newline-delimited structured JSON: every attempted date emits an API event and final run summary, and a successful backfill emits a completion event. Exit codes are stable: `0` success, `2` invalid arguments/range, `3` invalid configuration, `4` handled ingestion failure, and `5` runtime setup or cleanup failure. The command reads the ignored `.env` through the existing configuration contract and closes both HTTP and S3 clients explicitly.

Each HTTP 200 body is streamed under per-page time and size bounds, checksum-verified in raw storage before JSON/schema validation, and paired one-to-one with an immutable terminal manifest. Transient 429/5xx and permanent 4xx bodies are not stored as business pages. A page payload left without its manifest is repaired by rerunning the same command; complete successful pairs are verified and reused without rewriting their original manifest or `run_id`.

### REST failure and recovery exercises

The mock fault selectors are integration-test controls rather than production CLI flags. After the selected source fault is cleared, use the recovery shown below:

| Failure | Expected object state | Recovery |
| --- | --- | --- |
| `429 -> success` | No 429 body object; final successful page pair after a bounded retry | No action if it succeeds; otherwise rerun the same command after rate limiting clears |
| Permanent 500 | Earlier pages may be complete; no raw object for the failed non-200 page | Clear/fix the source fault and rerun the same `run` or full `backfill` command |
| Permanent 4xx | No raw object for the failed response; the request is not retried | Correct the source/date/configuration and rerun the corrected command |
| Malformed HTTP 200 | Exact malformed bytes and a terminal `failed` manifest remain | Retain the evidence; unchanged rerun re-surfaces it, while corrected bytes require a reviewed new logical identity |
| Stop after some pages | Completed page pairs remain unchanged; the failed non-200 page is absent | Rerun the same command; completed pages are reused |
| Stop after payload/before manifest | Verified raw payload remains in `payload_only` state | Rerun the same command; only the missing manifest is created |

Exact identity/key examples, retry and `Retry-After` rules, bounds, metric meanings, recovery states, handled error codes, and the sequential-writer limitation are documented in [`docs/api-ingestion.md`](docs/api-ingestion.md).

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

`run` validates the command and bounded local regular file before creating an S3 client, stages the exact bytes immutably, then invokes the same restart-safe service as `resume`. `resume` accepts only one canonical BI-060 landing key and does not need the original local file. Valid files retain exact bytes under canonical raw and archive keys. Invalid files retain exact bytes under canonical raw and reason-bearing rejected keys; parseable mixed files reject the whole delivery and publish no accepted subset.

Terminal accepted and retained-rejection outcomes emit newline-delimited structured JSON with one file event and one run summary; a pre-terminal state/storage failure emits a secret-free CLI error instead of fabricating a business outcome. Stable exits are `0` for an accepted new delivery or accepted duplicate, `2` for command/local admission errors, `3` for invalid configuration, `4` for retained rejection or recoverable ingestion/state failure, and `5` for runtime setup/cleanup failure. Same-name and renamed identical deliveries preserve one raw payload, final disposition, and original manifest while reporting distinct duplicate classes. Movement is recoverable `copy -> verify -> delete`, not an atomic rename; after interruption rerun `resume` with the same landing key. Concurrent writers for one logical delivery remain unsupported.

### File failure and recovery exercises

| Failure | Expected object state | Recovery |
| --- | --- | --- |
| Schema/business-invalid or mixed file | Exact bytes under raw and one reason-bearing rejected key; `rejected` manifest; no accepted subset | Correct and redeliver the complete file; retain the rejected evidence |
| Expected-checksum mismatch | Exact bytes under raw and `reason=checksum_mismatch`; `rejected` manifest | Confirm the supplier checksum/bytes and redeliver corrected input as a new content identity |
| Same-name redelivery | Same raw/final keys and unchanged original manifest/run ID; `same_name_redelivery` | No action; repeating `run` is idempotent |
| Same content under a new valid name | Different landing key but the same batch/raw/final keys; `renamed_content` | No action; no second business-data copy is created |
| Stop after raw/final disposition but before manifest | Raw evidence remains; disposition may remain; manifest is absent | Run `resume` with the original canonical landing key |
| Copy succeeds but landing deletion fails | Landing and destination temporarily contain identical verified bytes | Run the same `resume`; it retries delete and writes the manifest only after convergence |
| S3 temporarily unavailable or deterministic state conflicts | Existing evidence remains unchanged; no automatic overwrite/cleanup | Restore S3 and rerun; for a conflict, inspect and explicitly repair the exact retained keys first |

Identity/key examples, filename and schema rules, rejection precedence, bounds, metric meanings, duplicate classes, all recovery states, and limitations are documented in [`docs/file-ingestion.md`](docs/file-ingestion.md).

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
make check-local-env
make ingest-api ARGS="run --source fx-rates --date 2026-09-01"
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-07"
make ingest-file ARGS="run --source supplier-prices --date 2026-09-01 --path /path/to/supplier-prices-2026-09-01.csv"
make ingest-file ARGS="resume --landing-key 'landing/file/...canonical BI-060 key...'"
make up
make down
make logs
make seed
make integration
make reset
make reset-destructive CONFIRM=destroy-local-data
make verify
```

PostgreSQL ingestion and demonstration targets will be added by their atomic tasks in `tasks/backlog.yaml`. This README does not promise commands before they exist.

## Secrets

`.env.example` contains variable names and safe placeholders only. Never commit the real `.env`, Garage keys, or the PostgreSQL password.
