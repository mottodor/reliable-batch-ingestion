# Reliable Batch Ingestion Lab

A learning project for building restart-safe, idempotent batch ingestion from REST APIs, PostgreSQL, and CSV/JSON/Parquet files into S3-compatible object storage.

> Current stage: deterministic core contracts, local Compose infrastructure, and the shared S3 storage boundary are implemented; deterministic source fixtures are next.

## Project references

- Product requirements: [`docs/batch-ingestion.md`](docs/batch-ingestion.md)
- Core deterministic contracts: [`docs/core-contracts.md`](docs/core-contracts.md)
- Shared S3 storage contract: [`docs/storage-contract.md`](docs/storage-contract.md)
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

`make verify` validates `compose.yaml` against the committed placeholders in `.env.example`; it does not start containers. Commands that start or inspect services first run a redacting preflight against the ignored `.env` and reject placeholders. The preflight also requires positive `S3_TOTAL_MAX_ATTEMPTS`, `S3_CONNECT_TIMEOUT_SECONDS`, and `S3_READ_TIMEOUT_SECONDS` values. Generate a local Garage RPC secret, for example, with `openssl rand -hex 32`; never commit the result.

## Local infrastructure

```bash
make up           # Start PostgreSQL and Garage; fail if not healthy within 60 seconds.
make seed         # Idempotently ensure infrastructure bootstrap; no domain rows yet.
make integration  # Check bootstrap plus isolated down/up and destructive recovery.
make logs         # Follow service logs until interrupted.
make down         # Stop containers without deleting named volumes.
make reset        # Refuse implicit deletion and print the confirmed command.
```

`make integration` verifies the shared storage boundary against Garage, creates deterministic PostgreSQL and S3 markers, verifies repeat bootstrap, and runs down/up plus destructive-recovery checks in the dedicated `reliable-batch-ingestion-bi020-reset-test` Compose project. Storage checks cover immutable replay/conflict, metadata-preserving move recovery, payload-without-manifest reconciliation, and a bounded deterministic unavailable-endpoint scenario. Temporary credentials and ports are not persisted, all waits are bounded, isolated resources are removed in cleanup, and the default project's volumes are compared before and after the exercise. The default services remain running afterward.

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
```

## Shared S3 boundary

Application code constructs the path-style SigV4 client explicitly from `StorageConfig`. Botocore standard retries use the configured total-attempt budget, including the initial request, plus explicit connect/read timeouts. Shared operations stream and verify SHA-256 from object bytes; they never treat S3 `ETag` as a SHA-256 checksum.

`S3Storage` provides immutable write, recoverable `copy -> verify -> delete`, and read-only payload/manifest reconciliation. Different content at an existing deterministic key is a conflict, not an overwrite. A move can temporarily leave both source and destination and converges safely when rerun. Concurrent writers for one key remain unsupported because Garage has no versioning and conditional create is not a contract foundation. See [`docs/storage-contract.md`](docs/storage-contract.md) for exact outcomes and recovery states.

## Available commands

```bash
make help
make doctor
make policy
make setup
make format
make lint
make test
make check-local-env
make up
make down
make logs
make seed
make integration
make reset
make reset-destructive CONFIRM=destroy-local-data
make verify
```

Ingestion and demonstration targets will be added by their atomic tasks in `tasks/backlog.yaml`. This README does not promise commands before they exist.

## Secrets

`.env.example` contains variable names and safe placeholders only. Never commit the real `.env`, Garage keys, or the PostgreSQL password.
