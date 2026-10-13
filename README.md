# Reliable Batch Ingestion Lab

A learning project for building restart-safe, idempotent batch ingestion from REST APIs, PostgreSQL, and CSV/JSON/Parquet files into S3-compatible object storage.

> Current stage: deterministic core contracts are implemented; BI-020 local Compose infrastructure is in progress.

## Project references

- Product requirements: [`docs/batch-ingestion.md`](docs/batch-ingestion.md)
- Core deterministic contracts: [`docs/core-contracts.md`](docs/core-contracts.md)
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

`make verify` validates `compose.yaml` against the committed placeholders in `.env.example`; it does not start containers. Commands that start or inspect services first run a redacting preflight against the ignored `.env` and reject placeholders. Generate a local Garage RPC secret, for example, with `openssl rand -hex 32`; never commit the result.

## Local infrastructure

```bash
make up           # Start PostgreSQL and Garage; fail if not healthy within 60 seconds.
make seed         # Idempotently ensure infrastructure bootstrap; no domain rows yet.
make integration  # Check bootstrap plus isolated down/up and destructive recovery.
make logs         # Follow service logs until interrupted.
make down         # Stop containers without deleting named volumes.
make reset        # Refuse implicit deletion and print the confirmed command.
```

`make integration` creates deterministic PostgreSQL and S3 markers, verifies repeat bootstrap, and runs down/up plus destructive-recovery checks in the dedicated `reliable-batch-ingestion-bi020-reset-test` Compose project. Its temporary credentials and ports are not persisted, all waits are bounded, isolated resources are removed in cleanup, and the default project's volumes are compared before and after the exercise. The default services remain running afterward.

Normal `make down` preserves the three named volumes. Destructive reset is intentionally a separate, irreversible command and requires the exact confirmation token:

```bash
make reset-destructive CONFIRM=destroy-local-data
```

This deletes PostgreSQL data, Garage metadata, and Garage object data for the selected `COMPOSE_PROJECT_NAME`. Missing or incorrect confirmation fails before Compose is invoked.

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
