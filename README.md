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
make doctor
make policy
make setup
make verify
```

Docker Compose is used by `make verify` to validate `compose.yaml` against `.env.example`; the default tests do not start containers or access PostgreSQL or S3. Lifecycle commands and integration startup checks are not implemented yet.

## Available commands

```bash
make help
make doctor
make policy
make setup
make format
make lint
make test
make verify
```

Commands such as `make up`, `make seed`, ingestion/demo targets, and integration tests will be added by their atomic tasks in `tasks/backlog.yaml`. This README does not promise commands before they exist.

## Secrets

`.env.example` contains variable names and safe placeholders only. Never commit the real `.env`, Garage keys, or the PostgreSQL password.
