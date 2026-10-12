# Project state

Updated: 2026-10-12

## Current stage

The coding-agent foundation has been audited and normalized to English. Git is initialized on the `main` branch. Product implementation has not started.

```yaml
active_task_id: BI-010
last_completed_task_id: BI-006
```

`tasks/backlog.yaml` is canonical for task status and the active task. The value above is a handoff mirror checked by `scripts/check-repository-policy.sh`.

## Completed

- the product specification is recorded in `docs/batch-ingestion.md`;
- S3-compatible Garage `dxflrs/garage:v2.4.1` was selected;
- the required agent workflow, atomic backlog, and implementation plan exist;
- ADR 0001 records the Garage decision;
- the Python 3.12/uv foundation, Makefile, and verification scripts exist;
- a minimal import smoke test exists;
- repository-owned documentation is in English;
- document responsibilities are explicit and redundant startup instructions were removed;
- repository policy checks detect Cyrillic text and active-task drift;
- the project directory is initialized as a Git work tree on the `main` branch.

## Not completed

- runtime configuration model;
- manifest and status contracts;
- deterministic identifiers and object keys;
- Docker Compose for Garage, PostgreSQL, and the mock API;
- source generators and fixtures;
- REST, file, and PostgreSQL ingestion;
- backfill and recovery exercises;
- integration tests and demo commands.

## Known limitations and risks

- Garage has no bucket versioning, and conditional create with `If-None-Match` is not a contract foundation.
- Concurrent ingestion writers are outside the core contract.
- The Docker CLI is visible from WSL, but the daemon may require WSL integration to be enabled.

## Latest validation

2026-10-12, `./scripts/verify.sh` — PASS:

- doctor — PASS with one warning: the Docker daemon is unavailable from WSL;
- repository policy — PASS: no Cyrillic text, synchronized active task, valid active status, and completed-task handoff;
- locked dependency sync — PASS;
- Ruff lint/format — PASS;
- mypy — PASS;
- pytest — PASS, 1 test.

## Next atomic step

Implement `BI-010`: add and test the core configuration, checksum, deterministic ID/object-key, manifest, status, and structured-summary contracts without network calls or Compose.
