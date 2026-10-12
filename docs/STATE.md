# Project state

Updated: 2026-10-12

## Current stage

The deterministic core contracts are implemented and documented. BI-020 local Compose infrastructure is in progress: its static Compose contract is complete, and bounded lifecycle/bootstrap work is next.

```yaml
active_task_id: BI-020
last_completed_task_id: BI-010
```

`tasks/backlog.yaml` is canonical for task status and the active task. The value above is a handoff mirror checked by `scripts/check-repository-policy.sh`.

## Completed

- the product specification and repository-local agent workflow are recorded;
- S3-compatible Garage `dxflrs/garage:v2.4.1` was selected in ADR 0001;
- the Python 3.12/uv foundation, quality gates, and Git repository exist;
- typed runtime configuration is loaded only through an explicit call and redacts secrets from representations;
- bounded streaming SHA-256 calculation is implemented;
- canonical logical inputs, deterministic `batch_id` and object keys, and unique physical `run_id` values are implemented;
- manifest schema version 1 defines all required fields, source-specific nullability, one-to-one payload cardinality, UTC serialization, statuses, transitions, and terminal states;
- the structured final run-summary contract contains every metric required by the product specification;
- the initial identifier, object-key, and manifest contracts are documented in `docs/core-contracts.md`;
- unit tests exercise the contracts without network, PostgreSQL, S3, or Compose access;
- BI-020 has a detailed implementation plan covering pinned images, configuration, bounded readiness, idempotent Garage bootstrap, persistence, lifecycle commands, destructive reset, tests, and documentation;
- BI-020 slice 1 defines pinned PostgreSQL and Garage Compose services, separate named volumes, bounded healthchecks, secret-free Garage configuration, environment wiring, and static contract tests.

## Not completed

- operational Compose lifecycle/bootstrap and persistence checks for Garage and PostgreSQL;
- the deterministic mock API;
- the shared S3 storage boundary;
- deterministic source generators and fixtures;
- REST, file, and PostgreSQL ingestion;
- backfill and recovery exercises;
- integration tests and demo commands.

## Known limitations and risks

- Garage has no bucket versioning, and conditional create with `If-None-Match` is not a contract foundation.
- Concurrent ingestion writers are outside the core contract.
- Source adapters must include every value that changes selected logical data in `BatchIdentity.parameters`; omission would cause an identifier collision.

## Latest validation

2026-10-12, `./scripts/verify.sh` — PASS:

- doctor — PASS with no warnings;
- repository policy — PASS;
- locked dependency sync — PASS;
- Ruff lint/format — PASS;
- mypy — PASS;
- pytest — PASS, 36 tests;
- Compose configuration with `.env.example` — PASS.

## Next atomic step

Implement BI-020 slice 2 test-first: add bounded lifecycle/preflight commands and integration checks for PostgreSQL connectivity, Garage health, configured bucket access, and repeated idempotent bootstrap.
