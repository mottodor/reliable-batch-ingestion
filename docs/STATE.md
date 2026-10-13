# Project state

Updated: 2026-10-13

## Current stage

The deterministic core contracts, BI-020 local Compose infrastructure, and BI-030 shared S3 boundary are implemented and documented. The next gate is deterministic source generators and fixtures.

```yaml
active_task_id: BI-040
last_completed_task_id: BI-030
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
- BI-020 slice 1 defines pinned PostgreSQL and Garage Compose services, separate named volumes, bounded healthchecks, secret-free Garage configuration, environment wiring, and static contract tests;
- BI-020 slice 2 adds redacting `.env` preflight, bounded lifecycle commands, real PostgreSQL/Garage health checks, standard-S3 bucket access, and repeat-bootstrap sentinel verification;
- BI-020 slice 3 adds project-scoped lifecycle commands, PostgreSQL/S3 persistence markers, fail-closed reset guards, exact destructive confirmation, and isolated down/up plus destructive-recovery verification;
- BI-020 operator commands and limitations are documented, and its complete acceptance matrix passes against local Compose services;
- BI-030 provides explicit path-style SigV4 client construction with standard total-attempt retries and connect/read timeouts;
- immutable writes stream and verify actual bytes, accept identical replay, and reject different content without overwrite;
- S3 movement converges source-only, both-identical, and destination-only states through copy, byte/metadata verification, then delete;
- payload/manifest reconciliation reports absent, payload-only, complete, and manifest-only states and rejects corrupt or conflicting pairs;
- the shared storage contract, Garage integration checks, and deterministic bounded-endpoint failure test are documented and passing.

## Not completed

- the deterministic mock API;
- deterministic source generators and fixtures;
- REST, file, and PostgreSQL ingestion;
- backfill and recovery exercises;
- ingestion integration suites and demo commands.

## Known limitations and risks

- Garage has no bucket versioning, and conditional create with `If-None-Match` is not a contract foundation.
- Concurrent ingestion writers are outside the core contract.
- Source adapters must include every value that changes selected logical data in `BatchIdentity.parameters`; omission would cause an identifier collision.

## Latest validation

2026-10-13, BI-030 acceptance validation:

- LSP diagnostics for all changed Python files — PASS with no findings;
- `make lint` — PASS;
- `make test` — PASS, 72 unit/default tests;
- storage integration — PASS, 4 Garage/fault-endpoint tests;
- `make integration` — PASS, including all 8 initial infrastructure/storage tests, repeat bootstrap, isolated persistence, reset guards, destructive recovery, and cleanup;
- backlog JSON Schema validation and `./scripts/verify.sh` — PASS.

## Next atomic step

Plan BI-040 test-first, fixing deterministic seeds, source schemas, mock API fault scenarios, fixture byte-stability rules, and acceptance commands before implementation.
