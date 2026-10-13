# Project state

Updated: 2026-10-13

## Current stage

The deterministic core contracts, BI-020 local Compose infrastructure, and BI-030 shared S3 boundary are implemented and documented. BI-040 is in progress: slice 1 pure deterministic source records is complete, while source materialization remains unimplemented.

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
- the shared storage contract, Garage integration checks, and deterministic bounded-endpoint failure test are documented and passing;
- BI-040 has a detailed test-first plan fixing the source seed, PostgreSQL order schema/seed semantics, paginated mock API and faults, supplier fixture schemas/scenarios, byte-stability boundaries, and validation matrix;
- BI-040 slice 1 validates one bounded source seed and provides typed, pure, time-independent generators for FX rates, shipping statuses, orders, and supplier prices with stable schemas, ordering, UTC watermarks, and representative reference values.

## Not completed

- the versioned PostgreSQL source schema and transactional seed;
- the live paginated mock API and its fault scenarios;
- serialized CSV, JSON, and Parquet fixture files;
- REST, file, and PostgreSQL ingestion;
- backfill and recovery exercises;
- ingestion integration suites and demo commands.

## Known limitations and risks

- Garage has no bucket versioning, and conditional create with `If-None-Match` is not a contract foundation.
- Concurrent ingestion writers are outside the core contract.
- Source adapters must include every value that changes selected logical data in `BatchIdentity.parameters`; omission would cause an identifier collision.

## Latest validation

2026-10-13, BI-040 slice 1:

- test-first evidence — the new source-generation suite initially failed at collection because the planned modules did not exist;
- LSP diagnostics for all changed Python files — PASS with no findings after narrowly scoped ignores for the session's stale new-module import cache; strict mypy resolves and checks those imports;
- `make lint` — PASS, including Ruff formatting/lint and strict mypy over `src`, `mock_api`, and tests;
- `make test` — PASS, 96 default tests including 24 source-generation tests; 8 integration tests deselected;
- backlog JSON Schema validation and `./scripts/verify.sh` — PASS, including Compose configuration;
- integration tests — NOT RUN because slice 1 is pure and performs no database, HTTP, S3, or Compose I/O.

## Next atomic step

Start BI-040 slice 2 by adding failing static and command tests for the versioned PostgreSQL order schema, quoted seed variable, transactional repeatability, and fail-before-change seed validation.
