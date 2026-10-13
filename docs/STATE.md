# Project state

Updated: 2026-10-13

## Current stage

The deterministic core contracts, BI-020 local Compose infrastructure, BI-030 shared S3 boundary, and BI-040 deterministic source generators and fixtures are implemented and documented. BI-050 REST API ingestion and backfill is selected as the next task.

```yaml
active_task_id: BI-050
last_completed_task_id: BI-040
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
- BI-040 slice 1 validates one bounded source seed and provides typed, pure, time-independent generators for FX rates, shipping statuses, orders, and supplier prices with stable schemas, ordering, UTC watermarks, and representative reference values;
- BI-040 slice 2 adds ordered versioned PostgreSQL schema/seed SQL, explicit order constraints and snapshot index, pre-state-change seed validation, quoted `psql` variables, and transactional replacement of only `omni_retail.orders`;
- repeated default PostgreSQL seeds reproduce the 24 pure generated rows and ordered digest, alternate seeds change values without changing schema, invalid seeds preserve state, and BI-020 marker data remains unchanged;
- BI-040 slice 3 adds a non-root Compose-built standard-library mock API with canonical bounded pages for FX rates and shipping statuses, deterministic 4xx responses, and byte-stable bodies across repeat requests and service restart;
- fault injection now covers isolated thread-safe `429 -> success`, permanent `500`, exact malformed JSON, and repeatable partial-page failure, with validated keys, deterministic reset operations, and a fail-closed 128-key state bound;
- BI-040 slice 4 commits valid, empty, schema-invalid, and mixed supplier-price fixtures in CSV, JSON, and Parquet with equal cross-format valid rows and explicit stable schemas;
- same-name redelivery and renamed-content cases exist in every format as byte-identical copies without assigning future BI-060 outcomes;
- the sorted checksum index covers all 24 payloads, and `make fixtures-check` validates two temporary generations against committed paths, bytes, lengths, checksums, schemas, rows, and scenario relationships;
- BI-040 source schemas, seed behavior, mock fault controls, fixture layout, regeneration commands, and reproducibility boundaries are documented, and the full offline plus live Compose acceptance matrix passes.

## Not completed

- REST, file, and PostgreSQL ingestion;
- backfill and recovery exercises;
- ingestion integration suites and demo commands.

## Known limitations and risks

- Garage has no bucket versioning, and conditional create with `If-None-Match` is not a contract foundation.
- Concurrent ingestion writers are outside the core contract.
- Source adapters must include every value that changes selected logical data in `BatchIdentity.parameters`; omission would cause an identifier collision.

## Latest validation

2026-10-13, BI-040 slice 5 closeout:

- `./scripts/doctor.sh`, Compose configuration, and `make help` — PASS;
- LSP diagnostics for all four changed Python files — PASS with no findings;
- `make lint` — PASS, including Ruff formatting/lint and strict mypy over `src`, `mock_api`, and tests;
- `make test` — PASS, 140 default tests; 13 integration tests deselected;
- `make fixtures-check SOURCE_SEED=20260901` — PASS for 24 indexed payloads and two clean temporary regenerations;
- `make up` and `make seed SOURCE_SEED=20260901` — PASS with bounded health and repeatable 24-row PostgreSQL replacement;
- `make integration` — PASS: 13 source/storage/infrastructure tests, repeat seed and marker checks, plus isolated persistence and destructive-recovery verification;
- repository policy, backlog schema validation, and `./scripts/verify.sh` — PASS.

## Next atomic step

Create the detailed test-first implementation plan for BI-050 REST API ingestion and backfill without implementing ingestion behavior.
