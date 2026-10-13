# Project state

Updated: 2026-10-13

## Current stage

The deterministic core contracts and BI-020 local Compose infrastructure are implemented and documented. The next gate is the shared S3 storage boundary.

```yaml
active_task_id: BI-030
last_completed_task_id: BI-020
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
- BI-020 operator commands and limitations are documented, and its complete acceptance matrix passes against local Compose services.

## Not completed

- the shared S3 storage boundary;
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

2026-10-13, `./scripts/verify.sh` — PASS:

- doctor — PASS with no warnings;
- repository policy — PASS;
- locked dependency sync — PASS;
- Ruff lint/format — PASS;
- mypy — PASS;
- pytest — PASS, 43 unit/default tests;
- Compose configuration with `.env.example` — PASS;
- separate `make integration` — PASS for repeat bootstrap, isolated down/up persistence, reset guards, destructive volume removal, clean re-bootstrap, protected-volume comparison, and cleanup;
- default-project down/up marker verification, documented health commands, fail-closed reset checks, and tracked runtime-credential scan — PASS.

## Next atomic step

Start BI-030 test-first: define the configured path-style boto3 client contract and immutable-write failure cases before implementing shared S3 operations.
