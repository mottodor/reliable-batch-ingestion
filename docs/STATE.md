# Project state

Updated: 2026-10-14

## Current stage

The deterministic core contracts, BI-020 local Compose infrastructure, BI-030 shared S3 boundary, BI-040 deterministic source generators and fixtures, and BI-050 REST API ingestion/backfill are implemented, documented, and acceptance-tested. BI-060 file ingestion is selected for planning and implementation.

```yaml
active_task_id: BI-060
last_completed_task_id: BI-050
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
- BI-040 source schemas, seed behavior, mock fault controls, fixture layout, regeneration commands, and reproducibility boundaries are documented, and the full offline plus live Compose acceptance matrix passes;
- BI-050 has a detailed test-first plan fixing source identity, retry and `Retry-After` budgets, bounded page handling, raw-before-parse persistence, per-page manifests, reconciliation recovery, structured outcomes, CLI semantics, and inclusive fail-fast backfill;
- BI-050 slice 1 adds validated HTTP backoff, elapsed, pagination, response-size, and backfill bounds plus explicit four-phase HTTPX timeouts and one context-managed client with environment proxy use disabled;
- the allow-listed API descriptors now derive deterministic logical identities and four-digit page names, and strictly validate both source-specific item schemas and cross-page pagination invariants;
- pure retry policy classifies every HTTP/transport outcome, parses delta-seconds and HTTP-date `Retry-After`, applies capped exponential fallback, and enforces both attempt and monotonic elapsed budgets with injected clocks and sleep;
- BI-050 slice 2 streams raw HTTP 200 bodies in bounded chunks to seekable spooled files, enforces JSON media type, response-size and monotonic deadline bounds, and computes exact byte length and SHA-256 without content decoding;
- the single-date happy path traverses strict sequential pagination, reconciles exact payload/manifest keys, writes and verifies raw bytes before JSON/schema validation, creates canonical terminal API manifests, and emits exact `RunSummary` metrics;
- absent pages, payload-only recovery, and complete-success replay are covered without overwrite: complete pages retain their original manifests and `run_id`, count as duplicates, and produce no writes;
- live integration covers both API resources against the Compose mock API and Garage, comparing every stored raw body byte-for-byte and strictly parsing the exact page-manifest inventory;
- the application-owned HTTP loop now retries 429, 5xx, and transport failures under both attempt and monotonic elapsed budgets, honors valid `Retry-After`, and never retries permanent 4xx responses;
- handled API failures carry stable typed error codes, a secret-free structured event, and a final `RunSummary`; retry events record the actual next attempt, classification, status, and selected delay;
- malformed HTTP 200 bodies are immutably preserved before terminal `failed` manifests are written, while non-200 error bodies never become business page objects;
- reconciliation now safely reuses complete succeeded pages, re-surfaces complete failed pages, repairs payload-only pages by adding only the missing manifest, and fails without cleanup for manifest-only, conflict, and malformed-manifest states;
- live fault coverage proves `429 -> success`, exact permanent-500 attempt exhaustion, exact malformed-byte preservation, partial-page rerun, and payload-only recovery against the Compose mock API and Garage;
- the REST CLI exposes strict allow-listed single-date and backfill commands, emits newline-delimited structured JSON, explicitly closes HTTP/S3 clients, and maps usage, configuration, handled ingestion, and runtime failures to stable exit codes;
- inclusive ascending backfill reuses the single-date primitive, validates the configured maximum before opening clients, and stops at the first failed date;
- live repeated-backfill coverage proves exact key/checksum inventory stability, zero newly written payload bytes and duplicate metrics on replay, plus safe full-range resume after a middle-date failure;
- the REST ingestion contract documents source identity and key examples, all bounds and retry classifications, `Retry-After`, raw-before-parse ordering, page manifests, metrics, reconciliation states, recovery commands, fail-fast backfill, error/exit codes, and the sequential-writer limitation;
- README documents only the implemented REST commands and includes tested expected states and recovery actions for rate limiting, permanent HTTP failures, malformed HTTP 200 bodies, partial pages, and payload-only interruption;
- the complete BI-050 offline and local Compose acceptance matrix passes, so BI-050 is done and BI-060 is active.

## Not completed

- file ingestion and PostgreSQL ingestion;
- complete ingestion demo commands.

## Known limitations and risks

- Garage has no bucket versioning, and conditional create with `If-None-Match` is not a contract foundation.
- Concurrent ingestion writers are outside the core contract.
- Source adapters must include every value that changes selected logical data in `BatchIdentity.parameters`; omission would cause an identifier collision.
- Ignored `.env` files created before BI-050 must be refreshed with the committed HTTP settings before the full integration target or ingestion CLI can load complete configuration.

## Latest validation

2026-10-14, BI-050 slice 5:

- `./scripts/doctor.sh` — PASS;
- documentation-topic audit, repository policy, `git diff --check`, `make help`, and Compose configuration — PASS;
- LSP diagnostics for all 17 BI-050 Python implementation/test paths — PASS with no findings;
- `make lint` — PASS, including Ruff formatting/lint and strict mypy over `src`, `mock_api`, and tests;
- `make test` — PASS, 258 default tests; 22 integration tests deselected;
- explicit `make up` and `make seed SOURCE_SEED=20260901` using an ephemeral environment composed from ignored credentials and committed missing HTTP defaults — PASS;
- the first complete `make integration` attempt — FAIL because non-API integration fixtures read the pre-existing ignored `.env` directly instead of the supplied temporary env path; 9 BI-050 live tests passed before the unrelated fixture setup errors;
- full `make integration` with the ignored `.env` safely restored after temporary supplementation — PASS, 22 integration tests plus repeat-seed, persistence, and destructive-recovery checks;
- final repository policy and `./scripts/verify.sh` — PASS after task-state synchronization.

## Next atomic step

Create the detailed test-first BI-060 file-ingestion plan and record the required file-atomicity ADR before implementing behavior.
