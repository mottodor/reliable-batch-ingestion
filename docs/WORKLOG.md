# Worklog

A concise append-only record of work sessions. The detailed current handoff lives in `docs/STATE.md`.

## 2026-10-12 — BI-000 agent/project foundation

- Added `AGENTS.md` and `START_HERE.md`.
- Added the implementation plan and atomic backlog.
- Selected Garage `dxflrs/garage:v2.4.1` as S3-compatible object storage.
- Added the Python/uv foundation, Makefile, doctor/verify scripts, and smoke test.
- `./scripts/verify.sh` — PASS: Ruff, mypy, and pytest (1 test).
- Environment warnings: no Git work tree; Docker daemon unavailable from WSL.
- Next task: `BI-010`.

## 2026-10-12 — BI-005 agent scaffolding audit and English normalization

- Audited the complete repository-owned documentation and agent workflow.
- Translated repository-owned documentation to English without changing product scope or ADR 0001.
- Made document ownership explicit and removed the circular/duplicated startup guidance.
- Added policy checks for Cyrillic text and active-task synchronization.
- `./scripts/verify.sh` — PASS: repository policy, locked sync, Ruff, mypy, and pytest (1 test).
- Environment warnings remain: no Git work tree; Docker daemon unavailable from WSL.
- Next task: `BI-010`.

## 2026-10-12 — BI-006 Git repository initialization and first-task selection

- Initialized the Git repository with `main` as the default branch; no commit was created.
- Confirmed that local configuration, environments, caches, and generated data remain ignored.
- Kept `BI-010` as the first product implementation task with status `todo`.
- `./scripts/doctor.sh` — PASS with one warning: the Docker daemon is unavailable from WSL.
- `./scripts/verify.sh` — PASS: policy, locked sync, Ruff, mypy, and pytest (1 test).
- Next task: `BI-010`.

## 2026-10-12 — BI-010 core deterministic contracts

- Added typed explicit environment configuration with redacted secret representations and no import-time I/O.
- Added bounded streaming SHA-256, normalized logical identities, deterministic batch/object/manifest keys, and unique physical run IDs.
- Added manifest schema version 1 with strict serialization, source nullability, UTC timestamps, lifecycle transitions, terminal states, and outcome mapping.
- Added the structured run-summary contract and documented the initial identifier/object-key/manifest baseline in `docs/core-contracts.md`.
- Added unit coverage for determinism, checksum chunking, configuration validation, manifest validation/statuses, and summaries.
- `./scripts/verify.sh` — PASS: policy, locked sync, Ruff, mypy, and pytest (32 tests).
- LSP/Pyright reported stale missing-import findings for new untracked modules; Ruff and strict mypy resolved the package and passed.
- Next task: `BI-020`.

## 2026-10-12 — BI-020 implementation planning

- Refined BI-020 into verifiable scope and acceptance criteria while keeping it `todo` and active.
- Added `docs/plans/BI-020.md` with fixed image, configuration, persistence, readiness, bootstrap, lifecycle, destructive-reset, test-first, documentation, and validation decisions.
- Selected the available patch-level PostgreSQL image `postgres:17.6-bookworm`; retained ADR-mandated Garage `dxflrs/garage:v2.4.1`.
- Based readiness and bootstrap planning on current Docker Compose and Garage documentation obtained through Context7.
- Backlog JSON Schema validation — PASS.
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, mypy, and pytest (32 tests).
- Next task: start BI-020 slice 1 with static contract tests and valid Compose configuration; do not start services before that contract passes.

## 2026-10-12 — BI-020 slice 1 static Compose contract

- Committed completed BI-010 separately as `764205a` (`feat: add BI-010 deterministic core contracts`).
- Marked BI-020 `in_progress` after first recording four failing static contract tests.
- Added pinned PostgreSQL and Garage services, separate named volumes, bounded healthchecks, Garage single-node/default-bucket wiring, and a secret-free Garage configuration.
- Extended `.env.example` with infrastructure ports and the required local Garage RPC secret placeholder.
- Updated repository verification to render Compose with `.env.example`, without requiring or creating a real `.env`.
- LSP diagnostics for the changed Python test — PASS with no findings.
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, mypy, pytest (36 tests), and Compose configuration.
- BI-020 remains incomplete: containers, lifecycle commands, bootstrap idempotency, persistence, and destructive reset have not been exercised.
- Next task: BI-020 slice 2 bounded lifecycle/preflight commands and integration checks for service health, bucket access, and repeat bootstrap.

## 2026-10-12 — BI-020 slice 2 bounded startup and bootstrap

- Committed BI-020 slice 1 separately as `cca8019` (`feat: start BI-020 compose infrastructure`).
- Added four lifecycle/preflight tests first and observed all four fail before implementation.
- Added a redacting `.env` preflight that rejects missing values, placeholders, invalid ports, and a malformed Garage RPC secret without printing credential values.
- Added bounded `make up`, non-destructive `make down`, `make logs`, infrastructure-only `make seed`, and repeated-bootstrap `make integration` targets.
- Added integration checks for PostgreSQL queries, Garage `/health`, configured bucket access through boto3 path-style S3, and a checksummed sentinel retained across a second `make seed`.
- `make up` — PASS: PostgreSQL and Garage reached healthy state within the 60-second wait budget.
- `make integration` — PASS: three checks passed before and after repeat bootstrap; the sentinel remained byte-identical and accessible with the configured credentials.
- LSP diagnostics for the three changed Python files — PASS with no findings.
- BI-020 remains incomplete: ordinary down/up persistence and destructive-reset behavior are deferred to slice 3.
- Next task: BI-020 slice 3 persistence and fail-closed destructive reset.

## 2026-10-12 — BI-020 slice 3 planning

- Confirmed slice 2 was already committed as `1a86704` and began from a clean work tree.
- Expanded slice 3 into a test-first persistence, reset-guard, isolated destructive-recovery, and cleanup plan.
- Reserved a dedicated Compose project for destructive integration so default developer volumes cannot be selected by the test harness.
- Defined deterministic PostgreSQL and S3 marker states (`create`, `verify`, and `absent`) across ordinary down/up and confirmed volume deletion.
- Defined fail-closed `make reset` behavior and the exact `CONFIRM=destroy-local-data` gate for the separately named destructive target.
- Based project isolation and named-volume behavior on current `/docker/compose` documentation obtained through Context7.
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, mypy, pytest (40 tests), and Compose configuration.
- Next task: add the slice 3 reset-guard unit tests before implementing or executing destructive behavior.

## 2026-10-13 — BI-020 slice 3 persistence and destructive reset

- Added reset/project-scoping tests first and observed four expected failures before implementation.
- Made every Compose lifecycle command explicit about project name and environment file.
- Added fail-closed `make reset` and a separately implemented `make reset-destructive` that requires the exact `CONFIRM=destroy-local-data` token before invoking Compose.
- Added deterministic PostgreSQL and S3 marker modes for `create`, `verify`, and `absent` states without using `SELECT *` or overwriting an existing S3 sentinel.
- Added a bounded recovery harness using temporary ports/configuration and the dedicated `reliable-batch-ingestion-bi020-reset-test` project.
- Verified both markers across ordinary down/up, verified failed reset attempts preserved them, removed all three isolated volumes after exact confirmation, and verified a clean restart recreated infrastructure without prior markers.
- Verified the default project's volume fingerprints were unchanged and isolated containers/volumes were removed in `finally` cleanup.
- `make integration` — PASS: default repeat-bootstrap checks and all isolated persistence/reset/recovery states passed.
- `make lint` — PASS; `make test` — PASS with 43 tests and 4 integration tests deselected.
- LSP diagnostics for all four changed Python files — PASS with no findings.
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, mypy, pytest (43 tests), and Compose configuration.
- Next task: BI-020 slice 4 operator-documentation review, full acceptance validation, and task closeout.

## 2026-10-13 — BI-020 slice 4 acceptance closeout

- Committed BI-020 slice 3 separately as `4a8f4eb` (`feat: add BI-020 persistence recovery checks`).
- Reviewed README lifecycle, health, persistence, integration, and destructive-reset instructions against the implemented Make targets and added verified health-inspection commands.
- Ran `make up` and two explicit `make seed` executions; both services remained healthy and the configured bucket credentials/sentinel stayed usable.
- `make integration` — PASS for default repeat bootstrap and isolated ordinary down/up, reset guards, confirmed volume deletion, clean re-bootstrap, protected-volume comparison, and cleanup.
- Verified default PostgreSQL and S3 markers after a real `make down`/`make up`, then verified them again after `make reset`, missing confirmation, and wrong confirmation all failed closed.
- Verified all three default named volumes survived normal shutdown; isolated containers and volumes were absent after recovery cleanup.
- Ran the documented Compose `ps`, Garage status, and PostgreSQL `pg_isready` commands successfully.
- Scanned tracked files against the actual ignored `.env` credential values — PASS; `.env` is not tracked.
- Marked BI-020 done, selected BI-030 as the active task, and synchronized `docs/STATE.md`.
- Backlog JSON Schema validation and `./scripts/verify.sh` — PASS; verify covered doctor, policy, locked sync, Ruff, mypy, 43 default tests, and Compose configuration.
- Next task: start BI-030 with tests for path-style boto3 client configuration and immutable-write conflicts.

## 2026-10-13 — BI-030 implementation planning

- Refined BI-030 scope and acceptance criteria while keeping it active and `todo`.
- Added `docs/plans/BI-030.md` with fixed client configuration, retry/timeout, streamed verification, immutable-write, copy-verify-delete, reconciliation, failure-state, integration, and documentation decisions.
- Defined recovery convergence for source-only, both-identical, destination-only, payload-only, and complete states without claiming concurrent-writer safety or exactly-once delivery.
- Based Boto3 client, retry, and S3 operation planning on current `/boto/boto3` documentation obtained through Context7.
- Backlog JSON Schema validation — PASS.
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, mypy, pytest (43 default tests), and Compose configuration.
- Next task: start BI-030 slice 1 with failing configuration/client tests and immutable-write state/error tests.

## 2026-10-13 — BI-030 shared S3 storage boundary

- Added explicit S3 total-attempt, connect-timeout, and read-timeout configuration plus redacting local preflight validation.
- Added a typed path-style SigV4 Boto3 client factory using standard retries and no import-time configuration or connections.
- Implemented streamed byte-level immutable writes with local/post-write verification, identical replay, conflict protection, and checksum metadata without relying on `ETag`.
- Implemented metadata-preserving `copy -> verify -> delete` movement that converges source-only, both-identical, and destination-only recovery states.
- Implemented read-only payload/manifest reconciliation with bounded strict manifest parsing and explicit absent, payload-only, complete, and manifest-only states.
- Added 24 storage unit tests and 4 Garage/fault-endpoint integration tests, including deterministic enforcement of the configured three-request budget.
- Documented the public storage contract, failure states, recovery semantics, single-writer limitation, and operator configuration.
- LSP diagnostics — PASS with no findings; `make lint` and `make test` — PASS with 72 default tests.
- `make integration` — PASS for 8 initial infrastructure/storage tests, repeat bootstrap, isolated persistence/reset/recovery, and cleanup.
- Backlog JSON Schema validation and `./scripts/verify.sh` — PASS; BI-030 is done and BI-040 is active.
- Next task: plan BI-040 deterministic source generators and fixtures test-first.

## 2026-10-13 — BI-040 implementation planning

- Refined BI-040 scope and acceptance criteria while keeping it active and `todo`.
- Added `docs/plans/BI-040.md` with one canonical seed, versioned PostgreSQL schema/seed behavior, paginated API contracts, bounded fault scenarios, supplier fixture schemas, duplicate cases, and byte-stability boundaries.
- Selected a standard-library mock server in a Compose-built `python:3.12.12-slim-bookworm` image, avoiding a new runtime dependency; the patch-level image tag was verified as available.
- Defined five test-first slices and explicit evidence for same-seed PostgreSQL digests, API page bytes/faults, cross-format fixture rows, regeneration checks, and bounded local operation.
- Based PostgreSQL, Compose, and Parquet planning on current `/websites/postgresql_17`, `/docker/compose`, and `/apache/arrow` Context7 documentation.
- Backlog JSON Schema validation and repository policy checks — PASS.
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, mypy, 72 default tests, and Compose configuration.
- Next task: start BI-040 slice 1 with failing seed-validation and deterministic-record tests, then add only the minimum pure generators.

## 2026-10-13 — BI-040 slice 1 deterministic source records

- Added the source-generation contract tests first and observed collection fail because the planned modules did not exist; the completed slice now has 24 focused tests.
- Added strict ASCII/range validation for `SOURCE_SEED`, canonical SHA-256 field derivation, and a shared deterministic order-ID contract without environment, clock, random-state, or I/O access.
- Added typed pure generators for four FX rates, 12 shipping statuses, 24 PostgreSQL-shaped orders, and 15 supplier prices with explicit schemas, stable ordering, fixed UTC watermarks, and pinned reference values.
- Added the typed root `mock_api` package to Hatch, Ruff, mypy, Pyright, Make, and repository verification coverage without adding a runtime dependency.
- Documented the implemented seed/record contracts and the future HTTP, fixture-byte, and cross-PyArrow-version reproducibility boundaries in `docs/source-contracts.md`.
- Marked BI-040 `in_progress`; slices 2–5 remain unimplemented.
- LSP diagnostics — PASS; narrowly scoped Pyright ignores cover its stale new-module cache while strict mypy resolves and checks all imports.
- `make lint` and `make test` — PASS; 96 default tests passed and 8 integration tests were deselected.
- Backlog JSON Schema validation and `./scripts/verify.sh` — PASS, including strict mypy over `src`, `mock_api`, and tests plus Compose configuration.
- Next task: start BI-040 slice 2 with failing PostgreSQL schema/seed static and command tests.

## 2026-10-13 — BI-040 slice 2 repeatable PostgreSQL source

- Added seven PostgreSQL contract/command tests first and observed all fail before the versioned SQL and seed command existed.
- Added an idempotent `omni_retail.orders` schema with explicit columns, finite-value/non-negative constraints, unique UTC watermark, and `(updated_at, order_id)` snapshot index.
- Added a transaction-scoped seed script that reproduces the pure SHA-256-derived 24-order dataset, consumes a quoted `psql` variable, and replaces only the controlled orders table.
- Extended `make seed SOURCE_SEED=<integer>` to validate before Compose/`psql`, preserve bounded infrastructure startup, apply ordered SQL with `ON_ERROR_STOP`, and propagate failures.
- Added live PostgreSQL evidence for same-seed row/digest equality, pure-generator parity, unique ordered UTC timestamps, alternate-seed variation with stable schema, invalid-seed isolation, unchanged BI-020 marker state, and default-seed restoration.
- LSP diagnostics, `make lint`, `make test` (103 passed), Compose configuration, explicit default seeding, `make integration` (9 initial integration tests plus recovery checks), and `./scripts/verify.sh` — PASS.
- BI-040 remains in progress; the mock API and fixture corpus are not implemented.
- Next task: BI-040 slice 3 mock API pagination and bounded deterministic faults, starting with failing pure request/response tests.

## 2026-10-13 — BI-040 slice 3 paginated mock API and faults

- Added pure request/response tests for strict bounded pagination, canonical page bytes, deterministic 4xx responses, every planned fault, reset/isolation, thread safety, and bounded retained fault keys.
- Added four container/Compose contract tests first and observed all fail before the pinned image, build exclusions, service configuration, and environment values existed.
- Implemented a standard-library `ThreadingHTTPServer` transport over a socket-free pure contract, with structured request logs and no PostgreSQL, S3, public-network, or import-time connection behavior.
- Added a non-root `python:3.12.12-slim-bookworm` image, secret-excluding Docker context, configurable host port, explicit seed, and bounded healthcheck under the existing Compose wait deadline.
- Implemented canonical pages for FX rates and shipping statuses plus isolated `429 -> success`, permanent `500`, exact malformed JSON, repeatable partial-page failure, and one/all-key reset operations.
- Bounded transient state to 128 validated keys without eviction, protected transitions with a lock, and returned deterministic `503` at capacity.
- Added four live HTTP tests with explicit `httpx` timeouts covering both resources, all faults, reset behavior, deterministic 4xx responses, and byte equality after a bounded service restart.
- LSP diagnostics, Compose configuration, pinned image build, `make lint`, `make test` (132 passed), focused integration, `make integration` (13 initial integration tests plus recovery checks), and `./scripts/verify.sh` — PASS.
- BI-040 remains in progress; supplier fixture generation is not implemented.
- Next task: BI-040 slice 4 supplier CSV/JSON/Parquet corpus, starting with failing scenario-matrix and cross-format equality tests.

## 2026-10-13 — BI-040 slice 4 supplier fixture corpus

- Added fixture contract tests first: collection failed before the generator module existed, and the committed-corpus equality assertion then failed before default fixtures were generated.
- Added a typed deterministic generator for valid, empty, schema-invalid, and mixed supplier prices in CSV, JSON, and Parquet, with explicit stable serialization and Parquet writer options based on current `/apache/arrow` Context7 documentation.
- Committed 24 default-seed payloads: four canonical scenarios and both duplicate-delivery patterns in every format, without assigning BI-060 acceptance or rejection outcomes.
- Added a sorted `checksums.json` with fixture schema version, seed, source date, path, byte length, SHA-256, format, and scenario for every payload.
- Added validated `make fixtures` generation and `make fixtures-check`, which compares two temporary generations to committed bytes and verifies index integrity, schemas, logical rows, invalid/mixed cases, and duplicate relationships.
- Documented corpus layout, scenario semantics, commands, and the lockfile/Python/PyArrow byte-stability boundary; BI-040 remains in progress for slice 5 closeout.
- Focused fixture tests (8), LSP diagnostics, `make fixtures-check SOURCE_SEED=20260901`, `make lint`, `make test` (140 passed), Compose configuration, repository policy, and `./scripts/verify.sh` — PASS.
- `make integration` — NOT RUN because slice 4 changes only offline fixture generation; the complete source/Compose acceptance matrix remains BI-040 slice 5.
- Next task: BI-040 slice 5 documentation and acceptance closeout, including the full source/Compose validation matrix and task-state advancement only if all criteria pass.

## 2026-10-13 — BI-040 slice 5 acceptance closeout

- Audited `docs/source-contracts.md`, README, and `make help` against the implemented seed, mock API, fixture generation/check, lifecycle, and integration behavior; removed remaining in-progress wording after acceptance passed.
- Confirmed one explicit seed contract across pure generators, PostgreSQL, the live mock API, and supplier fixtures, with invalid-input isolation and alternate-seed schema stability covered by tests.
- Confirmed all documented live API scenarios through integration coverage: bounded pagination, isolated `429` then success with `Retry-After`, permanent `500`, exact malformed JSON, and successful pages before a repeatable partial failure.
- Confirmed the committed 24-file supplier corpus, cross-format schema/logical-row equality, checksum index, duplicate relationships, and byte-identical clean temporary regeneration.
- `./scripts/doctor.sh`, Compose configuration, `make help`, `make lint`, `make test` (140 passed), `make fixtures-check SOURCE_SEED=20260901`, `make up`, and `make seed SOURCE_SEED=20260901` — PASS.
- `make integration` — PASS: 13 initial PostgreSQL/Garage/mock API tests, repeat seed and infrastructure-marker verification, and isolated down/up plus destructive-recovery checks with cleanup.
- Marked BI-040 done, selected BI-050 as the active todo task, and synchronized the backlog, plan, README, source contract, and state handoff.
- Repository policy, backlog JSON Schema validation, and final `./scripts/verify.sh` — PASS.
- Next task: create the detailed test-first BI-050 REST API ingestion and backfill implementation plan without implementing behavior.

## 2026-10-13 — BI-050 REST ingestion and backfill planning

- Added an implementation-ready five-slice BI-050 plan without adding ingestion behavior or changing identifier, object-key, manifest, summary, or storage contracts.
- Fixed the two-source allow-list and logical identity inputs, deterministic page naming, strict pagination invariants, bounded body handling, raw-before-parse ordering, and source-specific page validation.
- Defined one application-owned retry loop with attempt and monotonic elapsed budgets, deterministic capped exponential backoff, direct HTTP status classification, and delta-seconds/HTTP-date `Retry-After` handling.
- Defined exact reconciliation behavior for absent, payload-only, complete, manifest-only, failed-manifest, and conflict states under the existing sequential-writer limitation.
- Defined metric meanings, structured failure outcomes, CLI commands, inclusive sequential fail-fast backfill, and repeated-run/backfill inventory evidence.
- Mapped every BI-050 acceptance criterion to unit or local Compose evidence, including `429 -> success`, permanent 500 exhaustion, one-request permanent 4xx, preserved malformed bytes, partial-run recovery, and idempotent replay.
- Based HTTP client, timeout, raw-stream, response, and transport-retry decisions on current Context7 `/encode/httpx/0.28.1` documentation.
- Marked BI-050 `in_progress`; `./scripts/doctor.sh`, repository policy, backlog validation, Ruff, mypy, 140 default tests, Compose configuration, and `./scripts/verify.sh` — PASS.
- LSP diagnostics and `make integration` — NOT RUN because only planning/state documentation changed.
- Next task: start BI-050 slice 1 with failing configuration, source/page contract, retry-classification, `Retry-After`, and attempt/elapsed-budget tests.

## 2026-10-13 — BI-050 slice 1 configuration, sources, and retry policy

- Added failing tests first for all new HTTP bounds, HTTPX lifecycle/timeouts, the two source schemas, pagination consistency, status/transport classification, `Retry-After`, and attempt/elapsed budgets.
- Extended explicit configuration with positive backoff-cap, elapsed-budget, response-size, and backfill settings plus page-size `<= 100` and maximum-pages `<= 9,999` protocol bounds; retained secret redaction and import-time purity.
- Added one context-managed HTTPX client boundary with `trust_env=False`, no hidden transport retries, explicit connect/read/write/pool timeouts, and per-request remaining-deadline clipping.
- Added allow-listed `fx-rates` and `shipping-statuses` descriptors, deterministic logical identities/page names, exact envelope/item field validation, strict types, and consistent page totals/item counts/next-page progression.
- Added pure direct-status and transport-error classification, capped deterministic exponential fallback, delta-seconds and HTTP-date `Retry-After`, and injected monotonic/wall clocks and sleep under attempt plus elapsed limits.
- Current HTTPX behavior was verified with Context7 `/encode/httpx/0.28.1`; focused 91-test suite, LSP diagnostics, `make lint`, `make test` (221 passed), Compose configuration, repository policy, and `./scripts/verify.sh` — PASS.
- `make integration` — NOT RUN because this slice adds pure contracts and no live HTTP fetching or S3 persistence.
- Next task: BI-050 slice 2 bounded HTTP page streaming and immutable page/manifest persistence for absent, payload-only, and complete-success states.

## 2026-10-14 — BI-050 slice 2 bounded fetching and immutable happy path

- Added failing tests first with HTTPX streaming transports and a recording storage fake for raw chunking, deadline/size bounds, checksums, raw-before-parse ordering, strict page traversal, reconciliation states, manifests, and metrics.
- Added a context-owned fetched-page result that streams raw bytes in bounded chunks to a seekable spooled file, requires `application/json`, clips request timeouts to the page deadline, checks elapsed time after chunks, and computes exact SHA-256 and length.
- Added the single-date happy-path service using only the shared storage interface: exact-key reconciliation, raw immutable write before decode/validation, strict multi-page traversal, canonical succeeded manifests, and final summary metrics.
- Implemented `absent`, `payload_only`, and complete-success behavior; payload-only writes only its missing manifest, while complete-success preserves the original manifest and `run_id`, performs no writes, and increments duplicate metrics.
- Added live integration for both allow-listed resources against the Compose mock API and Garage; every stored raw page equals the source response bytes and the exact manifest inventory parses under schema version 1.
- Current HTTPX behavior was checked through Context7 `/encode/httpx/0.28.1`; focused 12-test unit suite, LSP diagnostics, `make lint`, `make test` (233 passed), Compose configuration, repository policy, and `./scripts/verify.sh` — PASS.
- Focused live integration — PASS, 2 tests; full `make integration` — PASS, 15 integration tests plus repeat-seed, persistence, and destructive-recovery checks, using a deleted ephemeral merged environment file without changing ignored `.env`.
- Next task: BI-050 slice 3 bounded retry execution, typed failures, malformed-page failed manifests, and partial-run recovery across all reconciliation states.

## 2026-10-14 — BI-050 slice 3 failure recovery

- Added failing tests first for `429 -> success`, permanent 4xx, attempt/elapsed exhaustion for 5xx and transport failures, exact retry delays, malformed bodies, terminal failed manifests, every reconciliation failure state, and interruption between payload and manifest writes.
- Implemented one application-owned retry loop around synchronous HTTPX streaming; attempts and elapsed time remain bounded, valid `Retry-After` is honored, permanent 4xx returns after one request, and non-200 bodies are never persisted as business pages.
- Added stable API error codes, secret-free structured retry/final events, and failure exceptions carrying the final `RunSummary` for handled HTTP, validation, state, conflict, corruption, and storage failures.
- Preserved invalid HTTP 200 bytes before writing canonical terminal `failed` manifests; complete failed pages re-surface unchanged and succeeded/failed status mismatches fail explicitly.
- Added payload-only, complete-success, complete-failed, manifest-only, conflict, malformed-manifest, and post-payload interruption recovery coverage without overwrite or cleanup-based repair.
- Added live Garage/mock API scenarios for rate-limit recovery, exact permanent-500 exhaustion, malformed-byte preservation, partial-page rerun, and pre-created payload-only recovery.
- Checked HTTPX 0.28.1 streaming and transport behavior through Context7 `/encode/httpx/0.28.1`; focused unit tests (65), focused integration (7), LSP diagnostics, `make lint`, and `make test` (245 passed) — PASS.
- Full `make integration` with the older ignored `.env` temporarily supplemented by committed HTTP defaults — PASS: 20 integration tests plus repeat-seed, persistence, and destructive-recovery checks; repository policy and `./scripts/verify.sh` — PASS.
- Next task: BI-050 slice 4 CLI contracts and inclusive fail-fast backfill using the tested single-date primitive.

## 2026-10-14 — BI-050 slice 4 CLI and inclusive backfill

- Added failing tests first for inclusive ascending enumeration, maximum-range/reversed-range rejection, fail-fast behavior, strict CLI dates/source allow-list, structured output, stable exit codes, and explicit HTTP/S3 cleanup.
- Added one sequential inclusive backfill service that calls the existing single-date primitive for every date and naturally stops on the first `ApiIngestionError`; no alternate storage, retry, identity, or page path was introduced.
- Added `batch_ingestion.cli` with the documented `run` and `backfill` forms, pre-client validation, newline-delimited JSON output, and stable `0/2/3/4/5` exit codes.
- Added `make ingest-api`, loading the ignored environment through current uv `--env-file` semantics verified with Context7 `/websites/astral_sh_uv`; existing process values retain precedence.
- Added live multi-date evidence for exact key/checksum inventory stability, zero newly written payload bytes plus duplicate metrics on repeat, and full-range recovery after a fail-fast middle-date fault without changing earlier objects.
- Updated README command/behavior documentation, the BI-050 plan status, and current state; BI-050 remains `in_progress` for slice 5 documentation and acceptance closeout.
- Focused unit tests (13), focused integration (9), LSP diagnostics, `make lint`, `make test` (258 passed), Compose configuration, and `./scripts/verify.sh` — PASS.
- Plain `make integration` first failed because the older ignored `.env` lacks BI-050 HTTP settings; the full target passed with 22 integration tests plus recovery checks using a deleted temporary merge of ignored credentials and committed HTTP defaults.
- Next task: BI-050 slice 5 detailed API-ingestion documentation and full acceptance closeout.

## 2026-10-14 — BI-050 slice 5 documentation and acceptance closeout

- Added `docs/api-ingestion.md` with the implemented source identity and deterministic-key examples, timeout/retry/body bounds, status matrix, `Retry-After`, raw-before-parse ordering, page-manifest semantics, metrics, structured outcomes, reconciliation states, recovery commands, fail-fast backfill, and sequential-writer limitation.
- Updated README with only the implemented REST command forms and tested expected states/recovery actions for 429, permanent 500/4xx, malformed HTTP 200, partial-page failure, and payload-only interruption.
- Confirmed the complete BI-050 acceptance evidence: both source paginations, bounded retries/timeouts, one-request permanent 4xx, `429 -> success`, exact permanent-500 exhaustion, malformed-byte preservation, payload-only and partial-run recovery, repeated-run/backfill idempotency, and full-range fail-fast resume.
- `./scripts/doctor.sh`, documentation audit, repository policy, `make help`, Compose configuration, LSP diagnostics for 17 BI-050 Python paths, `make lint`, and `make test` (258 passed; 22 integration tests deselected) — PASS.
- Explicit local `make up` and `make seed SOURCE_SEED=20260901` — PASS using a deleted ephemeral environment that combined ignored credentials with committed missing HTTP defaults.
- The first full integration attempt with only `COMPOSE_ENV_FILE` overridden — FAIL because legacy non-API fixtures read the older ignored `.env` directly; all 9 BI-050 live tests passed before 11 unrelated fixture setup errors.
- Full `make integration` after temporarily supplementing and then restoring the ignored `.env` — PASS: 22 integration tests plus repeat-seed, persistence, destructive-reset guards/recovery, and cleanup.
- Marked BI-050 done, selected BI-060 as the active todo task, synchronized README/backlog/state/plan, and ran final `./scripts/verify.sh` — PASS.
- Next task: create the detailed test-first BI-060 file-ingestion plan and record the required file-atomicity ADR before implementing behavior.

## 2026-10-14 — BI-060 whole-file atomicity decision

- Accepted ADR 0002 after choosing whole-file rejection over accepted/rejected row splitting.
- Defined one delivered file as the acceptance unit: any required validation failure rejects every record, publishes no accepted subset, and retains the exact original bytes with a machine-readable reason.
- Distinguished business-level file atomicity from S3 movement, which remains recoverable `copy -> verify -> delete` and may temporarily leave identical source and destination objects.
- Kept manifest schema version 1 and the existing raw/manifest object-key contract unchanged; rejection-diagnostic and landing/archive/rejected key details remain for the BI-060 implementation plan.
- Marked BI-060 `in_progress` and synchronized the ADR index, backlog, and state handoff.
- Backlog JSON Schema validation, repository policy, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included Ruff, strict mypy, 258 default tests, and Compose configuration.
- LSP diagnostics and `make integration` — NOT RUN because no Python or runtime behavior changed.
- Next task: create the detailed test-first BI-060 file-ingestion plan, including rejection-diagnostic and landing/archive/rejected key contracts.

## 2026-10-14 — BI-060 file-ingestion planning

- Added an implementation-ready five-slice BI-060 plan without adding runtime behavior or changing manifest schema version 1 and the canonical raw/manifest key contract.
- Fixed one `supplier-prices` source contract, representable filename/date rules, exact CSV/JSON/Parquet schemas, strict row rules, whole-file counters, validation bounds, and stable rejection-code precedence.
- Defined content-based file identity, deterministic landing/archive/rejected keys, canonical raw payload names, and a reason-bearing rejected key plus bounded value-free structured diagnostics.
- Defined same-name versus renamed-content classification so renamed identical bytes use a different landing key but the same batch, raw payload, final disposition, and original manifest.
- Defined raw-before-validation processing, terminal-manifest-after-disposition ordering, exact recovery states, raw-based landing reconstruction, and copy/delete interruption convergence through the shared storage boundary.
- Mapped all BI-060 acceptance criteria to pure, format, service, CLI, and local Garage tests across five red-green slices.
- Based bounded Parquet schema inspection and `iter_batches()` planning on current Context7 `/apache/arrow` documentation; no dependency change is required.
- Backlog JSON Schema validation, repository policy, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included locked sync, Ruff, strict mypy, 258 default tests, and Compose configuration.
- LSP diagnostics and `make integration` — NOT RUN because only planning/state documentation changed.
- Next task: start BI-060 slice 1 with failing pure admission, identity/key, diagnostic, duplicate, counter, and configuration tests.

## 2026-10-14 — BI-060 slice 1 pure file contracts

- Added test-first pure supplier-file contracts for strict admission-safe source/name/checksum values, filename/date/format inspection, content-based `BatchIdentity`, canonical payload names, and deterministic landing/raw/manifest/archive/rejected keys.
- Proved same-name bytes retain every key, while renamed identical content changes only the landing key; content, declared format, expected checksum, and validation-contract changes produce distinct batches.
- Added stable rejection-code precedence, bounded value-free diagnostic ordering/truncation, same-name versus renamed duplicate classification, ADR 0002 whole-file counters, and secret-free terminal event serialization.
- Added explicit positive file byte, spool, record, Parquet batch, and diagnostic bounds with cross-field spool validation; updated `.env.example` without adding secrets or dependencies.
- `./scripts/doctor.sh`, LSP diagnostics for 10 changed Python paths, `make lint`, `make test` (297 passed; 22 integration tests deselected), and `./scripts/verify.sh` — PASS.
- `make integration` — NOT RUN because this slice adds pure contracts/configuration and no S3 or parser runtime path.
- Next task: start BI-060 slice 2 with failing format-parameterized tests for bounded CSV, JSON, and Parquet whole-file validation.

## 2026-10-14 — BI-060 slice 2 bounded file validation

- Added a whole-file validation result that exposes only acceptance/rejection, bounded value-free diagnostics, and ADR 0002 counters; it never returns or publishes an accepted-row subset.
- Implemented strict common supplier-price rules plus incremental strict UTF-8 CSV validation, byte-bounded strict JSON validation, and exact non-null Arrow schema validation through `ParquetFile.iter_batches()` without `read_table()`.
- Covered every committed valid, empty, schema-invalid, and mixed fixture in CSV, JSON, and Parquet; mixed files consistently reject the complete payload with exact `3/0/3` counters.
- Added format/type/business/date/checksum, malformed UTF-8/parser, missing/extra schema, byte/record/Parquet-batch/diagnostic bound, unchanged-byte, and restored-stream-position tests.
- Confirmed the PyArrow `ParquetFile.schema_arrow` and bounded `iter_batches(batch_size=...)` APIs against current Context7 `/apache/arrow` documentation; no dependency change was required.
- `./scripts/doctor.sh`, LSP diagnostics, `make lint`, `make test` (377 passed; 22 integration tests deselected), and `./scripts/verify.sh` — PASS.
- `make integration` — NOT RUN because this slice adds pure local-stream validation and no S3 runtime path.
- Next task: start BI-060 slice 3 with failing bounded-download and restart-safe file-service recovery tests.

## 2026-10-14 — BI-060 slice 3 shared-storage reads and restart-safe file service

- Added generic exact-key inspection and bounded context-owned S3 downloads with chunked SHA-256 verification, declared/actual byte limits, explicit missing/conflict/size errors, response-body cleanup, and seekable spool cleanup.
- Added the file service over a narrow shared-storage protocol: it obtains landing or raw evidence, preserves canonical raw bytes before validation, probes the finite disposition set, converges `copy -> verify -> delete`, and writes the terminal manifest last.
- Covered landing-only, landing+raw, landing+destination, raw+destination, raw-only, complete redelivery, both-identical after delete failure, manifest-only, alternative-disposition conflict, raw-write failure, move failure, and post-disposition/pre-manifest restart states.
- Preserved the first complete manifest and `run_id`, classified renamed-content redelivery, emitted whole-file summaries/events, and left transient failures with retryable evidence and no fabricated terminal manifest.
- Added Garage coverage for bounded reads plus exact valid/rejected raw and archive/rejected bytes, landing removal, and strict terminal manifest reconciliation.
- Refreshed the ignored local `.env` only with missing committed non-secret HTTP/file bounds so the current integration configuration could load; no credentials or local configuration were committed.
- Current Boto3 `get_object` streaming-body and `ClientError` behavior was checked against Context7 `/boto/boto3`; no dependency change was required.
- `./scripts/doctor.sh`, LSP diagnostics, `make lint`, `make test` (396 passed; 25 integration tests deselected), focused Garage tests (7 passed), `make integration` (25 initial integration tests plus recovery checks), and `./scripts/verify.sh` — PASS.
- Next task: start BI-060 slice 4 with failing file CLI tests for strict `run`/`resume` admission, staging, structured output, stable exits, and resource cleanup.

## 2026-10-14 — BI-060 slice 4 file CLI and all-format recovery matrix

- Added failing CLI tests first for strict `run`/`resume` parsing, canonical dates/checksums/landing keys, bounded local admission before client creation, immutable staging, structured output, stable exits, and local/S3 resource cleanup.
- Added `batch_ingestion.files.cli` and `make ingest-file`; `run` streams one seekable regular file under `FILE_MAX_BYTES`, derives and stages its deterministic landing key, and `resume` reconstructs the same delivery solely from a canonical key.
- Mapped accepted deliveries to exit `0`, admission/configuration/handled/runtime outcomes to stable `2/3/4/5` exits, retained business rejections as exit `4`, and kept emitted errors free of paths, payload values, secrets, and backend exception text.
- Added live CSV/JSON/Parquet happy and rejection coverage for exact raw/disposition bytes, every slice-4 rejection scenario, mixed-file `3/0/3` counters, and strict terminal manifests.
- Proved same-name and renamed-content replay for every format preserves one canonical raw/disposition/manifest and original `run_id`, reports distinct duplicate classes, and writes zero new raw bytes.
- Injected a live delete failure after verified copy for every format, inspected landing/raw/archive evidence, then proved `resume` converges to destination-only and completes the missing manifest without data loss.
- Updated Make help, README command behavior, the BI-060 plan status, and state handoff; BI-060 remains in progress for slice 5 documentation and acceptance closeout.
- `./scripts/doctor.sh`, focused CLI tests (13), focused integration (26), LSP diagnostics, `make lint`, `make test` (409 passed), Compose configuration, repository policy, `make integration` (49 initial integration tests plus recovery checks), and `./scripts/verify.sh` — PASS.
- Next task: complete BI-060 slice 5 detailed contract documentation, full acceptance audit, and task closeout.

## 2026-10-14 — BI-060 slice 5 documentation and acceptance closeout

- Added `docs/file-ingestion.md` with the implemented admission boundary, content identity and exact key example, filename/format/schema rules, configuration bounds, raw-before-validation ordering, whole-file semantics, rejection precedence and value-free diagnostics, metric meanings, duplicate classes, manifests, recovery matrix, CLI exits, failure exercises, and sequential-writer limitations.
- Audited and updated the shared storage contract for generic exact-key inspection and bounded context-owned downloads; updated README references and tested file failure/recovery states without promising unimplemented commands.
- Added a documentation contract test covering both CLI forms, exact fixture-derived keys, every rejection code and file bound, duplicate classes, and explicit non-claims for atomic rename, exactly-once delivery, and concurrent safety.
- Confirmed complete BI-060 acceptance evidence for all formats, raw-before-validation preservation, whole-file rejection, same-name and renamed-content redelivery, reason-bearing rejected objects, exact-byte dispositions, and post-copy/delete recovery through `resume`.
- `./scripts/doctor.sh`, repository policy, `git diff --check`, Make help, Compose configuration, LSP diagnostics, `make test` (410 passed), focused BI-060 units (142 passed), focused file integration (26 passed), explicit startup/seed, and full `make integration` (49 initial integration tests plus recovery checks) — PASS.
- The first `make lint` formatting check found two quote-style changes in the new test; the test was reformatted and the complete lint gate then passed with Ruff and strict mypy.
- Marked BI-060 done, selected BI-070, synchronized README/backlog/state/plan, and ran final `./scripts/verify.sh` — PASS.
- Next task: create the detailed test-first BI-070 plan for bounded PostgreSQL snapshot identity, watermark, chunking, and restart recovery.

## 2026-10-14 — BI-070 bounded PostgreSQL snapshot planning

- Added an implementation-ready five-slice BI-070 plan without adding runtime behavior or changing the identifier, raw/manifest key, manifest schema version 1, summary, or shared-storage contracts.
- Fixed one daily `orders` source contract, a UTC half-open date window, read-only `REPEATABLE READ` capture of an inclusive `(updated_at, order_id)` upper watermark and expected row count, and explicit replay descriptors for non-empty and empty snapshots.
- Defined identity inputs, four-digit deterministic chunk names, an exact non-null order Parquet schema, fixed writer options, one-chunk memory/spool bounds, and raw-before-manifest commit ordering.
- Defined replay-from-row-one recovery across absent, payload-only, complete, conflict, connection-drop, source-drift, and S3-unavailable states without trusting a mutable last-row checkpoint or claiming a durable MVCC snapshot after disconnect.
- Mapped every BI-070 acceptance criterion to pure, reader/serializer, service, CLI, and local PostgreSQL/Garage evidence, including backend termination and insertion above the captured upper bound.
- Based server-side cursor, transaction, parameter-binding, and Parquet-writer decisions on current Context7 `/websites/psycopg_psycopg3` and `/apache/arrow` documentation; no dependency or ADR is required.
- Marked BI-070 `in_progress` and synchronized the backlog, implementation-plan index, state handoff, and next atomic slice.
- `./scripts/doctor.sh`, repository policy, backlog JSON Schema validation, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included locked sync, Ruff, strict mypy, 410 default tests, and Compose configuration.
- One attempted ad hoc command, `uv run python scripts/validate_backlog.py`, failed because that helper does not exist; the backlog was then validated successfully against `tasks/backlog.schema.json` with the available system Python JSON Schema/YAML libraries.
- LSP diagnostics and `make integration` — NOT RUN because only planning/state documentation changed.
- Next task: start BI-070 slice 1 with failing pure descriptor, identity/key, Arrow schema, configuration, event, and metric contract tests.

## 2026-10-14 — BI-070 slice 1 pure snapshot contracts

- Added failing tests first, then implemented the pure allow-listed `orders` daily descriptor with canonical UTC half-open windows, exact empty/non-empty resume argument pairing, upper tuple/count validation, and a four-digit chunk namespace.
- Added versioned logical identity inputs, deterministic raw/manifest chunk keys, exact expected chunk cardinality, and explicit rejection of snapshots that cannot fit the `1..9999` key namespace.
- Added the exact seven-field non-null Arrow schema, immutable Parquet writer-option contract, schema-bearing zero-row behavior, and strict order-row validation without trimming, normalization, rounding, timezone conversion, or type coercion.
- Replaced the unused `SNAPSHOT_PAGE_SIZE` placeholder with bounded PostgreSQL connect/statement timeouts and snapshot elapsed/chunk/count/serialized-byte/spool settings; retained password redaction and explicit-only environment loading.
- Added secret-free typed snapshot lifecycle events, stable handled error codes, chunk outcomes, and a metric adapter that fixes PostgreSQL-inapplicable `records_rejected`, `pages_fetched`, and `retry_count` to zero.
- Updated `.env.example`, the core configuration contract, BI-070 plan status, backlog scope, and state handoff. No reader, serializer, PostgreSQL/S3 I/O, service, manifest writer, or CLI behavior was added in this slice.
- Used current Context7 `/apache/arrow` documentation for explicit `pa.field(..., nullable=False)`, decimal, and timezone-aware timestamp schema APIs; no dependency or ADR change was required.
- Focused snapshot/config tests (49), all offline tests (443 passed, 49 integration tests deselected), Ruff formatting/lint, strict mypy, LSP diagnostics, repository verification, and Compose configuration — PASS.
- `make integration` — NOT RUN because slice 1 contains pure/configuration contracts and no live PostgreSQL, Garage, service, or CLI behavior.
- Next task: implement BI-070 slice 2 with the read-only repeatable-read bounded reader and deterministic one-chunk Parquet serializer.

## 2026-10-14 — BI-070 slice 2 bounded reader and deterministic serializer

- Added failing reader and serializer tests first; collection initially failed because the planned modules did not exist, then the completed focused snapshot/config suite passed 64 tests.
- Added explicit Psycopg connection construction with bounded connect timeout and no application retry, read-only `REPEATABLE READ` characteristics configured before one explicit transaction, and a transaction-local parameterized statement timeout.
- Captured a daily upper tuple/count or validated a supplied non-empty descriptor in the same transaction as extraction; empty resume deliberately remains empty, while non-empty validation and extraction apply the inclusive tuple bound.
- Added one named server-side cursor over the exact seven-column ordered query, one-shot `fetchmany(chunk_rows)` iteration, strict source-row/order/window/boundary validation, final count/watermark checks, monotonic deadline checks, and stable handled connection/drift/deadline failures.
- Added deterministic one-chunk Parquet serialization with the exact non-null schema and fixed writer options, exactly one row group including an empty chunk, row and serialized-byte bounds, streaming SHA-256, rewound context-owned output, and tested in-memory/disk-spilled spool closure.
- Kept the slice intentionally below the S3/service/manifest/CLI boundary; no dependency, manifest, object-key, shared-storage, or ADR change was needed.
- Verified current APIs against Context7 `/websites/psycopg_psycopg3` and `/apache/arrow` documentation for transaction characteristics, named cursor/fetch behavior, parameter binding, and explicit Parquet writer controls.
- `./scripts/doctor.sh`, focused tests (64), LSP diagnostics, `make lint`, `make test` (458 passed; 49 integration tests deselected), and `./scripts/verify.sh` — PASS.
- `make integration` — NOT RUN because this slice uses fake PostgreSQL connections and local Parquet spools; Garage service integration is slice 3 and live PostgreSQL interruption coverage is slice 4.
- Next task: implement BI-070 slice 3 restart-safe chunk reconciliation, immutable raw-before-manifest persistence, replay-from-row-one recovery, and focused Garage integration.

## 2026-10-14 — BI-007 documentation information architecture

- Preserved the existing BI-070 slices 1–2 work in commit `c007f8d` before starting the independent documentation maintenance change.
- Kept only `IMPLEMENTATION_PLAN.md`, `STATE.md`, and the append-only `WORKLOG.md` as files directly under `docs/`.
- Moved the product specification to `docs/specification/`, shared core/source/storage contracts to `docs/contracts/`, and API/file behavior documents to `docs/ingestion/`; retained the conventional `docs/adr/` and `docs/plans/` directories.
- Updated active repository navigation, document cross-links, detailed plans, the doctor root check, backlog acceptance references, and the file-documentation contract test; historical worklog entries were not rewritten.
- Directed the future BI-070 behavior document to `docs/ingestion/postgres-snapshot.md` and retained BI-070 as the synchronized active product task.
- Exact root-file inventory, stale-active-path audit, relative Markdown link validation, focused documentation tests (34), LSP diagnostics, `make lint`, `make test` (458 passed; 49 integration tests deselected), `./scripts/doctor.sh`, and `./scripts/verify.sh` — PASS.
- `make integration` — NOT RUN because this maintenance task changes documentation paths and one path-sensitive unit test only.
- Next task: implement BI-070 slice 3 restart-safe chunk reconciliation, immutable raw-before-manifest persistence, replay-from-row-one recovery, and focused Garage integration.

## 2026-10-14 — BI-070 slice 3 restart-safe chunk service

- Added failing service tests first, then implemented a narrow shared-storage protocol and one restart-safe `ingest_order_snapshot` operation without adding CLI behavior, dependencies, object keys, manifest fields, or an aggregate checkpoint.
- The service emits the captured/resume descriptor before S3 mutation, streams one fetched chunk through one context-owned serializer spool, reconciles exact raw/manifest keys, writes and verifies raw Parquet before a succeeded manifest, and closes the spool before the next fetch.
- Complete chunks retain their original manifest and run ID, payload-only chunks receive only the missing manifest, and absent chunks create raw bytes before metadata; replay always begins at chunk 1 and final count/chunk/watermark checks fail closed.
- Added stable handled outcomes and failure events for manifest-only/malformed/link/status states, raw conflicts, write failures, source-contract violations, chunk overflow, connection interruption, and total-deadline exhaustion without creating failed manifests or mutable checkpoints.
- Added focused Garage coverage for a three-chunk snapshot with a pre-existing payload-only middle chunk, exact Parquet bytes/checksums/schema/rows, strict manifest inventory, and a complete replay with zero payload bytes written and unchanged original manifests/run IDs.
- Focused BI-070 tests (54), LSP diagnostics, `make lint`, `make test` (471 passed; 50 integration tests deselected), focused Garage integration (1), full `make integration` (50 initial integration tests plus recovery checks), and `./scripts/verify.sh` — PASS.
- BI-070 remains `in_progress`; the synchronized next task is slice 4, adding the strict snapshot CLI and live PostgreSQL interruption/resume coverage.

## 2026-10-14 — BI-070 slice 4 strict CLI and live interruption recovery

- Added failing CLI tests first, then implemented strict `run` and `resume` forms with canonical dates and resume fields, empty/non-empty exclusivity, allow-listed sources, configured-bound validation before client creation, compact NDJSON, stable `0/2/3/4/5` exits, and explicit S3 cleanup.
- Added `make ingest-postgres`; the adapter delegates connection/transaction/cursor ownership, descriptor capture, serialization, reconciliation, and storage behavior to the existing BI-070 boundaries rather than duplicating them.
- Added live seeded PostgreSQL/Garage coverage for ordered multi-chunk extraction, exact Arrow schema and rows, succeeded manifests, exact inventory, and complete replay with zero new raw bytes, unchanged checksums, and unchanged original manifests/run IDs.
- Terminated the active PostgreSQL backend from a second connection after the first committed chunk, verified partial S3 state, inserted a test-owned row above the emitted upper tuple, and proved resume replays from row one while preserving exactly the original bounded 24 rows.
- Added live payload-only repair, below-bound source-drift rejection with unchanged prior evidence, and a bounded unavailable-S3 CLI failure; tests remove only deterministic snapshot keys and test-owned rows, leaving the default seed restored.
- Verified Psycopg connection/cursor lifecycle and backend PID behavior against current Context7 `/websites/psycopg_psycopg3` documentation.
- Focused CLI tests (19), focused live snapshot integration (5), LSP diagnostics, `make lint`, `make test` (490 passed; 54 integration tests deselected), full `make integration` (54 initial integration tests plus recovery checks), and `./scripts/verify.sh` — PASS.
- BI-070 remains `in_progress`; the synchronized next task is slice 5, documenting the operator contract, running final acceptance, and closing BI-070.

## 2026-10-14 — BI-070 slice 5 documentation and acceptance closeout

- Added a failing documentation contract test first, then added `docs/ingestion/postgres-snapshot.md` with the implemented source/date boundary, exact query columns and Parquet schema, descriptor/identity/key example, read-only repeatable-read watermark behavior, bounds, persistence order, manifests, metrics, errors/exits, recovery matrix, and explicit non-claims.
- Updated README with only the tested `run`/`resume` commands and expected retained state plus recovery actions for PostgreSQL interruption, unavailable S3, payload-only interruption, source drift, rows above the bound, and idempotent replay.
- Audited BI-070 acceptance evidence for explicit queries without `SELECT *`/`fetchall()`, one-chunk memory/spool bounds, fixed upper watermark, above-bound exclusion, replay-from-chunk-1 recovery, payload-only repair, exact repeated inventory, cleanup, and honest sequential delivery semantics.
- Documentation contract tests (27), LSP diagnostics, `make test` (491 passed; 54 integration tests deselected), Compose configuration, `make help`, explicit `make up`, deterministic seed, and full `make integration` (54 initial integration tests plus infrastructure recovery checks) — PASS.
- The first `make lint` formatting check found one line-wrap change in the new test; it was corrected and the complete lint gate then passed with Ruff and strict mypy.
- Marked BI-070 done, selected BI-080, synchronized README/backlog/state/plan, and ran `./scripts/verify.sh` successfully before final task-state synchronization.
- Next task: create the detailed test-first BI-080 plan for recovery demos, final operator documentation, and the clean-clone walkthrough.

## 2026-10-14 — BI-080 recovery demo planning

- Added an implementation-ready four-slice BI-080 plan without changing ingestion, identifier, object-key, manifest, summary, or delivery-semantics contracts.
- Fixed the four specification Make targets, an allow-listed recovery runner, exact read-only inventory inspection, structured demo evidence, a three-minute warm happy-path budget, and explicit ignored local-environment initialization.
- Mapped all nine mandatory failures plus deliberate API-page and file-move process boundaries to deterministic intermediate S3 states and normal production recovery commands or honest terminal handling.
- Kept fault injection in demo-only child processes and wrappers, outside production CLI flags and `AppConfig`; recovery continues to use the existing API, file, PostgreSQL, and shared-storage primitives.
- Defined isolated clean-export evidence, bounded subprocess/readiness behavior, artifact redaction, test-owned cleanup, protected default volumes, and exact replay checksum/manifest comparisons.
- Based Compose project/service isolation and bounded readiness planning on current Context7 `/docker/compose` documentation; no dependency or ADR is required.
- Marked BI-080 `in_progress` and synchronized the backlog, detailed-plan index, state handoff, and next atomic slice.
- Backlog JSON Schema validation, changed-plan Markdown links, repository policy, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included doctor, locked sync, Ruff, strict mypy, 491 offline tests, and Compose configuration.
- LSP diagnostics and `make integration` — NOT RUN because only planning/state documentation changed.
- Next task: start BI-080 slice 1 with failing demo registry, timeout, report, environment-initializer, and Make wiring tests.

## 2026-10-14 — BI-080 slice 1 demo contracts and local environment initializer

- Added the slice-1 tests first and observed collection fail because the planned demo modules did not exist; the completed focused suite passes 16 tests.
- Added the exact four specification demo targets plus recovery/inspection support targets, explicit timeout/environment/project forwarding, and `make init-local-env`; live demo targets intentionally return a stable `not_implemented` summary until slice 2.
- Added an eleven-scenario allow-list, path-safe state and artifact contracts, a closed secret-free `demo_summary`, stable exit/error classifications, and compact deterministic JSON without changing `RunSummary`.
- Added a bounded subprocess supervisor with a separate process session, explicit timeout and cleanup grace, expected/non-expected exit classification, and TERM-then-KILL process-group cleanup.
- Added explicit local environment initialization that cryptographically replaces only the four recognized placeholders, validates a mode-`0600` temporary file through the existing redacting preflight before atomic publication, refuses unsafe/partial targets, and is byte-idempotent on rerun.
- Kept imports side-effect free and added no dependency, production CLI fault flag, ingestion implementation, object-key/manifest change, or ADR.
- Focused slice-1 tests (16), initializer CLI smoke/preflight, Make help, backlog JSON Schema, repository policy, `git diff --check`, LSP diagnostics, `make lint`, and `make test` (507 passed; 54 integration tests deselected) — PASS.
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, strict mypy, 507 default tests, and Compose configuration.
- `make integration` — NOT RUN because slice 1 has no live demo, source, storage, PostgreSQL, or Compose behavior.
- Next task: implement BI-080 slice 2 recording adapters, exact read-only inventory verification, and the four bounded public demos.
- Follow-up supervisor coverage exercises TERM-to-KILL escalation and bounded reaping; the final focused count is 17 tests and the default suite is 508 tests.
- A post-entry verification rerun exposed one Ruff import-order failure after the protocol refinement; the import was corrected before the final successful verification rerun.

## 2026-10-14 — BI-080 slice 2 public demos and exact inspection

- Added failing fake-S3 inventory and recording-adapter tests first, observed the expected missing-module/import failures, then implemented the public demo execution without adding a second ingestion implementation, production fault flags, dependencies, object-key changes, or manifest changes.
- The happy path supervises the existing REST, file, and PostgreSQL CLIs; duplicate replay requires zero canonical raw writes plus unchanged exact key/length/SHA-256/manifest-byte evidence for API, snapshot, same-name file, and renamed-content runs.
- Added a private bounded API-retry child that resets one allow-listed mock fault key and invokes the production ingestion primitive with `rate_limit_once`, proving one 429, the exact one-second `Retry-After`, one retry, no stored 429 body, and a succeeded page pair.
- The bad-file demo copies committed schema-invalid bytes to a canonical filename, creates one bounded malformed UTF-8 CSV, requires exact raw/rejected retention and stable reasons, then delivers corrected valid bytes without deleting rejected evidence.
- Added project-scoped mode-0600 state/report artifacts, strict record schemas, compact NDJSON relay/state/summary evidence, bounded process-group supervision, and stable failure codes without recording credentials, payload values, absolute paths, command arguments, or backend exception text.
- Exact inspection lists only calculated prefixes with bounded pagination, requires exact key sets, streams and independently hashes object bytes while ignoring `ETag`, strictly validates manifests and file dispositions, and checks snapshot Parquet schema, row order/count, and upper tuple.
- Added an isolated live test bucket and project that run all four Make targets plus `demo-inspect`, verify expected counts/retry/rejections and output redaction, and remove only test-owned objects, bucket, and artifacts.
- The first `make lint` format check found one changed Python file requiring Ruff formatting; formatting was applied and the complete lint gate then passed.
- Focused demo tests (21), LSP diagnostics, repository policy, `git diff --check`, Compose configuration, `make lint`, `make test` (512 passed; 55 integration tests deselected), all four public demos, `demo-inspect`, focused live demo integration, full `make integration` (55 initial integration tests plus infrastructure recovery), and `./scripts/verify.sh` — PASS.
- Observed warm happy-path runtime was 2.29 seconds on the validation workstation, below the three-minute acceptance budget; setup/image download time was excluded.
- Verified Boto3 prefix pagination and streamed object-body behavior against current Context7 `/boto/boto3` documentation; SHA-256 remains calculated from actual bytes rather than `ETag`.
- BI-080 remains `in_progress`; next task: implement slice 3 demo-only process failpoints and the complete mandatory recovery scenario matrix using normal production recovery commands.

## 2026-10-14 — BI-080 slice 3 deliberate failure boundaries and recovery matrix

- Added unit tests for one-shot demo-only manifest and delete failpoints, including the required verified-raw, complete-page, selected-key, copied-destination, and destination-read prerequisites; unrelated writes/deletes do not trigger them.
- Added a private worker with no production CLI or `AppConfig` fault flag. It exercises permanent 500 and malformed HTTP 200 through the existing API service seam, terminates after a complete API page or before its manifest, terminates a PostgreSQL backend from a second connection after chunk 1, and dies immediately before landing deletion after verified file copy.
- Implemented all eleven `demo-recovery` scenarios. The parent requires the expected child exit and exact intermediate S3 state, then delegates recovery to the normal API CLI, file `resume`, or PostgreSQL `resume`; malformed JSON is safely re-surfaced unchanged instead of overwritten.
- Added a bounded unreachable-loopback S3 exercise without stopping Garage or selecting unrelated data. Explicit loopback proxy bypass makes the handled exit deterministic even when the host exports HTTP proxy variables.
- Added safe replay behavior for completed API-page, payload-before-manifest, file-move, and PostgreSQL-drop demonstrations: reruns verify the already recovered state and use normal idempotent commands instead of deleting evidence to recreate a crash.
- Added isolated parameterized live coverage for every mandatory failure plus API-page and file-move process boundaries, exact intermediate/final state events, normal recovery, output credential/payload redaction, and test-owned bucket/artifact cleanup.
- Verified Psycopg `ConnectionInfo.backend_pid` against current Context7 `/websites/psycopg_psycopg3` documentation; no dependency, ADR, manifest, key, or production delivery-semantics change was required.
- Focused demo unit tests (24), LSP diagnostics, `make lint`, and `make test` (515 passed; 66 integration tests deselected) — PASS.
- The first full integration run exposed nondeterministic proxy handling for `127.0.0.1:1`; after explicit `NO_PROXY`/`no_proxy` isolation, the eleven-scenario focused run and full `make integration` (66 initial integration tests plus infrastructure recovery checks) passed.
- After adding live repeat-execution assertions for all four abrupt process boundaries, one `make lint` formatting check failed; Ruff formatting was applied, then final lint and full integration reruns passed.
- BI-080 remains `in_progress`; next task: implement slice 4 final recovery/operator documentation and the isolated clean-export acceptance walkthrough.

## 2026-10-15 — BI-080 slice 4 clean-export and documentation closeout

- Added failing clean-export and documentation contract tests first, then implemented `scripts/check_clean_export.py` with a Git-reported tracked/unignored export boundary, unsafe/local-state rejection, regular-file-only copying, explicit owner-only environment initialization, and an exact allow-list for isolated loopback ports, endpoints, and bucket values.
- The harness applies per-command and total deadlines to locked setup, doctor, Compose rendering, startup, deterministic seed, all four public demos, offline tests, and final exact inspection; it verifies one passing summary from each demo and the expected clean-project object counts.
- Cleanup runs normal `make down`, invokes the existing reset helper with exact destructive confirmation only for the generated project, requires all project-labeled containers/networks/volumes to be absent, and compares protected default-project volume fingerprints before and after.
- Added `docs/ingestion/recovery-demos.md` with all eleven allow-listed scenarios. Every row documents the exact trigger command and underlying exit, intermediate object state, normal recovery or terminal action, final state, and relevant limitations; README now exposes only implemented demo, recovery, inspection, and clean-export commands.
- The first clean-export run correctly exposed a configuration-dependent final inventory mismatch (one default snapshot chunk rather than the integration test's three); the assertion was corrected from eight to six raw/manifest pairs without changing the configured chunk bound. The second run correctly found retained bad-file input payloads under artifacts; report scanning was narrowed to NDJSON/state artifacts while keeping intentional rejected input bytes as business evidence. The third complete walkthrough passed.
- Accepted clean-export timing on the validation workstation was 11.11 seconds for setup, 2.59 seconds for the warm happy path, and 27.40 seconds through final inspection; cleanup also passed and no hardware-independent total-time promise was added.
- Current Compose project isolation, health waiting, and named-volume cleanup semantics were checked against Context7 `/docker/compose`; no dependency, ADR, manifest, object-key, or production fault-interface change was needed.
- Focused contracts (6), LSP diagnostics, `make lint`, `make test` (521 passed; 66 integration tests deselected), Compose rendering, isolated clean-export acceptance, full `make integration` (66 tests plus marker and infrastructure recovery checks), repository policy, and `./scripts/verify.sh` — PASS.
- Marked BI-080 done, selected bounded read-only BI-090 instead of adding Possible extensions, and synchronized README, backlog, state, detailed plan, and this worklog.
- Next task: perform BI-090 as one independent post-completion acceptance audit of the completed core against the specification, accepted ADRs, and recorded evidence.

## 2026-10-15 — BI-090 independent acceptance-audit planning

- Added an implementation-ready four-slice BI-090 plan without executing the audit or changing product code, tests, dependencies, contracts, README behavior, or accepted ADRs.
- Fixed an independence boundary that rebuilds traceability from the specification, ADRs, backlog criteria, implementation, tests, documentation, and fresh command evidence rather than accepting prior state/worklog claims as proof.
- Required exact Git/worktree provenance and an explicit distinction between a clean committed checkout and a dirty working-tree export; the audit may not commit, stash, reset, clean, or silently repair the reviewed tree.
- Defined a stable audit-report structure, severity/category taxonomy, per-finding evidence schema, full requirement matrix, adversarial static checks, isolated live validation, protected-resource comparison, and bounded cleanup.
- Kept the audit read-only for product artifacts: actionable repairs must become separately scoped maintenance tasks before implementation, and Possible extensions remain excluded.
- Marked BI-090 `in_progress` at the planning boundary and synchronized the backlog, implementation-plan index, state handoff, and next audit slice.
- `./scripts/doctor.sh`, backlog JSON Schema validation, changed-plan Markdown links, repository policy, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included locked dependency sync, Ruff, strict mypy, 521 offline tests, and Compose rendering.
- LSP diagnostics and `make integration` — NOT RUN because this change is planning/state documentation only.
- Next task: execute BI-090 slice 1 by freezing and classifying the exact audited baseline and building the authoritative requirement traceability matrix.

## 2026-10-15 — BI-090 slice 1 baseline and requirement traceability

- Froze the audit baseline before service startup or audit-owned edits: `HEAD` `a1ef506fd13545467c6caf6c04eb803ab2c77461` on `main`, full short status, tool versions, and a byte-hashed inventory of all 158 tracked/unignored paths.
- Classified the baseline as `dirty_worktree`; 9 tracked paths differed from `HEAD` and 19 implementation, test, script, plan, or operator-document paths were untracked.
- Added `docs/audits/BI-090.md` with the required fixed sections, exact baseline provenance, divergence hashes, core-scope boundary, and an authority-derived traceability inventory.
- Enumerated every in-scope specification requirement, accepted ADR obligation, and completed BI-000–BI-080 acceptance criterion with candidate implementation, tests, and documentation; every fresh-evidence cell remains explicitly `UNVERIFIED` pending slices 2–4.
- Recorded BI-090-F01: the frozen `HEAD` cannot reproduce completed snapshot/demo/clean-export behavior because required files existed only in the dirty export. This blocks a clean-commit claim for that baseline but not continuation of the bounded dirty-worktree audit.
- Changed no product code, tests, dependencies, contracts, README behavior, or accepted ADR as part of the audit slice; ignored provenance evidence remains under `artifacts/audit/BI-090-slice1/`.
- `./scripts/doctor.sh`, exact inventory capture, repository policy, backlog JSON Schema validation, `git diff --check`, report structure/population, LSP diagnostics, `make lint`, `make test` (521 passed; 66 integration tests deselected), Compose rendering, and `./scripts/verify.sh` — PASS.
- Retained two failed ad hoc backlog-validator attempts: uv lacked PyYAML/JSON Schema and Ruby was unavailable; validation then passed through the already available system-Python YAML/JSON Schema libraries. Isolated live integration and clean-export evidence were deliberately not inferred from earlier sessions.
- Next task: execute BI-090 slice 2 adversarial static review and convert mapped source/document observations into evidence-backed candidate findings without implementing repairs.

## 2026-10-15 — BI-090 slice 2 adversarial static review

- Reviewed every traceability row against implementation bodies, focused unit/integration coverage, operator documentation, dependency and Compose surfaces, reliability bounds, cleanup behavior, and module boundaries without changing product code, contracts, tests, dependencies, README, or accepted ADRs.
- Assigned grouped static status to the complete product, ADR, and completed-backlog matrices while retaining every fresh-runtime cell as `UNVERIFIED` for slices 3–4; static PASS means source/test/document closure only, not inferred runtime acceptance.
- Recorded BI-090-F02: REST persists only per-page manifests and an ephemeral `RunSummary`, despite the specification and BI-050 scope requiring a persisted manifest for the complete logical run; repair requires an ADR because cardinality and key contracts would change.
- Recorded BI-090-F03: backend S3 exceptions can bypass source terminal events and final `RunSummary` emission in REST, file, and snapshot paths; a bounded API reproduction emitted neither event nor summary.
- Recorded BI-090-F04: HTTPX response iteration requests a response-sized buffered chunk and checks the monotonic deadline only after a yield, so a below-read-timeout trickle crossed a one-second total budget and consumed for six seconds before detection.
- Recorded BI-090-F05: the local initializer/preflight validates only infrastructure fields but README claims validation of HTTP, file, PostgreSQL, and snapshot application bounds; an application-incomplete environment reproduced a false PASS.
- Confirmed statically that standard external clients remain centralized, snapshot extraction has no `SELECT *`/`fetchall()`, images are pinned, ETag is not used as SHA-256, destructive reset is explicit, cleanup is project-scoped, and no Possible extension or out-of-scope service was introduced.
- Checked HTTPX 0.28.1 `Response.iter_raw(chunk_size=...)` buffering and read-timeout semantics against current Context7 `/encode/httpx/0.28.1` documentation.
- Retained ignored supporting evidence at `artifacts/audit/BI-090-slice2/`; repository policy, `git diff --check`, deterministic reproductions/static scans, and `./scripts/verify.sh` — PASS. Verify included doctor, locked sync, Ruff, strict mypy, 521 offline tests, and Compose rendering.
- LSP diagnostics — NOT RUN because slice 2 changed documentation only. Isolated clean export and `make integration` — NOT RUN because they are the explicitly separate fresh-runtime scope of slice 3.
- BI-090 remains `in_progress`; no repair task is opened until final finding disposition. Next task: execute slice 3 isolated clean-export, full local integration/recovery, and exact cleanup validation with fresh timestamped evidence.

## 2026-10-15 — BI-090 slice 3 fresh bounded validation

- Freshly ran the complete audit validation matrix from product revision `6c4249977d3f1ccdebf0a98684e5e3139e15473d` plus audit-only documentation edits; corrected the report's post-baseline full-hash typo without changing the frozen `a1ef506f` baseline.
- Doctor, repository policy, backlog JSON Schema/active-state validation, `git diff --check`, `make lint`, `make test` (521 passed; 66 integration deselected), and Compose rendering passed with timestamped ignored logs.
- The isolated clean-export harness passed locked setup, startup/seed, all four public demos, offline tests, exact inspection, redaction, and cleanup in 24.44 seconds; the warm happy path took 1.73 seconds and enforced 6 raw, 6 manifest, 1 archive, 2 rejected, and 0 landing objects.
- Created an owner-only audit environment with generated credentials, unique bucket/project `bi090-audit-f3cf9dbe8106`, and four unique loopback ports only after proving that project and the fixed recovery project had no existing Compose-labeled resources.
- Isolated `make integration` passed all 66 API/file/infrastructure/mock/snapshot/source/recovery/storage cases in 77.23 seconds, then passed four repeat-seed marker cases and the bounded persistence, fail-closed reset, destructive-reset, and protected-project recovery check.
- Exact cleanup removed the audit containers, network, volumes, and uniquely owned integration/clean-export images. No labeled container/network/volume remained for any exercised project, all three default-project volume-inspection hashes matched before and after, evidence logs contained no generated credentials or recognizable supplier values, and the temporary credential file was deleted.
- Retained one auxiliary cleanup-check failure: the first assertion incorrectly required absence of the fixed recovery image without fingerprinting its pre-run existence. The corrected ownership-scoped assertion excluded that unattributed reusable image and passed; no product check was rerun or hidden, and no protected state changed.
- Fresh broad gates corroborate the happy, recovery, replay, exact-byte, and cleanup paths but do not resolve F01–F05: the suites do not require an aggregate run manifest or backend-failure summaries, do not model an indefinitely trickling success response, and accept the partial local preflight.
- Updated only `docs/audits/BI-090.md`, `docs/STATE.md`, and this append-only worklog. No product code, tests, dependencies, contracts, README behavior, backlog task status, or ADR changed; BI-090 remains active and `in_progress`.
- `./scripts/verify.sh` — PASS after the slice-3 report/state update: doctor, policy, locked sync, Ruff, strict mypy, 521 offline tests, and Compose rendering. LSP diagnostics — NOT RUN because only Markdown changed.
- Next task: execute BI-090 slice 4 by finalizing every finding and traceability status, creating bounded maintenance tasks for supported root causes, and closing the audit without implementing repairs.

## 2026-10-15 — BI-090 slice 4 findings and closeout

- Finalized BI-090-F01–F05 without changing product behavior: the frozen-baseline reproducibility gap, missing persisted REST logical-run manifest, missing terminal summaries on raw S3 failures, unbounded slow-trickle HTTP response consumption, and incomplete local application preflight all remain supported.
- Replaced all temporary traceability statuses with 133 final PASS/FAIL outcomes tied to the slice-2 static groups, slice-3 fresh runtime groups, or an exact finding; no row remains BLOCKED, NOT RUN, or unverified at closeout.
- Added bounded follow-up tasks BI-091 through BI-096. BI-091 records the required aggregate-manifest ADR before BI-092 implementation; BI-093, BI-094, BI-095, and BI-096 independently address F03, F04, F05, and F01.
- Marked BI-090 done, selected BI-091 as the next todo task, synchronized the plan/backlog/state/report, and left BI-010 through BI-080 closed.
- Changed no product code, tests, dependencies, contracts, README behavior, accepted ADR, Compose configuration, or fixture; Possible extensions remain excluded.
- Retained two closeout-check mistakes rather than hiding them: an ad hoc validator called nonexistent `jsonschema.loads`, then an instrumented checker expected 140 instead of the actual 133 matrix rows and its wrapper masked that assertion exit. The corrected schema/state/matrix/finding/task/policy/diff gate passed.
- `./scripts/verify.sh` — PASS: doctor, repository policy with BI-091 active and BI-090 done, locked sync, Ruff, strict mypy, 521 offline tests, and Compose rendering.
- LSP diagnostics over the five changed YAML/Markdown paths — PASS with no error or warning; the spell checker reported six information-only suggestions inside generated hexadecimal identifiers.
- Next task: execute BI-091 only by accepting the ADR for aggregate REST logical-run manifest cardinality, keys, linkage, completion, reconciliation, replay, and compatibility; do not implement BI-092 behavior in that task.

## 2026-10-15 — BI-091 logical-run manifest decision planning

- Added an implementation-ready three-slice BI-091 plan and completed its constraint inventory without writing the ADR or changing runtime behavior, tests, dependencies, current-behavior documentation, README claims, or stored evidence.
- Compared the existing page-set/summary interpretation, changing page manifests, synthetic raw payloads, per-attempt aggregates, and one deterministic API-specific aggregate against immutability, boundedness, replay, and compatibility requirements.
- Proposed for ADR evaluation one immutable batch-level aggregate with a separate strict schema/version, exact ordered page linkage, an explicit byte bound, success-only publication after all page pairs, exact-key reconciliation, and legacy page-only upgrade on replay.
- Preserved schema-version-1 page manifests, existing `batch_id` and page keys, standard-S3 access, sequential-writer semantics, and the prohibition on exactly-once claims; implementation remains exclusively BI-092.
- Refined BI-091 canonical scope/acceptance, marked it `in_progress`, indexed the detailed plan, and synchronized `docs/STATE.md` while keeping BI-090 as the last completed task.
- No Context7 lookup was needed because this planning change makes no external library API or CLI decision.
- `./scripts/doctor.sh`, repository policy, backlog JSON Schema validation, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included locked sync, Ruff, strict mypy, 521 offline tests, and Compose rendering.
- LSP diagnostics and `make integration` — NOT RUN because only YAML/Markdown planning and handoff files changed.
- Next task: execute BI-091 slice 2 by drafting and accepting ADR 0003, then refine BI-092 without implementing it.

## 2026-10-15 — BI-091 ADR acceptance and implementation handoff

- Accepted and indexed ADR 0003, selecting one immutable success-only API logical-run manifest at a deterministic batch key after comparing the required bounded alternatives.
- Fixed the separate strict schema-version-1 field set, canonical JSON, 8 MiB declared/actual byte bound, exact checksummed page linkage, publication-before-success order, complete reconciliation matrix, uncertain-write handling, mixed-run replay, and legacy page-only upgrade.
- Refined BI-092 scope and acceptance directly against every ADR 0003 required-validation row: model/parser/key helper, bounded exact-key streamed verification, completion ordering, all reconciliation states, unit and Garage coverage, backfill, exact inventory/demo inspection, documentation, metric compatibility, and honest delivery semantics.
- Confirmed that `docs/contracts/core-contracts.md`, `docs/ingestion/api-ingestion.md`, and README still describe only current page-only runtime behavior; BI-091 changed no Python, tests, dependencies, runtime behavior, current object-store evidence, or current-behavior claims.
- Marked BI-091 done, selected BI-092 as the active `todo` task, and synchronized its detailed plan, canonical backlog, and `docs/STATE.md`.
- `./scripts/doctor.sh`, repository policy, backlog JSON Schema validation, and `git diff --check` — PASS.
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, strict mypy, 521 offline tests with 66 integration tests deselected, and Compose rendering.
- LSP diagnostics and `make integration` — NOT RUN because BI-091 changed only YAML/Markdown architecture, planning, and handoff files.
- Next task: start BI-092 with test-first implementation of the strict ADR 0003 aggregate model, canonical parser/serializer, and deterministic key helper; do not combine another audit repair.

## 2026-10-15 — BI-092 implementation planning

- Added an implementation-ready five-slice BI-092 plan directly under ADR 0003 and marked the active task `in_progress`; no Python, test, dependency, runtime behavior, current-behavior contract, README claim, or stored evidence changed.
- Fixed ownership for a separate API aggregate model/key/parser and reconciliation module while preserving the shared standard-S3 boundary and unchanged one-payload schema-version-1 `Manifest`.
- Planned strict canonical bytes and nested duplicate-member rejection, identity/key rederivation, ordered gap-free page linkage, 9,999-page and separate 8 MiB bounds, and exact streamed raw/page-manifest/aggregate checksums.
- Planned a pre-mutation aggregate barrier plus post-page completion phase for every absent/incomplete/failed/eligible/complete/corrupt/conflicting/orphaned/uncertain-write state, including at most two writes of one frozen byte sequence after uncertain outcomes.
- Planned test-first service ordering, legacy page-only upgrade, mixed page-manifest run IDs, unchanged metric meanings, Garage/backfill coverage, exact demo inventory, clean-export validation, and operator documentation without expanding into BI-093–BI-096.
- Checked current Boto3 S3 streamed-body and metadata behavior through Context7 `/boto/boto3`; no new runtime dependency is required.
- `./scripts/doctor.sh`, repository policy, backlog JSON Schema validation, BI-092 plan coverage smoke check, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included locked sync, Ruff, strict mypy, 521 offline tests, and Compose rendering.
- LSP diagnostics and `make integration` — NOT RUN because this planning change touched only YAML/Markdown files.
- Next task: execute BI-092 slice 1 only by adding failing pure aggregate-contract tests, then the strict model, canonical parser/serializer, and deterministic key helper; do not combine storage/service wiring or BI-093.

## 2026-10-15 — BI-092 slice 1 pure aggregate contract

- Added the focused aggregate test module first and recorded the expected collection failure while `batch_ingestion.api.logical_manifest` did not exist.
- Added separate frozen `ApiLogicalRunManifest` and `ApiLogicalRunPage` models without changing the shared one-payload `Manifest` schema or runtime service behavior.
- Implemented both exact ADR key examples, allow-listed API identity and `batch_id` rederivation, exact raw/page-manifest key linkage, immutable identity parameters, and strict success-only schema constants.
- Added compact sorted UTF-8 serialization and a bytes-only parser that rejects oversized, malformed, noncanonical, BOM/trailing, unsupported, wrong-typed, and nested duplicate-member JSON.
- Enforced strict empty/full/final pagination relationships, ordered gap-free entries, checksums and signed-64-bit lengths, 9,999 pages, 999,900 records, and the separate 8 MiB bound; the exact maximum-width canonical shipping document is 6,098,762 bytes.
- Exported the pure aggregate surface from `batch_ingestion.api`; added no dependency, I/O, clock, environment, S3, service, demo, README, or current-behavior documentation change.
- Focused aggregate/source/page-manifest tests — PASS (92); LSP diagnostics — PASS with no findings; `make lint` — PASS; `make test` — PASS (574 offline tests, 66 integration deselected); `./scripts/verify.sh` — PASS with Compose rendering.
- `make integration` — NOT RUN because slice 1 is pure and changes no storage, service, or Compose behavior. BI-092 remains active and `in_progress`.
- Next task: execute BI-092 slice 2 only by implementing exact streamed evidence, reconciliation classifications, and bounded frozen-byte uncertain-write recovery behind tests; do not wire the service or start BI-093.

## 2026-10-15 — BI-092 slice 2 exact evidence and reconciliation

- Added the focused reconciliation test module first and recorded the expected collection failure while `batch_ingestion.api.logical_run` did not exist.
- Added an API-owned exact-key reader over the existing context-owned bounded storage spool; aggregate, raw-page, and exact canonical page-manifest checksums come from actual streamed bytes rather than ETag or prefix listing.
- Added typed absent, incomplete, failed, eligible, complete, corrupt, conflicting, orphaned, and uncertain-write outcomes with preflight mutation barriers for malformed, noncanonical, oversized, wrong-identity/count, missing-reference, raw-mismatch, and page-manifest-mismatch cases.
- Added strict duplicate-member and canonical-byte checks for referenced schema-version-1 page manifests while retaining mixed page-manifest run IDs and the unchanged shared manifest schema.
- Added publication convergence that requires all page evidence before writing, freezes one aggregate byte sequence, invokes immutable write at most twice, rewinds identical bytes on retry, accepts only fully verified identical uncertain writes, and leaves different present bytes unchanged as a conflict.
- Added no dependency and changed no shared storage implementation, REST service behavior, CLI, backfill, demo inventory, Garage integration, README, or current-behavior ingestion contract.
- Focused aggregate/evidence/service/source tests — PASS (117); LSP diagnostics — PASS with no findings; `make lint` — PASS; `make test` — PASS (599 offline tests, 66 integration deselected); `./scripts/verify.sh` — PASS with Compose rendering.
- `make integration` — NOT RUN because slice 2 does not yet wire runtime service or Garage behavior. BI-092 remains active and `in_progress`.
- Next task: execute BI-092 slice 3 only by wiring aggregate preflight, page evidence, publication, and final verification into the single-date service before terminal success output; do not start demo/Garage work or BI-093.

## 2026-10-15 — BI-092 slice 3 single-date service publication

- Added service tests first and recorded the expected collection failure because the planned logical-run error codes were absent.
- Wired exact aggregate preflight before source fetch or storage mutation, blocking corrupt, conflicting, and orphaned completion claims without healing or deleting retained evidence.
- Collected exact streamed raw/page-manifest evidence after every succeeded page pair, then published or reused one immutable aggregate and performed final full reconciliation before terminal success and `RunSummary` output.
- Added stable aggregate corrupt/conflict/orphaned/uncertain codes to the existing event shape and exposed the separately verified aggregate key on `ApiIngestionResult` without adding CLI/configuration arguments.
- Proved first-publication ordering, legacy page-only upgrade, mixed page-manifest run IDs, complete byte-stable replay with preserved `completion_run_id`/`completed_at`, unchanged raw-only `bytes_written` and reused-page-only `duplicate_count`, and retained complete pages after aggregate publication uncertainty.
- Updated narrow backfill/CLI result fixtures only; backfill still delegates to the single-date primitive and fails fast. Garage integration, exact demo inventory, clean-export counts, and operator documentation remain slices 4–5.
- Focused service/backfill/aggregate/CLI tests — PASS (110); LSP diagnostics — PASS with no findings; `make lint` — PASS; `make test` — PASS (605 offline tests, 66 integration deselected).
- `./scripts/verify.sh` — PASS: doctor, policy, locked sync, Ruff, strict mypy, 605 offline tests, and Compose rendering.
- `make integration` — NOT RUN because slice 4 owns Garage/backfill/demo live evidence. BI-092 remains active and `in_progress`.
- Next task: execute BI-092 slice 4 only by adding Garage publication/replay/failure coverage, aggregate-required backfill evidence, and exact demo inventory/clean-export updates; do not start BI-093.

## 2026-10-15 — BI-092 slice 4 Garage, backfill, and exact demo inventory

- Added aggregate-aware Garage key cleanup and exact inventory checks for every successful API logical date; both current API sources now prove first publication and complete byte-stable replay while preserving original completion facts and page-only metric meanings.
- Added live final-page/pre-aggregate interruption and legacy page-only upgrade evidence, mixed page-manifest `run_id` checks, and a standard-storage wrapper that writes exact aggregate bytes to Garage before raising; production exact-key reconciliation accepts the fully verified uncertain write.
- Added live malformed, noncanonical, conflicting, and orphaned aggregate barriers. Each rerun fails with the stable logical-run code before source mutation and leaves the damaged aggregate and remaining page evidence unchanged.
- Extended repeated/resumed backfill inventory to require one aggregate per successful date, preserve earlier dates byte-for-byte, and stop before later dates when a middle-date aggregate conflicts.
- Updated `DemoInventoryVerifier` to require the deterministic aggregate only for successful API records, parse it with the production strict parser, cross-check every raw/page-manifest checksum against independently streamed bytes, and include aggregate bytes in manifest evidence without increasing raw counts.
- Updated recovery-demo and clean-export exact counts. The isolated clean-export walkthrough passed in 26.18 seconds with a 1.82-second warm happy path, exact counts of 6 raw, 8 manifests, 1 archive, 2 rejected, and 0 landing objects, and verified cleanup.
- Test-first demo red phase — PASS as evidence: three focused tests failed before the verifier required and validated aggregate evidence. Focused aggregate/service/backfill/demo unit tests — PASS (132); focused API Garage integration — PASS (16); recovery-demo integration — PASS (12); LSP diagnostics — PASS with no findings.
- `make lint` — PASS; `make test` — PASS (606 offline tests, 73 integration deselected); `make integration` — PASS (73 integration tests plus repeat-seed and infrastructure recovery checks); `./scripts/verify.sh` — PASS with policy, locked sync, static checks, offline tests, and Compose rendering. No dependency, page-manifest schema, CLI, metric meaning, operator-documentation claim, or BI-093 behavior changed.
- BI-092 remains active and `in_progress`; slice 5 is the only remaining slice. Next task: update current-behavior contracts, REST/backfill and recovery/demo documentation, README wording, run final closeout gates, mark BI-092 done, and select exactly one next task.

## 2026-10-15 — BI-092 slice 5 documentation and acceptance closeout

- Updated the core contract to distinguish unchanged one-payload schema-version-1 manifests from the separate API-specific logical-run aggregate accepted by ADR 0003.
- Updated the implemented REST/backfill contract with both exact aggregate key examples, `N + N + 1` cardinality, strict fields and 8 MiB bound, preflight mutation barrier, publication-before-success order, exact streamed checksums, complete reconciliation/recovery table, stable aggregate error codes, legacy page-only upgrade, mixed-run replay, unchanged metrics, and aggregate-required fail-fast backfill.
- Updated README and recovery-demo guidance with aggregate-aware retained/final states, normal recovery actions, strict demo inspection, failed-batch non-completion, and the measured exact inventory of 6 raw, 8 manifests, 1 archive, 2 rejected, and 0 landing objects.
- Preserved standard-S3 access, immutable sequential redelivery, bounded uncertain-write convergence, no synthetic raw payload, and no exactly-once or concurrent-writer claim. No dependency, CLI/configuration surface, page-manifest schema, raw/page key, or BI-093 behavior changed.
- Focused aggregate/reconciliation/service/backfill/demo tests — PASS (126); LSP diagnostics over all 14 changed Python paths — PASS with no findings; `make lint` — PASS; `make test` — PASS (606 offline tests, 73 integration deselected); Compose rendering — PASS.
- `make integration` — PASS: 73 integration tests plus repeat-seed and infrastructure recovery gates. The isolated clean-export walkthrough — PASS in 24.78 seconds with a 1.75-second warm happy path, exact aggregate-aware inventory, redaction, protected-state checks, and verified cleanup.
- Repository policy, backlog/state synchronization, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included doctor, policy with BI-093 selected, locked sync, Ruff, strict mypy, 606 offline tests, and Compose rendering.
- Marked BI-092 done and selected BI-093 as the sole next atomic task. Next task: implement BI-093 terminal events and final summaries for backend S3 failures across REST, file, and PostgreSQL snapshot services; do not combine BI-094.

## 2026-10-15 — BI-093 backend-storage failure planning

- Added an implementation-ready five-slice BI-093 plan without changing Python, tests, dependencies, runtime behavior, object/manifest contracts, stored evidence, or accepted ADRs.
- Confirmed the three current gaps: REST can leak raw Botocore failures past its typed terminal path, file staging and service failures have no source terminal boundary, and snapshot S3 setup occurs outside the descriptor-backed attempt boundary.
- Planned source-neutral `StorageBackendError` translation with preserved cause chains, source-owned context-managed S3 sessions, exactly one terminal event followed by one `run_summary`, confirmed-only metrics, and no generic CLI duplicate after handled failure.
- Planned to move file landing staging into the file service attempt and to acquire snapshot storage only after descriptor capture, preserving raw-before-validation, whole-file atomicity, explicit snapshot identity, and all restart-safe evidence semantics.
- Mapped setup, inspection, read, copy, delete, and write failures across all three sources to focused unit tests and isolated unavailable-endpoint integration, including redaction and exact before/after object-state checks.
- Refined BI-093 against completed BI-092, marked it `in_progress`, indexed the detailed plan, and synchronized the canonical backlog and state handoff; BI-094 remains explicitly excluded.
- Checked current Botocore exception guidance through Context7 `/boto/botocore`; no new runtime dependency or ADR is required.
- `./scripts/doctor.sh`, repository policy, backlog JSON Schema/active-state validation, `git diff --check`, and `./scripts/verify.sh` — PASS; verify included locked sync, Ruff, formatting, strict mypy over 80 source files, 606 offline tests, and Compose rendering.
- Active LSP diagnostics over all five changed YAML/Markdown paths — PASS with no findings. `make integration` — NOT RUN because this planning change changed no runtime or infrastructure behavior.
- Next task: execute BI-093 slice 1 only with failing shared-storage and terminal-contract tests, then the minimal shared normalization/event contracts; do not wire source services or start BI-094.

## 2026-10-15 — BI-093 slice 1 shared normalization and terminal contracts

- Added source-neutral `StorageBackendError` translation for known Botocore client construction, S3 get/put/copy/delete, response-stream, and client-close failures while preserving the original exception through chaining and exposing only a fixed redacted message.
- Added context-owned `storage_session`, retained distinct missing/conflict/verification/size classifications, closed response bodies, spools, and clients on bounded paths, and left arbitrary programming exceptions outside backend normalization.
- Added reusable one-shot terminal-pair emission for API, file, and snapshot event forms.
- Extended the file terminal contract with system-only `failed`, nullable disposition/duplicate fields, stable `storage_failed`/`storage_state_failed` codes, and typed `FileIngestionError` without wiring the file service or changing ADR 0002 business rejection semantics.
- Test-first red phase — PASS as evidence: focused suites initially failed collection for the new contracts. Focused storage/file/summary and existing source service/CLI tests — PASS (192); `make lint` — PASS; active LSP diagnostics — PASS with no findings.
- `./scripts/verify.sh` — PASS with doctor, repository policy, locked sync, Ruff, formatting, strict mypy over 80 source files, 615 offline tests with 73 integration tests deselected, and Compose rendering. `make integration` — NOT RUN because source-service wiring and unavailable-S3 acceptance belong to later BI-093 slices.
- BI-093 remains active and `in_progress`. Next task: execute slice 2 only by adding table-driven REST storage-failure tests, then wire the source-owned storage session and exact terminal pair without starting file/snapshot wiring or BI-094.
- Final changed-path LSP sweep found no Python diagnostics; Markdown diagnostics reported only the pre-existing cspell false positive `dbe` inside the historical project identifier on `docs/WORKLOG.md:562`, which remains unchanged under the append-only worklog contract.

## 2026-10-15 — BI-093 slice 2 REST storage-failure boundary

- Added table-driven REST attempt tests for storage-session setup/close, aggregate preflight/final verification, page reconciliation/read, raw write, page-manifest write, and bounded aggregate publication failure.
- The tests assert exactly one failed `api_ingestion` followed by exactly one `run_summary`, stable source date/batch/run IDs, direct `StorageBackendError` chaining, fixed redacted `storage_error`, confirmed-only counters, no success record, and the exact retained key set at every failure point.
- Moved REST storage-session ownership from the CLI into `ingest_api_date()` after logical identity and `run_id` creation. Terminal success or failure now waits for session exit, while retry events remain non-terminal and source-owned.
- Preserved ADR 0003 uncertain-write convergence: a fully verified aggregate can still recover a write-then-error result, while unresolved backend failure is surfaced through the storage terminal path without changing page, aggregate, key, metric, or retry contracts.
- Kept backfill on the same fail-fast single-date primitive; focused evidence shows the failed date emits once, prior dates remain represented, later dates are not attempted, and the backfill layer emits no duplicate date summary.
- Simplified the REST CLI to inject a context-owned storage-session provider. Attempted S3 setup failures now return handled exit `4` with only the source terminal pair; non-storage HTTP setup/cleanup failures retain generic runtime exit `5` and one redacted `cli_error`.
- Test-first red phase — PASS as evidence: the nine service cases initially failed because `storage_session_provider` was not implemented. Focused API service/backfill/logical-run/CLI tests — PASS (67); `make test` — PASS (625 offline tests, 73 integration deselected); `make lint` — PASS; active LSP diagnostics over all six changed Python paths — PASS with no findings.
- `./scripts/verify.sh` — PASS with doctor, repository policy, locked sync, Ruff, formatting, strict mypy over 80 source files, 625 offline tests with 73 integration tests deselected, and Compose rendering. `make integration` — NOT RUN because isolated unavailable-S3 acceptance across REST, file, and snapshot commands belongs to slice 4.
- BI-093 remains active and `in_progress`. Next task: execute slice 3 only by moving file landing staging and storage ownership into the file-service attempt under table-driven storage-failure tests; do not start snapshot wiring, live unavailable-S3 acceptance, or BI-094.

## 2026-10-15 — BI-093 slice 3 file storage-failure boundary

- Added table-driven file-attempt tests for storage-session setup/close, landing staging, landing/raw download, payload/manifest reconciliation, exact disposition inspection, canonical raw and terminal manifest writes, and disposition copy/delete failures.
- Each backend case asserts exactly one `file_ingestion` outcome `failed` followed by exactly one `run_summary`, stable source date/batch/run IDs, direct `StorageBackendError` chaining, redacted output, confirmed-only counters, nullable pre-validation facts, and exact retained keys.
- Moved production CLI landing staging and S3-session ownership into `ingest_file_delivery()` after bounded local admission; terminal accepted/rejected/failure output is delayed until session exit, and `resume` uses the same service without a staging stream.
- Added the typed `storage_state_failed` terminal path for missing/conflicting/non-convergent evidence while preserving the underlying storage cause. Arbitrary fake/programming `RuntimeError` defects remain unclassified and emit no misleading terminal pair.
- Preserved raw-before-validation, ADR 0002 whole-file counters, immutable manifest ordering, same-name/renamed duplicate classification, and copy-verify-delete restart states. The private file-move demo worker now delegates its admitted stream to the same service-owned staging path.
- Updated the file CLI to return handled exit `4` for an already emitted `FileIngestionError` without appending a generic `cli_error`; non-storage runtime setup/cleanup fallback remains exit `5`.
- Test-first red phase — PASS as evidence: 14 focused cases initially failed because the service lacked session/staging arguments and direct state failures were not normalized. Focused file/validation/contract/demo tests — PASS (181); `make lint` — PASS; `make test` — PASS (637 offline tests, 73 integration deselected); active LSP diagnostics over all six changed Python paths — PASS with no findings.
- `./scripts/verify.sh` — PASS with doctor, repository policy, locked sync, Ruff, formatting, strict mypy over 80 source files, 637 offline tests with 73 integration tests deselected, and Compose rendering. `make integration` — NOT RUN because isolated unavailable-S3 acceptance belongs to slice 4; the file interruption integration assertion was updated for the typed cause chain but not executed in this slice.
- BI-093 remains active and `in_progress`; canonical backlog status and active task remain unchanged. Next task: execute slice 4 only by wiring the snapshot attempt boundary and isolated unavailable-S3 acceptance for all three sources; do not start BI-094 or slice-5 documentation closeout.

## 2026-10-16 — BI-093 slice 4 snapshot boundary and unavailable-S3 acceptance

- Added table-driven snapshot tests for storage-session setup/close, reconciliation, raw write, and manifest write failures. Each case proves descriptor capture precedes storage, one `snapshot_failed` plus one final summary, stable IDs, direct `StorageBackendError` chaining, confirmed-only counters, exact retained state, and redaction.
- Proved descriptor mismatch fails before S3 acquisition with no provisional batch ID or terminal summary, while arbitrary non-storage fake defects remain runtime failures rather than being mislabeled as outages.
- Moved snapshot S3-session ownership into `ingest_order_snapshot()` after descriptor capture/revalidation and `snapshot_started`; success is delayed until clean session exit, and the typed handled error records its already emitted event/summary for duplicate-free CLI handling.
- Narrowed snapshot chunk helper translation from broad `Exception` to `StorageError`, preserving reconciliation/conflict codes and allowing process-boundary/programming failures to remain unhandled without changing payload-only restart recovery.
- Added isolated loopback-unavailable command acceptance for REST, file, and snapshot with one short Botocore attempt, unique exact keys, before/after absence checks, bounded execution, exit `4`, exact terminal cardinality, complete summaries, and credential/DSN/endpoint/payload redaction.
- Updated the `s3-unavailable` recovery demo to require one failed API terminal event plus one summary and handled exit `4` before recovery through the unchanged normal API command.
- Test-first red phase — PASS as evidence: focused snapshot cases initially failed because the service had no storage-session provider and handled errors carried no terminal evidence. Focused snapshot CLI/service and demo tests — PASS (63); isolated unavailable-S3 tests — PASS (3); focused snapshot integration — PASS (5); focused recovery demo — PASS (1).
- `make lint` — PASS; `make test` — PASS (643 offline tests, 76 integration deselected); active LSP diagnostics over all seven changed Python paths — PASS with no findings; Compose rendering — PASS.
- `make integration` — PASS (76 integration tests plus repeat-seed and infrastructure recovery checks); `./scripts/verify.sh` — PASS with doctor, repository policy, locked sync, Ruff, formatting, strict mypy over 81 source files, 643 offline tests, and Compose rendering.
- BI-093 remains active and `in_progress`; backlog status and active task are unchanged. Next task: execute slice 5 only by updating operator/contracts documentation, removing superseded BI-093 limitations, running final closeout gates, and closing BI-093 without starting BI-094.

## 2026-10-16 — BI-093 slice 5 documentation and acceptance closeout

- Updated the core and shared-storage contracts with source-owned attempt boundaries, redacted Botocore normalization, exact terminal-event then `run_summary` ordering, handled exit `4`, confirmed-only counters, exception chaining, and retained-evidence recovery.
- Updated REST, file, and PostgreSQL snapshot operator contracts with their exact setup/operation/close boundaries, stable `storage_error`/`storage_failed`/`storage_state_failed` codes, nullable pre-validation facts, no duplicate `cli_error`, and unchanged durable manifest semantics.
- Updated README and recovery-demo guidance for the proven unavailable-S3 behavior across all three production commands; removed obsolete BI-093 gap and exit-5 wording while retaining the separate BI-094 response-stream deadline limitation.
- Changed no runtime code, dependency, object key, schema, retry budget, file atomicity, REST aggregate rule, or delivery guarantee in slice 5.
- Focused storage/API/file/snapshot/demo tests — PASS (176); active LSP diagnostics over all 37 changed Python paths — PASS with no findings; `make lint` — PASS; `make test` — PASS (643 offline tests, 76 integration deselected); Compose rendering — PASS.
- Isolated unavailable-S3 acceptance — PASS (3 commands); `make integration` — PASS (76 tests plus repeat-seed and infrastructure recovery); isolated clean-export — PASS in 26.97 seconds with a 2.04-second warm happy path, exact inventory, redaction, protected-state checks, and verified cleanup.
- `./scripts/verify.sh` — PASS with doctor, repository policy, locked sync, Ruff, formatting, strict mypy over 81 source files, 643 offline tests, and Compose rendering.
- Marked BI-093 done and selected BI-094 as the sole next atomic task. Next task: add deterministic slow-trickle tests, then enforce the REST total response deadline without changing retry, raw-preservation, or terminal-output contracts.
