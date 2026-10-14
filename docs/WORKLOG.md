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
