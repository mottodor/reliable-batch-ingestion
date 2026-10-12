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
