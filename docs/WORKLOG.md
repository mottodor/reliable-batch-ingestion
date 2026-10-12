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
