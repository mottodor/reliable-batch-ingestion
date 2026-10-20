# AGENTS.md — coding-agent operating contract

This repository is the source of truth for the Reliable Batch Ingestion Lab. Chat history and an individual agent session are not project state.

## 1. Required session startup

Before changing code or documentation, the agent must:

1. Read `docs/START_HERE.md`.
2. Read `docs/STATE.md`.
3. Read `docs/specification/batch-ingestion.md`, excluding **Possible extensions** from the current scope.
4. Read `tasks/backlog.yaml` and confirm that its `active_task_id` matches `docs/STATE.md`. If it names a task, locate that task; if it is `null`, confirm that every recorded task is done before creating a newly authorized atomic task.
5. Read the relevant accepted ADRs from the index in `docs/adr/README.md`.
6. Run `./scripts/doctor.sh`.
7. State a short plan and the verifiable acceptance criteria for the task being performed.

If the user's mutation request differs from the active backlog task, follow the explicit request. Keep the work atomic and, before finishing, represent completed project changes in the backlog and synchronize `docs/STATE.md`. Read-only reviews, questions, and analysis that do not change project state do not require backlog mutation.

## 2. Authority and document responsibilities

When requirements conflict, use this precedence:

1. the user's explicit request in the current task;
2. `AGENTS.md`;
3. accepted ADRs;
4. `docs/specification/batch-ingestion.md`, excluding **Possible extensions**;
5. `tasks/backlog.yaml`;
6. existing implementation and tests.

Document roles are intentionally distinct:

- `docs/specification/batch-ingestion.md` defines product scope and required behavior;
- accepted ADRs define binding architectural decisions;
- `tasks/backlog.yaml` is the canonical machine-readable task list, task status, and active-task or terminal-state marker;
- `active_task_id` names the task selected for the next or current implementation: its status may be `todo` before work starts or `in_progress` when durable partial work exists; `null` means no task is active and is valid only when every recorded task is `done`;
- `docs/STATE.md` is the concise handoff snapshot and must mirror the backlog's active task or terminal state;
- after a terminal state, an explicitly authorized improvement or correction must be added as a new atomic backlog task and selected as `active_task_id` before implementation;
- `docs/IMPLEMENTATION_PLAN.md` explains task ordering but does not duplicate status;
- `docs/WORKLOG.md` is append-only historical evidence;
- `README.md` describes only commands and behavior that currently exist;
- `docs/START_HERE.md` is navigation, not a second operating contract.

Do not silently change an architectural decision. Replacing a service, changing the manifest or object-key contract, or strengthening delivery semantics requires an ADR first.

## 3. Fixed architecture

- Python: 3.12.
- Dependency management: `uv`; `uv.lock` must remain committed and reproducible.
- PostgreSQL is a snapshot source, not the ingestion service database.
- Object storage is S3-compatible Garage, using the exact image `dxflrs/garage:v2.4.1`.
- Domain modules access object storage only through the standard S3 API (`boto3`); Garage-specific administration must remain in infrastructure code.
- HTTP client: `httpx`.
- PostgreSQL client: Psycopg 3.
- Parquet: `pyarrow`, only where Parquet is required.
- Local infrastructure: Docker Compose.
- Orchestrators, Kafka, CDC, lakehouse components, BI, and **Possible extensions** are out of scope.

Do not use floating Docker tags such as `latest`, `stable`, or `edge`.

## 4. Reliability invariants

- Preserve the raw payload before transformation and never mutate it after a successful write.
- Never place secrets, Authorization headers, or password-bearing DSNs in a manifest.
- Derive object keys, `batch_id`, and checksums deterministically from a documented contract.
- Use `run_id` for a physical attempt; it must not replace the logical `batch_id`.
- A repeated run must not create uncontrolled duplicates.
- Bound every retry by an attempt count and/or total elapsed time.
- Do not retry permanent HTTP 4xx responses; for 429, honor valid `Retry-After` values within the retry budget.
- Never load an entire snapshot into memory, and never use `SELECT *` for snapshot extraction.
- Backfill must use the same primitives as a normal run, not a separate implementation.
- Move an S3 object with `copy -> verify -> delete`; do not claim an atomic rename.
- Do not claim exactly-once delivery. Describe the actual semantics: deterministic keys, idempotent writes, and safe redelivery.

Garage constraints:

- bucket versioning is unavailable;
- conditional `PutObject` with `If-None-Match` is not a contract foundation;
- the application and tests enforce raw immutability;
- concurrent writers are unsafe unless a separate coordination mechanism is introduced.

## 5. Module boundaries

- `src/batch_lab/api/` — REST ingestion and retry classification.
- `src/batch_lab/files/` — file intake, validation, and archive/rejected flow.
- `src/batch_lab/postgres_snapshot/` — bounded snapshot extraction and watermarks.
- `src/batch_lab/manifests/` — manifest contract and statuses.
- Shared S3 primitives belong in a separate storage module; do not duplicate them in ingestion modules.
- `mock_api/` contains only the deterministic mock source and fault injection; ingestion business logic does not belong there.
- `postgres/init/` contains versioned schema and seed SQL for the source database.

Network, PostgreSQL, and S3 connections at import time are forbidden.

## 6. Task implementation rules

1. Work on one atomic backlog task at a time.
2. Capture the contract or failure mode in a test first, then add the minimal implementation.
3. Do not expand scope opportunistically.
4. Use current Context7 documentation for library-specific APIs when the runtime provides it. Otherwise, use official version-matched documentation and cite that fallback in the final report.
5. Add a runtime dependency only with justification, then update `uv.lock`.
6. Pass configuration through environment variables and configuration objects; never commit secrets.
7. Use structured logs with applicable `source_date`, `batch_id`, `run_id`, and `attempt` fields.
8. After changing Python files, run LSP diagnostics and relevant tests. If the runtime has no LSP tool, run `make lint` and report LSP diagnostics as `NOT RUN`.
9. Before finishing, run `./scripts/verify.sh`; run integration checks separately when the task requires them.
10. Update documentation in the same change as behavior.

## 7. Test strategy

- Unit tests must not access the public network or real external services.
- Integration tests use only local Compose services and carry the `integration` marker.
- Fault injection must be deterministic, with explicit scenario and seed values.
- Recovery tests inspect object-storage state before and after restart.
- The application verifies checksums; never treat `ETag` as a SHA-256 digest.
- All waits must be bounded; arbitrary long sleeps are forbidden.

Minimum local gates:

```bash
make lint
make test
```

Full repository gate:

```bash
make verify
```

When Compose exists and the task requires infrastructure:

```bash
docker compose config
make integration
```

Never report a command as passing unless it was run successfully.

## 8. Definition of Done for a task

A task is complete only when:

- its acceptance criteria are met;
- critical behavior and failure paths are covered by tests;
- `make lint` and relevant tests pass;
- configuration is reproducible and contains no secrets;
- documentation is current;
- `docs/STATE.md` reflects the current state and one next atomic step;
- `docs/WORKLOG.md` has a concise entry;
- the task status in `tasks/backlog.yaml` is updated.

## 9. Session closeout

The final report must contain:

- **Implemented** — only work actually completed;
- **Changed files**;
- **Validation** — commands and PASS/FAIL/NOT RUN results;
- **Limitations/risks**;
- **Next step** — exactly one recommended atomic task.

## 10. Prohibited actions

- committing `.env`, passwords, access keys, or tokens;
- replacing Garage without a new ADR;
- using `SELECT *` in snapshot extraction;
- loading a whole snapshot or large file into memory without a bounded contract;
- implementing unbounded retries;
- swallowing exceptions;
- deleting invalid input without recording a rejection reason;
- making a destructive reset implicit;
- disabling tests to obtain a green result;
- implementing **Possible extensions** before the core version is complete.
