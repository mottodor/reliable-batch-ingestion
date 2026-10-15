# Implementation plan

This plan decomposes `docs/specification/batch-ingestion.md` into small vertical tasks. **Possible extensions** are deliberately excluded.

Exact task status, scope, dependencies, and acceptance criteria live only in `tasks/backlog.yaml`.

## Ordering principle

Implement stable pure contracts first, then infrastructure and the shared storage adapter, followed by individual ingestion paths. Backfill and recovery reuse already tested primitives.

## Gate 0 — Foundation (`BI-000`)

- coding-agent rules;
- reproducible Python 3.12/uv toolchain;
- task/state/worklog protocol;
- Garage ADR;
- baseline lint and test gates.

Outcome: a new session can recover project state using repository files only.

## Maintenance — Agent scaffolding audit (`BI-005`)

- independent consistency review;
- English-only repository-owned documentation;
- reduced duplication between navigation, policy, state, and planning documents;
- automated policy checks for language and active-task synchronization.

Outcome: the agent workflow has explicit document ownership and drift checks.

## Maintenance — Git initialization (`BI-006`)

- initialize the repository on the `main` branch;
- preserve ignore rules for secrets and local/generated state;
- confirm `BI-010` as the first product implementation task.

Outcome: version control is ready, while product implementation remains at the planned `BI-010` boundary.

## Maintenance — Documentation information architecture (`BI-007`)

- keep only the implementation plan, current state, and append-only worklog as root-level `docs/` files;
- group the product specification, shared contracts, and source-specific ingestion documentation by responsibility;
- update active links, path-sensitive checks, and future documentation destinations without rewriting historical worklog entries;
- retain BI-070 as the active product task.

Outcome: documentation has a scalable responsibility-based layout without changing product or architecture contracts.

## Gate 1 — Core contracts (`BI-010`)

- environment configuration without import-time I/O;
- streaming SHA-256 checksum;
- deterministic `batch_id` and object keys;
- unique attempt `run_id`;
- manifest schema and status transitions;
- JSON structured-summary contract.

Outcome: unit tests cover the key semantics without external services.

## Gate 2 — Local infrastructure (`BI-020`)

Detailed execution plan: [`docs/plans/BI-020.md`](plans/BI-020.md).

- Compose with a pinned PostgreSQL image and `dxflrs/garage:v2.4.1`;
- persistent volumes;
- health and readiness checks without arbitrary sleeps;
- idempotent bucket/bootstrap flow;
- `make up/down/reset`, with reset explicitly marked destructive.

Outcome: a clean clone starts local dependencies and preserves data across restarts.

## Gate 3 — Shared S3 boundary (`BI-030`)

Detailed execution plan: [`docs/plans/BI-030.md`](plans/BI-030.md).

- path-style `boto3` client;
- bounded retry policy;
- immutable-write contract using a deterministic key and checksum verification;
- `copy -> verify -> delete` primitive;
- payload-without-manifest recovery;
- S3 integration tests against Garage.

Outcome: ingestion modules contain no duplicated S3 code.

## Gate 4 — Deterministic sources (`BI-040`)

Detailed execution plan: [`docs/plans/BI-040.md`](plans/BI-040.md).

- PostgreSQL schema and deterministic seed;
- mock REST API with pagination, 429, 500, and malformed JSON scenarios;
- CSV/JSON/Parquet fixtures, including invalid and duplicate cases.

Outcome: every failure scenario is reproducible through an explicit command and seed.

## Gate 5 — REST ingestion (`BI-050`)

Detailed execution plan: [`docs/plans/BI-050.md`](plans/BI-050.md).

- timeouts and bounded exponential backoff;
- `Retry-After` handling for 429;
- terminal 4xx classification;
- persistence of every raw page;
- resume after a partial run;
- manifests and date-range backfill.

Outcome: integration tests cover happy path, retries, terminal failure, malformed responses, and repeated backfill.

## Gate 6 — File ingestion (`BI-060`)

Detailed execution plan: [`docs/plans/BI-060.md`](plans/BI-060.md).

- source/name/checksum/schema/required-field/type validation;
- whole-file rejection under ADR 0002 with stable machine-readable reasons;
- deterministic landing/raw/archive/rejected keys and duplicate classification;
- `landing -> archive | rejected` through verified copy/delete movement;
- recovery between raw preservation, disposition movement, and manifest write.

Outcome: valid, invalid, empty, mixed, and duplicate fixtures have a verifiable final state.

## Gate 7 — PostgreSQL snapshot (`BI-070`)

Detailed execution plan: [`docs/plans/BI-070.md`](plans/BI-070.md).

- explicit columns;
- server-side cursor or bounded keyset pages;
- consistent upper bound and watermark;
- chunked output without retaining the full snapshot in memory;
- restart after connection interruption.

Outcome: snapshot and restart integration tests pass against seeded PostgreSQL.

## Gate 8 — Recovery and demos (`BI-080`)

Detailed execution plan: [`docs/plans/BI-080.md`](plans/BI-080.md).

- demo-only bounded process faults after payload/before manifest, after API pages/PostgreSQL chunks, and during file movement;
- deterministic 429, permanent 500, malformed body, PostgreSQL disconnect, and unavailable-S3 exercises;
- normal production reconciliation/resume commands plus exact read-only S3 inventory evidence;
- structured ingestion and demo summaries;
- the four Make targets from the demonstration scenario;
- explicit ignored environment initialization and an isolated clean-export walkthrough;
- every mandatory failure documented with expected state, recovery action, and honest limitations.

Outcome: the complete specification scenario is reproducible from a clean repository export within bounded time and without hidden local state.

## Post-completion acceptance audit (`BI-090`)

Detailed execution plan: [`docs/plans/BI-090.md`](plans/BI-090.md).

- freeze and classify the exact audited Git/worktree baseline;
- derive fresh traceability from the in-scope specification, accepted ADRs, and backlog acceptance criteria;
- adversarially review reliability invariants, module boundaries, failure handling, cleanup, and operator claims;
- rerun bounded offline, isolated Compose, recovery, clean-export, and verification evidence;
- publish only actionable prioritized findings with exact evidence, or explicitly record no findings;
- defer every repair to a separately scoped maintenance task and keep Possible extensions excluded.

Outcome: the completed core has an independent, reproducible acceptance record without mixing audit and implementation work.

## Common quality gates

For every gate:

```bash
make lint
make test
```

For tasks involving infrastructure:

```bash
docker compose config
make integration
```

Do not advance to the next gate while required checks fail or semantic changes remain undocumented.
