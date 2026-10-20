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

## Maintenance — REST logical-run manifest decision (`BI-091`)

Detailed execution plan: [`docs/plans/BI-091.md`](plans/BI-091.md).

- resolve `BI-090-F02` through an accepted ADR before implementation;
- compare bounded aggregate-manifest alternatives against immutable page evidence and replay requirements;
- define exact aggregate cardinality, deterministic key, separate bounded schema, page linkage, completion, reconciliation, and compatibility semantics;
- preserve raw pages, schema-version-1 page manifests, standard-S3 access, and sequential-writer/no-exactly-once limitations;
- refine BI-092 against the accepted decision without changing runtime behavior in BI-091.

Outcome: BI-092 has one binding, implementation-ready aggregate logical-run manifest contract, while existing page evidence and current runtime behavior remain unchanged during the decision task.

## Maintenance — REST logical-run manifest implementation (`BI-092`)

Detailed execution plan: [`docs/plans/BI-092.md`](plans/BI-092.md).

- implement the separate strict aggregate model, canonical parser/serializer, and deterministic batch-level key from ADR 0003;
- verify exact raw/page-manifest/aggregate bytes under separate finite bounds and reconcile every accepted state without prefix inference;
- publish and verify one immutable success-only aggregate after all page evidence and before terminal success output;
- converge legacy page-only and uncertain-write recovery while preserving mixed page runs, existing bytes, and metric meanings;
- require the aggregate in successful backfill dates, Garage coverage, exact demo inventory, and operator documentation.

Outcome: every successful REST logical batch has one durable, verified completion record while page evidence and honest sequential-redelivery semantics remain unchanged.

## Maintenance — backend-storage terminal output (`BI-093`)

Detailed execution plan: [`docs/plans/BI-093.md`](plans/BI-093.md).

- translate raw Botocore request failures once at the shared storage boundary while preserving cause chains and existing missing/conflict/verification classifications;
- own S3 session setup and final output inside each source attempt after stable trace identifiers exist;
- emit exactly one source terminal failure event followed by exactly one final `RunSummary` for REST, file, and snapshot backend failures;
- move file landing staging under the same attempt boundary without changing raw-before-validation or whole-file semantics;
- retain confirmed-only counters, stable handled exits, existing object evidence, bounded retries, and normal reconciliation/recovery;
- prove setup/inspection/read/copy/delete/write failures offline and exercise an isolated unreachable endpoint for all three sources.

Outcome: temporary S3 failure remains bounded and restart-safe while every attempted source ingestion retains complete, secret-free terminal observability.

## Maintenance — REST total response deadline (`BI-094`)

Detailed execution plan: [`docs/plans/BI-094.md`](plans/BI-094.md).

- consume successful response bodies as unbuffered HTTPX transport chunks so fixed-size iterator buffering cannot hide elapsed time;
- check the one-page monotonic budget immediately before and after every transport receive and before accepting end-of-stream;
- retain the existing response-size ceiling, per-operation timeouts, retry classifications, raw-before-parse ordering, and resource ownership;
- prove slow-trickle termination with an in-memory fake clock and stream, including response/spool cleanup, no partial S3 write, and exact terminal output;
- rerun normal chunked, 429, retry-budget, malformed-body, backfill, live API, and clean-export regressions.

Outcome: one successful-response body cannot be kept alive indefinitely by below-read-timeout byte trickles, while existing REST evidence and recovery contracts remain unchanged.

## Maintenance — full local configuration preflight (`BI-095`)

Detailed execution plan: [`docs/plans/BI-095.md`](plans/BI-095.md).

- delegate application-level local preflight to the complete redacting `AppConfig.from_env()` validation path;
- retain non-overlapping Compose-only port, Garage RPC-secret, and recognized-placeholder checks;
- reject missing, malformed, protocol-invalid, and cross-field-invalid PostgreSQL, S3, HTTP, file, snapshot, and logging settings before reporting success;
- preserve owner-only atomic publication, symlink/tracked-file refusal, placeholder limits, no partial credential rotation, redaction, and byte-stable valid reruns;
- prove invalid generated content is never published, then align Make/README wording and rerun offline, integration, and clean-export acceptance.

Outcome: local initialization and preflight cannot report success for a dotenv mapping that the application's own configuration loader rejects, without adding connectivity checks or weakening credential-file safety.

## Maintenance — clean-commit core reproducibility attestation (`BI-096`)

Detailed execution plan: [`docs/plans/BI-096.md`](plans/BI-096.md).

- select one immutable full commit ID and create a separate detached checkout without copying developer-worktree files;
- record exact commit/tree provenance, empty initial status, a canonical source-inventory digest, and closure of every path named by `BI-090-F01`;
- run repository gates and an isolated integration suite from the candidate checkout;
- run the bounded clean-export walkthrough through locked setup, seed, four demos, offline tests, and exact inspection;
- prove exact cleanup, unchanged protected-resource fingerprints, and continued historical classification of the original BI-090 dirty baseline;
- publish only documentation and evidence, with no product behavior, dependency, contract, ADR, or delivery-claim change.

Outcome: the completed core source set is reproducible from one exact clean committed checkout, while the original BI-090 baseline remains truthfully classified as a dirty worktree.

## Maintenance — session entry-point relocation (`BI-097`)

- move the repository navigation entry point from the root to `docs/START_HERE.md`;
- update active agent guidance, README navigation, and path-sensitive policy checks;
- preserve historical worklog wording and leave product behavior and architectural contracts unchanged.

Outcome: session navigation is colocated with project documentation and the repository gate prevents the legacy root path from returning.

## Maintenance — cohesive Git history rewrite (`BI-099`)

Detailed execution plan: [`docs/plans/BI-099.md`](plans/BI-099.md).

- preserve the exact original history through a named archive reference;
- split the largest mixed commits at task and independently reviewable behavior boundaries;
- normalize rewritten subjects and add concise purpose, boundary, and validation context;
- retain historical audit text and exact cited objects rather than relabeling rewritten commits as audited candidates;
- prove byte-identical pre-task trees, linear topology, and passing repository gates.

Outcome: `main` presents reviewable, task-focused changes while the original audited history remains reachable and product behavior is unchanged.

## Maintenance — remove archived pre-rewrite history (`BI-100`)

Detailed execution plan: [`docs/plans/BI-100.md`](plans/BI-100.md).

- apply the owner's explicit decision that no external consumer depends on the old tip;
- delete the pre-BI-099 archive tag and expire local reflogs;
- prune unreachable old-history objects immediately;
- preserve rewritten `main`, current product behavior, and immutable historical audit text;
- prove old unique SHA absence and current-history integrity before terminal closeout.

Outcome: only the cohesive rewritten history remains in the local Git object database, while documentation truthfully records the superseding retention decision.

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
