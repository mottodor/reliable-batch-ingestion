# Project state

Updated: 2026-10-15

## Current stage

The core lab through BI-080 remains implemented, but BI-090 independently qualified its prior acceptance claim. The audit is complete: every specification, accepted-ADR, and completed-backlog row has a final evidence status; fresh offline, clean-export, integration, recovery, exact-inspection, redaction, and cleanup gates passed; and five requirement-specific findings remain actionable. BI-091 through BI-096 separate the ADR, repairs, and clean-commit attestation without reopening completed product tasks or adding Possible extensions. BI-091 is selected next because F02 changes manifest cardinality/key semantics and therefore requires an architectural decision before implementation.

```yaml
active_task_id: BI-091
last_completed_task_id: BI-090
```

`tasks/backlog.yaml` is canonical for task status and the active task. The value above is a handoff mirror checked by `scripts/check-repository-policy.sh`.

## Completed

- the product specification and repository-local agent workflow are recorded;
- BI-007 keeps only the implementation plan, state, and worklog at the `docs/` root and groups the remaining documentation under `specification/`, `contracts/`, `ingestion/`, `adr/`, and `plans/`;
- S3-compatible Garage `dxflrs/garage:v2.4.1` was selected in ADR 0001;
- the Python 3.12/uv foundation, quality gates, and Git repository exist;
- typed runtime configuration is loaded only through an explicit call and redacts secrets from representations;
- bounded streaming SHA-256 calculation is implemented;
- canonical logical inputs, deterministic `batch_id` and object keys, and unique physical `run_id` values are implemented;
- manifest schema version 1 defines all required fields, source-specific nullability, one-to-one payload cardinality, UTC serialization, statuses, transitions, and terminal states;
- the structured final run-summary contract contains every metric required by the product specification;
- the initial identifier, object-key, and manifest contracts are documented in `docs/contracts/core-contracts.md`;
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
- the complete BI-050 offline and local Compose acceptance matrix passes, so BI-050 is done and BI-060 is active;
- ADR 0002 selects whole-file atomicity: any required validation failure rejects the complete delivered file, publishes no accepted rows, preserves the original bytes, and uses recoverable `copy -> verify -> delete` movement without claiming an atomic S3 rename;
- the BI-060 test-first plan fixes supplier source/name/schema rules, content-based identity, deterministic landing/raw/archive/rejected keys, reason-bearing rejection keys, bounded diagnostics and validation, same-name versus renamed-content classification, raw-before-validation ordering, and restart convergence;
- BI-060 slice 1 implements strict admission-safe delivery descriptors, source/name/date/format inspection, checksum tokens, content-based identities, canonical payload names, and exact landing/raw/manifest/archive/rejected keys;
- stable rejection precedence, value-free bounded diagnostics, duplicate classes, ADR 0002 whole-file counters, secret-free terminal event serialization, and explicit file byte/spool/record/Parquet-batch/diagnostic configuration bounds are unit-tested without I/O;
- BI-060 slice 2 adds one whole-file validation result with no accepted-row subset, strict common supplier-price rules, incremental strict UTF-8 CSV, byte-bounded strict JSON, and exact-schema Parquet validation through configured `iter_batches()`;
- valid, empty, schema-invalid, mixed, malformed, checksum-mismatched, date-mismatched, type-invalid, business-invalid, record-limited, byte-limited, and diagnostic-limited cases are covered across the applicable formats; every mixed fixture reports exact `3/0/3` counters and validation restores the unchanged source stream position;
- BI-060 slice 3 adds exact-key inspection plus bounded context-owned S3 downloads that stream SHA-256, enforce declared and actual byte limits, close response bodies, and close seekable spools on every exit path;
- the file service downloads verified landing or raw evidence, reconciles the canonical raw/manifest pair, preserves raw before validation, probes every finite alternative disposition, converges movement, and writes a terminal manifest only after verified disposition completion;
- landing-only, payload-only, destination-only, raw-only, complete duplicate, both-after-delete-failure, manifest-only, conflicting-disposition, raw-write failure, move failure, and post-disposition/pre-manifest recovery paths are unit-tested under the sequential-writer contract;
- live Garage coverage verifies bounded reads and exact valid/rejected CSV bytes under raw and archive/rejected keys, landing absence, and strict succeeded/rejected manifest reconciliation;
- the file CLI now exposes strict `run` and `resume` forms, bounded local-file admission, immutable landing staging, optional expected-checksum handling, newline-delimited events/summaries, stable `0/2/3/4/5` exits, and explicit local/S3 resource cleanup;
- `make ingest-file` uses the ignored environment and delegates both staged and resumed deliveries to the same restart-safe service;
- live CSV, JSON, and Parquet coverage proves exact happy/rejected bytes, all planned rejection classes, ADR 0002 mixed-file counters, same-name and renamed-content duplicate classifications, unchanged original manifests/run IDs, zero replay raw bytes, and copy/delete interruption convergence through `resume`;
- `docs/ingestion/file-ingestion.md` documents the implemented admission, identity/key examples, filename/schema rules, bounds, whole-file semantics, rejection precedence, metrics, duplicate classes, recovery matrix, CLI exits, and sequential-writer limitation;
- README now includes tested file failure states and recovery actions, while the shared storage contract documents generic exact-key inspection and bounded spooled download;
- the complete BI-060 offline and local Compose acceptance matrix passes, so BI-060 is done and BI-070 is active;
- BI-070 has a detailed five-slice test-first plan fixing daily snapshot identity, read-only repeatable-read upper-watermark capture, server-side cursor chunking, deterministic Parquet payloads, exact reconciliation, replay-from-start recovery, CLI semantics, and live interruption evidence;
- BI-070 slice 1 implements the allow-listed `orders` daily/empty/non-empty resume descriptor, canonical UTC windows and upper tuple, deterministic versioned identity plus four-digit chunk keys, exact non-null Arrow schema and Parquet writer options, and strict no-coercion row validation;
- positive PostgreSQL connect/statement timeout and snapshot elapsed/chunk/count/byte/spool settings are explicit and cross-validated, while typed snapshot events, handled error codes, and metric adapters expose no DSN, SQL, row values, or exception text;
- BI-070 slice 2 opens one bounded PostgreSQL connection without hidden retries, applies read-only `REPEATABLE READ` characteristics before the explicit transaction, sets transaction-local statement timeout, captures or validates the daily upper descriptor, and extracts the seven explicit columns through one named cursor using only `fetchmany(chunk_rows)`;
- reader validation enforces exact row types, strict `(updated_at, order_id)` ordering, daily and inclusive-upper bounds, final count/watermark equality, monotonic deadlines, and cursor/transaction/connection cleanup without mutable checkpoint advancement;
- deterministic one-chunk serialization writes the exact Arrow schema as one uncompressed Parquet row group (including the zero-row chunk), applies fixed writer options, enforces row/serialized-byte/spool bounds, streams SHA-256, rewinds output, and closes in-memory or disk-spilled spools on every exit;
- BI-070 slice 3 adds one restart-safe service that validates/emits the descriptor before mutation, processes one reader chunk and spool at a time, reconciles exact deterministic keys, and commits verified raw Parquet before one succeeded manifest;
- absent, payload-only, complete, complete replay, manifest-only, malformed/link/status manifest, raw conflict, write failure, source-contract, overflow, deadline, and post-first-chunk connection failures have stable tested outcomes without checkpoints or failed manifests;
- replay starts from chunk 1, preserves complete manifests and original run IDs, repairs only payload-only manifests, reports exact counters/events, and closes each spool before the next fetch;
- focused Garage integration proves a three-chunk payload-only recovery and complete replay with exact Parquet bytes, checksums, Arrow schemas, rows, manifest inventory, zero replay payload bytes, and unchanged manifests;
- BI-070 slice 4 adds strict `run` and `resume` CLI forms, canonical empty/non-empty descriptor reconstruction, pre-client argument and configured-bound checks, compact NDJSON, stable `0/2/3/4/5` exits, explicit S3 cleanup, and `make ingest-postgres`;
- live seeded PostgreSQL extraction verifies ordered multi-chunk Parquet rows, exact schemas/manifests/inventory, and a complete replay with unchanged checksums, manifests, and original run IDs;
- live backend termination after one complete chunk proves replay-from-start recovery, preservation of prior evidence, and exclusion of a later row above the captured upper tuple;
- live payload-only repair, below-bound source-drift rejection, and bounded unavailable-S3 behavior retain inspectable evidence and return stable handled outcomes;
- `docs/ingestion/postgres-snapshot.md` and README document source/date semantics, exact columns and Parquet schema, descriptor identity and key examples, transaction/watermark behavior, all bounds and metrics, chunk manifests, CLI forms/exits, and tested recovery actions;
- the complete BI-070 offline and local Compose acceptance matrix passes, so BI-070 is done and BI-080 is active;
- BI-080 has an implementation-ready four-slice plan covering explicit local environment initialization, the four specification demo targets, exact inventory inspection, demo-only process failpoints, all mandatory failures, normal-command recovery, structured evidence, and an isolated clean-export walkthrough;
- BI-080 slice 1 adds the exact public/supporting Make surface, an eleven-scenario allow-list, path-safe state/report contracts, deterministic compact `demo_summary` serialization, and a bounded process-group supervisor that classifies expected failures and terminates/reaps timed-out children;
- explicit local environment initialization now creates or updates only recognized placeholders with cryptographic values, preflights a mode-`0600` temporary file before atomic publication, refuses tracked/symlink/partial/unknown-placeholder targets, never prints generated values, and leaves initialized content byte-stable on rerun;
- BI-080 slice 2 replaces the public skeleton with bounded adapters over the existing API, file, and PostgreSQL CLIs; the one-time 429 demo uses a private child over the existing API service seam and adds no production fault flag;
- `demo-happy-path` creates or safely replays REST, archived supplier CSV, and bounded snapshot evidence; its measured warm ingestion path remains below the three-minute acceptance budget;
- `demo-duplicate` proves zero canonical raw writes and unchanged key/length/checksum/manifest-byte inventory across API/snapshot replay, same-name file redelivery, and renamed identical content;
- `demo-api-retry` resets one isolated fault key, proves one 429 with the exact valid `Retry-After` delay and one retry, and retains no 429 body; `demo-bad-file` retains exact schema-invalid and malformed UTF-8 bytes under raw/rejected keys before corrected delivery;
- project-scoped state records contain only strict non-secret descriptors, while NDJSON reports relay production events, add exact state assertions, and finish with one closed `demo_summary`;
- read-only inspection paginates only calculated prefixes, requires exact key sets and strict manifest linkage/status, streams bytes for independent SHA-256 without using `ETag`, checks absent landing/forbidden dispositions, and validates snapshot Parquet schema, rows, order, count, and upper tuple;
- focused live validation uses a unique test-owned bucket and artifact project, executes all four Make targets plus `demo-inspect`, scans output for credentials/payload values, and removes only isolated resources;
- BI-080 slice 3 adds private one-shot storage/client failpoints that terminate only after verified raw persistence, after a complete API page pair, or after verified file copy and immediately before landing deletion; production CLIs and `AppConfig` expose no fault selector;
- the private PostgreSQL worker captures the reader backend PID, terminates it from a second bounded connection after chunk 1 is complete, and records the canonical resume descriptor without introducing a checkpoint;
- `demo-recovery` now executes all eleven allow-listed rows: 429, permanent 500, malformed JSON, API-page death, both file redelivery classes, schema rejection, file-move death, PostgreSQL disconnect, isolated unreachable S3, and payload-before-manifest death;
- the parent requires expected crash/handled exits plus exact intermediate state before invoking normal API, file `resume`, or snapshot `resume`; malformed JSON instead re-surfaces unchanged terminal evidence, and completed process scenarios replay safely without recreating a fault;
- isolated live parameterized coverage proves every recovery scenario, final exact inventory, bounded subprocesses, secret/payload-value redaction, and test-owned bucket cleanup;
- BI-080 slice 4 adds a bounded clean-export harness that copies only Git-reported tracked/unignored regular files, rejects local/generated state and unsafe paths, creates an isolated owner-only environment, and executes locked setup, doctor, Compose rendering, seed, all four demos, offline tests, and final exact inspection;
- clean-export cleanup uses exact destructive confirmation only for its unique project, requires its labeled containers/networks/volumes to be absent, and proves protected default-project volume fingerprints are unchanged;
- `docs/ingestion/recovery-demos.md` and README document every allow-listed failure with exact trigger/exit, intermediate state, normal recovery or terminal action, final state, artifacts, timing boundary, cleanup, and honest limitations;
- the accepted clean-export walkthrough measured 11.11 seconds of setup, a 2.59-second warm happy path, and 27.40 seconds through final inspection on the validation workstation;
- the complete BI-080 unit, integration, clean-export, repository-policy, and verification acceptance matrix passes, so BI-080 is done and the core product scope is complete;
- BI-090 has an implementation-ready four-slice audit plan fixing the independence boundary, exact baseline provenance, full requirement traceability, adversarial static review, isolated fresh validation, finding schema, and separate maintenance-task handoff;
- BI-090 slice 1 recorded the exact `dirty_worktree` baseline at `HEAD` `a1ef506fd13545467c6caf6c04eb803ab2c77461`, a 158-path byte inventory, tool versions, full status, and all paths/hashes that diverged from the commit;
- the audit report maps every in-scope specification requirement, accepted-ADR obligation, and completed BI-000–BI-080 acceptance criterion to implementation, tests, documentation, and final PASS/FAIL evidence;
- BI-090-F01 confirms that the frozen `HEAD` lacks completed snapshot/demo/clean-export files present only in the dirty export, so that baseline cannot be described as a clean committed checkout;
- BI-090-F02 confirms that page manifests plus an ephemeral summary do not satisfy the explicit persisted logical-run-manifest requirement;
- BI-090-F03 confirms that raw backend S3 failures can bypass source terminal events and the required final `RunSummary`;
- BI-090-F04 confirms that buffered HTTP response iteration can consume a continuously trickling response beyond the configured total elapsed budget;
- BI-090-F05 confirms that local preflight accepts incomplete application configuration despite the broader README validation claim;
- BI-090 freshly passed policy/backlog synchronization, Ruff/format/mypy, 521 offline tests, Compose rendering, the isolated clean-export walkthrough, 66 integration tests, repeat-seed marker verification, and bounded infrastructure persistence/reset recovery;
- the clean export completed in 24.44 seconds with a 1.73-second warm happy path and exact inspected counts of 6 raw, 6 manifests, 1 archive, 2 rejected, and 0 landing objects;
- live audit state used unique project/bucket/ports and owner-only generated credentials; cleanup removed its containers, networks, volumes, and uniquely owned images, deleted the credential file, and preserved all three default-project volume fingerprints;
- BI-090 slice 4 finalized all five findings, closed every traceability row, created BI-091 through BI-096 as bounded follow-up, marked the audit done, and changed no product code, tests, dependency, contract, README behavior, or ADR.

## Not completed

- BI-091: decide the REST aggregate logical-run manifest contract in an accepted ADR before implementation;
- BI-092: implement and reconcile that accepted aggregate manifest contract;
- BI-093: emit terminal events and final summaries for backend S3 failures;
- BI-094: enforce the REST total monotonic deadline during response streaming;
- BI-095: align local initialization/preflight with full application configuration;
- BI-096: attest the completed source set from an exact clean committed checkout.

## Known limitations and risks

- Garage has no bucket versioning, and conditional create with `If-None-Match` is not a contract foundation.
- Concurrent ingestion writers are outside the core contract.
- Source adapters must include every value that changes selected logical data in `BatchIdentity.parameters`; omission would cause an identifier collision.
- Ignored `.env` files created before BI-070 slice 1 must be refreshed with the committed HTTP, `FILE_*`, PostgreSQL timeout, and `SNAPSHOT_*` settings before integration targets or ingestion CLIs can load complete configuration.
- BI-090-F01–F05 are confirmed and not repaired; the exact failed traceability rows and follow-up task mapping are authoritative in `docs/audits/BI-090.md`.

## Latest validation

2026-10-15, BI-090 final acceptance audit:

- doctor, repository policy, backlog JSON Schema/active-state validation, `git diff --check`, `make lint`, `make test` (521 passed; 66 integration deselected), and Compose rendering — PASS;
- isolated clean export — PASS: all four public demos, exact inspection, offline tests, 1.73-second warm happy path, 24.44-second walkthrough, and internal cleanup/protected comparison;
- isolated `make integration` — PASS: all 66 integration tests, 4 repeat-seed marker checks, and bounded persistence/destructive-reset recovery;
- ownership-scoped cleanup, protected default-project fingerprint comparison, evidence secret/payload scan, and credential-file removal — PASS;
- the first auxiliary post-cleanup assertion — FAIL because it assumed ownership of an unfingerprinted reusable fixed-project image; the corrected ownership-scoped check passed, and both outcomes are retained rather than hidden;
- all requirement matrices now contain only final PASS/FAIL outcomes, and F01–F05 map to BI-091 through BI-096;
- slice-4 backlog schema, repository policy, `git diff --check`, and final `./scripts/verify.sh` closeout results are recorded in the audit report.

## Next atomic step

Execute BI-091 only: write and accept the ADR that defines deterministic aggregate REST logical-run manifest cardinality, keys, page linkage, terminal/reconciliation states, replay, and compatibility; do not implement BI-092 behavior in the same task.
