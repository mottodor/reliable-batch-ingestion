# Project state

Updated: 2026-10-20

## Current stage

The core lab through BI-080 is implemented, and BI-090 independently qualified its acceptance claim. All five bounded audit follow-ups are complete. BI-097 moved the session navigation entry point, BI-098 restructured the learner README, and BI-099 replaced the largest mixed Git commits with smaller task-focused series. After the project owner confirmed that no external consumer depends on the old tip, BI-100 removed the temporary archive tag, expired local reflogs, and pruned unreachable pre-rewrite objects. Only rewritten `main` remains locally; historical audit/worklog SHA strings are retained as immutable evidence text but no longer resolve when unique to the deleted history. Product behavior, tests, dependencies, contracts, and ADRs are unchanged. Every recorded task is done and no task is active.

```yaml
active_task_id: null
last_completed_task_id: BI-100
```

`tasks/backlog.yaml` is canonical for task status and the active or terminal state. The value above is a handoff mirror checked by `scripts/check-repository-policy.sh`; `null` is valid only while every recorded task is `done`.

## Completed

- the product specification and repository-local agent workflow are recorded;
- BI-007 established the responsibility-based documentation layout, and BI-097 later colocated the session navigation entry point at `docs/START_HERE.md`; specification, contract, ingestion, ADR, and plan documents remain grouped by responsibility;
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
- BI-090 slice 4 finalized all five findings, closed every traceability row, created BI-091 through BI-096 as bounded follow-up, marked the audit done, and changed no product code, tests, dependency, contract, README behavior, or ADR;
- BI-091 has an implementation-ready three-slice decision plan covering the current gap, bounded alternatives, preferred separate aggregate schema/key, exact page linkage, completion ordering, reconciliation/replay, legacy compatibility, BI-092 handoff, and a strict no-runtime-change boundary;
- ADR 0003 accepts exactly one success-only API logical-run manifest at a deterministic batch key, with a separate strict schema version, canonical JSON, an 8 MiB bound, checksummed ordered page evidence, write-before-success ordering, exact reconciliation/replay states, legacy page-only upgrade, and unchanged page evidence and delivery limitations;
- BI-091 refined BI-092 directly against ADR 0003, closed the decision-only task, and selected implementation next without changing Python, tests, dependencies, current-behavior documentation, README claims, or stored evidence;
- BI-092 has an implementation-ready five-slice test-first plan covering the pure aggregate model/key/parser, exact streamed evidence, two-phase reconciliation, bounded uncertain writes, service publication ordering, legacy/mixed-run replay, Garage/backfill/demo inventory, documentation, and closeout gates;
- BI-092 slice 1 adds frozen API aggregate/page models, exact ADR key derivation for both sources, strict identity and page-key rederivation, compact canonical UTF-8 bytes, nested duplicate-member rejection, immutable identity parameters, gap-free pagination/count checks, and the 9,999-page, 999,900-record, signed-64-bit payload-length, and separate 8 MiB aggregate bounds;
- the pure aggregate contract has 53 focused tests, including the exact 6,098,762-byte maximum-width 9,999-page shipping manifest, while existing page-manifest schema/runtime behavior and storage contents remain unchanged;
- BI-092 slice 2 adds API-owned exact-key reconciliation over the existing bounded storage spool, with actual-byte SHA-256 for aggregate, raw-page, and exact canonical page-manifest evidence and no ETag or prefix-list dependency;
- absent, incomplete, failed, eligible, complete, corrupt, conflicting, orphaned, and uncertain-write outcomes are typed and covered, while present invalid completion claims remain unchanged and block publication;
- aggregate publication now requires all candidate page evidence, freezes one canonical byte sequence, makes at most two immutable-write invocations, rewinds on retry, accepts only the same fully verified bytes after uncertainty, and retains different present bytes as conflict;
- slice 2 changes no shared storage implementation, REST service success ordering, CLI, backfill, demo inventory, Garage evidence, or current-behavior operator documentation;
- BI-092 slice 3 wires aggregate preflight before HTTP mutation, exact page-evidence collection after every succeeded page pair, aggregate publication/reuse, and final full reconciliation before terminal success and `RunSummary`;
- successful results expose the verified aggregate key separately while raw-only `bytes_written`, reused-page-only `duplicate_count`, CLI arguments, and backfill delegation remain unchanged;
- legacy page-only replay creates only the aggregate, mixed page-manifest run IDs remain byte-bound, complete replay preserves all raw/page-manifest/aggregate bytes and first-completion facts, and corrupt/conflicting/orphaned claims block fetch and mutation with stable codes;
- aggregate publication uncertainty retains complete page evidence, emits the existing failure event/summary shapes, and never claims success;
- BI-092 slice 4 proves first aggregate creation and byte-stable replay for both API sources against Garage, plus final-page interruption, legacy page-only upgrade, mixed-run evidence, and write-then-raise convergence through production exact-key reconciliation;
- live malformed, noncanonical, conflicting, and orphaned aggregate claims block mutation and remain byte-stable, while repeated/resumed backfill requires one aggregate for each successful date and fails fast before later dates on aggregate conflict;
- demo inventory now strictly parses each successful API aggregate, cross-checks its raw and page-manifest checksums against independently streamed evidence, counts it as a manifest but not raw data, and requires no aggregate for a failed API batch;
- recovery-demo exact counts and the isolated clean-export contract include aggregate manifests; the measured clean export contains 6 raw and 8 manifest objects while preserving one archive, two rejected objects, and zero landing objects;
- BI-092 slice 5 updates the core, REST/backfill, recovery-demo, and README contracts with exact aggregate keys, cardinality, fields/bounds, publication order, reconciliation/recovery, legacy and mixed-run replay, unchanged metrics, aggregate-required backfill/inspection, measured inventory, and honest standard-S3 sequential-redelivery semantics;
- the complete BI-092 focused, offline, integration, infrastructure-recovery, clean-export, LSP, policy, and verification matrix passes, so BI-092 is done and BI-093 is selected;
- BI-093 has an implementation-ready five-slice plan covering shared Botocore failure translation, source-owned S3 sessions, exact terminal cardinality, confirmed-only metrics, file staging ownership, snapshot descriptor-before-storage ordering, all backend operation classes, isolated unavailable-S3 acceptance, documentation, and closeout gates;
- BI-093 slice 1 translates known Botocore construction/request/stream/close failures to redacted `StorageBackendError` with exact cause chaining, adds context-owned S3 sessions and one-shot terminal-pair emission, and defines a strict file `failed` event plus typed handled error without wiring any source service;
- focused slice-1 tests preserve missing/conflict classifications, prove body/spool/client cleanup and no backend-text disclosure, reject mixed file business/system fields, and exercise the terminal guard for REST, file, and snapshot event forms;
- BI-093 slice 2 moves REST S3 session ownership inside the stable logical attempt, delays terminal output until clean session exit, and maps setup/close plus aggregate/page inspection, read, and write backend failures to one redacted `storage_error` event followed by one final summary;
- table-driven REST tests prove direct backend cause chaining, confirmed-only metrics, exact retained evidence at nine failure points, no success or duplicate terminal output, and fail-fast backfill without a duplicate date summary;
- the REST CLI now delegates S3 lifecycle to the source service, returns handled exit `4` for attempted storage failures, and retains generic exit `5` for non-storage HTTP runtime setup/cleanup failures;
- BI-093 slice 3 moves landing staging and S3-session ownership into the file-service attempt after bounded local admission, delaying terminal output until clean session exit;
- table-driven file tests cover setup/close, staging, landing/raw download, reconciliation, inspection, raw/manifest write, and disposition copy/delete failures with one redacted `failed` event plus one final summary, direct cause chaining, confirmed-only counters, nullable pre-validation facts, and exact retained object state;
- file storage-state conflicts now use the same typed terminal path with `storage_state_failed`, while arbitrary non-storage defects remain runtime failures and accepted/rejected/duplicate/whole-file/recovery behavior remains unchanged;
- the file CLI delegates storage lifecycle and staging to the service, returns handled exit `4` without a duplicate `cli_error`, and the private file-move worker uses the same service-owned staging path;
- BI-093 slice 4 captures or validates the snapshot descriptor and derives stable trace IDs before opening a service-owned S3 session, delays terminal success until session close, and attaches the emitted failure event/summary to `SnapshotHandledError`;
- snapshot setup/close, reconciliation, raw-write, and manifest-write backend failures now emit exactly one `snapshot_failed` plus one final summary with direct cause chaining and confirmed-only counters, while descriptor failures open no S3 session and arbitrary non-storage defects retain runtime classification;
- isolated loopback-unavailable integration proves handled exit `4`, exact terminal cardinality, redaction, bounded duration, and unchanged exact object state for REST, file, and snapshot commands; the recovery demo now requires the REST terminal pair before normal-command recovery;
- BI-093 slice 5 documents shared and source-owned setup/operation/close failure behavior, exact terminal ordering, stable codes/exits, confirmed-only metrics, redaction, and retained-state recovery; removes the superseded backend-output limitation; and closes the task after focused, offline, integration, clean-export, LSP, policy, and verification gates pass;
- BI-094 planning fixes unbuffered HTTPX transport-chunk iteration, monotonic checks around every receive, deterministic fake-clock slow-trickle evidence, response/spool cleanup, no partial raw persistence, stable handled output, regression coverage, and a two-slice implementation boundary without changing runtime code;
- BI-094 slice 1 consumes successful HTTP bodies through `iter_raw(chunk_size=None)`, checks the existing monotonic budget before and after every transport receive and at end-of-stream, rejects the crossing chunk before spool/hash admission, and retains the existing size and cleanup boundaries;
- deterministic fetcher tests prove a ten-byte trickle stops after two transport chunks under a one-second fake-clock budget, returns the existing first-attempt transport exhaustion with zero retries, closes response and spool resources, and preserves exact normal-response bytes, length, and SHA-256;
- BI-094 slice 2 proves at the service boundary that the same deadline crossing performs no raw/page-manifest/aggregate write, reports zero confirmed persistence counters, and emits exactly one stable failed event plus one final summary; REST/README wording now describes the implemented total body-streaming budget, and focused, offline, live, integration, clean-export, LSP, and verification gates pass;
- BI-095 planning fixes direct delegation to the complete `AppConfig` validation surface, retained Compose-only checks, redacted missing/malformed/cross-field evidence, prepublication failure atomicity, byte-stable reruns, exact documentation wording, and a two-slice test-first implementation boundary without changing runtime behavior;
- BI-095 slice 1 delegates application validation to `AppConfig.from_env()` over the parsed dotenv mapping, catches only `ConfigurationError`, and preserves independent Compose-only ports, Garage RPC-secret syntax, and recognized-placeholder policy;
- complete-template tests now reject omitted PostgreSQL, S3, HTTP, file, snapshot, and logging settings; malformed URLs/enums/numeric bounds; HTTP/snapshot protocol maxima; and file/snapshot cross-field violations without printing sentinels or invalid values;
- BI-095 slice 2 proves that full-validation failure publishes no absent target, preserves placeholder and initialized target bytes, removes temporary siblings, emits no success message, and discloses neither generated nor invalid values;
- valid initializer reruns execute the same complete preflight, preserve exact bytes and mode `0600`, and never regenerate credentials;
- Make help and README now distinguish complete application validation from Compose-only port, Garage RPC-secret, and placeholder checks, and explicitly make no connectivity, credential-acceptance, readiness, or `SOURCE_SEED` claim;
- the complete BI-095 focused, offline, integration, clean-export, LSP, policy, and verification matrix passes, so BI-095 is done and BI-096 is selected;
- BI-096 has an execution-ready two-slice plan covering immutable commit selection, a separate clean detached checkout, canonical Git-tree inventory, exact F01 path closure, repository gates, isolated integration, the full clean-export walkthrough, exact final inventory, bounded cleanup, protected fingerprints, and a documentation-only attestation report;
- BI-096 replacement commit `a4ef6d34fad5629c992385c26ede74a12b937563` passes both slices: clean committed source closure, repository gates, 76 isolated integration tests, bounded recovery, the complete clean-export walkthrough, exact `6/8/1/2/0` inspection, protected fingerprints, and exact cleanup all pass without changing product behavior or BI-090 history;
- BI-097 moved the session entry point from the repository root to `docs/START_HERE.md`, updated agent guidance and README navigation, and added a policy guard against path regression;
- BI-098 reduced the root README from 308 to 116 lines, centered it on the learning goal and happy path, retained all nine mandatory failure exercises in a compact state/recovery table, and moved the complete validated operator reference to `docs/guides/operator-guide.md`;
- documentation checks now enforce the concise learner path, operator-guide details, mandatory recovery exercises, and links to source-specific contracts;
- BI-098 is done, and BI-099 preserved the complete original history under `archive/pre-bi099-history` while replacing seven oversized history regions with task-focused commits;
- the rewritten pre-task commit `80df9640874b7af381a36d4ae7807b0f18c1ce2f` and archived original tip `c6d4b1537b3deeda0d5d1568db97a27394dfd207` share tree `1b8f1f3ae0d8732b5e51232e93074d5ea0e469bd` and have an empty diff;
- BI-100 applied the owner's superseding retention decision: `archive/pre-bi099-history` is deleted, all local reflogs were expired, unreachable old-history objects were pruned, and the representative old unique SHA values no longer resolve;
- pre-BI-100 commit `0c85343b3318ad0375ce5a6b2547f65f92203b81` and tree `ab24eb3895e9a01137e9aa82e6cb52c40796e668` remain unchanged under linear `main`;
- the project is again in terminal backlog state, BI-100 is done, and `active_task_id: null` records that no task is currently authorized or active.

## Not completed

- No authorized backlog task remains. Future improvements and corrections are permitted but are not current project work until recorded as a new atomic task.

## Known limitations and risks

- Garage has no bucket versioning, and conditional create with `If-None-Match` is not a contract foundation.
- Concurrent ingestion writers are outside the core contract.
- Source adapters must include every value that changes selected logical data in `BatchIdentity.parameters`; omission would cause an identifier collision.
- Ignored `.env` files created before BI-070 slice 1 must be refreshed with the committed HTTP, `FILE_*`, PostgreSQL timeout, and `SNAPSHOT_*` settings before integration targets or ingestion CLIs can load complete configuration.
- BI-090-F01–F05 remain the authoritative findings for the frozen audit baseline. Their follow-ups have technical PASS evidence from BI-096 for F01, accepted ADR 0003 plus completed BI-092 for F02, completed BI-093 for F03, completed BI-094 for F04, and completed BI-095 for F05. BI-096 does not retroactively reclassify F01's original dirty baseline. The exact original failed traceability rows and follow-up mapping remain in `docs/audits/BI-090.md`.
- After BI-100, SHA values unique to the deleted pre-rewrite history remain documentary evidence identifiers only; this local repository intentionally no longer retains the corresponding Git objects.

## Latest validation

2026-10-20, BI-100 archived-history removal:

- owner authorization established that no external consumer depends on the old tip;
- deleted `archive/pre-bi099-history`; `refs/heads/main` is the only Git ref — PASS;
- expired all 106 prior reflog entries and immediately pruned unreachable objects — PASS;
- representative old unique commits `c6d4b15…`, `5ee3781…`, and `a4ef6d3…` no longer resolve — PASS;
- pre-BI-100 commit `0c85343…` and tree `ab24eb3…` remain unchanged, `main` remains linear, and full `git fsck` passes;
- repository policy and `git diff --check` — PASS;
- `make lint` — PASS with Ruff, formatting, and strict mypy over 81 source files;
- `make test` — PASS with 683 offline tests and 76 integration tests deselected;
- `./scripts/verify.sh` — PASS with doctor, policy, locked sync, Ruff, formatting, strict mypy, 683 offline tests, and Compose rendering;
- integration and clean-export checks were NOT RUN because no product/runtime tree changed.

2026-10-20, BI-099 cohesive Git history rewrite:

- annotated tag `archive/pre-bi099-history` resolves to original tip `c6d4b1537b3deeda0d5d1568db97a27394dfd207`; all exact historical audit candidates remain reachable;
- `main` is linear and contains 36 commits instead of 24, with seven oversized regions replaced by task-focused series using explanatory commit bodies;
- rewritten pre-task tree comparison and empty Git diff against the archived original tip — PASS;
- median files per commit improved from 16 to 9.5, commits over 3,000 changed lines from four to two, and commits touching more than 20 files from eight to five;
- refreshed active LSP diagnostics over the representative rewritten Python paths — PASS with no findings;
- repository policy and `git diff --check` — PASS;
- `make lint` — PASS with Ruff, formatting, and strict mypy over 81 source files;
- `make test` — PASS with 683 offline tests and 76 integration tests deselected;
- `./scripts/verify.sh` — PASS with doctor, policy, locked sync, Ruff, formatting, strict mypy, 683 offline tests, and Compose rendering;
- integration and clean-export checks were NOT RUN because the rewritten pre-task product tree is byte-identical and BI-099 changes only Git history and handoff documentation.

2026-10-20, BI-098 learner-facing README restructure:

- focused documentation tests failed first against the 308-line README and absent operator guide, then passed with 98 tests after implementation;
- README and guide link validation — PASS with every relative Markdown target present;
- active LSP diagnostics over the four changed Python test files — PASS with no findings;
- `make lint` — PASS with Ruff, formatting, and strict mypy over 81 source files;
- `make test` — PASS with 683 offline tests and 76 integration tests deselected;
- `./scripts/verify.sh` — PASS with doctor, policy, locked sync, Ruff, formatting, strict mypy, 683 offline tests, and Compose rendering;
- no product/runtime behavior, dependency, configuration, object contract, ADR, or delivery semantics changed, so integration and clean-export checks were not rerun.

2026-10-19, BI-097 session-navigation relocation:

- the new path policy failed before the move because `docs/START_HERE.md` was absent, establishing the expected pre-implementation failure;
- repository policy — PASS with `docs/START_HERE.md` present, the legacy root path absent, updated agent guidance, synchronized `active_task_id: null`, every task `done`, and `last_completed_task_id: BI-097`;
- `./scripts/verify.sh` — PASS with doctor, policy, locked sync, Ruff, formatting, strict mypy over 81 source files, 682 offline tests with 76 integration tests deselected, and Compose rendering;
- no product/runtime behavior changed, so integration and clean-export checks were not rerun; the latest live acceptance remains the BI-096 evidence below.

### Latest live validation

2026-10-17, BI-096 slice 2:

- exact replacement commit `a4ef6d34fad5629c992385c26ede74a12b937563`, tree `f4c9a9658a417aca5f24472a8f4adbd445732e26`, remained detached with zero-byte status and diffs before and after live acceptance;
- isolated `make integration` — PASS in 155.749 seconds with 76 integration tests and the bounded persistence/reset recovery exercise;
- clean-export walkthrough — PASS in 39.920 seconds outer duration; emitted `cleanup_verified=true`, measured 12.295 seconds of setup, a 2.183-second warm happy path, and 35.021 seconds through exact inspection;
- exact final inventory — PASS with 6 raw, 8 manifest, 1 archive, 2 rejected, and 0 landing objects plus strict aggregate, checksum, disposition, and snapshot validation;
- integration and clean-export containers, networks, volumes, environments, and exact test-owned image tags are absent; all three protected default-project volume fingerprints are unchanged;
- the detached checkout was removed after evidence capture; retained logs are under ignored `artifacts/audit/BI-096/slice2/`;
- no product code, test behavior, dependency, runtime configuration, contract, ADR, README behavior, delivery claim, or BI-090 historical evidence changed.

## Next atomic step

No implementation task is scheduled. When the owner requests another concrete improvement or correction, create and activate one bounded backlog task before implementation.
