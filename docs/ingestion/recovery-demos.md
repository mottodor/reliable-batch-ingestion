# Recovery demonstrations

This guide turns the implemented REST, file, PostgreSQL, and shared-storage recovery contracts into bounded operator exercises. The runner injects faults only in private demo workers and then invokes the normal production ingestion command or documents the terminal action. Production CLIs do not accept fault flags.

## Setup and evidence

Initialize an ignored local environment explicitly, start the pinned local services, and seed deterministic source data:

```bash
cp .env.example .env
make init-local-env
make up
make seed SOURCE_SEED=20260901
```

Run the public sequence before or after individual recovery exercises:

```bash
make demo-happy-path
make demo-duplicate
make demo-api-retry
make demo-bad-file
make demo-inspect
```

Every command is bounded by `DEMO_TIMEOUT_SECONDS` (default `210`). Standard output is compact NDJSON. Production ingestion events and `RunSummary` values are relayed unchanged; `demo_state` records asserted intermediate/final states; one terminal `demo_summary` reports whether the orchestration passed. Reports and strict non-secret descriptors are stored under ignored `artifacts/demo/<compose-project>/`.

The public demos preserve evidence. They do not delete a matching batch to manufacture a first run. `demo-inspect` is read-only: it loads recorded descriptors, calculates exact prefixes, compares exact key sets, streams object bytes for SHA-256 independently of `ETag`, validates page-manifest linkage/status, strictly parses one logical-run aggregate for every successful API batch, cross-checks its raw/page-manifest checksums, checks file dispositions, and validates snapshot Parquet rows and bounds. Failed API batches must not claim aggregate completion.

## Reading recovery exits

A successful `make demo-recovery ...` command exits `0` only after the expected underlying failure and object state have been observed and the recovery/final action has been verified. `demo_summary.attempted_exit_class` is therefore `expected_failure` even when the injected operation itself exited with a source-specific code. Unexpected exits, timeouts, or state differences make the wrapper non-zero.

Underlying production CLI exits remain `0` for success, `2` for invalid command input, `3` for invalid configuration, `4` for a handled ingestion failure or retained file rejection, and `5` for unexpected non-source runtime setup/cleanup failure. A known backend S3 failure inside an identified REST, file, or snapshot attempt uses exit `4` after its source terminal event and final summary. Abrupt demo-only process boundaries use private exit `86`; that code is not a production CLI contract.

## Failure and recovery exercises

### `api-rate-limit`

```bash
make demo-recovery SCENARIO=api-rate-limit
```

**Trigger and expected exit:** The private API adapter resets a unique mock fault key, receives one HTTP 429 with a valid one-second `Retry-After`, and then succeeds. The production operation exits `0`; the demo wrapper exits `0` after classifying the injected 429 as the expected failure condition.

**Intermediate state:** The 429 body is not stored as a business payload. Exactly one retry event records status `429`, selected delay `1.0`, and the next attempt; successful page payload/manifest pairs are complete.

**Recovery or terminal action:** No separate command is needed when the bounded retry succeeds. If real rate limiting outlasts the configured budget, rerun the same normal `make ingest-api ARGS="run ..."` command after the source permits requests.

**Final state:** The calculated API inventory is complete with succeeded page manifests and one verified logical-run aggregate, exactly one retry is reported, and no error-response object exists.

### `api-permanent-500`

```bash
make demo-recovery SCENARIO=api-permanent-500
```

**Trigger and expected exit:** The demo worker selects permanent HTTP 500 through the exact application attempt budget. The underlying API CLI exits handled code `4`; the wrapper expects that failure and remains bounded.

**Intermediate state:** No raw object or page manifest exists for the failed non-200 page, and no logical-run aggregate exists. If an operation had completed earlier pages, those complete pairs would remain immutable; this fixed first-page exercise asserts `absent` for page 1.

**Recovery or terminal action:** The worker removes the private fault selection, and the parent reruns the same normal API operation. Outside the demo, fix or clear the source fault and rerun `make ingest-api ARGS="run --source fx-rates --date <same-date>"` (or the original full backfill).

**Final state:** Normal ingestion creates the exact successful page pairs and publishes one verified logical-run aggregate last. Previously complete evidence, when present, is verified and reused rather than rewritten.

### `api-malformed-json`

```bash
make demo-recovery SCENARIO=api-malformed-json
```

**Trigger and expected exit:** The mock source returns HTTP 200 with deterministic malformed JSON. Raw bytes are preserved before parsing, one terminal failed manifest is written, and the API operation exits handled code `4`.

**Intermediate state:** The raw payload and its linked page manifest form a complete pair; manifest status is `failed`. The logical-run aggregate is absent. The runner records the payload SHA-256 and original manifest bytes.

**Recovery or terminal action:** The same faulted operation is run again and must exit `4` while re-surfacing the unchanged terminal pair. This batch is not repaired in place. Corrected upstream bytes require a reviewed new logical identity/source date and a normal ingestion command for that identity.

**Final state:** Malformed bytes, checksum, page-manifest bytes, failed status, and original `run_id` remain unchanged. No aggregate is published and no corrected content overwrites the retained batch.

### `api-after-page`

```bash
make demo-recovery SCENARIO=api-after-page
```

**Trigger and expected exit:** A private worker exits `86` only after page 1 raw and manifest writes are complete. On safe repeat execution, an already recovered worker may exit `0` instead of recreating the crash.

**Intermediate state:** On first execution page 1 is `complete`, page 2 is `absent`, and the logical-run aggregate is absent. No mutable page checkpoint is written.

**Recovery or terminal action:** The parent invokes the normal API CLI with the same source date and page size. Operators use the same `make ingest-api ARGS="run ..."` command that was interrupted.

**Final state:** Replay verifies and reuses page 1, completes every later page, preserves original complete evidence, publishes and verifies one aggregate, and only then emits a succeeded summary.

### `file-same-name`

```bash
make demo-recovery SCENARIO=file-same-name
```

**Trigger and expected exit:** The valid supplier bytes are delivered again under the original valid name through normal file intake. Both normal runs exit `0`; the wrapper treats the intentional redelivery as the expected duplicate condition.

**Intermediate state:** The original raw payload, archive object, and succeeded manifest already form one canonical delivery. Temporary landing is absent after each completed command.

**Recovery or terminal action:** No repair is required. Repeating `make ingest-file ARGS="run --source supplier-prices --date 2026-09-01 --path <same-file>"` is safe.

**Final state:** Classification is `same_name_redelivery`; canonical raw bytes, archive bytes, manifest bytes, and original `run_id` are unchanged, and canonical `bytes_written` is zero.

### `file-renamed-content`

```bash
make demo-recovery SCENARIO=file-renamed-content
```

**Trigger and expected exit:** Byte-identical valid content is staged under the committed different valid filename. Normal intake exits `0`; the wrapper classifies the deliberate redelivery rather than expecting a process error.

**Intermediate state:** The content-based batch already has one canonical raw/archive/manifest set. The renamed delivery has a distinct landing descriptor only while the command is active.

**Recovery or terminal action:** No repair is required. Use normal `make ingest-file ARGS="run ... --path <renamed-file>"`; do not copy or rename S3 objects manually.

**Final state:** Classification is `renamed_content`, landing is absent, no second business payload is created, and the original canonical inventory and manifest remain unchanged.

### `file-schema-invalid`

```bash
make demo-recovery SCENARIO=file-schema-invalid
```

**Trigger and expected exit:** Normal file intake receives the deterministic schema-invalid CSV and exits handled code `4` after retaining the whole file. The demo wrapper then delivers corrected valid content and exits `0` after both outcomes are verified.

**Intermediate state:** Exact invalid bytes exist under canonical raw and one `reason=schema_mismatch` rejected key, linked by one rejected manifest. There is no archive object and no accepted-row subset.

**Recovery or terminal action:** Correct the complete supplier file and redeliver it with normal `make ingest-file ARGS="run ..."`. Never delete or overwrite the rejected evidence to reuse its content identity.

**Final state:** Rejected invalid evidence remains unchanged. Corrected bytes have a new content identity and a separate succeeded raw/archive/manifest set.

### `file-during-move`

```bash
make demo-recovery SCENARIO=file-during-move
```

**Trigger and expected exit:** A private S3 client wrapper exits `86` after copy and destination byte verification but immediately before deleting landing. On safe repeat execution it may exit `0` after verifying an already recovered state.

**Intermediate state:** On first execution, landing and archive contain identical verified bytes, canonical raw exists, and the manifest is absent. The two physical copies represent one recoverable move, not two accepted files.

**Recovery or terminal action:** The parent passes the recorded canonical landing key to the normal `make ingest-file ARGS="resume --landing-key '<key>'"` path. Operators use the same resume form and must not manually rename S3 objects.

**Final state:** Movement converges to destination-only: landing is absent, raw/archive bytes are exact, and the succeeded manifest is written last.

### `postgres-connection-drop`

```bash
make demo-recovery SCENARIO=postgres-connection-drop
```

**Trigger and expected exit:** After the first complete snapshot chunk, a second bounded PostgreSQL connection terminates the reader backend. The private worker exits non-zero (normally handled code `4`); a safely repeated completed scenario can exit `0`.

**Intermediate state:** Chunk 1 has a complete immutable raw/manifest pair. Later chunks are absent on the first interruption; no failed manifest, mutable checkpoint, or retained database snapshot is created. The emitted descriptor contains the original upper tuple and expected count.

**Recovery or terminal action:** The parent invokes the normal snapshot `resume` form with every recorded descriptor field and the same chunk-row configuration. Operators use `make ingest-postgres ARGS="resume --source orders --date <date> --upper-updated-at <utc> --upper-order-id <id> --expected-record-count <count>"` (or the emitted `--empty` form).

**Final state:** A fresh read-only repeatable-read transaction revalidates the descriptor and replays from chunk 1. Complete chunk evidence is reused and the exact bounded snapshot inventory completes.

### `s3-unavailable`

```bash
make demo-recovery SCENARIO=s3-unavailable
```

**Trigger and expected exit:** Only the child API command receives an unreachable loopback S3 endpoint with one short attempt; Garage itself and unrelated projects remain running. The child exits handled code `4`, which the wrapper expects. Its final two records are one failed `api_ingestion` with `error_code=storage_error` and one `run_summary` carrying the same source date, batch ID, and run ID; no duplicate `cli_error` is emitted.

**Intermediate state:** The selected page pair and its logical-run aggregate are `absent`, and the summary reports zero confirmed records and raw bytes. Any previously recorded demo inventory is hashed before and after the failed child and must remain unchanged. Output and demo reports contain no credentials, DSN, unreachable endpoint, backend exception text, or payload values.

**Recovery or terminal action:** The parent reruns the same normal API command using the real endpoint from the ignored `.env`. In an operator incident, restore S3 and rerun the original API, file resume, or snapshot resume command with the same logical descriptor.

**Final state:** Normal reconciliation creates or reuses the exact selected page evidence and publishes or reuses its aggregate, while prior unrelated evidence remains byte-identical. Separate isolated acceptance also exercises the production file and snapshot commands against the same bounded unavailable-endpoint class; each returns `4` with its own failure event (`storage_failed`) followed by one complete summary and leaves its exact target keys absent.

### `payload-before-manifest`

```bash
make demo-recovery SCENARIO=payload-before-manifest
```

**Trigger and expected exit:** A private storage wrapper exits `86` immediately before the first manifest write and only after immutable raw upload and verification have returned successfully. A safely repeated completed scenario can exit `0` rather than deleting evidence to recreate the fault.

**Intermediate state:** The first API page is exactly `payload_only`: verified raw bytes exist and its deterministic page-manifest key is absent. Later pages and the logical-run aggregate are absent on the first interruption.

**Recovery or terminal action:** The parent invokes the normal API CLI with the same source date and page size. Operators rerun the original `make ingest-api ARGS="run ..."` command; no special repair flag is needed.

**Final state:** Reconciliation verifies the existing raw checksum, adds only the missing page manifest, completes later pages, publishes one aggregate last, and leaves the raw payload unchanged.

## Clean-export acceptance walkthrough

The acceptance harness proves that repository-owned, non-ignored files are sufficient:

```bash
uv run python scripts/check_clean_export.py
```

The harness obtains its source set from `git ls-files --cached --others --exclude-standard`; rejects `.env`, virtual environments, caches, artifacts, Git metadata, unsafe paths, and symlinks; copies only regular files to a temporary directory; and initializes a new mode-`0600` `.env` without printing credentials. It changes only allow-listed loopback ports, endpoint URLs, and a unique bucket, and passes a unique Compose project name to every Make command.

Under per-command and total deadlines it runs locked dependency sync, doctor, Compose rendering, `make up`, deterministic seed, all four public demos, offline tests, and final exact inspection. The accepted BI-092 inventory contains exactly 6 raw objects, 8 manifest objects (six one-payload manifests plus two API logical-run aggregates), 1 archive object, 2 rejected objects, and 0 landing objects. It separately reports setup time, warm `demo-happy-path` time, and total walkthrough time; warm happy path must remain at or below three minutes.

Cleanup first runs normal `make down`, then calls the existing reset helper with exact `destroy-local-data` confirmation. It requires no containers, networks, or volumes with the isolated project label to remain and compares the protected default project's three volume fingerprints before and after. Generated command output plus NDJSON report/state artifacts are rejected if they contain generated credentials or supplier row values; intentionally retained bad-file input bytes are business evidence and are not misclassified as reports.

First-time package or pinned-image acquisition may affect setup and total duration, so no hardware-independent total-time promise is made. The measured three-minute criterion covers only the warm happy-path command after setup and healthy services.

## Cleanup and limitations

Public demos intentionally leave evidence and services available for inspection:

```bash
make demo-inspect
make down
```

To erase a deliberately isolated project, select that project explicitly and use the existing exact destructive confirmation. Never point clean-export cleanup at the default project.

Delivery is deterministic and idempotent for sequential writers, but it is **not exactly-once**. S3 movement uses verified `copy -> verify -> delete` and is **not an atomic rename**. Garage has no bucket versioning, conditional create is not the immutability foundation, and **concurrent writers** for the same deterministic key are unsupported. A disconnected PostgreSQL transaction does not leave a durable MVCC snapshot: resume opens a new transaction, revalidates the recorded upper descriptor, and restarts from chunk 1. Malformed successful HTTP content is terminal under its original identity and cannot be overwritten in place.
