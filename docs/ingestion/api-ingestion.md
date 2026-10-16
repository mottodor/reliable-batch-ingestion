# REST API ingestion contract

This document describes the implemented bounded, restart-safe REST ingestion and inclusive backfill behavior from `BI-050`, including the persisted logical-run completion contract added by `BI-092` under [ADR 0003](../adr/0003-persist-rest-logical-run-manifest.md) and the source-owned backend-failure boundary added by `BI-093`. The identifier, one-payload manifest, summary, and shared S3 contracts remain governed by [`core-contracts.md`](../contracts/core-contracts.md) and [`storage-contract.md`](../contracts/storage-contract.md).

The delivery claim is limited to deterministic keys, immutable idempotent writes, and safe sequential redelivery. It is not an exactly-once claim, and concurrent writers for the same logical batch are unsupported.

## Commands and source allow-list

Start the local services, then select one implemented source and a strict `YYYY-MM-DD` date:

```bash
make up
make ingest-api ARGS="run --source fx-rates --date 2026-09-01"
make ingest-api ARGS="run --source shipping-statuses --date 2026-09-01"
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-07"
```

The production CLI accepts only these source descriptors:

| CLI/source name | Request path |
| --- | --- |
| `fx-rates` | `/v1/fx-rates` |
| `shipping-statuses` | `/v1/shipping-statuses` |

Arbitrary URLs, paths, headers, query parameters, and mock fault controls are not CLI options. `MOCK_API_BASE_URL` is deployment configuration and the two paths above are fixed application descriptors.

## Source identity and deterministic keys

One date invocation creates a logical API `BatchIdentity` from:

- canonical `source_name`;
- source type `api`;
- source date;
- `resource-path`, such as `/v1/fx-rates`;
- configured `page-size`.

The base URL, page number, physical attempt, `run_id`, and mock fault controls do not belong to this identity. A page number identifies one payload inside the batch. Each command attempt receives a new `run_id`, while the same source, date, resource path, and page size retain the same `batch_id` and keys.

For `fx-rates`, `2026-09-01`, and the committed default `HTTP_PAGE_SIZE=25`, the identity produces:

```text
batch_id=150b7a9c57cadf18d4d1f5e51fc66575da0d3934b97cf79dac3fc91f3c2becf6
raw/api/fx-rates/source_date=2026-09-01/batch_id=150b7a9c57cadf18d4d1f5e51fc66575da0d3934b97cf79dac3fc91f3c2becf6/page-0001.json
manifests/api/fx-rates/source_date=2026-09-01/batch_id=150b7a9c57cadf18d4d1f5e51fc66575da0d3934b97cf79dac3fc91f3c2becf6/page-0001.json.manifest.json
manifests/api/fx-rates/source_date=2026-09-01/batch_id=150b7a9c57cadf18d4d1f5e51fc66575da0d3934b97cf79dac3fc91f3c2becf6/logical-run.manifest.json
```

The corresponding aggregate key for `shipping-statuses` is:

```text
manifests/api/shipping-statuses/source_date=2026-09-01/batch_id=d1b4d7361db024cb942708a5d45ca1731addb831f9551299f623f8e4d5a2a185/logical-run.manifest.json
```

Page names range from `page-0001.json` through the configured maximum, which cannot exceed `page-9999.json`. The logical-run key contains neither `run_id` nor an attempt and has no companion object under `raw/`. It is derived by the API-specific helper rather than `manifest_key_for()`. If a source returns different bytes for an existing identity and page, ingestion reports a storage conflict and does not overwrite the original bytes.

## Bounds and client lifecycle

All settings are loaded explicitly from the ignored `.env`; importing the package reads no configuration and opens no connection.

| Setting | Implemented meaning |
| --- | --- |
| `HTTP_CONNECT_TIMEOUT_SECONDS` | HTTP connect and pool-acquisition timeout |
| `HTTP_READ_TIMEOUT_SECONDS` | HTTP read and write timeout |
| `HTTP_MAX_ATTEMPTS` | Total page requests, including the initial request |
| `HTTP_BACKOFF_BASE_SECONDS` | Initial deterministic fallback delay |
| `HTTP_BACKOFF_MAX_SECONDS` | Cap for deterministic exponential fallback |
| `HTTP_MAX_ELAPSED_SECONDS` | Monotonic request/retry budget for one page; the known slow-trickle response gap remains BI-094 |
| `HTTP_PAGE_SIZE` | Positive request size, at most 100; part of logical identity |
| `HTTP_MAX_PAGES` | Positive logical-batch page bound, at most 9,999 |
| `HTTP_MAX_RESPONSE_BYTES` | Maximum raw bytes accepted for one HTTP 200 page |
| `HTTP_MAX_BACKFILL_DAYS` | Maximum inclusive number of dates in one backfill |

One context-managed `httpx.Client` is reused for a command and is configured with environment-proxy use disabled. HTTP transport retries are disabled; one application-owned loop controls the complete request budget. Before each request, all HTTP timeout phases are clipped to the remaining monotonic page budget. Retry sleeps are also bounded by that budget.

The date service derives `source_date`, `batch_id`, and a fresh `run_id` before opening its context-owned S3 session. Aggregate preflight, page evidence, aggregate publication/final verification, and S3 client close all remain inside that attempt. Success is emitted only after the storage context closes cleanly; known setup or close failures therefore have the same trace identifiers and terminal contract as request-time S3 failures.

Each HTTP 200 response must have an `application/json` base media type. Its raw body is streamed in bounded chunks to a seekable spooled file while byte length and SHA-256 are calculated. Reading stops as soon as the configured size limit would be exceeded. Only one bounded page may be loaded for JSON validation; a multi-page batch is never accumulated in memory.

The separate logical-run manifest has a fixed 8 MiB (8,388,608-byte) declared and actual read limit; the existing page-manifest read limit remains 64 KiB. The aggregate parser holds at most that bounded metadata document in memory. Its `page_count` is in `1..9999`, `record_count` is in `0..999900`, and each raw payload length is in the signed 64-bit non-negative range.

## Retry and status behavior

Fallback delay for the failure after request `attempt` is deterministic and has no jitter:

```text
min(HTTP_BACKOFF_BASE_SECONDS * 2 ** (attempt - 1), HTTP_BACKOFF_MAX_SECONDS)
```

A retry starts only if both another attempt and the complete selected delay fit inside the page's monotonic elapsed budget.

| Outcome | Behavior | Raw business-page object |
| --- | --- | --- |
| HTTP 200 | Do not retry; stream and preserve before decoding or validating | Preserved before parsing |
| HTTP 429 | Retry within attempt and elapsed budgets | Error body is not stored |
| Other HTTP 4xx | Permanent failure after one request | Error body is not stored |
| HTTP 5xx | Retry with bounded exponential fallback | Error body is not stored |
| Connect/read/write/pool timeout or HTTP transport failure | Retry with bounded exponential fallback | None |
| Redirect, non-200 2xx, or another status | Permanent protocol failure | Error body is not stored |
| Missing/non-JSON media type or oversized HTTP 200 response | Handled protocol/size failure; no content validation retry | No completed raw page |
| Malformed UTF-8/JSON, invalid schema, or invalid pagination in HTTP 200 | No retry of that response; preserve bytes and write a failed manifest | Preserved |
| Known backend S3 failure | Never enters the HTTP retry loop; emit handled `storage_error` terminal output | Existing state is retained |
| Configuration or programming failure | Never enters the HTTP retry loop; retain its non-storage CLI/runtime classification | Existing state is retained |

### `Retry-After`

For HTTP 429, `Retry-After` accepts either non-negative decimal seconds or an HTTP date. A past HTTP date means zero delay. Missing or malformed values use exponential fallback. A valid server-selected delay is used exactly; if it cannot fit in the remaining elapsed budget, the page fails with `http_retry_exhausted` rather than truncating the delay.

`retry_count` records only additional HTTP requests that actually start. Structured `api_retry` events identify the date, batch, run, page, next request attempt, classification, HTTP status when present, and selected delay. They do not contain headers, bodies, credentials, or complete query strings.

## Page validation and persistence order

A successful media-type/size check does not mean the page is valid. After raw preservation, the service requires the exact source envelope documented in [`source-contracts.md`](../contracts/source-contracts.md):

```text
source, source_date, page, page_size, total_pages,
total_records, next_page, items
```

The response source, date, page, and page size must equal the request. Page 1 establishes bounded `total_pages` and non-negative `total_records`; later pages must repeat both values. Traversal must increment by one, `next_page` must point to the next page or be null on the last page, and item counts must agree with page size and total records. Source-specific items are strictly validated without reserializing or transforming the raw body.

Before any HTTP fetch or storage mutation, the service inspects the exact logical-run manifest key. A present corrupt, conflicting, or orphaned completion claim stops the operation unchanged; the service does not fetch a page in order to heal that claim.

For every complete HTTP 200 body, the operation order is:

1. stream the bounded response and calculate exact length/SHA-256;
2. derive the deterministic raw and page-manifest keys;
3. reconcile those exact keys with the fetched checksum and length;
4. create and byte-verify the raw object if absent, or verify/reuse an identical existing object;
5. only then decode and validate the fetched bytes;
6. immutably create one terminal page manifest when it is missing;
7. read the exact stored page-manifest bytes and collect verified aggregate evidence.

After the final page validates, the service publishes or reuses the logical-run manifest, then re-reads and fully verifies it and every referenced exact page pair. Only this `complete` state permits the terminal succeeded `api_ingestion` event and final `RunSummary`. Transient non-200 bodies are delivery errors, not source pages, and are never written under a deterministic business-page key.

## Page manifests and logical-run representation

A successful `N`-page API batch has exactly:

- `N` immutable raw page objects;
- `N` unchanged schema-version-1 page manifests, one per raw page;
- one immutable success-only `api_logical_run_manifest` at the deterministic batch key.

API page manifests use the shared one-payload schema version 1 and always set `original_file_name` to null. A valid preserved page receives terminal status `succeeded`. A preserved HTTP 200 page that fails body, item, or pagination validation receives terminal status `failed`. Its `attempt` is the 1-based HTTP request attempt that returned that body.

The aggregate uses an independent schema version 1 and contains exactly: `manifest_type`, `schema_version`, `source_name`, `source_type`, `source_date`, `batch_id`, `identity_parameters`, `completion_run_id`, `completed_at`, `status`, `page_count`, `record_count`, and `pages`. Status is always `succeeded`. `identity_parameters` contains exactly canonical string values for `page-size` and the allow-listed `resource-path`. Each ordered, gap-free page entry contains its page number, exact raw key, exact page-manifest key, actual raw length and SHA-256, SHA-256 of the exact stored page-manifest bytes, and validated record count.

Aggregate JSON is canonical compact UTF-8 with recursively sorted object keys, canonical integers/timestamps, no BOM or trailing newline, and no duplicate or unknown members. Parsing rederives the descriptor, `batch_id`, and every key; it rejects noncanonical bytes even if they decode to otherwise plausible JSON. Raw, page-manifest, and aggregate checksums come from streamed bytes and never from `ETag`.

A repeated invocation does not rewrite a page manifest merely to record its new `run_id`. A reused complete page keeps the original manifest, status, timestamp, and `run_id` and increments `duplicate_count` for the new run. Page manifests completed by different attempts are valid: the aggregate binds each one's exact bytes by checksum. `completion_run_id` and `completed_at` identify the attempt that first committed logical completion and remain unchanged on replay.

Partial, interrupted, and failed attempts never persist a logical-run manifest. A batch created before BI-092 is upgraded only through normal source replay: every existing page pair is fetched, validated, and verified, then only the missing aggregate is added. The aggregate is completion evidence, not an offline source substitute, mutable checkpoint, failed-attempt history, or synthetic raw payload.

## Reconciliation and recovery states

Recovery inspects exact deterministic keys; it does not infer correctness from prefix listings.

| Observed state | Ingestion behavior | Operator recovery |
| --- | --- | --- |
| `absent` | Write and verify the raw payload, validate it, then write its manifest | No intervention |
| `payload_only` | Verify/reuse the payload and write only the missing manifest | Rerun the same command |
| `complete` + `succeeded` | Verify the fetched result, reuse both objects without mutation, count one duplicate | Rerun is safe |
| `complete` + `failed` and same invalid bytes | Re-surface the terminal `invalid_page` result without mutation | Retain evidence; see malformed-page recovery below |
| `manifest_only` | Fail with `manifest_only`; do not fabricate or delete data | Investigate/repair storage state, then rerun |
| Different payload bytes at the deterministic key | Fail with `storage_conflict`; original object remains | Correct the source/identity contract; never overwrite in place |
| Corrupt or inconsistent object/manifest | Fail with `storage_corruption`, `complete_status_conflict`, or another explicit state error | Investigate/repair retained state, then rerun |

Logical completion is reconciled separately after exact page-pair reconciliation:

| Aggregate state | Ingestion behavior | Operator recovery |
| --- | --- | --- |
| `absent` + incomplete page evidence | Continue normal ingestion; never claim logical completion early | Rerun the same command after the source/service recovers |
| `absent` + terminal failed page | Re-surface the page failure; do not publish completion | Retain failed evidence and follow the page-failure action |
| `absent` + every required succeeded page pair | Build and immutably publish one bounded aggregate | No intervention; normal run converges |
| Valid `complete` aggregate | Still fetch/validate the source, verify all exact evidence, then reuse aggregate bytes and first-completion facts | Rerun is safe |
| Oversized, malformed, noncanonical, duplicate-keyed, or unsupported aggregate | Fail with `logical_run_manifest_corrupt`; retain bytes | Investigate the exact key; do not overwrite or delete automatically |
| Identity, key, ordering, count, status, or checksum mismatch | Fail with `logical_run_manifest_conflict`; retain all evidence | Correct the contract/storage state explicitly, then rerun |
| Aggregate references a missing raw page or page manifest | Fail with `logical_run_manifest_orphaned` before fetching; never fabricate evidence | Investigate and explicitly repair the retained completion claim |
| Aggregate write outcome is uncertain | Inspect the exact key; accept identical fully verified bytes or retry the same frozen bytes once only when absent | If still absent/uncertain, rerun; different present bytes are a retained conflict |

Publication uses at most two immutable-write invocations over one frozen aggregate byte sequence during same-attempt uncertainty. A later ordinary rerun accepts any fully valid complete aggregate and preserves its original `completion_run_id` and `completed_at`. Prefix listing is never used as the production completeness oracle.

A process stop after raw upload but before manifest upload intentionally leaves `payload_only`. The direct recovery command is the original command, for example:

```bash
make ingest-api ARGS="run --source fx-rates --date 2026-09-01"
```

The rerun fetches the page again, verifies the existing bytes, and writes only the missing manifest using the new physical attempt's `run_id`.

A complete failed manifest is terminal. Repeating unchanged malformed bytes re-surfaces the same failure. If the upstream source changes those bytes under the same logical identity, the immutable key conflicts rather than erasing failure evidence. Retain the old pair and ingest corrected data only under a reviewed new logical identity—for example, a deliberately changed configured page size—or on the corrected source date:

```bash
HTTP_PAGE_SIZE=20 make ingest-api ARGS="run --source fx-rates --date 2026-09-01"
```

Changing page size changes `batch_id` and the key set; it is not an in-place repair. Do not delete or overwrite the failed raw evidence as an automatic recovery step.

## Structured output, metrics, and exit codes

Output is newline-delimited JSON. Handled source/contract outcomes emit exactly one terminal `api_ingestion` event followed by exactly one `run_summary`; retries may emit `api_retry` events first. A successful backfill emits one final `api_backfill` completion event after all date summaries.

Known Botocore failures from S3 session setup/close, aggregate preflight or final verification, page-pair reconciliation and bounded reads, raw/page-manifest/aggregate writes, and uncertain-write inspection map to `storage_error`. The failure event and summary use the same date, batch, and run IDs and contain no backend exception text, endpoint, credentials, response values, or payload values. The original `StorageBackendError` remains chained to the handled source exception for internal diagnosis. The CLI returns exit `4` without adding `cli_error`; arbitrary non-storage setup or cleanup defects retain runtime classification.

Handled failures use stable codes:

```text
http_permanent_client_error
http_permanent_protocol_error
http_retry_exhausted
http_response_protocol_error
http_response_too_large
invalid_page
manifest_only
complete_status_conflict
storage_conflict
storage_corruption
storage_error
logical_run_manifest_corrupt
logical_run_manifest_conflict
logical_run_manifest_orphaned
logical_run_manifest_write_uncertain
```

Terminal events include nullable `page`, `error_code`, `http_status`, and retry `classification` fields. Output is intentionally secret-free.

The REST `RunSummary` counters retain their original meanings after aggregate publication:

| Field | Meaning |
| --- | --- |
| `records_read` | Item records in successfully validated pages |
| `records_accepted` | Equal to `records_read`; REST ingestion performs no transformation/rejection split |
| `records_rejected` | Zero; a malformed page has no trustworthy item count |
| `pages_fetched` | Complete HTTP 200 bodies received, including malformed bodies |
| `bytes_written` | Newly created raw payload bytes only; excludes page/aggregate manifests and reused objects |
| `retry_count` | Additional HTTP requests actually made after first attempts |
| `duplicate_count` | Complete successful page/manifest pairs reused without mutation; aggregate reuse does not increment it |
| `duration_seconds` | Monotonic elapsed time for one date invocation |
| `source_date`, `batch_id`, `run_id` | Trace identifiers for that physical date attempt |

On storage failure, counters stop at their last confirmed boundary: raw bytes count only after verified creation, page/record counters reflect already completed source steps, duplicate count reflects only verified reused page pairs, and an uncertain aggregate or payload write does not invent progress. A failure during initial S3 setup therefore reports zero progress; a failure after confirmed pages preserves and reports that progress.

CLI exits are stable:

| Exit | Meaning |
| --- | --- |
| `0` | Success |
| `2` | Invalid command, source, date, or backfill range |
| `3` | Invalid/missing configuration |
| `4` | Handled ingestion failure after terminal event and summary output |
| `5` | Unexpected non-storage HTTP/runtime setup or cleanup failure outside the handled attempt path |

## Backfill behavior

Backfill validates the whole inclusive range before opening HTTP or S3 clients. The start must not follow the end, and the inclusive day count must not exceed `HTTP_MAX_BACKFILL_DAYS`.

Dates run sequentially in ascending order through the same single-date primitive. Backfill stops at the first failed date and returns exit `4`; later dates are not attempted. Rerun the complete original range after the source or service recovers:

```bash
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-07"
```

A date reports success only after its logical-run manifest is fully reconciled. Earlier complete pages and aggregates are verified and reused, the failed date is retried safely, and then remaining dates continue. An aggregate conflict on one date stops the range before later dates. A repeated successful backfill retains the exact object/checksum inventory, writes zero new raw payload bytes, preserves original aggregate completion facts, and reports reused pages through `duplicate_count`.

## Failure exercises and recovery commands

The local mock's scenario selectors are integration-test controls and are deliberately absent from the production CLI. The table describes the tested final state and the operator action after the injected/source condition is removed.

| Failure | Expected retained state | Recovery command/action |
| --- | --- | --- |
| `429 -> success` | No 429 body object; successful page pairs and one verified aggregate after one bounded retry | None when within budget; otherwise rerun the same `run`/`backfill` after rate limiting clears |
| Permanent 500 | Earlier pages may be complete; no raw object for the failed non-200 page and no aggregate completion claim | Fix/clear the source fault, then rerun the same command |
| Permanent 4xx | No raw object for the failed response, no aggregate, and exactly one request | Correct source/date/configuration, then rerun the corrected command |
| Malformed HTTP 200 | Exact malformed bytes plus terminal `failed` page manifest; no aggregate | Retain evidence; unchanged rerun re-surfaces failure, corrected bytes require a reviewed new logical identity |
| Failure after some pages | Earlier page pairs remain complete; the non-200 failed page and aggregate are absent | Clear the fault and rerun the same command; completed pages are reused and the aggregate is written last |
| Stop after payload/before page manifest | Verified immutable payload without page manifest (`payload_only`); no aggregate | Rerun the same command; the missing page manifest is created before eventual aggregate completion |
| Stop after final page manifest/before aggregate | Every page pair is complete but logical completion is absent | Rerun the same command; verify all page evidence and create only the aggregate |
| Corrupt/conflicting/orphaned aggregate | Bad completion bytes and all remaining page evidence are retained unchanged; no fetch or automatic repair occurs | Investigate and explicitly repair the exact object-store state; never overwrite the immutable claim automatically |
| Uncertain aggregate write | Complete page evidence remains; exact-key inspection may find a fully valid committed aggregate or an absent key | Same attempt performs bounded frozen-byte convergence; otherwise rerun the same command |
| S3 session/setup/read/write/close failure | Any already verified page pairs remain unchanged; unconfirmed objects are not counted and no failed aggregate is created; output ends with `api_ingestion`/`storage_error` then `run_summary`, exit `4` | Restore S3 and rerun the same `run` or full `backfill`; exact-key reconciliation reuses confirmed evidence |

## Limitations

- One process must own a logical batch at a time. Garage has no versioning, and conditional create is not a foundation of this contract; concurrent writers remain unsafe.
- Backfill is sequential and fail-fast. Parallel and continue-on-error modes are not implemented.
- Authentication schemes and arbitrary REST connectors are outside BI-050.
- Raw and aggregate immutability can expose upstream mutation or bad completion evidence as a conflict. Automatic deletion, overwrite, and cleanup-based repair are intentionally forbidden.
- A complete aggregate does not avoid source access: normal replay still fetches and validates every source page.
- The 8 MiB aggregate bound and 9,999-page namespace are fixed version-1 limits; increasing either or changing aggregate cardinality/key/status requires architectural review.
- A peer that continuously trickles successful response bytes below the HTTP read-timeout threshold can currently exceed `HTTP_MAX_ELAPSED_SECONDS`; BI-094 owns that repair.
- The CLI does not list or download objects. Inspect retained objects through a standard S3 client using the configured bucket and exact keys.
