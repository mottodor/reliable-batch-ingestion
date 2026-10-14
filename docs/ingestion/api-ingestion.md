# REST API ingestion contract

This document describes the implemented `BI-050` behavior for bounded, restart-safe REST ingestion and inclusive backfill. The identifier, manifest, summary, and shared S3 contracts remain governed by [`core-contracts.md`](../contracts/core-contracts.md) and [`storage-contract.md`](../contracts/storage-contract.md).

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
```

Page names range from `page-0001.json` through the configured maximum, which cannot exceed `page-9999.json`. If a source returns different bytes for an existing identity and page, ingestion reports a storage conflict and does not overwrite the original bytes.

## Bounds and client lifecycle

All settings are loaded explicitly from the ignored `.env`; importing the package reads no configuration and opens no connection.

| Setting | Implemented meaning |
| --- | --- |
| `HTTP_CONNECT_TIMEOUT_SECONDS` | HTTP connect and pool-acquisition timeout |
| `HTTP_READ_TIMEOUT_SECONDS` | HTTP read and write timeout |
| `HTTP_MAX_ATTEMPTS` | Total page requests, including the initial request |
| `HTTP_BACKOFF_BASE_SECONDS` | Initial deterministic fallback delay |
| `HTTP_BACKOFF_MAX_SECONDS` | Cap for deterministic exponential fallback |
| `HTTP_MAX_ELAPSED_SECONDS` | Monotonic total request/retry/body-read budget for one page |
| `HTTP_PAGE_SIZE` | Positive request size, at most 100; part of logical identity |
| `HTTP_MAX_PAGES` | Positive logical-batch page bound, at most 9,999 |
| `HTTP_MAX_RESPONSE_BYTES` | Maximum raw bytes accepted for one HTTP 200 page |
| `HTTP_MAX_BACKFILL_DAYS` | Maximum inclusive number of dates in one backfill |

One context-managed `httpx.Client` is reused for a command and is configured with environment-proxy use disabled. HTTP transport retries are disabled; one application-owned loop controls the complete request budget. Before each request, all HTTP timeout phases are clipped to the remaining monotonic page budget. The same deadline is checked after every streamed body chunk and before a retry sleep, so slow streaming cannot bypass the total elapsed limit.

Each HTTP 200 response must have an `application/json` base media type. Its raw body is streamed in bounded chunks to a seekable spooled file while byte length and SHA-256 are calculated. Reading stops as soon as the configured size limit would be exceeded. Only one bounded page may be loaded for JSON validation; a multi-page batch is never accumulated in memory.

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
| S3, configuration, or programming failure | Never enters the HTTP retry loop | Existing state is retained |

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

For every complete HTTP 200 body, the operation order is:

1. stream the bounded response and calculate exact length/SHA-256;
2. derive the deterministic raw and manifest keys;
3. reconcile those exact keys with the fetched checksum and length;
4. create and byte-verify the raw object if absent, or verify/reuse an identical existing object;
5. only then decode and validate the fetched bytes;
6. immutably create one terminal page manifest when it is missing.

Transient non-200 bodies are delivery errors, not source pages, and are never written under a deterministic business-page key.

## Page manifests and logical-run representation

Manifest cardinality is one manifest per raw page. All pages for one date and identity share a `batch_id`; pages newly completed by one invocation share its `run_id`. The complete page-manifest set plus the final `RunSummary` represents the logical run. There is no mutable aggregate run manifest.

API manifests use schema version 1 and always set `original_file_name` to null. A valid preserved page receives terminal status `succeeded`. A preserved HTTP 200 page that fails body, item, or pagination validation receives terminal status `failed`. Its `attempt` is the 1-based HTTP request attempt that returned that body.

A repeated invocation does not rewrite a complete manifest merely to record its new `run_id`. A reused complete page keeps the original manifest, status, timestamp, and `run_id` and increments `duplicate_count` for the new run.

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

Output is newline-delimited JSON. Every attempted date emits exactly one terminal `api_ingestion` event and one `run_summary`; retries may emit `api_retry` events first. A successful backfill emits one final `api_backfill` completion event after all date summaries.

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
```

Terminal events include nullable `page`, `error_code`, `http_status`, and retry `classification` fields. Output is intentionally secret-free.

The BI-050 `RunSummary` counters mean:

| Field | Meaning |
| --- | --- |
| `records_read` | Item records in successfully validated pages |
| `records_accepted` | Equal to `records_read`; REST ingestion performs no transformation/rejection split |
| `records_rejected` | Zero; a malformed page has no trustworthy item count |
| `pages_fetched` | Complete HTTP 200 bodies received, including malformed bodies |
| `bytes_written` | Newly created raw payload bytes only; excludes manifests and reused objects |
| `retry_count` | Additional HTTP requests actually made after first attempts |
| `duplicate_count` | Complete successful page/manifest pairs reused without mutation |
| `duration_seconds` | Monotonic elapsed time for one date invocation |
| `source_date`, `batch_id`, `run_id` | Trace identifiers for that physical date attempt |

CLI exits are stable:

| Exit | Meaning |
| --- | --- |
| `0` | Success |
| `2` | Invalid command, source, date, or backfill range |
| `3` | Invalid/missing configuration |
| `4` | Handled ingestion failure after terminal event and summary output |
| `5` | Runtime client setup or cleanup failure |

## Backfill behavior

Backfill validates the whole inclusive range before opening HTTP or S3 clients. The start must not follow the end, and the inclusive day count must not exceed `HTTP_MAX_BACKFILL_DAYS`.

Dates run sequentially in ascending order through the same single-date primitive. Backfill stops at the first failed date and returns exit `4`; later dates are not attempted. Rerun the complete original range after the source or service recovers:

```bash
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-07"
```

Earlier complete pages are verified and reused, the failed date is retried safely, and then remaining dates continue. A repeated successful backfill retains the exact object/checksum inventory, writes zero new raw payload bytes, and reports reused pages through `duplicate_count`.

## Failure exercises and recovery commands

The local mock's scenario selectors are integration-test controls and are deliberately absent from the production CLI. The table describes the tested final state and the operator action after the injected/source condition is removed.

| Failure | Expected retained state | Recovery command/action |
| --- | --- | --- |
| `429 -> success` | No 429 body object; successful pages and manifests after one bounded retry | None when within budget; otherwise rerun the same `run`/`backfill` after rate limiting clears |
| Permanent 500 | Earlier pages may be complete; no raw object for the failed non-200 page; terminal event/summary reports retry exhaustion | Fix/clear the source fault, then rerun the same command |
| Permanent 4xx | No raw object for the failed response and exactly one request | Correct source/date/configuration, then rerun the corrected command |
| Malformed HTTP 200 | Exact malformed bytes plus terminal `failed` page manifest | Retain evidence; unchanged rerun re-surfaces failure, corrected bytes require a reviewed new logical identity |
| Failure after some pages | Earlier page pairs remain complete; the non-200 failed page is absent | Clear the fault and rerun the same command; completed pages are reused |
| Stop after payload/before manifest | Verified immutable payload without manifest (`payload_only`) | Rerun the same command; only the missing manifest is created |

## Limitations

- One process must own a logical batch at a time. Garage has no versioning, and conditional create is not a foundation of this contract; concurrent writers remain unsafe.
- Backfill is sequential and fail-fast. Parallel and continue-on-error modes are not implemented.
- Authentication schemes and arbitrary REST connectors are outside BI-050.
- Raw immutability can expose upstream mutation as a conflict. Automatic deletion, overwrite, and cleanup-based repair are intentionally forbidden.
- The CLI does not list or download objects. Inspect retained objects through a standard S3 client using the configured bucket and exact keys.
