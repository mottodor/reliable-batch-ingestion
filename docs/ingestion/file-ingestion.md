# File ingestion contract

This document describes the implemented `BI-060` behavior for bounded, restart-safe supplier-price ingestion from CSV, JSON, and Parquet files. [ADR 0002](../adr/0002-use-whole-file-atomicity-for-file-ingestion.md) makes one delivered file the business acceptance unit. The identifier, manifest, summary, source-schema, and shared S3 rules remain governed by [`core-contracts.md`](../contracts/core-contracts.md), [`source-contracts.md`](../contracts/source-contracts.md), and [`storage-contract.md`](../contracts/storage-contract.md).

The delivery claim is limited to deterministic keys, immutable/idempotent writes, and safe sequential redelivery. S3 movement is recoverable `copy -> verify -> delete`, not an atomic rename. This is not an exactly-once claim, and concurrent writers for the same logical delivery are unsupported.

## Commands and admission boundary

Start Garage, then stage and ingest one local file:

```bash
make up
make ingest-file ARGS="run --source supplier-prices --date 2026-09-01 --path /path/to/supplier-prices-2026-09-01.csv"
make ingest-file ARGS="run --source supplier-prices --date 2026-09-01 --path /path/to/supplier-prices-2026-09-01.parquet --expected-sha256 <64-lowercase-hex>"
```

Resume a previously staged or partially moved delivery using its complete canonical landing key:

```bash
make ingest-file ARGS="resume --landing-key 'landing/file/supplier-prices/source_date=2026-09-01/format=csv/content_sha256=<sha256>/expected_sha256=none/supplier-prices-2026-09-01.csv'"
```

`run` performs these admission checks before creating an S3 client or object:

- the date uses canonical `YYYY-MM-DD` form;
- an optional expected checksum is exactly 64 lowercase hexadecimal characters;
- the path can be opened as one seekable regular file;
- the local byte count does not exceed `FILE_MAX_BYTES`;
- the source and basename can be represented by the existing canonical identifier and manifest contracts.

The local file is streamed in 64 KiB chunks to calculate its exact length and SHA-256. Its basename, not its local directory, becomes `original_file_name`. The exact stream is then written immutably to the deterministic landing key; the storage boundary verifies the stream again, so a file changed between admission hashing and upload cannot be published with stale identity metadata.

An OS-level missing, unreadable, non-regular, oversized, or unrepresentable local path is an admission failure and creates no S3 state. Once bytes have been staged, a business or parser validation failure is retained under canonical raw and rejected keys. This distinction is necessary because bytes that were never readable cannot be preserved.

`resume` does not open the original local file. It strictly parses one canonical BI-060 landing key and reconstructs all non-secret identity inputs from that key. It can continue when landing exists, when landing and destination both exist after an interrupted delete, or when landing is absent but canonical raw evidence remains.

The production source allow-list contains only `supplier-prices`. A different but syntactically canonical source can enter the retained validation flow and is rejected as `unsupported_source`; unsafe source text fails admission.

## Filename and declared format

A valid supplier filename is one manifest-safe basename matching:

```text
supplier-prices[-<label>...]-YYYY-MM-DD.(csv|json|parquet)
```

Each optional label is one or more lowercase ASCII letters or digits. The filename date must be canonical and equal the explicit `--date`. Examples include:

```text
supplier-prices-2026-09-01.csv
supplier-prices-original-2026-09-01.json
supplier-prices-region1-extra2-2026-09-01.parquet
```

Format is declared only by the lowercase extension; content sniffing is not used. The stable mapping is:

| Declared format | Content type | Canonical raw payload name |
| --- | --- | --- |
| `csv` | `text/csv` | `payload.csv` |
| `json` | `application/json` | `payload.json` |
| `parquet` | `application/vnd.apache.parquet` | `payload.parquet` |
| `unknown` | `application/octet-stream` | `payload.bin` |

A representable filename with an unrelated stem, uppercase/unsupported extension, invalid embedded date, or otherwise invalid business grammar is staged and retained as `invalid_file_name`. A basename containing a path separator, surrounding whitespace, or another value that cannot satisfy the manifest-safe segment contract fails admission instead.

## Logical identity and deterministic keys

One file `BatchIdentity` contains:

- canonical source name;
- source type `file`;
- explicit source date;
- `content-sha256`: SHA-256 of the exact delivered bytes;
- `declared-format`: `csv`, `json`, `parquet`, or `unknown`;
- `expected-sha256`: the supplied checksum or the literal `none`;
- `validation-contract`: `supplier-prices-v1`.

The original filename is deliberately absent from logical identity. Identical bytes delivered under another valid filename therefore have the same `batch_id`, raw key, and final disposition key, but a distinct filename-bearing landing key. Different bytes, declared formats, expected checksums, validation-contract versions, sources, or dates produce a different logical identity.

For the committed `fixtures/supplier-prices/canonical/valid.csv` payload, source date `2026-09-01`, filename `supplier-prices-2026-09-01.csv`, and no expected checksum, the exact checksum, identity, and keys are:

```text
content_sha256=9e97174cbc43aef5f3277a9e5142991d472c742d55a150a100051d9794893d2f
batch_id=d074472a37c4490ef1497196969783088a93f1fc17199fa64817b388d801ccef

landing/file/supplier-prices/source_date=2026-09-01/format=csv/content_sha256=9e97174cbc43aef5f3277a9e5142991d472c742d55a150a100051d9794893d2f/expected_sha256=none/supplier-prices-2026-09-01.csv
raw/file/supplier-prices/source_date=2026-09-01/batch_id=d074472a37c4490ef1497196969783088a93f1fc17199fa64817b388d801ccef/payload.csv
manifests/file/supplier-prices/source_date=2026-09-01/batch_id=d074472a37c4490ef1497196969783088a93f1fc17199fa64817b388d801ccef/payload.csv.manifest.json
archive/file/supplier-prices/source_date=2026-09-01/batch_id=d074472a37c4490ef1497196969783088a93f1fc17199fa64817b388d801ccef/payload.csv
rejected/file/supplier-prices/source_date=2026-09-01/batch_id=d074472a37c4490ef1497196969783088a93f1fc17199fa64817b388d801ccef/reason=business_rule_violation/payload.csv
```

A conflicting object at any deterministic key is not overwritten. Keys contain no local directory, credentials, free-form diagnostics, or source field values.

## Supplier-price schema and validation

The exact required field order is:

```text
supplier_id, product_id, price_minor_units, currency, effective_date
```

Missing and extra fields are rejected. Values are not trimmed, normalized, reordered, filtered, or reserialized. Every valid row satisfies:

| Field | Required type and rule |
| --- | --- |
| `supplier_id` | Non-empty string matching `SUP-[0-9]{3}` |
| `product_id` | Non-empty string matching `SKU-[0-9]{4}` |
| `price_minor_units` | Strict integer, excluding booleans, greater than zero |
| `currency` | Exact value `EUR`, `GBP`, or `USD` |
| `effective_date` | Canonical date equal to the command source date |

Format-specific rules are:

- **CSV** — strict UTF-8, standard-library strict CSV parsing, the exact header once, exact column count, and incremental record processing. Integer/date text must already be canonical; whitespace and signs are not normalized.
- **JSON** — strict UTF-8, one top-level array, and objects with the exact keys in contract order and strict JSON value types. The complete JSON value is loaded only after the file-byte bound has been enforced.
- **Parquet** — a seekable verified stream and this exact non-null Arrow schema in order: `supplier_id: string`, `product_id: string`, `price_minor_units: int64`, `currency: string`, `effective_date: date32`. Validation uses `ParquetFile.iter_batches(batch_size=FILE_PARQUET_BATCH_ROWS)` and never a whole-table read API.

A malformed parser input is a retained `malformed_file` rejection. Schema mismatch is separate from a missing required value, an invalid field type, or a business-rule violation.

## Bounds and resource ownership

All settings are loaded explicitly from the ignored `.env`; importing file modules reads no configuration, opens no local file, and creates no S3 client.

| Setting | Implemented meaning |
| --- | --- |
| `FILE_MAX_BYTES` | Maximum admitted local and downloaded object bytes; JSON's explicit input-memory ceiling |
| `FILE_SPOOL_THRESHOLD_BYTES` | Maximum in-memory spool size before temporary-file spill; must not exceed `FILE_MAX_BYTES` |
| `FILE_MAX_RECORDS` | Positive maximum records inspected in one delivery |
| `FILE_PARQUET_BATCH_ROWS` | Positive maximum rows requested from each Parquet `iter_batches()` batch |
| `FILE_MAX_DIAGNOSTIC_DETAILS` | Positive maximum value-free details retained in the terminal event |
| `S3_TOTAL_MAX_ATTEMPTS` | Botocore total attempts for each S3 request, including the initial request |
| `S3_CONNECT_TIMEOUT_SECONDS` | Bounded S3 connect timeout |
| `S3_READ_TIMEOUT_SECONDS` | Bounded S3 read timeout |

The shared storage boundary downloads exact landing/raw keys in 64 KiB chunks, verifies SHA-256 from actual bytes, enforces both declared and observed length bounds, and yields a rewound seekable `SpooledTemporaryFile`. It closes every S3 response body before validation and closes the spool on every context exit. CSV is record-streamed, Parquet is batch-iterated, and JSON memory is bounded by `FILE_MAX_BYTES`.

There is no file-specific retry loop. Bounded Botocore retries own individual S3 request retry behavior; `retry_count` therefore remains zero.

## Whole-file atomicity and persistence order

Under ADR 0002, one delivered file is one indivisible business acceptance unit:

- a completely valid file is accepted and its exact bytes are moved to `archive`;
- any required validation failure rejects the complete file and moves its exact bytes to `rejected`;
- no accepted-row subset or transformed row output is created;
- empty, malformed, schema-invalid, mixed-validity, and business-invalid files retain the original bytes;
- row-level findings are correction diagnostics, not independently published records.

For one delivery, processing occurs in this order:

1. download and byte-verify exact landing evidence, falling back to canonical raw evidence when landing is absent;
2. reconcile the canonical raw/manifest pair by exact key, length, checksum, content type, and manifest linkage;
3. create and verify immutable raw bytes when absent, or reuse a matching raw object;
4. validate only after canonical raw preservation;
5. derive the one expected archive/rejected key and probe every finite alternative disposition for conflicts;
6. reconstruct landing from verified raw bytes if movement still needs a source and landing is absent;
7. converge movement with shared `copy -> verify -> delete`, preserving content type and checksum metadata;
8. create one immutable terminal manifest only after destination verification and confirmed landing absence.

The raw payload is never mutated after successful creation. Archive/rejected objects are exact-byte disposition copies and do not receive separate manifests.

A valid file receives manifest status `succeeded`; an invalid file receives `rejected`. File manifests use schema version 1, `attempt=1`, the admitted original basename, and point only to the canonical raw object. Rejection reason/details are deliberately not added to manifest schema version 1: the durable primary reason is in the rejected key, while bounded details are in structured output.

## Rejection codes and bounded diagnostics

When several failures apply, the first code in this stable precedence determines `rejection_code` and the `reason=<code>` rejected-key segment:

| Precedence | Code | Meaning |
| ---: | --- | --- |
| 1 | `unsupported_source` | Canonical source is outside the production allow-list |
| 2 | `invalid_file_name` | Representable basename does not satisfy the supplier filename/format grammar |
| 3 | `source_date_mismatch` | Valid filename date differs from the explicit source date |
| 4 | `checksum_mismatch` | Optional expected SHA-256 differs from actual delivered bytes |
| 5 | `malformed_file` | Declared parser cannot safely parse the payload |
| 6 | `schema_mismatch` | Required field set/order or Parquet schema is wrong |
| 7 | `empty_file` | Format is valid but contains no records |
| 8 | `missing_required_value` | A required field is null or empty |
| 9 | `invalid_field_type` | A field does not have its strict format-specific type |
| 10 | `business_rule_violation` | A typed value violates identifier, positivity, currency, or date rules |
| 11 | `record_limit_exceeded` | The delivery exceeds `FILE_MAX_RECORDS` |

Each structured detail contains only:

```json
{"code":"business_rule_violation","record_number":2,"field":"price_minor_units"}
```

`record_number` is one-based when available; `field` is one canonical supplier field or null. Details never contain rejected values, complete rows, payload bytes, local absolute paths, credentials, or backend exception text. They are deterministic, capped by `FILE_MAX_DIAGNOSTIC_DETAILS`, and accompanied by `details_truncated` when further findings were omitted.

## Counters and structured output

A terminal accepted or retained-rejection operation emits newline-delimited JSON: one `file_ingestion` event followed by one `run_summary`.

The terminal event includes outcome, nullable primary rejection code, bounded details, source/date, current physical `batch_id`/`run_id`, all relevant keys, admitted original filename, declared format, and duplicate class. A pre-terminal storage/state failure emits a secret-free `cli_error` instead of inventing a successful business outcome or manifest.

The BI-060 `RunSummary` fields mean:

| Field | Meaning |
| --- | --- |
| `records_read` | Records whose boundaries were trustworthy before validation completed or became unsafe |
| `records_accepted` | Equal to `records_read` only if the complete file is valid; otherwise zero |
| `records_rejected` | Equal to `records_read` for a rejected parseable file, or the trustworthy records reached before malformed parsing became unsafe |
| `pages_fetched` | Always zero for file ingestion |
| `bytes_written` | Newly created canonical raw payload bytes only; excludes landing/disposition copies and manifests |
| `retry_count` | Always zero; bounded Botocore request retries are not counted as application retries |
| `duplicate_count` | One for an understood complete-pair redelivery; otherwise zero |
| `duration_seconds` | Monotonic elapsed time for the restart-safe service attempt |
| `source_date`, `batch_id`, `run_id` | Trace identifiers for this physical attempt |

A parseable mixed three-record file reports `records_read=3`, `records_accepted=0`, and `records_rejected=3`. An empty rejection reports zero for all three record counters and remains visible through outcome, manifest status, rejected key, and rejection code. If parsing becomes unsafe, counters include only records already read with trustworthy boundaries; the rejection code is authoritative.

## Duplicate classification

Duplicate classification is independent of validation outcome:

| Class | Condition | Canonical effect |
| --- | --- | --- |
| `new_delivery` | No complete raw/manifest pair existed before this attempt | Create missing canonical objects and first terminal manifest |
| `same_name_redelivery` | Complete manifest exists and records the same original filename | Revalidate/converge current landing; preserve all canonical objects |
| `renamed_content` | Complete manifest exists for the same content identity but records another filename | Use distinct landing key, same raw/final keys; preserve all canonical objects |

A complete duplicate is revalidated from preserved exact bytes. A valid duplicate exits successfully; a rejected duplicate remains a handled rejection. Both report `duplicate_count=1` and `bytes_written=0` for canonical raw bytes.

The first terminal manifest is immutable. A duplicate never replaces its original `run_id`, timestamp, status, or `original_file_name`; the current event and summary carry the new physical attempt's `run_id`. If an interrupted first attempt left raw bytes without a manifest, the attempt that completes the manifest establishes original-name provenance.

## Recovery states

Recovery inspects deterministic exact keys; it does not infer correctness from prefix listings.

| Observed state | Rerun behavior | Operator action |
| --- | --- | --- |
| Landing only | Verify/download, create raw, validate, move, then write manifest | `resume` with the same landing key |
| Landing + raw, no manifest | Verify both, validate, move, then write only the missing manifest | `resume` with the same landing key |
| Landing + expected disposition, no manifest | Verify identical destination, retry landing deletion, then write manifest | `resume` with the same landing key |
| Raw + expected disposition, no landing/manifest | Read raw, recognize completed movement, then write manifest | `resume` with the original canonical landing key |
| Raw only, no landing/manifest | Read raw, reconstruct landing immutably, validate, move, then write manifest | `resume` with the original canonical landing key |
| Complete pair + newly staged identical landing | Revalidate, classify duplicate, converge landing, write no canonical object | Repeating `run` or using `resume` is safe |
| Landing and identical destination after delete failure | Verify destination, retry delete, converge to destination-only | `resume` with the same landing key |
| Landing + conflicting destination | Fail without overwrite or deletion; retain landing/raw evidence | Investigate the conflicting exact key; do not rerun destructively |
| Manifest only, malformed/wrong manifest linkage, or raw conflict | Fail explicitly without cleanup or fabricated repair | Investigate and repair retained storage state before rerun |
| Neither landing nor raw evidence | Fail as missing evidence | Recover the original bytes; never fabricate success |

If a process stops after verified copy but before landing deletion, both objects are expected to contain identical bytes. This is a recoverable intermediate state, not duplicate acceptance. The terminal manifest is written only after rerun has verified the destination and confirmed landing absence.

Transient storage/system failures do not create a terminal `failed` file manifest. Such a manifest could incorrectly block safe recovery; raw/disposition evidence and deterministic keys are the recovery journal.

## Exit codes

| Exit | Meaning |
| ---: | --- |
| `0` | Accepted new delivery or accepted duplicate |
| `2` | Invalid arguments or local-file admission failure; no bytes entered S3 through this command |
| `3` | Invalid/missing runtime configuration |
| `4` | Retained business rejection, recoverable ingestion/state conflict, or S3 operation failure |
| `5` | Runtime S3 client setup or cleanup failure |

A rejected file intentionally exits non-zero even though its bytes, reason-bearing disposition, terminal manifest, event, and summary were successfully retained.

## Failure exercises and recovery commands

| Failure | Expected retained state | Recovery command/action |
| --- | --- | --- |
| Schema/business-invalid or mixed file | Exact bytes under raw and one reason-bearing rejected key; one `rejected` manifest; landing absent; no accepted subset | Correct and redeliver the complete file. Changed bytes create a new content identity; retain the rejected evidence |
| Expected checksum mismatch | Exact bytes under raw and `reason=checksum_mismatch`; one `rejected` manifest | Confirm supplier checksum and bytes, then redeliver corrected bytes/checksum as a new identity |
| Same-name redelivery | Same landing/raw/final keys; original manifest/run ID unchanged; duplicate class `same_name_redelivery` | No recovery needed; repeating the same `run` is idempotent |
| Same content under another valid name | Different landing key, same batch/raw/final keys; original manifest/run ID unchanged; duplicate class `renamed_content` | No recovery needed; no second business-data copy is created |
| Stop after raw write or after final move but before manifest | Raw evidence remains; final destination may also remain; no terminal manifest | `make ingest-file ARGS="resume --landing-key '<original-canonical-landing-key>'"` |
| Copy succeeds but landing delete fails | Landing and destination both contain verified identical bytes; raw is preserved; manifest is not yet written | Run the same `resume`; it verifies destination, retries delete, and then writes the manifest |
| S3 temporarily unavailable | Existing deterministic objects remain unchanged; command emits `ingestion_failed` and exit `4` once bounded request retries end | Restore S3, then rerun the same `run` or `resume` |
| Conflicting deterministic object or manifest-only/corrupt state | Evidence is retained; nothing is overwritten or automatically deleted | Inspect exact keys and repair only through an explicit reviewed operator action, then rerun |

## Limitations

- One process must own a logical delivery at a time. Garage has no versioning, and check-then-write cannot coordinate concurrent writers.
- The CLI ingests one file at a time; directory watching, scheduling, multipart upload, and parallel intake are not implemented.
- JSON parsing may use memory up to `FILE_MAX_BYTES`; CSV and Parquet validation are streamed/batched, while the verified spool may spill to local disk.
- There is no accepted/rejected row split, normalized output, transformed dataset, schema evolution, XLSX support, diagnostic sidecar, second manifest, or mutable attempt journal.
- The CLI does not list or download objects. Inspect retained state with a standard S3 client against the configured bucket and exact keys.
- Automatic deletion or overwrite is never a conflict-recovery strategy. The contract prioritizes preserved evidence and visible failure over silent cleanup.
