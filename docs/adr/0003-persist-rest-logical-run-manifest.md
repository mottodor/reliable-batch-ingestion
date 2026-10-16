# ADR 0003: Persist one immutable REST logical-run manifest

- Status: Accepted
- Date: 2026-10-15

## Context

The product specification requires REST ingestion to preserve every raw page, persist a manifest for the complete logical run, and repeat without duplicating successfully persisted pages. The existing REST contract preserves each page under a deterministic raw key and writes one immutable schema-version-1 `Manifest` beside that page. A physical attempt also emits a final `RunSummary`, but that summary is not persisted.

After interruption and replay, a complete page set may legitimately contain page manifests with different `run_id` values. Those page pairs remain valid immutable evidence, but neither an individual page manifest nor the ephemeral summary states durably that the exact logical page set completed. `BI-090-F02` therefore found that the current page set does not satisfy the explicit complete-logical-run manifest requirement.

Changing manifest cardinality or object-key placement is architectural. The decision must preserve all existing raw page bytes and schema-version-1 page manifests, converge at one logical `batch_id`, remain bounded at the 9,999-page ceiling, and work through the standard S3 API under the sequential-writer limitation in ADR 0001. It must not invent a source payload, depend on an `ETag` as a checksum, or imply exactly-once delivery.

This ADR defines the binding contract for BI-092. Acceptance of the decision does not make the aggregate object exist in the current runtime; BI-092 is responsible for implementation, tests, inventory updates, and current-behavior documentation.

## Decision

Persist exactly one immutable, API-specific logical-run manifest at one deterministic key after, and only after, every required page pair has been verified as succeeded.

### Cardinality, identity, and key

For a successful API logical batch with `N` pages, durable cardinality is exactly:

- `N` immutable raw page objects;
- `N` unchanged schema-version-1 page manifests, one for each raw page;
- `1` immutable API logical-run manifest with no companion raw payload.

The logical-run manifest uses the existing normalized `BatchIdentity`:

```text
source_name
source_type = api
source_date
parameters = {
  "page-size": <canonical decimal string>,
  "resource-path": <allow-listed descriptor path>
}
batch_id = sha256(canonical BatchIdentity UTF-8 bytes)
```

Its exact key is:

```text
manifests/api/{source_name}/source_date={YYYY-MM-DD}/batch_id={batch_id}/logical-run.manifest.json
```

The key contains neither `run_id` nor an attempt number. It is reserved for API completion metadata, is not produced by `manifest_key_for()`, and cannot collide with the existing `page-NNNN.json.manifest.json` namespace.

For the two current sources on `2026-09-01` with `page-size=25`, examples are:

```text
manifests/api/fx-rates/source_date=2026-09-01/batch_id=150b7a9c57cadf18d4d1f5e51fc66575da0d3934b97cf79dac3fc91f3c2becf6/logical-run.manifest.json
manifests/api/shipping-statuses/source_date=2026-09-01/batch_id=d1b4d7361db024cb942708a5d45ca1731addb831f9551299f623f8e4d5a2a185/logical-run.manifest.json
```

The aggregate is metadata, not a synthetic source payload. There is no object under `raw/` corresponding to it.

### Independent schema and exact fields

The aggregate has a separate strict schema and parser. Its `schema_version` is the version of `api_logical_run_manifest`, initially `1`; it neither increments nor reinterprets schema version 1 of the existing one-payload `Manifest`.

The top-level JSON object contains exactly these fields:

| Field | JSON type | Version 1 rule |
| --- | --- | --- |
| `manifest_type` | string | Exactly `api_logical_run_manifest`. |
| `schema_version` | integer | Exactly `1`. Booleans are not integers. |
| `source_name` | string | Canonical normalized name of an allow-listed API descriptor. |
| `source_type` | string | Exactly `api`. |
| `source_date` | string | Canonical calendar date in `YYYY-MM-DD`. |
| `batch_id` | string | Exactly 64 lowercase hexadecimal characters and equal to the ID rederived from the document identity. |
| `identity_parameters` | object | Exactly `page-size` and `resource-path`, both strings equal to the normalized identity values for the resolved descriptor. `page-size` is a canonical decimal string in `1..100`. |
| `completion_run_id` | string | `run_` followed by 32 lowercase hexadecimal characters; the physical attempt that first committed completion. |
| `completed_at` | string | A timezone-aware UTC instant in `YYYY-MM-DDTHH:MM:SS.ffffffZ` form, fixed when the object is first created. |
| `status` | string | Exactly `succeeded`; version 1 has no other persisted aggregate status. |
| `page_count` | integer | In `1..9999` and equal to the length of `pages`. |
| `record_count` | integer | In `0..999900`, equal to the sum of page-entry `record_count` values and to the validated source total. |
| `pages` | array | Exactly `page_count` entries in strictly increasing page order, with no gaps or duplicates. |

Each `pages` entry contains exactly these fields:

| Field | JSON type | Version 1 rule |
| --- | --- | --- |
| `page_number` | integer | The entry's one-based position in `1..page_count`. |
| `object_key` | string | The exact canonical raw key ending in `page-{page_number:04d}.json` for this identity. |
| `page_manifest_key` | string | The exact existing one-to-one manifest key for `object_key`. |
| `payload_content_length` | integer | Actual raw byte length in `0..9223372036854775807`. |
| `payload_checksum_sha256` | string | SHA-256 of the actual raw bytes as 64 lowercase hexadecimal characters. |
| `page_manifest_checksum_sha256` | string | SHA-256 of the exact stored page-manifest bytes as 64 lowercase hexadecimal characters. |
| `record_count` | integer | Validated item count in `0..page-size`; all page counts and their sum must satisfy the existing pagination rules. |

Unknown or missing fields, duplicate JSON member names, unknown manifest types or versions, noncanonical strings, wrong JSON types, duplicate or out-of-order entries, inconsistent counts, and identity/key mismatches are invalid. An aggregate reader must resolve the API descriptor and rederive `batch_id`, every page raw key, and every page-manifest key; stored strings are never accepted only because they have a plausible prefix.

The page-manifest checksum binds the aggregate to the exact immutable schema-version-1 page-manifest bytes, including each page's original `run_id`, `extracted_at`, and attempt. Page manifests from multiple physical attempts are therefore valid when every referenced page pair independently satisfies the completion predicate.

### Canonical bytes and byte bound

Serialization is UTF-8 JSON with:

- one top-level object and no byte-order mark or trailing newline;
- object keys sorted lexicographically at every level;
- `,` and `:` separators with no insignificant whitespace;
- JSON strings emitted without ASCII escaping where JSON permits it;
- `pages` retained in strictly increasing `page_number` order;
- canonical integer and timestamp forms defined above.

The object content type is `application/json`. Its SHA-256 is calculated from these exact serialized bytes and verified by streaming the stored bytes; an S3 `ETag` is not checksum evidence.

The aggregate's declared and actual byte lengths must each be at most **8 MiB (8,388,608 bytes)** before parsing. Both limits are enforced: declared length alone is not trusted, and reads stop when actual bytes would exceed the bound. This bound is separate from the 64 KiB schema-version-1 page-manifest bound. With the exact version-1 schema, maximum-width allowed values, the longer current `shipping-statuses` key layout, and all 9,999 entries, canonical output is 6,098,762 bytes, leaving more than 2 MiB of margin. A generated aggregate that exceeds 8 MiB is a handled contract failure and is not published.

### Completion predicate and publication order

An API logical batch is eligible for aggregate publication only when all of the following are true:

1. Strict source pagination has reached and validated its final page.
2. Observed page numbers are exactly `1..page_count`, with no gap or duplicate, and `page_count <= 9999`.
3. Page item counts, `total_pages`, `total_records`, `next_page`, and the aggregate `record_count` satisfy the existing strict pagination rules.
4. Every referenced raw page exists at its exact derived key and its actual length and SHA-256 equal the aggregate entry.
5. Every referenced page manifest exists at its exact derived key, is at most 64 KiB, parses as strict schema version 1, has terminal status `succeeded`, links to the same raw key/length/checksum/content type, and matches the aggregate source, date, and `batch_id`.
6. SHA-256 of each exact stored page-manifest byte sequence equals `page_manifest_checksum_sha256`.
7. The ordered page inventory can be canonically serialized within 8 MiB.
8. The aggregate is immutably written and byte-verified, or a previously written aggregate at the exact key is fully reconciled as complete.

Publication order is therefore raw page, succeeded page manifest, repeated through the validated final page, then logical-run manifest, then terminal success event and final `RunSummary`. A stop after the final page manifest but before aggregate verification leaves a recoverable incomplete logical batch and must not emit or claim logical completion.

`completion_run_id` identifies only the attempt that first publishes the aggregate. It does not replace page-manifest `run_id` values and does not claim that one attempt fetched or wrote every page. `completed_at` records that first publication. Neither field changes on replay.

### Terminal status and failed attempts

Schema version 1 persists only `status = "succeeded"`. Partial, interrupted, and failed physical attempts do not write an aggregate at the deterministic completion key.

Persisting a failed immutable object at that key would permanently block a later safe convergence to success. Mutating a failed object to succeeded would violate immutability. Persisting one object per failed `run_id` would create an unbounded attempt history and would not provide one convergent logical completion record. Failures remain observable through retained raw/page-manifest evidence and the existing terminal events and summaries.

### Reconciliation and recovery

Correctness uses the one exact aggregate key and the exact page keys in a strictly validated `1..page_count` inventory. Prefix listing is not a completeness oracle.

| Observed durable state | Classification and required behavior |
| --- | --- |
| Aggregate absent and required page evidence is incomplete | **Absent/incomplete.** Retain evidence, continue normal source ingestion on rerun, and do not claim completion. |
| Aggregate absent and a required page has a terminal failed manifest | **Absent/failed.** Re-surface the existing page failure under current rules; do not write an aggregate. |
| Aggregate absent and every required page pair satisfies the completion predicate | **Eligible.** Build one bounded aggregate with the current physical attempt's `completion_run_id` and publish it immutably. |
| Aggregate present, strict, within bounds, and all exact referenced evidence matches | **Complete.** Reuse it without rewriting its bytes, `completion_run_id`, or `completed_at`, even when the current run or individual page runs differ. |
| Aggregate present but oversized, malformed, noncanonical, duplicate-keyed, or unsupported | **Corrupt.** Fail visibly; do not replace, delete, or reinterpret it. |
| Aggregate present with wrong identity, key derivation, count, ordering, checksum, page status, or other non-missing mismatch | **Conflicting.** Fail visibly and retain all evidence. |
| Aggregate present but a referenced raw page or page manifest is missing | **Orphaned completion claim.** Fail visibly and retain the aggregate and remaining evidence; never fabricate the missing object. |
| Aggregate write returns an uncertain outcome | **Uncertain write.** Inspect the deterministic key. Within the same attempt, reuse a byte-identical, fully valid object or retry the same frozen bytes if the key is absent; report conflict for any other present bytes. A later rerun accepts any fully valid complete aggregate as the already committed first completion and preserves it. |

A present aggregate is never judged conflicting merely because a later attempt would have generated a different `completion_run_id` or `completed_at`; those are first-publication facts. Conversely, a syntactically valid aggregate is not accepted without streaming and verifying all referenced page and page-manifest bytes.

Objects that happen to exist under the same prefix but are not exact keys required by the validated inventory do not prove or disprove completion. Investigation may use listing, but reconciliation must not depend on it.

### Replay and compatibility

- Existing raw pages and schema-version-1 page manifests remain byte-for-byte unchanged. This ADR performs no migration or storage write.
- The existing one-payload `Manifest`, `Manifest.from_json()`, `manifest_key_for()`, and its 64 KiB read bound do not parse or derive the aggregate.
- After BI-092, replay of a successful pre-aggregate batch follows normal source fetching and exact page reconciliation, then creates only the missing logical-run manifest after all page evidence passes the completion predicate.
- A complete aggregate is fully validated and reused. Replay never rewrites its completion fields, page manifests, or raw pages.
- Mixed page-manifest `run_id` values are valid and remain bound through each page-manifest checksum. Failed page manifests remain terminal and cannot be upgraded in place.
- Backfill continues to invoke the same single-date primitive sequentially and fail fast. Each successful date must reconcile its aggregate before that date reports success.
- Existing exact-key readers of page manifests continue to work. Readers that claim a complete API inventory must be updated by BI-092 to require and validate the aggregate.
- Normal ingestion still fetches and validates source pages. The aggregate is durable completion evidence, not an offline replacement for the source and not a mutable checkpoint.
- Current `RunSummary` field meanings remain unchanged: `bytes_written` counts only newly created raw payload bytes; `duplicate_count` counts reused successful page pairs; manifest bytes and aggregate reuse do not alter either counter. Existing event and CLI field shapes remain unchanged.

### Delivery and storage semantics

The implementation must use only the standard S3 API and the shared immutable-write/streamed-verification principles. SHA-256 of actual bytes is authoritative. Garage bucket versioning and conditional create are not contract foundations.

This decision is safe only for the existing sequential-writer model. Without separate coordination, concurrent writers can race between inspection and write and are unsupported. The claim remains deterministic keys, immutable idempotent writes, and safe sequential redelivery—not exactly-once delivery.

## Alternatives considered

| Alternative | Decision | Reason |
| --- | --- | --- |
| Keep page manifests plus emitted `RunSummary` | Rejected | The summary is ephemeral and no persisted object states that the complete exact page set succeeded; this is the `BI-090-F02` gap. |
| Change schema-version-1 page manifests to carry aggregate state | Rejected | It would rewrite or fork existing immutable evidence, break one-manifest-per-payload cardinality, and affect file and PostgreSQL manifest readers. A page also cannot immutably know completion before later pages exist. |
| Represent the aggregate as a synthetic raw payload plus an ordinary `Manifest` | Rejected | There is no corresponding source payload. Metadata would masquerade as raw business data, distort raw cardinality and `bytes_written`, and weaken the raw-preservation boundary. |
| Persist one aggregate per physical `run_id` | Rejected | Attempts are unbounded, keys would not converge on one logical completion record, partial/failed attempts would accumulate, and replay would require choosing among competing attempt documents. |
| Persist one immutable API-specific aggregate at the deterministic logical-batch key | Accepted | It preserves all page evidence, provides one inspectable completion record, supports mixed-attempt recovery, converges on replay, and permits a separate strict bounded schema. Its cost is a new parser, key helper, and exact reconciliation path in BI-092. |

A mutable deterministic aggregate was also rejected: S3 does not provide the transaction or versioning semantics needed to make status transitions the reliability foundation, and mutation would lose first-published evidence.

## Consequences

Benefits:

- one durable object explicitly states that the exact bounded logical page set completed;
- page bytes and existing schema-version-1 manifests remain immutable and compatible;
- replay of partial, mixed-attempt, and legacy page-only batches converges without duplicating raw data;
- exact checksums bind both raw payloads and page-manifest bytes;
- operators can distinguish incomplete page evidence from a complete logical batch without prefix inference.

Costs and limitations:

- BI-092 must add and maintain a separate aggregate model, parser, key helper, bounded reader, and reconciliation state machine;
- successful REST ingestion has one additional verified S3 object write and must delay success output until that write is reconciled;
- verifying a complete aggregate can require bounded streaming reads of all referenced page and page-manifest objects;
- an immutable corrupt, conflicting, or orphaned aggregate blocks automatic completion and requires explicit investigation rather than overwrite;
- the 8 MiB format bound and schema version must be reconsidered before adding fields or increasing cardinality;
- no attempt history, concurrent-writer safety, or exactly-once guarantee is introduced.

## Required validation

BI-092 must provide, at minimum:

- unit tests for both source key examples, identity rederivation, strict field/type validation, duplicate-key rejection, canonical bytes, ordered page linkage, count relationships, the 8 MiB declared/actual read limit, and the 9,999-entry maximum;
- unit tests for every absent/incomplete/failed/eligible/complete/corrupt/conflicting/orphaned/uncertain-write row above;
- proof that creation occurs only after all exact raw/page-manifest pairs succeed and before the terminal success event and `RunSummary`;
- proof that payload and page-manifest checksums are calculated from actual streamed bytes and never inferred from `ETag`;
- Garage integration for first creation, complete replay, interruption after final page manifest but before aggregate publication, uncertain-write recovery, malformed/conflicting/orphaned aggregate handling, and legacy page-only upgrade;
- replay coverage with page manifests from different `run_id` values and assertions that no existing raw, page-manifest, or aggregate bytes are rewritten;
- backfill, exact inventory, demo inspection, and operator-document updates that require one aggregate per successful date and preserve current metric meanings;
- assertions and wording that retain standard-S3 access, sequential writers, and no exactly-once claim.

BI-091 itself changes no Python, tests, dependencies, runtime behavior, current object-store contents, or current-behavior README/API contract claims.

## Reconsidering the decision

A replacement ADR is required before changing aggregate cardinality, key placement, completion status semantics, identity fields, the separate schema/version strategy, or immutable replay behavior.

Reconsider the format if a real requirement introduces more than 9,999 pages, makes the exact inventory exceed 8 MiB, requires durable per-attempt history, or requires coordinated concurrent writers. Such a decision must define bounded migration and reader compatibility, preserve raw evidence, and state the actual storage/delivery guarantees without retroactively rewriting existing aggregates.
