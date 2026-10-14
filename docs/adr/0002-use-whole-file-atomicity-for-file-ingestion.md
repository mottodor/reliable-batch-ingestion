# ADR 0002: Use whole-file atomicity for file ingestion

- Status: Accepted
- Date: 2026-10-14

## Context

Supplier-price files can be valid, empty, structurally invalid, or contain a mixture of valid and invalid rows. File ingestion must choose whether a mixed file publishes its valid rows while rejecting only invalid rows, or treats the delivered file as one acceptance unit.

The project must preserve the original payload, retain invalid input with a machine-readable reason, avoid uncontrolled duplicates, and recover safely while moving an object through `landing -> archive | rejected`. The existing contracts define one immutable raw payload and one manifest per payload. They do not define derived accepted-row and rejected-row payloads.

The term **file atomicity** here describes the business acceptance boundary. S3-compatible storage does not provide an atomic rename, so physical movement remains a recoverable `copy -> verify -> delete` operation.

## Decision

Use **whole-file atomicity**. One delivered file is one indivisible validation and acceptance unit.

The ingestion flow must apply these rules:

1. Preserve and checksum the delivered bytes without modifying, normalizing, filtering, or reserializing them.
2. Validate the allowed source, file name, expected checksum when supplied, format, schema, required fields, field types, business constraints, and emptiness.
3. Accept the file only if the complete file passes every required validation.
4. If any row fails validation, reject the entire file. No row from that file is published as accepted output.
5. Treat an empty file, an unreadable file, and a structurally or schema-invalid file as whole-file rejections.
6. Move a fully valid original payload to `archive`; move a rejected original payload to `rejected`. In both cases, retain the exact original bytes.
7. Record a stable machine-readable rejection code and bounded diagnostic details. Row-level details may identify invalid rows, but they are diagnostics only and do not create accepted or rejected row payloads.
8. Keep duplicate classification separate from row validity. Recognizing a redelivery must not create a partially accepted result or overwrite an earlier immutable payload.

For a parseable mixed file containing `N` records, the terminal run summary reports `records_read = N`, `records_accepted = 0`, and `records_rejected = N`, because none of its records cross the acceptance boundary. An empty-file rejection reports zero for all three record counters and remains observable through its rejected status and rejection code. If parsing fails before a trustworthy total is available, counters describe only records actually read; the rejection code remains authoritative.

Validation may stream or process bounded batches to satisfy memory limits, but it must not publish accepted rows before whole-file validation succeeds.

Physical movement uses the shared storage operation and retains its recovery semantics:

- source only: copy, verify, then delete the source;
- source and identical destination: verify the destination, then retry source deletion;
- destination only with the expected bytes: treat movement as already completed;
- conflicting destination or missing source and destination: fail visibly without deleting evidence.

The ingestion operation must not report the disposition as complete until the destination is verified and the source is confirmed absent. A transient state containing identical source and destination objects is recoverable and does not mean the file was split or accepted twice.

This decision does not change manifest schema version 1 or the existing raw/manifest object-key contract. The BI-060 implementation plan must define how rejection diagnostics and landing/archive/rejected keys are represented; any change to the existing manifest or object-key contract requires a separate ADR.

## Consequences

Benefits:

- a delivered business file has one unambiguous outcome;
- downstream consumers never observe a partial supplier batch;
- the original CSV, JSON, or Parquet bytes remain the audit artifact;
- CSV, JSON, and Parquet share the same acceptance semantics;
- recovery can reuse the existing checksum verification and `copy -> verify -> delete` primitive;
- no new row-level output format, manifest cardinality, or merge contract is required.

Costs and limitations:

- one bad row prevents otherwise valid rows in the same file from being accepted;
- suppliers must correct and redeliver the complete file;
- row-level diagnostics are needed to make correction practical;
- this is not a transactional or atomic S3 rename, and a recoverable interval may contain two identical copies;
- concurrent writers remain outside the core contract.

## Alternatives considered

### Split accepted and rejected records

Declined for the core lab. It would require deterministic derived payloads for both subsets, format and ordering rules for CSV/JSON/Parquet, additional object keys and manifests, metrics for partial publication, and recovery across several independently written objects. It would also make the delivered file and the published records different artifacts.

### Accept valid rows and only log invalid rows

Declined because it silently weakens the supplier-file boundary, makes replay and reconciliation harder to explain, and does not retain rejected data as a first-class outcome.

### Stop at the first invalid row without preserving diagnostics

Declined because invalid input must have a clear machine-readable reason. Implementations may stop when further parsing is unsafe, but they must retain the original bytes and record the detected failure.

## Required validation

BI-060 tests must prove that:

- valid CSV, JSON, and Parquet files are archived byte-for-byte;
- empty, schema-invalid, unreadable, and mixed files are rejected byte-for-byte with stable machine-readable reasons;
- the mixed fixture publishes no accepted rows and reports whole-file counters;
- no validation path rewrites the original payload;
- same-name redelivery and same content under a different name remain distinguishable cases;
- interruption after copy but before source deletion converges on rerun without data loss or conflicting terminal outcomes;
- no code or documentation claims an atomic S3 rename or exactly-once delivery.

## Reconsidering the decision

Reconsider whole-file atomicity only if a real requirement permits partial supplier-batch publication. A replacement ADR must define deterministic accepted/rejected derivative formats and keys, manifest cardinality, metrics, idempotency, and multi-object recovery before split-row behavior is implemented.
