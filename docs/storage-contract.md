# Shared S3 storage contract

This document defines the BI-030 application boundary implemented by `batch_ingestion.storage`. It supplements the object-key and manifest rules in [`core-contracts.md`](core-contracts.md) and the Garage constraints in [ADR 0001](adr/0001-use-garage-for-s3-compatible-storage.md).

## Configuration and client construction

`create_s3_client(StorageConfig)` constructs a client only when called. Importing the package does not read environment variables, resolve credentials, or open a connection.

The client receives the endpoint, region, access key, and secret key explicitly and uses:

- Signature Version 4;
- the configured S3 addressing style (`path` locally);
- Botocore standard retry mode;
- `S3_TOTAL_MAX_ATTEMPTS`, including the initial request;
- `S3_CONNECT_TIMEOUT_SECONDS` and `S3_READ_TIMEOUT_SECONDS`.

Botocore owns request retries; the storage boundary does not wrap it in another retry loop. This prevents multiplicative retry budgets. A multi-call operation can still fail between requests and must be rerun using the recovery states below.

## Checksum authority and memory bounds

SHA-256 of the actual payload bytes is the content-identity authority. `ETag` is never interpreted as SHA-256. The boundary streams local and remote raw objects in 64 KiB chunks and closes every S3 response body. Raw objects are not loaded wholly into memory.

An immutable write requires a seekable binary stream positioned at byte zero, expected byte length, expected lowercase SHA-256, content type, and key. It verifies the local stream before any write and rewinds it. The checksum is also stored as `checksum-sha256` user metadata for inspection, but metadata alone is never accepted as proof of content.

Manifest reads are different because manifests are intentionally small: their declared and actual size must not exceed 64 KiB before strict `Manifest.from_json()` parsing.

## Immutable write

`S3Storage.write_immutable()` has two successful outcomes:

- `created` — the key was absent, `PutObject` completed, and a streamed read verified the stored length, SHA-256, content type, and checksum metadata;
- `unchanged` — the key already held the same verified bytes, so no `PutObject` occurred.

Different bytes at an existing deterministic key raise `ObjectConflictError` and are not overwritten. A local or post-write mismatch raises `ObjectVerificationError`. Access denial, throttling, endpoint failure, and other non-404 S3 failures remain visible as the original `ClientError`; only `404`, `NoSuchKey`, and `NotFound` mean absence.

If a write returns an uncertain transport outcome, rerun the whole primitive. It will inspect the deterministic key and return `unchanged` for the same bytes or a conflict for different bytes.

## Copy-verify-delete movement

`S3Storage.move_verified()` requires distinct source/destination keys and the expected source length and SHA-256. It never claims an atomic rename:

1. stream and verify the source when present;
2. reject a different destination without deleting the source;
3. issue `CopyObject` with metadata-copy semantics when the destination is absent;
4. stream and verify destination bytes, content type, and user metadata;
5. issue `DeleteObject` only after successful verification;
6. confirm source absence and reverify the destination.

Recovery converges as follows:

| Observed state | Rerun behavior |
| --- | --- |
| Source only | Copy, verify, then delete source |
| Source and identical destination | Skip copy, verify, then retry source delete |
| Destination only and expected bytes | Return `already_moved` |
| Source and different destination | Raise conflict; retain source |
| Neither object | Raise `ObjectNotFoundError` |

A copy or verification failure retains the source. A delete failure can leave both identical objects; this is an expected recoverable state.

## Payload/manifest reconciliation

`S3Storage.reconcile_payload_manifest()` derives the manifest key with `manifest_key_for()` and only observes state. It never writes, repairs, or deletes:

| State | Meaning |
| --- | --- |
| `absent` | Neither payload nor manifest exists |
| `payload_only` | Payload bytes match the expected length/SHA-256; manifest is absent |
| `complete` | Payload is verified and the strict manifest points to the same key, length, SHA-256, and content type |
| `manifest_only` | Manifest exists without its payload; higher-level recovery must handle it explicitly |

Malformed or oversized manifests, wrong linkage, and payload/manifest mismatches raise verification or conflict errors instead of returning a healthy state. A future source adapter may reuse a verified `payload_only` object and immutably write its missing manifest for a new physical attempt. A `complete` pair is reused without replacing its recorded `run_id`.

## Reliability boundary

Garage has no bucket versioning, and conditional `PutObject` is not a correctness foundation. The immutable-write check followed by `PutObject` is safe for the project's sequential writer model, not for concurrent writers racing on one key. The project claims deterministic keys, idempotent writes, and safe redelivery—not exactly-once delivery.

Multipart upload/copy and distributed coordination remain outside the core scope. Garage-specific administration remains in infrastructure code; domain code uses only the standard S3 API through this shared boundary.
