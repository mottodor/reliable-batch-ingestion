# ADR 0001: Use Garage for S3-compatible object storage

- Status: Accepted
- Date: 2026-10-12

## Context

The project needs local S3-compatible object storage for raw payloads, manifests, and the `landing/archive/rejected` states.

The implementation must provide a public versioned image on Docker Hub, start from a clean clone through Docker Compose, and work through the standard `boto3` API.

## Decision

Use the pinned Docker image:

```text
dxflrs/garage:v2.4.1
```

The application accesses Garage only through the S3 API. Configuration supplies the endpoint, region, bucket, and credentials. The local environment uses path-style addressing.

Garage-specific bootstrap and administration remain isolated in infrastructure scripts and are not imported by domain ingestion modules.

## Consequences

Benefits:

- a public versioned image is available on Docker Hub;
- the single-node runtime is small;
- the required core S3 operations are supported;
- the project may use one bucket with prefixes or multiple buckets;
- production AWS S3 can be selected through configuration without rewriting domain logic.

Constraints:

- bucket versioning is unavailable;
- conditional `PutObject` cannot be the foundation of idempotency;
- deterministic keys, checksum verification, and application rules enforce raw immutability;
- the core lab does not promise safety for concurrent writers;
- an `ETag` is not treated as a SHA-256 payload digest.

## Required validation

The integration suite must cover:

- create/list bucket;
- put/get/head/list/copy/delete object;
- metadata preservation;
- data persistence after restart;
- correct recovery from payload-without-manifest state;
- application behavior while the endpoint is temporarily unavailable.

## Reconsidering the decision

Replacing Garage requires a new ADR that compares the required S3 semantics and describes migration consequences.
