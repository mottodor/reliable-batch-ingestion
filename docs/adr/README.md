# Architecture Decision Records

ADRs record decisions that affect architecture, delivery semantics, or project reproducibility.

## Status values

- `Proposed` — under discussion;
- `Accepted` — binding for implementation;
- `Superseded` — replaced by a newer ADR;
- `Rejected` — considered and declined.

## Index

| ADR | Status | Decision |
| --- | --- | --- |
| [0001](0001-use-garage-for-s3-compatible-storage.md) | Accepted | Use Garage `dxflrs/garage:v2.4.1` for local S3-compatible object storage |

## A new ADR is required for

- replacing Garage, PostgreSQL, or another foundational component;
- changing the manifest or object-key contract;
- changing file-ingestion atomicity;
- adding coordination for concurrent writers;
- claiming stronger delivery semantics;
- adding a service or runtime dependency that changes the architecture.
