# Deterministic source contracts

This document describes the completed BI-040 source behavior: deterministic PostgreSQL materialization, the live mock HTTP source, and the committed supplier fixture corpus.

## Seed contract

All source generators accept an explicit integer `SOURCE_SEED`. The default project seed is:

```text
20260901
```

Valid seeds are ASCII decimal integers from `0` through `2147483647`, inclusive. Empty values, signs, separators, Unicode digits, fractions, booleans, and out-of-range values are rejected. Generators do not choose a fallback from the current time or a random-number generator.

Each derived value is a pure function of:

- the validated integer seed;
- a source namespace;
- an explicit `source_date`;
- a zero-based stable row ordinal;
- a field name.

Those values are serialized as canonical JSON and hashed with SHA-256. The first eight digest bytes are interpreted as an unsigned big-endian integer and reduced by the field's explicit positive modulus. Generated order IDs use the separate stable formula `seed * 1000 + ordinal + 1` so the PostgreSQL and shipping-status sources can refer to the same orders.

Generators require a `datetime.date`; they reject `datetime` values so an accidental clock timestamp cannot become part of logical source identity. They do not read environment variables, perform I/O, or consume Python's process-global random state.

## Pure record schemas

The current generators use `2026-09-01` in tests, but callers must always pass the desired source date explicitly.

### FX rates

`mock_api.data.generate_fx_rates()` emits four records ordered by `quote_currency`:

```text
base_currency
quote_currency
rate
effective_date
```

The base is `USD`; quote currencies are `AUD`, `EUR`, `GBP`, and `JPY`. `rate` is a decimal string with exactly six fractional digits, avoiding binary floating-point differences.

### Shipping statuses

`mock_api.data.generate_shipping_statuses()` emits 12 records ordered by `shipment_id`:

```text
shipment_id
order_id
status
updated_at
```

Shipment IDs are stable for the source date. Order IDs share the deterministic order-ID formula. Timestamps begin at UTC midnight and advance by 37 minutes per ordinal, so they are fixed, unique, and ascending.

### PostgreSQL order records

`batch_ingestion.source_records.generate_order_records()` emits 24 records ordered by `order_id`:

```text
order_id
customer_id
order_status
shipping_status
currency
total_amount
updated_at
```

`total_amount` is a `Decimal` with two fractional digits. Timestamps use the same fixed UTC sequence as shipping statuses.

## PostgreSQL order source

The ordered scripts `postgres/init/001_omni_retail_schema.sql` and `002_seed_orders.sql` are applied explicitly by `make seed`; they are not one-time container initialization hooks. The schema script safely creates `omni_retail.orders`, finite-value and non-negative checks, the unique UTC watermark, and the `(updated_at, order_id)` index required by future bounded snapshot extraction. It also defines the deterministic SHA-256 derivation helper used to reproduce the pure Python order generator.

The seed script uses an explicit insert column list and runs as one transaction with `ON_ERROR_STOP`. It validates the selected seed through a constrained temporary parameter row, truncates only `omni_retail.orders`, and inserts 24 rows ordered by stable ordinal. It never uses `SELECT *`, changes the BI-020 probe table, or accesses Garage.

Use the default seed or choose another valid value explicitly:

```bash
make seed
make seed SOURCE_SEED=20260901
```

`SOURCE_SEED` is validated before Compose or `psql` is invoked. The Make target passes the validated value to `psql` as a variable, and SQL consumes it through the quoted-literal form `:'source_seed'`; it is not interpolated into SQL text by the shell. Any validation or SQL failure returns non-zero. Repeating the default command yields the same row count, ordered digest, UTC bounds, and row bytes at the logical-value level. A different valid seed changes generated values without changing the table schema. Existing unrelated PostgreSQL rows and S3 objects are retained.

## Mock REST API

Compose builds `mock-api` from the pinned `python:3.12.12-slim-bookworm` image with no added runtime dependency. The service listens on container port `8080`, publishes `MOCK_API_PORT`, validates `SOURCE_SEED` before binding, and uses a bounded `/health` check. It imports only the pure seed helper and mock source package; it does not connect to PostgreSQL, S3, or the public network.

The versioned resources are:

```text
GET /v1/fx-rates
GET /v1/shipping-statuses
```

Both require exactly one `source_date`, `page`, and `page_size` query value. `source_date` is canonical ISO `YYYY-MM-DD`; page values are positive ASCII integers; `page_size` is limited to `100`. Unknown routes and invalid, duplicate, unexpected, or out-of-range parameters produce canonical permanent `4xx` JSON. Successful UTF-8 bodies end with LF, use sorted JSON keys and compact separators, and contain `source`, `source_date`, `page`, `page_size`, `total_pages`, `total_records`, `next_page`, and stably ordered `items`.

The optional `scenario` parameter defaults to `happy`:

- `happy` returns every valid page normally;
- `rate_limit_once` requires `scenario_key`; the first request for that validated key returns `429` and `Retry-After: 1`, then later requests succeed;
- `permanent_500` always returns the same canonical `500` body;
- `malformed_json` requires `fault_page`; only that page returns status `200`, JSON content type, and the fixed malformed body;
- `partial_pages` requires `fault_page >= 2`; earlier pages succeed and the selected page repeatedly returns the same `500` body.

Scenario keys match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`. The threaded server protects fault state with a lock and retains at most 128 keys; a new key at capacity receives a deterministic `503` rather than evicting an existing key and replaying its one-time fault. Health, permanent, malformed, and partial requests do not consume transient state.

The local-only control route resets one key or every key without changing generated records:

```text
POST /__control/reset?scenario_key=<key>
POST /__control/reset
```

Tests and exercises must use unique keys or reset them explicitly. Restarting the service also clears transient counters, while identical seed/resource queries retain byte-identical successful response bodies.

### Supplier prices

`batch_ingestion.source_records.generate_supplier_price_records()` emits 15 records ordered by `(supplier_id, product_id, effective_date)`:

```text
supplier_id
product_id
price_minor_units
currency
effective_date
```

Prices are positive integer minor units. This avoids float-dependent representations in equivalent CSV, JSON, and Parquet fixtures.

## Supplier fixture corpus

The committed default corpus is rooted at `fixtures/`, uses seed `20260901` and source date `2026-09-01`, and contains 24 payload files plus `checksums.json`. The canonical scenarios exist in every supported format:

```text
fixtures/supplier-prices/canonical/{valid,empty,schema_invalid,mixed}.{csv,json,parquet}
```

- `valid` contains the same 15 sorted logical rows in all three formats;
- `empty` retains the CSV header, a JSON empty array, or the full zero-row Parquet schema;
- `schema_invalid` remains parseable but omits the required `currency` field/column;
- `mixed` retains the valid schema and contains positive prices plus one business-invalid zero price. It records a condition only and does not decide BI-060 file atomicity.

Duplicate-delivery inputs exist for every format:

```text
fixtures/supplier-prices/same-name-delivery-a/supplier-prices-2026-09-01.<format>
fixtures/supplier-prices/same-name-delivery-b/supplier-prices-2026-09-01.<format>
fixtures/supplier-prices/renamed-content/supplier-prices-original-2026-09-01.<format>
fixtures/supplier-prices/renamed-content/supplier-prices-renamed-2026-09-01.<format>
```

The two same-name delivery directories contain equal basenames and bytes. The renamed-content pair has different basenames and equal bytes. Both duplicate cases copy the corresponding canonical valid payload and assign no ingestion outcome.

CSV uses UTF-8, fixed column order, LF line endings, deterministic minimal quoting, and a final newline. JSON uses a top-level array, fixed key order and indentation/separators, UTF-8, and a final newline. Parquet uses non-null `string`, `string`, `int64`, `string`, and `date32` fields, Parquet version 2.6, a fixed row-group bound, no compression or dictionary encoding, no statistics, and data page version 2.0. No generated timestamp or path is stored in payload metadata.

`fixtures/checksums.json` is sorted by relative path and records fixture schema version 1, seed, source date, byte length, SHA-256, format, and scenario for every payload. It is generation evidence, not an ingestion manifest.

Regenerate the committed corpus or verify it without changing tracked files:

```bash
make fixtures SOURCE_SEED=20260901
make fixtures-check SOURCE_SEED=20260901
```

`make fixtures` validates `SOURCE_SEED` before changing the output and replaces the corpus only after a complete temporary generation. `make fixtures-check` creates two temporary corpora, validates their indices, schemas, rows, invalid/mixed scenarios, and duplicate relationships, then requires both byte inventories to equal the committed corpus. A non-default seed is supported for explicit generation experiments but intentionally cannot pass against the committed default corpus.

## Reproducibility boundary

For the same code, seed, and source date, the pure record tuples and their ordering are exact. Unit tests also pin representative default-seed values so an algorithm change is explicit.

The PostgreSQL source now reproduces the pure order records exactly for the same seed and fixed source date. Integration coverage compares explicit ordered rows and their SHA-256 digest across repeated runs, verifies unique ascending UTC watermarks, changes seed without changing schema, proves invalid input leaves state unchanged, and restores the default seed afterward.

Mock API successful pages and deterministic error/fault bodies are byte-stable for the same implementation, seed, and request. Transport-added headers such as HTTP `Date` are not part of the byte-stability contract. Live tests traverse both resources, exercise every fault, and compare a successful page across a bounded service restart.

Supplier CSV, JSON, and Parquet payloads are byte-stable for repeated generation with the same code, seed, source date, committed `uv.lock`, Python 3.12, and supported local toolchain. Tests and `make fixtures-check` compare exact relative path sets, bytes, lengths, checksums, parsed schemas, and logical rows. PyArrow writes its implementation version into Parquet `created_by` metadata, so another PyArrow version guarantees schema and logical-row equality rather than identical Parquet bytes.
