# Deterministic source contracts

This document describes only source behavior that currently exists. BI-040 remains in progress: PostgreSQL SQL, live mock HTTP routes, and generated CSV/JSON/Parquet files are deferred to later slices.

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

`total_amount` is a `Decimal` with two fractional digits. Timestamps use the same fixed UTC sequence as shipping statuses. Slice 2 will express this contract as versioned PostgreSQL schema and transactional seed SQL.

### Supplier prices

`batch_ingestion.source_records.generate_supplier_price_records()` emits 15 records ordered by `(supplier_id, product_id, effective_date)`:

```text
supplier_id
product_id
price_minor_units
currency
effective_date
```

Prices are positive integer minor units. This avoids float-dependent representations when slice 4 writes equivalent CSV, JSON, and Parquet fixtures.

## Reproducibility boundary

For the same code, seed, and source date, the pure record tuples and their ordering are exact. Unit tests also pin representative default-seed values so an algorithm change is explicit.

The following byte-level guarantees are not implemented yet:

- HTTP response serialization belongs to BI-040 slice 3;
- CSV, JSON, and Parquet serialization belongs to BI-040 slice 4;
- committed fixture checksums do not exist yet.

When Parquet generation is added, exact bytes will be guaranteed only with the committed `uv.lock`, Python 3.12, the supported local toolchain, and explicit writer options. PyArrow writes its implementation version into Parquet `created_by` metadata, so another PyArrow version will guarantee logical schema and row equality rather than identical file bytes.
