\set ON_ERROR_STOP on

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS omni_retail;

CREATE TABLE IF NOT EXISTS omni_retail.orders (
    order_id bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    order_status text NOT NULL
        CHECK (order_status IN ('cancelled', 'confirmed', 'fulfilled', 'pending')),
    shipping_status text NOT NULL
        CHECK (shipping_status IN ('delivered', 'in_transit', 'label_created', 'returned')),
    currency character(3) NOT NULL
        CHECK (currency IN ('EUR', 'GBP', 'USD')),
    total_amount numeric(12, 2) NOT NULL
        CHECK (total_amount >= 0),
    updated_at timestamp with time zone NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS orders_updated_at_order_id_idx
    ON omni_retail.orders (updated_at, order_id);

CREATE OR REPLACE FUNCTION omni_retail.seeded_int(
    source_seed integer,
    source_namespace text,
    source_date date,
    row_ordinal integer,
    field_name text,
    modulus integer
)
RETURNS integer
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $function$
    WITH RECURSIVE canonical AS (
        SELECT digest(
            convert_to(
                format(
                    '{"field":"%s","namespace":"%s","ordinal":%s,"seed":%s,"source_date":"%s"}',
                    field_name,
                    source_namespace,
                    row_ordinal,
                    source_seed,
                    to_char(source_date, 'YYYY-MM-DD')
                ),
                'UTF8'
            ),
            'sha256'
        ) AS hash_bytes
    ),
    folded (byte_index, remainder) AS (
        VALUES (-1, 0::bigint)
        UNION ALL
        SELECT
            folded.byte_index + 1,
            (
                folded.remainder * 256
                + get_byte(canonical.hash_bytes, folded.byte_index + 1)
            ) % modulus
        FROM folded
        CROSS JOIN canonical
        WHERE folded.byte_index < 7
    )
    SELECT remainder::integer
    FROM folded
    WHERE byte_index = 7
$function$;

COMMIT;
