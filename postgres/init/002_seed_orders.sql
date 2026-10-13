\set ON_ERROR_STOP on

BEGIN;

CREATE TEMPORARY TABLE source_seed_parameters (
    seed integer NOT NULL CHECK (seed BETWEEN 0 AND 2147483647),
    source_date date NOT NULL
) ON COMMIT DROP;

INSERT INTO source_seed_parameters (seed, source_date)
VALUES ((:'source_seed')::integer, DATE '2026-09-01');

TRUNCATE TABLE omni_retail.orders;

INSERT INTO omni_retail.orders (
    order_id,
    customer_id,
    order_status,
    shipping_status,
    currency,
    total_amount,
    updated_at
)
SELECT
    parameters.seed::bigint * 1000 + ordinals.ordinal + 1 AS order_id,
    10000 + omni_retail.seeded_int(
        parameters.seed,
        'postgres-orders',
        parameters.source_date,
        ordinals.ordinal,
        'customer-id',
        2000
    ) AS customer_id,
    (ARRAY['cancelled', 'confirmed', 'fulfilled', 'pending'])[
        1 + omni_retail.seeded_int(
            parameters.seed,
            'postgres-orders',
            parameters.source_date,
            ordinals.ordinal,
            'order-status',
            4
        )
    ] AS order_status,
    (ARRAY['delivered', 'in_transit', 'label_created', 'returned'])[
        1 + omni_retail.seeded_int(
            parameters.seed,
            'postgres-orders',
            parameters.source_date,
            ordinals.ordinal,
            'shipping-status',
            4
        )
    ] AS shipping_status,
    (ARRAY['EUR', 'GBP', 'USD'])[
        1 + omni_retail.seeded_int(
            parameters.seed,
            'postgres-orders',
            parameters.source_date,
            ordinals.ordinal,
            'currency',
            3
        )
    ] AS currency,
    (
        500 + omni_retail.seeded_int(
            parameters.seed,
            'postgres-orders',
            parameters.source_date,
            ordinals.ordinal,
            'total-amount',
            100000
        )
    )::numeric / 100 AS total_amount,
    make_timestamptz(
        EXTRACT(YEAR FROM parameters.source_date)::integer,
        EXTRACT(MONTH FROM parameters.source_date)::integer,
        EXTRACT(DAY FROM parameters.source_date)::integer,
        0,
        0,
        0,
        'UTC'
    ) + ordinals.ordinal * INTERVAL '37 minutes' AS updated_at
FROM source_seed_parameters AS parameters
CROSS JOIN generate_series(0, 23) AS ordinals (ordinal)
ORDER BY ordinals.ordinal;

COMMIT;
