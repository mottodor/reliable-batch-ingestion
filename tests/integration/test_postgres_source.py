"""Integration contract for deterministic transactional PostgreSQL source seeding."""

import hashlib
import os
import subprocess
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import psycopg
import pytest

from batch_ingestion.config import AppConfig
from batch_ingestion.source_records import generate_order_records
from batch_ingestion.source_seed import DEFAULT_SOURCE_SEED

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DATE = date(2026, 9, 1)
OrderRow = tuple[int, int, str, str, str, Decimal, datetime]


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator or not name:
            raise ValueError(f"invalid environment line for {name or '<unknown>'}")
        values[name] = value
    return values


@pytest.fixture(scope="module")
def env_path() -> Path:
    configured_path = Path(os.environ.get("BI020_ENV_FILE", ".env"))
    return configured_path if configured_path.is_absolute() else REPOSITORY_ROOT / configured_path


@pytest.fixture(scope="module")
def app_config(env_path: Path) -> AppConfig:
    return AppConfig.from_env(_load_dotenv(env_path))


def _run_seed(env_path: Path, source_seed: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "make",
            "seed",
            f"SOURCE_SEED={source_seed}",
            f"COMPOSE_ENV_FILE={env_path}",
        ],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )


def _connect(app_config: AppConfig) -> psycopg.Connection[tuple[object, ...]]:
    return psycopg.connect(
        host=app_config.postgres.host,
        port=app_config.postgres.port,
        dbname=app_config.postgres.database,
        user=app_config.postgres.user,
        password=app_config.postgres.password,
        connect_timeout=5,
    )


def _read_orders_and_schema(
    connection: psycopg.Connection[tuple[object, ...]],
) -> tuple[tuple[OrderRow, ...], tuple[tuple[object, ...], ...]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                order_id,
                customer_id,
                order_status,
                shipping_status,
                currency,
                total_amount,
                updated_at
            FROM omni_retail.orders
            ORDER BY updated_at, order_id
            """
        )
        rows = cast(Sequence[OrderRow], cursor.fetchall())
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'omni_retail' AND table_name = 'orders'
            ORDER BY ordinal_position
            """
        )
        schema = cursor.fetchall()
    return tuple(rows), tuple(schema)


def _ordered_digest(rows: Sequence[OrderRow]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        order_id, customer_id, order_status, shipping_status, currency, amount, updated_at = row
        canonical = "|".join(
            (
                str(order_id),
                str(customer_id),
                order_status,
                shipping_status,
                currency,
                format(amount, ".2f"),
                updated_at.astimezone(UTC).isoformat(timespec="seconds"),
            )
        )
        digest.update(canonical.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _capture_bi020_marker_state(
    connection: psycopg.Connection[tuple[object, ...]],
) -> tuple[tuple[object, ...], ...]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bi020_infrastructure_probe (
                marker_id text PRIMARY KEY,
                marker_value text NOT NULL,
                checksum_sha256 char(64) NOT NULL
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO bi020_infrastructure_probe
                (marker_id, marker_value, checksum_sha256)
            VALUES (%s, %s, %s)
            ON CONFLICT (marker_id) DO NOTHING
            """,
            (
                "bi040-seed-preservation-marker",
                "BI-040 must preserve unrelated PostgreSQL state",
                hashlib.sha256(b"BI-040 must preserve unrelated PostgreSQL state").hexdigest(),
            ),
        )
        cursor.execute(
            """
            SELECT marker_id, marker_value, checksum_sha256
            FROM bi020_infrastructure_probe
            ORDER BY marker_id
            """
        )
        return tuple(cursor.fetchall())


def test_postgres_seed_is_repeatable_validated_and_isolated(
    app_config: AppConfig,
    env_path: Path,
) -> None:
    first_result = _run_seed(env_path, str(DEFAULT_SOURCE_SEED))
    assert first_result.returncode == 0, first_result.stderr

    with _connect(app_config) as connection:
        marker_before = _capture_bi020_marker_state(connection)
        first_rows, first_schema = _read_orders_and_schema(connection)

    second_result = _run_seed(env_path, str(DEFAULT_SOURCE_SEED))
    assert second_result.returncode == 0, second_result.stderr

    with _connect(app_config) as connection:
        second_rows, second_schema = _read_orders_and_schema(connection)
        marker_after_repeat = _capture_bi020_marker_state(connection)

    expected_records = generate_order_records(DEFAULT_SOURCE_SEED, SOURCE_DATE)
    expected_rows: tuple[OrderRow, ...] = tuple(
        (
            record.order_id,
            record.customer_id,
            record.order_status,
            record.shipping_status,
            record.currency,
            record.total_amount,
            record.updated_at,
        )
        for record in expected_records
    )

    assert len(first_rows) == 24
    assert first_rows == second_rows == expected_rows
    assert _ordered_digest(first_rows) == _ordered_digest(second_rows)
    assert first_schema == second_schema
    assert marker_after_repeat == marker_before
    timestamps = tuple(row[6] for row in first_rows)
    assert len(set(timestamps)) == len(timestamps)
    assert timestamps == tuple(sorted(timestamps))
    assert all(timestamp.utcoffset() == UTC.utcoffset(timestamp) for timestamp in timestamps)

    invalid_result = _run_seed(env_path, "invalid")
    assert invalid_result.returncode != 0
    assert "SOURCE_SEED" in invalid_result.stderr
    with _connect(app_config) as connection:
        rows_after_invalid, schema_after_invalid = _read_orders_and_schema(connection)
        marker_after_invalid = _capture_bi020_marker_state(connection)
    assert rows_after_invalid == second_rows
    assert schema_after_invalid == second_schema
    assert marker_after_invalid == marker_before

    try:
        different_result = _run_seed(env_path, str(DEFAULT_SOURCE_SEED + 1))
        assert different_result.returncode == 0, different_result.stderr
        with _connect(app_config) as connection:
            different_rows, different_schema = _read_orders_and_schema(connection)
            marker_after_different = _capture_bi020_marker_state(connection)
        assert different_rows != second_rows
        assert _ordered_digest(different_rows) != _ordered_digest(second_rows)
        assert different_schema == second_schema
        assert marker_after_different == marker_before
    finally:
        restore_result = _run_seed(env_path, str(DEFAULT_SOURCE_SEED))
        assert restore_result.returncode == 0, restore_result.stderr
