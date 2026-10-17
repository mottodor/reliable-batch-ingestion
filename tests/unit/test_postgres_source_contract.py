"""Static and command contracts for the BI-040 PostgreSQL source seed."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
POSTGRES_INIT = REPOSITORY_ROOT / "postgres" / "init"
SCHEMA_SQL = POSTGRES_INIT / "001_omni_retail_schema.sql"
SEED_SQL = POSTGRES_INIT / "002_seed_orders.sql"
MAKEFILE_PATH = REPOSITORY_ROOT / "Makefile"

VALID_LOCAL_ENV = (
    (REPOSITORY_ROOT / ".env.example")
    .read_text(encoding="utf-8")
    .replace("POSTGRES_PASSWORD=change-me", "POSTGRES_PASSWORD=local-password")
    .replace("S3_ACCESS_KEY=change-me", "S3_ACCESS_KEY=local-access-key")
    .replace("S3_SECRET_KEY=change-me", "S3_SECRET_KEY=local-secret-key")
    .replace(
        "GARAGE_RPC_SECRET=change-me-64-character-hex-value",
        "GARAGE_RPC_SECRET=" + "0123456789abcdef" * 4,
    )
)


def test_versioned_postgres_scripts_are_ordered_and_explicit() -> None:
    assert [path.name for path in sorted(POSTGRES_INIT.glob("*.sql"))] == [
        "001_omni_retail_schema.sql",
        "002_seed_orders.sql",
    ]

    schema_sql = SCHEMA_SQL.read_text(encoding="utf-8").lower()
    seed_sql = SEED_SQL.read_text(encoding="utf-8").lower()

    assert "create schema if not exists omni_retail" in schema_sql
    assert "create table if not exists omni_retail.orders" in schema_sql
    for column in (
        "order_id bigint primary key",
        "customer_id bigint not null",
        "order_status text not null",
        "shipping_status text not null",
        "currency character(3) not null",
        "total_amount numeric(12, 2) not null",
        "updated_at timestamp with time zone not null unique",
    ):
        assert column in schema_sql
    assert "check (order_status in (" in schema_sql
    assert "check (shipping_status in (" in schema_sql
    assert "check (currency in (" in schema_sql
    assert "check (total_amount >= 0)" in schema_sql
    assert "(updated_at, order_id)" in schema_sql

    assert "\\set on_error_stop on" in seed_sql
    assert "begin;" in seed_sql
    assert "commit;" in seed_sql
    assert "truncate table omni_retail.orders" in seed_sql
    assert ":'source_seed'" in seed_sql
    assert "insert into omni_retail.orders (" in seed_sql
    assert "on conflict" not in seed_sql
    assert "select *" not in f"{schema_sql}\n{seed_sql}"


def _write_compose_recorder(tmp_path: Path, *, fail_psql: bool = False) -> Path:
    recorder = tmp_path / "record_compose.py"
    recorder.write_text(
        f"""\
import os
from pathlib import Path
import sys

record_path = Path(os.environ["COMPOSE_RECORD_PATH"])
with record_path.open("a", encoding="utf-8") as stream:
    stream.write(" ".join(sys.argv[1:]) + "\\n")
if {fail_psql!r} and any("psql" in argument for argument in sys.argv):
    raise SystemExit(9)
""",
        encoding="utf-8",
    )
    return recorder


def _run_seed(
    tmp_path: Path,
    source_seed: str,
    *,
    fail_psql: bool = False,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    env_path = tmp_path / ".env"
    env_path.write_text(VALID_LOCAL_ENV, encoding="utf-8")
    record_path = tmp_path / "compose-arguments.txt"
    recorder = _write_compose_recorder(tmp_path, fail_psql=fail_psql)
    environment = os.environ.copy()
    environment["COMPOSE_RECORD_PATH"] = str(record_path)
    result = subprocess.run(
        [
            "make",
            "seed",
            f"SOURCE_SEED={source_seed}",
            f"COMPOSE={sys.executable} {recorder}",
            f"COMPOSE_ENV_FILE={env_path}",
            "COMPOSE_PROJECT_NAME=bi040-postgres-unit-test",
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    return result, record_path


@pytest.mark.parametrize("source_seed", ["", "-1", "not-a-number", "2147483648"])
def test_invalid_source_seed_fails_before_compose_or_psql(
    tmp_path: Path,
    source_seed: str,
) -> None:
    result, record_path = _run_seed(tmp_path, source_seed)

    assert result.returncode != 0
    assert "SOURCE_SEED" in result.stderr
    assert not record_path.exists()


def test_seed_sql_failure_propagates_after_bounded_startup(tmp_path: Path) -> None:
    result, record_path = _run_seed(tmp_path, "20260901", fail_psql=True)

    assert result.returncode != 0
    invocations = record_path.read_text(encoding="utf-8").splitlines()
    assert any("up --detach --wait --wait-timeout" in invocation for invocation in invocations)
    assert any("psql" in invocation for invocation in invocations)


def test_make_seed_passes_validated_seed_as_a_psql_variable() -> None:
    makefile = MAKEFILE_PATH.read_text(encoding="utf-8")

    assert "SOURCE_SEED ?= 20260901" in makefile
    assert "scripts/validate_source_seed.py" in makefile
    assert "--set=ON_ERROR_STOP=on" in makefile
    assert "export SOURCE_SEED" in makefile
    assert "exec -T -e SOURCE_SEED postgres" in makefile
    assert '"--set=source_seed=$$SOURCE_SEED"' in makefile
    assert "postgres/init/001_omni_retail_schema.sql" in makefile
    assert "postgres/init/002_seed_orders.sql" in makefile
