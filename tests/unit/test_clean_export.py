"""Offline contracts for the BI-080 isolated clean-export walkthrough."""

from __future__ import annotations

import stat
from pathlib import Path

import pytest
from scripts.check_clean_export import (
    ISOLATED_ENVIRONMENT_FIELDS,
    CleanExportError,
    copy_export_files,
    parse_export_paths,
    redact_output,
    rewrite_isolated_environment,
    walkthrough_commands,
)


def _environment_text() -> str:
    return """POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_PASSWORD=postgres-private
S3_ENDPOINT_URL=http://localhost:3900
S3_PORT=3900
S3_BUCKET=batch-lab
S3_ACCESS_KEY=access-private
S3_SECRET_KEY=secret-private
GARAGE_ADMIN_PORT=3903
GARAGE_RPC_SECRET=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
MOCK_API_PORT=8080
MOCK_API_BASE_URL=http://localhost:8080
"""


def _assignments(path: Path) -> dict[str, str]:
    return {
        name: value
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
        for name, separator, value in (line.partition("="),)
        if separator
    }


def test_export_path_parser_accepts_only_repository_owned_non_ignored_paths() -> None:
    parsed = parse_export_paths(b"README.md\0scripts/check_clean_export.py\0fixtures/data.csv\0")

    assert parsed == (
        Path("README.md"),
        Path("fixtures/data.csv"),
        Path("scripts/check_clean_export.py"),
    )

    for unsafe in (
        b".env\0",
        b"artifacts/demo/report.ndjson\0",
        b".venv/bin/python\0",
        b"src/../.env.example\0",
        b"/tmp/private\0",
        b".git/config\0",
    ):
        with pytest.raises(CleanExportError):
            parse_export_paths(unsafe)


def test_export_copy_preserves_bytes_and_executable_mode_but_refuses_symlinks(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "export"
    source.mkdir()
    script = source / "scripts" / "run.sh"
    script.parent.mkdir()
    script.write_bytes(b"#!/bin/sh\nexit 0\n")
    script.chmod(0o755)

    copy_export_files(source, destination, (Path("scripts/run.sh"),))

    copied = destination / "scripts" / "run.sh"
    assert copied.read_bytes() == script.read_bytes()
    assert stat.S_IMODE(copied.stat().st_mode) == 0o755

    link = source / "README-link"
    link.symlink_to(script)
    second_destination = tmp_path / "symlink-export"
    with pytest.raises(CleanExportError, match="symlink"):
        copy_export_files(source, second_destination, (Path("README-link"),))


def test_isolated_environment_rewrite_changes_only_test_owned_endpoint_fields(
    tmp_path: Path,
) -> None:
    path = tmp_path / ".env"
    path.write_text(_environment_text(), encoding="utf-8")
    path.chmod(0o600)
    before = _assignments(path)
    overrides = {
        "POSTGRES_PORT": "15432",
        "S3_ENDPOINT_URL": "http://localhost:13900",
        "S3_PORT": "13900",
        "S3_BUCKET": "bi080-clean-a1b2c3",
        "GARAGE_ADMIN_PORT": "13903",
        "MOCK_API_PORT": "18080",
        "MOCK_API_BASE_URL": "http://localhost:18080",
    }

    rewrite_isolated_environment(path, overrides)

    after = _assignments(path)
    assert set(overrides) == ISOLATED_ENVIRONMENT_FIELDS
    assert {name for name in before if before[name] != after[name]} == set(overrides)
    assert all(after[name] == value for name, value in overrides.items())
    assert after["POSTGRES_PASSWORD"] == before["POSTGRES_PASSWORD"]
    assert after["S3_ACCESS_KEY"] == before["S3_ACCESS_KEY"]
    assert after["S3_SECRET_KEY"] == before["S3_SECRET_KEY"]
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    with pytest.raises(CleanExportError, match="allow-list"):
        rewrite_isolated_environment(path, {"POSTGRES_PASSWORD": "replacement"})


def test_walkthrough_command_plan_is_locked_bounded_and_complete() -> None:
    commands = walkthrough_commands(
        env_file=Path(".env"),
        project_name="bi080-clean-a1b2c3",
    )

    assert [command.name for command in commands] == [
        "locked-setup",
        "doctor",
        "compose-config",
        "up",
        "seed",
        "demo-happy-path",
        "demo-duplicate",
        "demo-api-retry",
        "demo-bad-file",
        "offline-tests",
        "demo-inspect",
    ]
    assert commands[0].argv == ("uv", "sync", "--locked", "--all-groups")
    assert all(command.timeout_seconds > 0 for command in commands)
    make_commands = [command.argv for command in commands if command.argv[0] == "make"]
    assert all("COMPOSE_ENV_FILE=.env" in command for command in make_commands)
    assert all("COMPOSE_PROJECT_NAME=bi080-clean-a1b2c3" in command for command in make_commands)
    assert any(command[:2] == ("make", "test") for command in make_commands)


def test_output_redaction_never_repeats_credentials_or_payload_values() -> None:
    output = "password=private-value row=SUP-0001 safe=evidence"

    redacted = redact_output(output, ("private-value",))

    assert "private-value" not in redacted
    assert "SUP-0001" not in redacted
    assert "[REDACTED]" in redacted


def test_recovery_operator_document_covers_the_complete_allow_list() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    document_path = repository_root / "docs" / "ingestion" / "recovery-demos.md"
    readme = (repository_root / "README.md").read_text(encoding="utf-8")
    operator_guide = (repository_root / "docs" / "guides" / "operator-guide.md").read_text(
        encoding="utf-8"
    )

    document = document_path.read_text(encoding="utf-8")
    scenarios = (
        "api-rate-limit",
        "api-permanent-500",
        "api-malformed-json",
        "api-after-page",
        "file-same-name",
        "file-renamed-content",
        "file-schema-invalid",
        "file-during-move",
        "postgres-connection-drop",
        "s3-unavailable",
        "payload-before-manifest",
    )
    for scenario in scenarios:
        assert f"### `{scenario}`" in document
        assert f"SCENARIO={scenario}" in document
    for field in (
        "**Trigger and expected exit:**",
        "**Intermediate state:**",
        "**Recovery or terminal action:**",
        "**Final state:**",
    ):
        assert document.count(field) == len(scenarios)
    assert "not exactly-once" in document
    assert "not an atomic rename" in document
    assert "concurrent writers" in document
    assert "docs/ingestion/recovery-demos.md" in readme
    assert "docs/guides/operator-guide.md" in readme
    assert "scripts/check_clean_export.py" in operator_guide
    assert "not_implemented" not in readme
    assert "not_implemented" not in operator_guide
