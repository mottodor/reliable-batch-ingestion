#!/usr/bin/env python3
"""Validate local Compose configuration without exposing credential values."""

import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from batch_ingestion.config import AppConfig, ConfigurationError

LOCAL_REQUIRED_VARIABLES = (
    "S3_PORT",
    "GARAGE_ADMIN_PORT",
    "GARAGE_RPC_SECRET",
    "MOCK_API_PORT",
)
LOCAL_PORT_VARIABLES = ("S3_PORT", "GARAGE_ADMIN_PORT", "MOCK_API_PORT")
PLACEHOLDER_VARIABLES = (
    "POSTGRES_PASSWORD",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "GARAGE_RPC_SECRET",
)
RPC_SECRET_PATTERN = re.compile(r"[0-9a-fA-F]{64}")


def read_env(path: Path) -> tuple[dict[str, str], list[str]]:
    """Read simple Compose dotenv assignments and return redacted validation errors."""
    values: dict[str, str] = {}
    errors: list[str] = []

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return values, [f"configuration file is missing: {path}"]

    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        name = name.strip()
        if not separator or not name:
            errors.append(f"invalid assignment at line {line_number}")
            continue
        if name in values:
            errors.append(f"duplicate variable: {name}")
            continue
        values[name] = value.strip()

    return values, errors


def validate_environment(values: Mapping[str, str]) -> list[str]:
    """Validate complete application and local-only settings without exposing values."""
    errors: list[str] = []

    for name in LOCAL_REQUIRED_VARIABLES:
        if not values.get(name, ""):
            errors.append(f"{name} is required")

    placeholder_names: set[str] = set()
    for name in PLACEHOLDER_VARIABLES:
        value = values.get(name, "")
        if value and "change-me" in value.lower():
            errors.append(f"{name} still contains a placeholder")
            placeholder_names.add(name)

    for name in LOCAL_PORT_VARIABLES:
        value = values.get(name, "")
        if not value:
            continue
        try:
            port = int(value)
        except ValueError:
            errors.append(f"{name} must be an integer port")
            continue
        if not 1 <= port <= 65535:
            errors.append(f"{name} must be between 1 and 65535")

    rpc_secret = values.get("GARAGE_RPC_SECRET", "")
    if (
        rpc_secret
        and "GARAGE_RPC_SECRET" not in placeholder_names
        and RPC_SECRET_PATTERN.fullmatch(rpc_secret) is None
    ):
        errors.append("GARAGE_RPC_SECRET must be exactly 64 hexadecimal characters")

    try:
        AppConfig.from_env(values)
    except ConfigurationError as error:
        errors.append(str(error))

    return errors


def main(argv: Sequence[str] | None = None) -> int:
    """Run local environment validation."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) > 1:
        print("usage: check_local_env.py [ENV_FILE]", file=sys.stderr)
        return 2

    path = Path(arguments[0] if arguments else ".env")
    values, parse_errors = read_env(path)
    errors = [*parse_errors, *validate_environment(values)]
    if errors:
        print("Local environment preflight: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"Local environment preflight: PASS ({path})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
