#!/usr/bin/env python3
"""Explicitly initialize an ignored local environment without exposing credentials."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from scripts.check_local_env import read_env, validate_environment

_PLACEHOLDERS: Mapping[str, str] = {
    "POSTGRES_PASSWORD": "change-me",
    "S3_ACCESS_KEY": "change-me",
    "S3_SECRET_KEY": "change-me",
    "GARAGE_RPC_SECRET": "change-me-64-character-hex-value",
}


class InitializationError(RuntimeError):
    """A safe-to-display local environment initialization failure."""


@dataclass(frozen=True, slots=True)
class InitializationResult:
    """Outcome that deliberately contains no generated values."""

    changed: bool
    target_path: Path


def _parse_assignments(content: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        name = name.strip()
        if not separator or not name:
            raise InitializationError(f"invalid assignment at line {line_number}")
        if name in values:
            raise InitializationError(f"duplicate variable: {name}")
        values[name] = value.strip()
    return values


def _default_is_tracked(path: Path) -> bool:
    repository_root = Path(__file__).resolve().parents[1]
    try:
        relative_path = path.resolve().relative_to(repository_root)
    except ValueError:
        return False
    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative_path.as_posix()],
        cwd=repository_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.returncode == 0


def _default_preflight(path: Path) -> None:
    values, parse_errors = read_env(path)
    errors = [*parse_errors, *validate_environment(values)]
    if errors:
        joined = "; ".join(errors)
        raise InitializationError(f"local environment preflight failed: {joined}")


def _generated_values() -> dict[str, str]:
    return {
        "POSTGRES_PASSWORD": secrets.token_urlsafe(24),
        "S3_ACCESS_KEY": secrets.token_hex(16),
        "S3_SECRET_KEY": secrets.token_hex(32),
        "GARAGE_RPC_SECRET": secrets.token_hex(32),
    }


def _replace_placeholders(content: str, generated: Mapping[str, str]) -> str:
    output_lines: list[str] = []
    for raw_line in content.splitlines(keepends=True):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            output_lines.append(raw_line)
            continue
        name, separator, value = stripped.partition("=")
        if separator and name in _PLACEHOLDERS and value == _PLACEHOLDERS[name]:
            if raw_line.endswith("\r\n"):
                line_ending = "\r\n"
            elif raw_line.endswith("\n"):
                line_ending = "\n"
            else:
                line_ending = ""
            output_lines.append(f"{name}={generated[name]}{line_ending}")
        else:
            output_lines.append(raw_line)
    return "".join(output_lines)


def _write_atomically(
    path: Path,
    content: str,
    *,
    preflight: Callable[[Path], None],
) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        preflight(temporary_path)
        os.replace(temporary_path, path)
        path.chmod(0o600)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def initialize_local_environment(
    *,
    example_path: Path,
    target_path: Path,
    is_tracked: Callable[[Path], bool] = _default_is_tracked,
    preflight: Callable[[Path], None] = _default_preflight,
    output: Callable[[str], None] = print,
) -> InitializationResult:
    """Initialize recognized placeholders once, atomically, and run redacting preflight."""
    if target_path.is_symlink():
        raise InitializationError("target must not be a symlink")
    if is_tracked(target_path):
        raise InitializationError("target is tracked by Git; refusing to write credentials")
    if target_path.exists() and not target_path.is_file():
        raise InitializationError("target must be a regular file")
    if not example_path.is_file():
        raise InitializationError("example environment file is missing")
    if example_path.is_symlink():
        raise InitializationError("example environment file must not be a symlink")
    if not target_path.parent.is_dir():
        raise InitializationError("target parent directory is missing")

    try:
        example_content = example_path.read_text(encoding="utf-8")
        content = (
            target_path.read_text(encoding="utf-8") if target_path.exists() else example_content
        )
    except (OSError, UnicodeError) as error:
        raise InitializationError("environment file could not be read as UTF-8") from error

    values = _parse_assignments(content)
    for name, placeholder in _PLACEHOLDERS.items():
        if name not in values:
            raise InitializationError(f"required credential variable is missing: {name}")
        value = values[name]
        if "change-me" in value.lower() and value != placeholder:
            raise InitializationError(f"unrecognized placeholder form for {name}")
    for name, value in values.items():
        if "change-me" in value.lower() and name not in _PLACEHOLDERS:
            raise InitializationError(f"unrecognized placeholder variable: {name}")

    placeholder_names = {
        name for name, placeholder in _PLACEHOLDERS.items() if values[name] == placeholder
    }
    if placeholder_names and len(placeholder_names) != len(_PLACEHOLDERS):
        raise InitializationError(
            "environment is partially initialized; refusing to overwrite existing credentials"
        )

    changed = bool(placeholder_names)
    if changed:
        initialized_content = _replace_placeholders(content, _generated_values())
        _write_atomically(target_path, initialized_content, preflight=preflight)
    else:
        preflight(target_path)
        target_path.chmod(0o600)

    output(f"Local environment initialization: PASS ({'created' if changed else 'unchanged'})")
    return InitializationResult(changed=changed, target_path=target_path)


def main(argv: Sequence[str] | None = None) -> int:
    """Initialize the selected local dotenv path."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) > 1:
        print("usage: init_local_env.py [ENV_FILE]", file=sys.stderr)
        return 2

    repository_root = Path(__file__).resolve().parents[1]
    target_path = Path(arguments[0] if arguments else ".env")
    try:
        initialize_local_environment(
            example_path=repository_root / ".env.example",
            target_path=target_path,
        )
    except InitializationError as error:
        print(f"Local environment initialization: FAIL - {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
