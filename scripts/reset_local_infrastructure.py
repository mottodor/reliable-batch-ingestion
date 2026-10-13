#!/usr/bin/env python3
"""Execute an explicitly confirmed, project-scoped local Compose reset."""

import argparse
import shlex
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

CONFIRMATION_TOKEN = "destroy-local-data"
COMMAND_TIMEOUT_SECONDS = 120


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm", default="")
    parser.add_argument("--compose", default="docker compose")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--project-name", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Validate confirmation/configuration, then remove one project's volumes."""
    arguments = _parser().parse_args(argv)
    if arguments.confirm != CONFIRMATION_TOKEN:
        print(
            f"ERROR: destructive reset requires CONFIRM={CONFIRMATION_TOKEN}. Nothing was deleted.",
            file=sys.stderr,
        )
        return 2

    preflight_command = [
        sys.executable,
        str(Path(__file__).with_name("check_local_env.py")),
        str(arguments.env_file),
    ]
    preflight = subprocess.run(
        preflight_command,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    if preflight.returncode != 0:
        return preflight.returncode

    compose_command = [
        *shlex.split(arguments.compose),
        "--project-name",
        arguments.project_name,
        "--env-file",
        str(arguments.env_file),
        "down",
        "--volumes",
        "--remove-orphans",
    ]
    result = subprocess.run(
        compose_command,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
