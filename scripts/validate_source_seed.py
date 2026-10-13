#!/usr/bin/env python3
"""Validate one SOURCE_SEED before any stateful seed command runs."""

import os
import sys
from collections.abc import Sequence

from batch_ingestion.source_seed import SourceSeedError, parse_source_seed


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the exported seed without echoing rejected input."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments:
        print("usage: validate_source_seed.py", file=sys.stderr)
        return 2

    try:
        source_seed = parse_source_seed(os.environ.get("SOURCE_SEED", ""))
    except SourceSeedError as error:
        print(f"SOURCE_SEED validation failed: {error}", file=sys.stderr)
        return 1

    print(f"SOURCE_SEED validation: PASS ({source_seed})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
