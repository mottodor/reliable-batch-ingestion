#!/usr/bin/env python3
"""Generate the deterministic supplier fixture corpus into an explicit directory."""

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from batch_ingestion.source_seed import SourceSeedError, parse_source_seed
from batch_ingestion.supplier_fixtures import (
    DEFAULT_FIXTURE_SOURCE_DATE,
    generate_supplier_fixture_corpus,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Validate SOURCE_SEED, then replace the selected output corpus."""
    arguments = _parser().parse_args(argv)
    try:
        seed = parse_source_seed(os.environ.get("SOURCE_SEED", ""))
        index = generate_supplier_fixture_corpus(
            arguments.output,
            seed=seed,
            source_date=DEFAULT_FIXTURE_SOURCE_DATE,
        )
    except (SourceSeedError, OSError, ValueError) as error:
        print(f"Fixture generation failed: {error}", file=sys.stderr)
        return 1

    print(
        f"Generated {len(index.files)} supplier fixture files in {arguments.output} "
        f"with SOURCE_SEED={seed}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
