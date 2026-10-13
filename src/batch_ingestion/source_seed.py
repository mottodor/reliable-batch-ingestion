"""Validated deterministic seed primitives for generated source data."""

import json
import re
from datetime import date, datetime
from hashlib import sha256

DEFAULT_SOURCE_SEED = 20260901
MAX_SOURCE_SEED = 2_147_483_647
_DECIMAL_SEED = re.compile(r"[0-9]+", flags=re.ASCII)


class SourceSeedError(ValueError):
    """Raised when SOURCE_SEED is missing, malformed, or out of range."""


def parse_source_seed(value: str) -> int:
    """Parse an ASCII decimal SOURCE_SEED without accepting implicit coercions."""
    if not isinstance(value, str):
        raise TypeError("SOURCE_SEED must be text")
    if _DECIMAL_SEED.fullmatch(value) is None:
        raise SourceSeedError("SOURCE_SEED must contain only ASCII decimal digits")
    try:
        seed = int(value)
    except ValueError as error:  # Defensive if integer parsing rules ever diverge from the regex.
        raise SourceSeedError("SOURCE_SEED must contain only ASCII decimal digits") from error
    return validate_source_seed(seed)


def validate_source_seed(seed: int) -> int:
    """Return a seed inside the public deterministic generation range."""
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError("SOURCE_SEED must be an integer")
    if not 0 <= seed <= MAX_SOURCE_SEED:
        raise SourceSeedError(f"SOURCE_SEED must be between 0 and {MAX_SOURCE_SEED}")
    return seed


def validate_source_date(source_date: date) -> date:
    """Require an explicit calendar date rather than a wall-clock timestamp."""
    if isinstance(source_date, datetime) or not isinstance(source_date, date):
        raise TypeError("source_date must be a date")
    return source_date


def derive_seeded_int(
    seed: int,
    *,
    namespace: str,
    source_date: date,
    ordinal: int,
    field: str,
    modulo: int,
) -> int:
    """Derive a stable bounded integer from canonical logical source inputs."""
    validated_seed = validate_source_seed(seed)
    validated_date = validate_source_date(source_date)
    if not namespace or not field:
        raise ValueError("namespace and field must not be empty")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
        raise ValueError("ordinal must be a non-negative integer")
    if isinstance(modulo, bool) or not isinstance(modulo, int) or modulo <= 0:
        raise ValueError("modulo must be a positive integer")

    canonical = json.dumps(
        {
            "field": field,
            "namespace": namespace,
            "ordinal": ordinal,
            "seed": validated_seed,
            "source_date": validated_date.isoformat(),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return int.from_bytes(sha256(canonical).digest()[:8], byteorder="big") % modulo


def deterministic_order_id(seed: int, ordinal: int) -> int:
    """Return the stable order identifier shared by generated source records."""
    validated_seed = validate_source_seed(seed)
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
        raise ValueError("ordinal must be a non-negative integer")
    return validated_seed * 1_000 + ordinal + 1
