"""Streaming checksum primitives."""

from collections.abc import Callable
from hashlib import sha256
from typing import BinaryIO

DEFAULT_CHUNK_SIZE = 64 * 1024


def sha256_stream(
    stream: BinaryIO,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    on_chunk: Callable[[int], None] | None = None,
) -> str:
    """Return a lowercase SHA-256 digest without reading the stream all at once."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    digest = sha256()
    while chunk := stream.read(chunk_size):
        digest.update(chunk)
        if on_chunk is not None:
            on_chunk(len(chunk))
    return digest.hexdigest()
