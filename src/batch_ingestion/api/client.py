"""Explicit bounded HTTPX client construction for REST ingestion."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import httpx

from batch_ingestion.config import HttpConfig


def timeout_for_remaining(
    config: HttpConfig,
    *,
    remaining_seconds: float | None = None,
) -> httpx.Timeout:
    """Build four explicit phase timeouts, clipped to one monotonic budget."""
    if remaining_seconds is not None and remaining_seconds <= 0:
        raise ValueError("remaining_seconds must be positive")

    def clipped(configured: float) -> float:
        if remaining_seconds is None:
            return configured
        return min(configured, remaining_seconds)

    return httpx.Timeout(
        connect=clipped(config.connect_timeout_seconds),
        read=clipped(config.read_timeout_seconds),
        write=clipped(config.read_timeout_seconds),
        pool=clipped(config.connect_timeout_seconds),
    )


@contextmanager
def open_http_client(config: HttpConfig) -> Iterator[httpx.Client]:
    """Open exactly one pooled client with environment integration disabled."""
    with httpx.Client(
        base_url=config.base_url,
        timeout=timeout_for_remaining(config),
        trust_env=False,
    ) as client:
        yield client
