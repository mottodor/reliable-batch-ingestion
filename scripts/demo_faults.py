"""Demo-only one-shot failpoints for BI-080 process-boundary exercises.

Production packages never import this module.  Every wrapper delegates to an existing
storage/client seam and is inert until its narrow prerequisite has completed.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from batch_ingestion.identifiers import manifest_key_for

EXPECTED_CRASH_EXIT = 86
Crash = Callable[[str, str], None]


def hard_crash(boundary: str, object_key: str) -> None:
    """Emit one bounded marker and terminate without Python cleanup."""
    payload = (
        '{"boundary":"'
        + boundary
        + '","event":"demo_worker_boundary","object_key":"'
        + object_key
        + '"}\n'
    ).encode("utf-8")
    os.write(1, payload)
    os._exit(EXPECTED_CRASH_EXIT)


class ManifestBoundaryStorage:
    """Crash immediately before or after the first matching manifest write."""

    def __init__(
        self,
        storage: Any,
        *,
        after_manifest: bool,
        crash: Crash = hard_crash,
    ) -> None:
        self._storage = storage
        self._after_manifest = after_manifest
        self._crash = crash
        self._payload_keys: set[str] = set()
        self.fired = False

    def reconcile_payload_manifest(self, *args: object, **kwargs: object) -> object:
        method = self._storage.reconcile_payload_manifest
        return method(*args, **kwargs)

    def write_immutable(self, key: str, stream: object, **kwargs: object) -> object:
        method = self._storage.write_immutable
        if key.startswith("raw/"):
            result = method(key, stream, **kwargs)
            self._payload_keys.add(key)
            return result

        matching_payload = next(
            (payload for payload in self._payload_keys if manifest_key_for(payload) == key),
            None,
        )
        if key.startswith("manifests/") and matching_payload is not None and not self.fired:
            self.fired = True
            if not self._after_manifest:
                self._crash("payload_before_manifest", matching_payload)
            result = method(key, stream, **kwargs)
            self._crash("api_page_complete", matching_payload)
            return result
        return method(key, stream, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._storage, name)


class DeleteBoundaryClient:
    """Crash at DeleteObject only after CopyObject and destination verification."""

    def __init__(
        self,
        client: Any,
        *,
        source_key: str,
        destination_key: str,
        crash: Crash = hard_crash,
    ) -> None:
        self._client = client
        self._source_key = source_key
        self._destination_key = destination_key
        self._crash = crash
        self._copied = False
        self._destination_read = False
        self.fired = False

    def copy_object(self, **kwargs: object) -> object:
        result = self._client.copy_object(**kwargs)
        copy_source = kwargs.get("CopySource")
        if (
            kwargs.get("Key") == self._destination_key
            and isinstance(copy_source, dict)
            and copy_source.get("Key") == self._source_key
        ):
            self._copied = True
        return result

    def get_object(self, **kwargs: object) -> object:
        result = self._client.get_object(**kwargs)
        if self._copied and kwargs.get("Key") == self._destination_key:
            self._destination_read = True
        return result

    def delete_object(self, **kwargs: object) -> object:
        if (
            not self.fired
            and self._copied
            and self._destination_read
            and kwargs.get("Key") == self._source_key
        ):
            self.fired = True
            self._crash("file_destination_verified_before_landing_delete", self._source_key)
        return self._client.delete_object(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)
