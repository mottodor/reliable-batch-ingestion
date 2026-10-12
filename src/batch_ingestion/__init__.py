"""Reliable batch ingestion lab core contracts."""

from .checksums import sha256_stream
from .config import (
    AppConfig,
    ConfigurationError,
    HttpConfig,
    PostgresConfig,
    StorageConfig,
)
from .identifiers import (
    BatchIdentity,
    SourceType,
    derive_batch_id,
    manifest_key_for,
    new_run_id,
    raw_object_key,
)
from .manifests import (
    MANIFEST_SCHEMA_VERSION,
    TERMINAL_STATUSES,
    Manifest,
    ManifestOutcome,
    ManifestStatus,
    can_transition,
    status_for_outcome,
    validate_status_transition,
)
from .summary import RunSummary

__version__ = "0.1.0"

__all__ = [
    "MANIFEST_SCHEMA_VERSION",
    "TERMINAL_STATUSES",
    "AppConfig",
    "BatchIdentity",
    "ConfigurationError",
    "HttpConfig",
    "Manifest",
    "ManifestOutcome",
    "ManifestStatus",
    "PostgresConfig",
    "RunSummary",
    "SourceType",
    "StorageConfig",
    "can_transition",
    "derive_batch_id",
    "manifest_key_for",
    "new_run_id",
    "raw_object_key",
    "sha256_stream",
    "status_for_outcome",
    "validate_status_transition",
]
