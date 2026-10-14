"""Pure and runtime contracts for supplier-file ingestion."""

from .contracts import (
    DEFAULT_VALIDATION_CONTRACT,
    SUPPLIER_SOURCE_NAME,
    DeliveryDescriptor,
    FileFormat,
    is_supported_source,
    parse_landing_key,
)
from .diagnostics import (
    REJECTION_PRECEDENCE,
    BoundedDiagnostics,
    DuplicateClass,
    FileDiagnostic,
    FileRecordCounters,
    RejectionCode,
    bounded_diagnostics,
    classify_duplicate,
    delivery_rejection_codes,
    file_record_counters,
    primary_rejection_code,
)
from .events import FileIngestionEvent, FileOutcome
from .service import (
    FileIngestionResult,
    FileIngestionStateError,
    FileStorage,
    ingest_file_delivery,
)
from .validators import FileSizeLimitError, FileValidationResult, validate_file

__all__ = [
    "DEFAULT_VALIDATION_CONTRACT",
    "REJECTION_PRECEDENCE",
    "SUPPLIER_SOURCE_NAME",
    "BoundedDiagnostics",
    "DeliveryDescriptor",
    "DuplicateClass",
    "FileDiagnostic",
    "FileFormat",
    "FileIngestionEvent",
    "FileIngestionResult",
    "FileIngestionStateError",
    "FileOutcome",
    "FileRecordCounters",
    "FileSizeLimitError",
    "FileStorage",
    "FileValidationResult",
    "RejectionCode",
    "bounded_diagnostics",
    "classify_duplicate",
    "delivery_rejection_codes",
    "file_record_counters",
    "ingest_file_delivery",
    "is_supported_source",
    "parse_landing_key",
    "primary_rejection_code",
    "validate_file",
]
