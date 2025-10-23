"""Schema metadata used by the UI and ingestion helpers."""

from .canonical import (
    CANONICAL,
    ECU_OPTIONAL,
    ECU_REQUIRED,
    GPS_OPTIONAL,
    GPS_REQUIRED,
    PEMS_OPTIONAL,
    PEMS_REQUIRED,
    UNIT_HINTS,
    DatasetSchema,
    as_payload,
    get_schema,
)

__all__ = [
    "CANONICAL",
    "DatasetSchema",
    "as_payload",
    "ECU_OPTIONAL",
    "ECU_REQUIRED",
    "GPS_OPTIONAL",
    "GPS_REQUIRED",
    "PEMS_OPTIONAL",
    "PEMS_REQUIRED",
    "UNIT_HINTS",
    "get_schema",
]
