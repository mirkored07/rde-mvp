from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

# Canonical columns (more can be added later)
PEMS_REQUIRED: Sequence[str] = ("timestamp",)
PEMS_OPTIONAL: Sequence[str] = (
    "exhaust_flow_kg_s",
    "nox_mg_s",
    "pn_1_s",
    "co_ppm",
    "thc_ppm",
    "pm_mg_s",
    "thc_mg_s",
    "exhaust_temp_c",
    "amb_temp_c",
    "veh_speed_m_s",
)
GPS_REQUIRED: Sequence[str] = ("timestamp", "lat", "lon")
GPS_OPTIONAL: Sequence[str] = ("speed_m_s", "alt_m")
ECU_REQUIRED: Sequence[str] = ()  # if no timestamps, require counter+rate in the wizard
ECU_OPTIONAL: Sequence[str] = (
    "timestamp",
    "veh_speed_m_s",
    "engine_speed_rpm",
    "engine_load_pct",
    "throttle_pct",
)


@dataclass(frozen=True)
class DatasetSchema:
    """Description of a canonical schema for a telemetry dataset."""

    key: str
    label: str
    required: tuple[str, ...]
    optional: tuple[str, ...]

    @property
    def all_fields(self) -> tuple[str, ...]:
        return self.required + self.optional

    def as_payload(self) -> Mapping[str, object]:
        return {
            "key": self.key,
            "label": self.label,
            "required": list(self.required),
            "optional": list(self.optional),
        }


CANONICAL: Mapping[str, DatasetSchema] = {
    "pems": DatasetSchema(
        key="pems",
        label="PEMS",
        required=tuple(PEMS_REQUIRED),
        optional=tuple(PEMS_OPTIONAL),
    ),
    "gps": DatasetSchema(
        key="gps",
        label="GPS",
        required=tuple(GPS_REQUIRED),
        optional=tuple(GPS_OPTIONAL),
    ),
    "ecu": DatasetSchema(
        key="ecu",
        label="ECU",
        required=tuple(ECU_REQUIRED),
        optional=tuple(ECU_OPTIONAL),
    ),
}

# Suggested unit expectations
UNIT_HINTS: Mapping[str, str] = {
    "exhaust_flow_kg_s": "kg/s",
    "nox_mg_s": "mg/s",
    "pn_1_s": "1/s",
    "pm_mg_s": "mg/s",
    "thc_mg_s": "mg/s",
    "exhaust_temp_c": "degC",
    "amb_temp_c": "degC",
    "speed_m_s": "m/s",
    "veh_speed_m_s": "m/s",
}


def get_schema(dataset: str) -> DatasetSchema:
    try:
        return CANONICAL[dataset]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise KeyError(f"Unknown dataset '{dataset}'.") from exc


def as_payload() -> list[Mapping[str, object]]:
    """Return all canonical schemas as JSON-serialisable payloads."""

    return [schema.as_payload() for schema in CANONICAL.values()]


__all__ = [
    "CANONICAL",
    "DatasetSchema",
    "ECU_OPTIONAL",
    "ECU_REQUIRED",
    "GPS_OPTIONAL",
    "GPS_REQUIRED",
    "PEMS_OPTIONAL",
    "PEMS_REQUIRED",
    "UNIT_HINTS",
    "as_payload",
    "get_schema",
]
