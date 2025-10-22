"""Schemas for PEMS ingestion configuration."""

from __future__ import annotations

from typing import Mapping

from pydantic import BaseModel, field_validator

# Normalized columns (SI-like names; we’ll enforce units later):
CORE_REQUIRED = ("timestamp", "exhaust_flow_kg_s")  # MVP minimal
GASES_OPTIONAL = (
    "nox_ppm",
    "nox_mg_s",
    "no_ppm",
    "no_mg_s",
    "no2_ppm",
    "no2_mg_s",
    "co_ppm",
    "co_mg_s",
    "co_g_s",
    "co2_ppm",
    "co2_mg_s",
    "co2_g_s",
    "thc_ppm",
    "thc_mg_s",
    "nmhc_ppm",
    "nmhc_mg_s",
    "ch4_ppm",
    "ch4_mg_s",
    "nh3_ppm",
    "nh3_mg_s",
    "o2_pct",
    "lambda_ratio",
)
PARTICLE_OPTIONAL = (
    "pn_1_s",
    "pn10_1_s",
    "pn23_1_s",  # count rates (1/s)
    "pm_mg_s",  # mass rate
)
AUX_OPTIONAL = (
    "temp_c",
    "exhaust_temp_c",
    "exhaust_pressure_kpa",
    "amb_temp_c",
    "amb_pressure_kpa",
    "rel_humidity_pct",
    "veh_speed_m_s",
)
ALLOWED = set(CORE_REQUIRED) | set(GASES_OPTIONAL) | set(PARTICLE_OPTIONAL) | set(AUX_OPTIONAL)


class PEMSConfig(BaseModel):
    """
    Mapping from normalized names -> source column names.
    Only keys from ALLOWED are accepted.
    """

    columns: Mapping[str, str]

    @field_validator("columns")
    @classmethod
    def _only_allowed(cls, v: Mapping[str, str]) -> Mapping[str, str]:
        unknown = set(v.keys()) - ALLOWED
        if unknown:
            raise ValueError(
                f"Unknown normalized keys: {sorted(unknown)}. Allowed: {sorted(ALLOWED)}"
            )
        return v


__all__ = [
    "PEMSConfig",
    "CORE_REQUIRED",
    "GASES_OPTIONAL",
    "PARTICLE_OPTIONAL",
    "AUX_OPTIONAL",
    "ALLOWED",
]
