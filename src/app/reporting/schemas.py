"""Typed models for EU7-LD conformity reporting payloads."""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class PassFail(str, Enum):
    """Canonical pass/fail state for conformity criteria."""

    PASS = "pass"
    FAIL = "fail"
    NA = "n/a"

    @classmethod
    def from_literal(cls, value: Literal["pass", "fail", "n/a"]) -> "PassFail":
        """Coerce raw literal values into the enum."""

        return cls(value)


class Criterion(BaseModel):
    """Single evaluation line item within a conformity section."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="Stable identifier for lookups/guards")
    section: str = Field(description="Section the criterion belongs to")
    clause: str | None = Field(default=None, description="Regulation clause reference")
    description: str = Field(description="Human readable summary of the requirement")
    limit: str = Field(description="Requirement limit with units")
    value: float | int | str | None = Field(
        default=None,
        description="Raw computed value without any unit decoration",
    )
    measured: str | None = Field(default=None, description="Measured value rendered as text")
    unit: str | None = Field(default=None, description="Unit string (for tabular displays)")
    result: PassFail = Field(description="Outcome of the criterion evaluation")


class EmissionBlock(BaseModel):
    """Emission metrics for a specific drive block (urban/trip)."""

    model_config = ConfigDict(extra="ignore")

    label: str
    CO2_g_km: float | None = Field(default=None, alias="CO2_g_km")
    CO_mg_km: float | None = Field(default=None)
    NOx_mg_km: float | None = Field(default=None)
    PN_hash_km: float | None = Field(default=None, alias="PN_hash_km")


class FinalLimitsEU7LD(BaseModel):
    """Regulatory limits applicable to the final conformity assessment."""

    model_config = ConfigDict(extra="ignore")

    CO_mg_km_WLTP: float
    NOx_mg_km_RDE: float
    PN_hash_km_RDE: float


class TripMeta(BaseModel):
    """Metadata describing the trip and legislative context."""

    model_config = ConfigDict(extra="ignore")

    testId: str
    engine: str
    propulsion: str
    legislation: str
    testStart: str
    printout: str
    velocitySource: Literal["ECU", "GPS"]


class DeviceInfo(BaseModel):
    """Measurement device identifiers."""

    model_config = ConfigDict(extra="ignore")

    gasPEMS: str
    pnPEMS: str
    efm: str | None = None


class EmissionSummary(BaseModel):
    """Container for the urban/trip emission aggregates."""

    model_config = ConfigDict(extra="ignore")

    urban: EmissionBlock
    trip: EmissionBlock


class ReportData(BaseModel):
    """Full EU7-LD conformity report payload."""

    model_config = ConfigDict(extra="ignore")

    meta: TripMeta
    limits: FinalLimitsEU7LD
    criteria: list[Criterion]
    emissions: EmissionSummary
    device: DeviceInfo


__all__ = [
    "PassFail",
    "Criterion",
    "EmissionBlock",
    "EmissionSummary",
    "FinalLimitsEU7LD",
    "TripMeta",
    "DeviceInfo",
    "ReportData",
]

