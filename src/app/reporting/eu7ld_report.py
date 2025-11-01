"""Helpers to assemble, validate, and persist EU7-LD conformity reports."""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .schemas import (
    Criterion,
    DeviceInfo,
    EmissionBlock,
    EmissionSummary,
    FinalLimitsEU7LD,
    PassFail,
    ReportData,
    TripMeta,
)


DEFAULT_LIMITS = FinalLimitsEU7LD(
    CO_mg_km_WLTP=1000.0,
    NOx_mg_km_RDE=60.0,
    PN_hash_km_RDE=6.0e11,
)

_REPORT_DIR = Path(os.environ.get("REPORT_DIR", "reports"))


def _ensure_report_dir() -> Path:
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    return _REPORT_DIR


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _format_value(value: float | None, unit: str, precision: int = 1) -> str:
    if value is None:
        return "n/a"
    formatted = f"{value:.{precision}f}".rstrip("0").rstrip(".")
    return f"{formatted} {unit}".strip()


def _parse_numeric(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"-?\d+(?:[.,]\d+)?(?:e[+-]?\d+)?", value)
    if not match:
        return None
    cleaned = match.group(0).replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _build_trip_meta(meta: Mapping[str, Any]) -> TripMeta:
    fallback_time = _iso_now()
    velocity_source = (meta.get("velocity_source") or "GPS").upper()
    if velocity_source not in {"ECU", "GPS"}:
        velocity_source = "GPS"
    return TripMeta(
        testId=str(meta.get("test_id") or meta.get("testId") or "demo-sample"),
        engine=str(meta.get("engine") or "WLTP-ICE"),
        propulsion=str(meta.get("propulsion") or "ICE"),
        legislation=str(
            meta.get("legislation")
            or "UN 168 LD – Certification (EU7 LD aligned)"
        ),
        testStart=str(meta.get("test_start") or meta.get("testStart") or fallback_time),
        printout=str(meta.get("printout") or fallback_time),
        velocitySource="ECU" if velocity_source == "ECU" else "GPS",
    )


def _build_device(meta: Mapping[str, Any]) -> DeviceInfo:
    devices = meta.get("devices") if isinstance(meta.get("devices"), Mapping) else {}
    return DeviceInfo(
        gasPEMS=str(devices.get("gas_pems") or devices.get("gasPEMS") or "AVL GAS 601"),
        pnPEMS=str(devices.get("pn_pems") or devices.get("pnPEMS") or "AVL PN PEMS 483"),
        efm=(devices.get("efm") if isinstance(devices.get("efm"), str) else None),
    )


def _derive_multiplier(criteria: Iterable[Criterion]) -> float:
    for item in criteria:
        if item.id == "preconditioning:soak-temperature":
            temp = _parse_numeric(item.measured)
            if temp is not None and (temp < -7 or temp > 35):
                return 1.6
    return 1.0


def _clamp_non_negative(value: float | None) -> float | None:
    if value is None:
        return None
    return value if value >= 0 else 0.0


def _derive_emissions(
    source: Mapping[str, Any],
    criteria: list[Criterion],
) -> EmissionSummary:
    emissions = source.get("emissions") if isinstance(source.get("emissions"), Mapping) else {}
    urban_src = emissions.get("urban") if isinstance(emissions.get("urban"), Mapping) else {}
    trip_src = emissions.get("trip") if isinstance(emissions.get("trip"), Mapping) else {}
    meta = source.get("meta") if isinstance(source.get("meta"), Mapping) else {}
    if not trip_src and meta:
        trip_src = {
            "NOx_mg_km": meta.get("nox_mg_per_km"),
            "PN_hash_km": meta.get("pn_per_km"),
            "CO_mg_km": meta.get("co_mg_per_km"),
        }

    multiplier = _derive_multiplier(criteria)

    def _block(label: str, payload: Mapping[str, Any], apply_multiplier: bool) -> EmissionBlock:
        co2 = payload.get("CO2_g_km")
        co = payload.get("CO_mg_km")
        nox = payload.get("NOx_mg_km")
        pn = payload.get("PN_hash_km")
        if apply_multiplier:
            if isinstance(nox, (int, float)):
                nox = float(nox) * multiplier
            if isinstance(pn, (int, float)):
                pn = float(pn) * multiplier
        return EmissionBlock(
            label=label,
            CO2_g_km=float(co2) if isinstance(co2, (int, float)) else None,
            CO_mg_km=float(co) if isinstance(co, (int, float)) else None,
            NOx_mg_km=_clamp_non_negative(
                float(nox) if isinstance(nox, (int, float)) else None
            ),
            PN_hash_km=_clamp_non_negative(
                float(pn) if isinstance(pn, (int, float)) else None
            ),
        )

    urban_block = _block("Urban", urban_src, apply_multiplier=False)
    trip_block = _block("Trip", trip_src or urban_src, apply_multiplier=True)

    return EmissionSummary(urban=urban_block, trip=trip_block)


def _result_from_limit(value: float | None, limit: float | None) -> PassFail:
    if limit is None or value is None:
        return PassFail.NA
    return PassFail.PASS if value <= limit else PassFail.FAIL


def _update_conformity_criteria(
    criteria: list[Criterion],
    emissions: EmissionSummary,
    limits: FinalLimitsEU7LD,
) -> None:
    mapping = {
        "conformity:nox": (emissions.trip.NOx_mg_km, limits.NOx_mg_km_RDE, "mg/km"),
        "conformity:pn": (emissions.trip.PN_hash_km, limits.PN_hash_km_RDE, "#/km"),
        "conformity:co": (emissions.trip.CO_mg_km, limits.CO_mg_km_WLTP, "mg/km"),
    }
    for item in criteria:
        if item.id in mapping:
            value, limit, unit = mapping[item.id]
            if value is not None:
                item.measured = _format_value(value, unit)
            item.result = _result_from_limit(value, limit)


def _group_default_inputs(meta: Mapping[str, Any]) -> dict[str, Any]:
    defaults = {
        "co2_zero_drift_pct": 0.4,
        "co_zero_drift_pct": 0.3,
        "nox_zero_drift_pct": 0.5,
        "co2_span_drift_pct": 0.8,
        "co_span_drift_pct": 0.9,
        "nox_span_drift_pct": 0.7,
        "pn_zero_pre": 250,
        "pn_zero_post": 260,
        "span_co2_pct": 92,
        "span_co_pct": 90,
        "span_nox_pct": 94,
        "op_time_minutes": 12,
        "soak_hours": 9,
        "soak_temp_c": 8,
        "cold_start_window_avg_kmh": 24,
        "cold_start_window_max_kmh": 58,
        "cold_start_movement_s": 12,
        "cold_start_stops_s": 60,
        "urban_km": 18,
        "expressway_km": 21,
        "urban_share_pct": 55,
        "expressway_share_pct": 45,
        "trip_duration_min": 102,
        "delta_elevation_m": 80,
        "cumulative_elev_trip": 900,
        "cumulative_elev_urban": 780,
        "extended_conditions": False,
        "gps_loss_single_s": 18,
        "gps_loss_total_s": 110,
        "gps_ecu_delta_pct": 1.8,
        "accel_count_urban": 240,
        "accel_count_expressway": 180,
        "va_pos95_urban": 0.18,
        "va_pos95_expressway": 0.23,
        "rpa_urban": 0.28,
        "rpa_expressway": 0.32,
        "co2_window_low_pct": 58,
        "co2_window_high_pct": 54,
        "leak_checks_pass": True,
    }
    conformity = meta.get("conformity_inputs")
    if isinstance(conformity, Mapping):
        for key, value in conformity.items():
            defaults[key] = value
    return defaults


def _criterion(
    *,
    ident: str,
    section: str,
    description: str,
    limit: str,
    clause: str | None,
    measured: str,
    unit: str | None,
    passed: bool | None,
) -> Criterion:
    result: PassFail
    if passed is None:
        result = PassFail.NA
    else:
        result = PassFail.PASS if passed else PassFail.FAIL
    return Criterion(
        id=ident,
        section=section,
        clause=clause,
        description=description,
        limit=limit,
        measured=measured,
        unit=unit,
        result=result,
    )


def _build_criteria(meta: Mapping[str, Any]) -> list[Criterion]:
    inputs = _group_default_inputs(meta)
    sections: list[Criterion] = []

    sections.extend(
        [
            _criterion(
                ident="zero-span:co2-zero",
                section="Zero/Span Drift",
                clause="§6.1",
                description="CO₂ zero drift",
                limit="≤ 2 % drift",
                measured=_format_value(inputs["co2_zero_drift_pct"], "%"),
                unit="%",
                passed=inputs["co2_zero_drift_pct"] <= 2,
            ),
            _criterion(
                ident="zero-span:co2-span",
                section="Zero/Span Drift",
                clause="§6.1",
                description="CO₂ span drift",
                limit="≤ 3 % drift",
                measured=_format_value(inputs["co2_span_drift_pct"], "%"),
                unit="%",
                passed=inputs["co2_span_drift_pct"] <= 3,
            ),
            _criterion(
                ident="zero-span:co-zero",
                section="Zero/Span Drift",
                clause="§6.1",
                description="CO zero drift",
                limit="≤ 2 % drift",
                measured=_format_value(inputs["co_zero_drift_pct"], "%"),
                unit="%",
                passed=inputs["co_zero_drift_pct"] <= 2,
            ),
            _criterion(
                ident="zero-span:co-span",
                section="Zero/Span Drift",
                clause="§6.1",
                description="CO span drift",
                limit="≤ 3 % drift",
                measured=_format_value(inputs["co_span_drift_pct"], "%"),
                unit="%",
                passed=inputs["co_span_drift_pct"] <= 3,
            ),
            _criterion(
                ident="zero-span:nox-zero",
                section="Zero/Span Drift",
                clause="§6.1",
                description="NOx zero drift",
                limit="≤ 2 % drift",
                measured=_format_value(inputs["nox_zero_drift_pct"], "%"),
                unit="%",
                passed=inputs["nox_zero_drift_pct"] <= 2,
            ),
            _criterion(
                ident="zero-span:nox-span",
                section="Zero/Span Drift",
                clause="§6.1",
                description="NOx span drift",
                limit="≤ 3 % drift",
                measured=_format_value(inputs["nox_span_drift_pct"], "%"),
                unit="%",
                passed=inputs["nox_span_drift_pct"] <= 3,
            ),
        ]
    )

    sections.extend(
        [
            _criterion(
                ident="pn:zero-pre",
                section="Particle Number Zeros",
                clause="§4.6",
                description="PN zero (pre-test)",
                limit="≤ 500 #/cm³",
                measured=_format_value(inputs["pn_zero_pre"], "#/cm³", precision=0),
                unit="#/cm³",
                passed=inputs["pn_zero_pre"] <= 500,
            ),
            _criterion(
                ident="pn:zero-post",
                section="Particle Number Zeros",
                clause="§4.6",
                description="PN zero (post-test)",
                limit="≤ 500 #/cm³",
                measured=_format_value(inputs["pn_zero_post"], "#/cm³", precision=0),
                unit="#/cm³",
                passed=inputs["pn_zero_post"] <= 500,
            ),
        ]
    )

    sections.extend(
        [
            _criterion(
                ident="span:co2",
                section="Span Coverage",
                clause="§6.3",
                description="CO₂ span coverage",
                limit="≥ 85 % (void if >200 %)",
                measured=_format_value(inputs["span_co2_pct"], "%"),
                unit="%",
                passed=85 <= inputs["span_co2_pct"] <= 200,
            ),
            _criterion(
                ident="span:co",
                section="Span Coverage",
                clause="§6.3",
                description="CO span coverage",
                limit="≥ 85 % (void if >200 %)",
                measured=_format_value(inputs["span_co_pct"], "%"),
                unit="%",
                passed=85 <= inputs["span_co_pct"] <= 200,
            ),
            _criterion(
                ident="span:nox",
                section="Span Coverage",
                clause="§6.3",
                description="NOx span coverage",
                limit="≥ 85 % (void if >200 %)",
                measured=_format_value(inputs["span_nox_pct"], "%"),
                unit="%",
                passed=85 <= inputs["span_nox_pct"] <= 200,
            ),
        ]
    )

    sections.extend(
        [
            _criterion(
                ident="preconditioning:operation-time",
                section="Preconditioning & Soak",
                clause="§8.3.2",
                description="10 min per operation type",
                limit="≥ 10 min",
                measured=_format_value(inputs["op_time_minutes"], "min", precision=0),
                unit="min",
                passed=inputs["op_time_minutes"] >= 10,
            ),
            _criterion(
                ident="preconditioning:soak-duration",
                section="Preconditioning & Soak",
                clause="§10.6",
                description="Soak duration",
                limit="6–72 h",
                measured=_format_value(inputs["soak_hours"], "h", precision=1),
                unit="h",
                passed=6 <= inputs["soak_hours"] <= 72,
            ),
            _criterion(
                ident="preconditioning:soak-temperature",
                section="Preconditioning & Soak",
                clause="§10.6",
                description="Soak temperature",
                limit="-7 to 35 °C",
                measured=_format_value(inputs["soak_temp_c"], "°C", precision=1),
                unit="°C",
                passed=-7 <= inputs["soak_temp_c"] <= 35,
            ),
            _criterion(
                ident="preconditioning:cold-start-multiplier",
                section="Preconditioning & Soak",
                clause="§10.6",
                description="Cold-start multiplier applied",
                limit="1.6× if extended",
                measured="Applied" if inputs["soak_temp_c"] < -7 or inputs["soak_temp_c"] > 35 else "Not required",
                unit=None,
                passed=True,
            ),
        ]
    )

    sections.append(
        _criterion(
            ident="start-end:ice",
            section="Start/End Information",
            clause="§8.1",
            description="ICE start/end confirmation",
            limit="Logged",
            measured="Engine start 07:32 / stop 09:14",
            unit=None,
            passed=True,
        )
    )

    sections.extend(
        [
            _criterion(
                ident="cold-start:avg-speed",
                section="Cold-start Window",
                clause="§9.3.4",
                description="Average speed",
                limit="15–40 km/h",
                measured=_format_value(
                    inputs["cold_start_window_avg_kmh"], "km/h", precision=1
                ),
                unit="km/h",
                passed=15 <= inputs["cold_start_window_avg_kmh"] <= 40,
            ),
            _criterion(
                ident="cold-start:max-speed",
                section="Cold-start Window",
                clause="§3.8.2",
                description="Maximum speed",
                limit="≤ 60 km/h",
                measured=_format_value(
                    inputs["cold_start_window_max_kmh"], "km/h", precision=1
                ),
                unit="km/h",
                passed=inputs["cold_start_window_max_kmh"] <= 60,
            ),
            _criterion(
                ident="cold-start:movement",
                section="Cold-start Window",
                clause="§3.8.2",
                description="Movement after engine start",
                limit="≤ 15 s",
                measured=_format_value(inputs["cold_start_movement_s"], "s", precision=0),
                unit="s",
                passed=inputs["cold_start_movement_s"] <= 15,
            ),
            _criterion(
                ident="cold-start:stops",
                section="Cold-start Window",
                clause="§3.8.2",
                description="Stops within window",
                limit="≤ 90 s",
                measured=_format_value(inputs["cold_start_stops_s"], "s", precision=0),
                unit="s",
                passed=inputs["cold_start_stops_s"] <= 90,
            ),
        ]
    )

    sections.extend(
        [
            _criterion(
                ident="trip:urban-distance",
                section="Trip Shares & Distances",
                clause="§9.2",
                description="Urban distance",
                limit="≥ 16 km",
                measured=_format_value(inputs["urban_km"], "km", precision=1),
                unit="km",
                passed=inputs["urban_km"] >= 16,
            ),
            _criterion(
                ident="trip:expressway-distance",
                section="Trip Shares & Distances",
                clause="§9.2",
                description="Expressway distance",
                limit="≥ 16 km",
                measured=_format_value(inputs["expressway_km"], "km", precision=1),
                unit="km",
                passed=inputs["expressway_km"] >= 16,
            ),
            _criterion(
                ident="trip:urban-share",
                section="Trip Shares & Distances",
                clause="§9.3.2",
                description="Urban share",
                limit="55 ± 10 %",
                measured=_format_value(inputs["urban_share_pct"], "%"),
                unit="%",
                passed=45 <= inputs["urban_share_pct"] <= 65,
            ),
            _criterion(
                ident="trip:expressway-share",
                section="Trip Shares & Distances",
                clause="§9.3.2",
                description="Expressway share",
                limit="45 ± 10 %",
                measured=_format_value(inputs["expressway_share_pct"], "%"),
                unit="%",
                passed=35 <= inputs["expressway_share_pct"] <= 55,
            ),
        ]
    )

    sections.extend(
        [
            _criterion(
                ident="trip:duration",
                section="Trip Requirements",
                clause="§9.3.3",
                description="Trip duration",
                limit="90–120 min",
                measured=_format_value(inputs["trip_duration_min"], "min", precision=0),
                unit="min",
                passed=90 <= inputs["trip_duration_min"] <= 120,
            ),
            _criterion(
                ident="trip:delta-elevation",
                section="Trip Requirements",
                clause="§8.1",
                description="Δ elevation",
                limit="≤ 100 m",
                measured=_format_value(inputs["delta_elevation_m"], "m", precision=0),
                unit="m",
                passed=inputs["delta_elevation_m"] <= 100,
            ),
            _criterion(
                ident="trip:cumulative-elevation-trip",
                section="Trip Requirements",
                clause="§8.1",
                description="Cumulative +elevation (trip)",
                limit="≤ 1200 m/100 km",
                measured=_format_value(
                    inputs["cumulative_elev_trip"], "m/100 km", precision=0
                ),
                unit="m/100 km",
                passed=inputs["cumulative_elev_trip"] <= 1200,
            ),
            _criterion(
                ident="trip:cumulative-elevation-urban",
                section="Trip Requirements",
                clause="§8.1",
                description="Cumulative +elevation (urban)",
                limit="≤ 1200 m/100 km",
                measured=_format_value(
                    inputs["cumulative_elev_urban"], "m/100 km", precision=0
                ),
                unit="m/100 km",
                passed=inputs["cumulative_elev_urban"] <= 1200,
            ),
            _criterion(
                ident="trip:extended-conditions",
                section="Trip Requirements",
                clause="§8.1",
                description="Extended conditions rule",
                limit="No auto-void",
                measured="Extended" if inputs["extended_conditions"] else "Nominal",
                unit=None,
                passed=not inputs["extended_conditions"],
            ),
        ]
    )

    sections.extend(
        [
            _criterion(
                ident="gps:ecu-delta",
                section="GPS Consistency",
                clause="§4.7",
                description="ECU vs GPS velocity",
                limit="± 4 %",
                measured=_format_value(inputs["gps_ecu_delta_pct"], "%"),
                unit="%",
                passed=inputs["gps_ecu_delta_pct"] <= 4,
            ),
            _criterion(
                ident="gps:single-loss",
                section="GPS Consistency",
                clause="§6.5",
                description="Single GPS loss",
                limit="≤ 120 s",
                measured=_format_value(inputs["gps_loss_single_s"], "s", precision=0),
                unit="s",
                passed=inputs["gps_loss_single_s"] <= 120,
            ),
            _criterion(
                ident="gps:total-loss",
                section="GPS Consistency",
                clause="§6.5",
                description="Total GPS loss",
                limit="≤ 300 s",
                measured=_format_value(inputs["gps_loss_total_s"], "s", precision=0),
                unit="s",
                passed=inputs["gps_loss_total_s"] <= 300,
            ),
        ]
    )

    sections.extend(
        [
            _criterion(
                ident="dynamics:accel-urban",
                section="Dynamics",
                clause="§3.1.3.1",
                description="Count(a>0.1) urban",
                limit="Tracked",
                measured=_format_value(inputs["accel_count_urban"], "events", precision=0),
                unit="events",
                passed=True,
            ),
            _criterion(
                ident="dynamics:accel-expressway",
                section="Dynamics",
                clause="§3.1.3.1",
                description="Count(a>0.1) expressway",
                limit="Tracked",
                measured=_format_value(inputs["accel_count_expressway"], "events", precision=0),
                unit="events",
                passed=True,
            ),
            _criterion(
                ident="dynamics:va95-urban",
                section="Dynamics",
                clause="§4.1.1",
                description="va_pos95 urban",
                limit="≤ 0.3 m/s²",
                measured=_format_value(inputs["va_pos95_urban"], "m/s²", precision=2),
                unit="m/s²",
                passed=inputs["va_pos95_urban"] <= 0.3,
            ),
            _criterion(
                ident="dynamics:va95-expressway",
                section="Dynamics",
                clause="§4.1.2",
                description="va_pos95 expressway",
                limit="≤ 0.5 m/s²",
                measured=_format_value(inputs["va_pos95_expressway"], "m/s²", precision=2),
                unit="m/s²",
                passed=inputs["va_pos95_expressway"] <= 0.5,
            ),
            _criterion(
                ident="dynamics:rpa-urban",
                section="Dynamics",
                clause="§4.1.1",
                description="RPA urban",
                limit="≤ 0.3 m²/s³",
                measured=_format_value(inputs["rpa_urban"], "m²/s³", precision=2),
                unit="m²/s³",
                passed=inputs["rpa_urban"] <= 0.3,
            ),
            _criterion(
                ident="dynamics:rpa-expressway",
                section="Dynamics",
                clause="§4.1.2",
                description="RPA expressway",
                limit="≤ 0.6 m²/s³",
                measured=_format_value(inputs["rpa_expressway"], "m²/s³", precision=2),
                unit="m²/s³",
                passed=inputs["rpa_expressway"] <= 0.6,
            ),
        ]
    )

    sections.extend(
        [
            _criterion(
                ident="co2-window:low",
                section="CO₂ Characteristic Windows",
                clause="App. 8 §4.5.1.2",
                description="Low range coverage",
                limit="≥ 50 % within tolerance",
                measured=_format_value(inputs["co2_window_low_pct"], "%"),
                unit="%",
                passed=inputs["co2_window_low_pct"] >= 50,
            ),
            _criterion(
                ident="co2-window:high",
                section="CO₂ Characteristic Windows",
                clause="App. 8 §4.5.1.2",
                description="High range coverage",
                limit="≥ 50 % within tolerance",
                measured=_format_value(inputs["co2_window_high_pct"], "%"),
                unit="%",
                passed=inputs["co2_window_high_pct"] >= 50,
            ),
        ]
    )

    sections.append(
        _criterion(
            ident="leak-device:checks",
            section="Leak & Device Errors",
            clause="§6.5",
            description="Leak checks & device diagnostics",
            limit="No unresolved errors",
            measured="Clear" if inputs["leak_checks_pass"] else "Error logged",
            unit=None,
            passed=inputs["leak_checks_pass"],
        )
    )

    sections.extend(
        [
            Criterion(
                id="conformity:nox",
                section="Conformity of Emissions",
                clause="App. 11 §4",
                description="NOx vs limit",
                limit="≤ 60 mg/km",
                measured="n/a",
                unit="mg/km",
                result=PassFail.NA,
            ),
            Criterion(
                id="conformity:pn",
                section="Conformity of Emissions",
                clause="App. 11 §4",
                description="PN vs limit",
                limit="≤ 6.0e11 #/km",
                measured="n/a",
                unit="#/km",
                result=PassFail.NA,
            ),
        ]
    )

    sections.append(
        Criterion(
            id="conformity:co",
            section="Conformity of Emissions",
            clause="App. 11 §4",
            description="CO vs limit",
            limit="≤ 1000 mg/km",
            measured="n/a",
            unit="mg/km",
            result=PassFail.NA,
        )
    )

    return sections


def build_report_data(source: Mapping[str, Any]) -> ReportData:
    """Build a :class:`ReportData` instance from a loose payload mapping."""

    if {"meta", "limits", "criteria", "emissions", "device"}.issubset(source.keys()):
        return ReportData.model_validate(source)

    meta_block = source.get("meta") if isinstance(source.get("meta"), Mapping) else {}
    criteria = _build_criteria(meta_block)
    emissions = _derive_emissions(source, criteria)
    limits = DEFAULT_LIMITS
    _update_conformity_criteria(criteria, emissions, limits)

    report = ReportData(
        meta=_build_trip_meta(meta_block),
        limits=limits,
        criteria=criteria,
        emissions=emissions,
        device=_build_device(meta_block),
    )
    return report


def apply_guardrails(report: ReportData) -> ReportData:
    """Apply cold-start and extended-condition guardrails to a report."""

    data = report.model_copy(deep=True)
    _update_conformity_criteria(data.criteria, data.emissions, data.limits)
    return data


def group_criteria_by_section(criteria: Iterable[Criterion]) -> dict[str, list[Criterion]]:
    grouped: dict[str, list[Criterion]] = defaultdict(list)
    for item in criteria:
        grouped[item.section].append(item)
    return dict(grouped)


def save_report_json(report: ReportData, *, report_dir: Path | None = None) -> Path:
    """Persist a report payload to disk in canonical JSON form."""

    directory = report_dir or _ensure_report_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{report.meta.testId}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(report.model_dump(mode="json"), handle, indent=2, sort_keys=True)
    return path


def load_report(test_id: str, *, report_dir: Path | None = None) -> ReportData:
    """Load a conformity report from disk and apply guardrails."""

    directory = report_dir or _REPORT_DIR
    path = directory / f"{test_id}.json"
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    report = ReportData.model_validate(raw)
    return apply_guardrails(report)


__all__ = [
    "DEFAULT_LIMITS",
    "apply_guardrails",
    "build_report_data",
    "group_criteria_by_section",
    "load_report",
    "save_report_json",
]

