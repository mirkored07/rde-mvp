"""Helpers to assemble, validate, and persist EU7-LD conformity reports."""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.app.regulation.eu7ld_un168_limits import (
    ACCELERATION_EVENTS_MIN,
    COLD_START_AVG_SPEED_RANGE_KMH,
    COLD_START_EXTENDED_HIGH_RANGE,
    COLD_START_EXTENDED_LOW_RANGE,
    COLD_START_MAX_SPEED_KMH_MAX,
    COLD_START_MOVE_WITHIN_SECONDS_MAX,
    COLD_START_TOTAL_STOP_SECONDS_MAX,
    CO2_SPAN_DRIFT_PPM_MAX,
    CO2_ZERO_DRIFT_PPM_MAX,
    CO_SPAN_DRIFT_PPM_MAX,
    CO_ZERO_DRIFT_PPM_MAX,
    CUMULATIVE_ELEVATION_TRIP_MAX_M_PER_100KM,
    CUMULATIVE_ELEVATION_URBAN_MAX_M_PER_100KM,
    DEVICE_ERROR_COUNT_MAX,
    EXPRESSWAY_DISTANCE_MIN_KM,
    EXPRESSWAY_SHARE_PERCENT_RANGE,
    FINAL_CO_MG_KM_LIMIT,
    FINAL_NOX_MG_KM_LIMIT,
    FINAL_PN_HASH_KM_LIMIT,
    GAS_PEMS_LEAK_RATE_PERCENT_MAX,
    GPS_DISTANCE_DELTA_ABS_PERCENT_MAX,
    GPS_MAX_GAP_SECONDS_MAX,
    GPS_TOTAL_GAPS_SECONDS_MAX,
    MAW_HIGH_SPEED_VALID_PERCENT_MIN,
    MAW_LOW_SPEED_VALID_PERCENT_MIN,
    NOX_SPAN_DRIFT_PPM_MAX,
    NOX_ZERO_DRIFT_PPM_MAX,
    PN_DILUTE_PRESSURE_RISE_MBAR_MAX,
    PN_SAMPLE_PRESSURE_RISE_MBAR_MAX,
    PN_ZERO_HASH_CM3_MAX,
    PRECONDITIONING_OPERATION_MIN_MINUTES,
    RPA_EXPRESSWAY_MIN,
    RPA_URBAN_MIN,
    SOAK_DURATION_HOURS_RANGE,
    SOAK_TEMPERATURE_C_RANGE,
    SPAN_COVERAGE_MIN,
    SPAN_EXCEED_MAX_COUNT,
    SPAN_TWO_X_BAND_MAX_PERCENT,
    START_END_ELEVATION_DELTA_MAX_M,
    TRIP_DURATION_MINUTES_RANGE,
    TRIP_ORDER_SEQUENCE,
    URBAN_DISTANCE_MIN_KM,
    URBAN_SHARE_PERCENT_RANGE,
    VA_POS95_EXPRESSWAY_MAX,
    VA_POS95_URBAN_MAX,
)

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
    CO_mg_km_WLTP=FINAL_CO_MG_KM_LIMIT,
    NOx_mg_km_RDE=FINAL_NOX_MG_KM_LIMIT,
    PN_hash_km_RDE=FINAL_PN_HASH_KM_LIMIT,
)

_REPORT_DIR = Path(os.environ.get("REPORT_DIR", "reports"))


def _ensure_report_dir() -> Path:
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    return _REPORT_DIR


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _format_numeric(value: float | None, unit: str | None, *, precision: int = 1) -> str:
    if value is None:
        return "n/a"
    abs_value = abs(value)
    if abs_value >= 1e6:
        text = f"{value:.2e}".replace("e+", "e").replace("e0", "e0")
    elif precision == 0:
        text = f"{int(round(value))}"
    else:
        text = f"{value:.{precision}f}".rstrip("0").rstrip(".")
    return f"{text} {unit}".strip() if unit else text


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


def _get_float(metrics: Mapping[str, Any], key: str) -> float | None:
    value = metrics.get(key)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return _parse_numeric(value)
    return None


def _get_int(metrics: Mapping[str, Any], key: str) -> int | None:
    value = metrics.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        parsed = _parse_numeric(value)
        return int(parsed) if parsed is not None else None
    return None


def _get_bool(metrics: Mapping[str, Any], key: str) -> bool | None:
    value = metrics.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n"}:
            return False
    return None


def _criterion(
    *,
    ident: str,
    section: str,
    clause: str | None,
    description: str,
    limit: str,
    measured: str,
    unit: str | None,
    result: PassFail,
) -> Criterion:
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


def _result_leq(value: float | None, limit: float) -> PassFail:
    if value is None:
        return PassFail.NA
    return PassFail.PASS if value <= limit else PassFail.FAIL


def _result_geq(value: float | None, limit: float) -> PassFail:
    if value is None:
        return PassFail.NA
    return PassFail.PASS if value >= limit else PassFail.FAIL


def _result_range(value: float | None, lower: float, upper: float) -> PassFail:
    if value is None:
        return PassFail.NA
    return PassFail.PASS if lower <= value <= upper else PassFail.FAIL


def _result_abs_leq(value: float | None, limit: float) -> PassFail:
    if value is None:
        return PassFail.NA
    return PassFail.PASS if abs(value) <= limit else PassFail.FAIL


def _build_pre_post_checks(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "Pre/Post Checks (Zero/Span)"
    clause = "§6.1"
    return [
        _criterion(
            ident="zero-span:co2-zero",
            section=section,
            clause=clause,
            description="CO₂ absolute zero drift",
            limit="≤ 2000 ppm",
            measured=_format_numeric(_get_float(metrics, "co2_zero_drift_ppm"), "ppm", precision=0),
            unit="ppm",
            result=_result_leq(_get_float(metrics, "co2_zero_drift_ppm"), CO2_ZERO_DRIFT_PPM_MAX),
        ),
        _criterion(
            ident="zero-span:co2-span",
            section=section,
            clause=clause,
            description="CO₂ absolute span drift",
            limit="≤ 3914 ppm",
            measured=_format_numeric(_get_float(metrics, "co2_span_drift_ppm"), "ppm", precision=0),
            unit="ppm",
            result=_result_leq(_get_float(metrics, "co2_span_drift_ppm"), CO2_SPAN_DRIFT_PPM_MAX),
        ),
        _criterion(
            ident="zero-span:co-zero",
            section=section,
            clause=clause,
            description="CO absolute zero drift",
            limit="≤ 75 ppm",
            measured=_format_numeric(_get_float(metrics, "co_zero_drift_ppm"), "ppm", precision=0),
            unit="ppm",
            result=_result_leq(_get_float(metrics, "co_zero_drift_ppm"), CO_ZERO_DRIFT_PPM_MAX),
        ),
        _criterion(
            ident="zero-span:co-span",
            section=section,
            clause=clause,
            description="CO absolute span drift",
            limit="≤ 943.4 ppm",
            measured=_format_numeric(_get_float(metrics, "co_span_drift_ppm"), "ppm", precision=1),
            unit="ppm",
            result=_result_leq(_get_float(metrics, "co_span_drift_ppm"), CO_SPAN_DRIFT_PPM_MAX),
        ),
        _criterion(
            ident="zero-span:nox-zero",
            section=section,
            clause=clause,
            description="NOx absolute zero drift",
            limit="≤ 3 ppm",
            measured=_format_numeric(_get_float(metrics, "nox_zero_drift_ppm"), "ppm", precision=1),
            unit="ppm",
            result=_result_leq(_get_float(metrics, "nox_zero_drift_ppm"), NOX_ZERO_DRIFT_PPM_MAX),
        ),
        _criterion(
            ident="zero-span:nox-span",
            section=section,
            clause=clause,
            description="NOx absolute span drift",
            limit="≤ 144.3 ppm",
            measured=_format_numeric(_get_float(metrics, "nox_span_drift_ppm"), "ppm", precision=1),
            unit="ppm",
            result=_result_leq(_get_float(metrics, "nox_span_drift_ppm"), NOX_SPAN_DRIFT_PPM_MAX),
        ),
        _criterion(
            ident="pn:zero-pre",
            section=section,
            clause="§4.6",
            description="PN pre-test zero",
            limit="≤ 5000 #/cm³",
            measured=_format_numeric(_get_float(metrics, "pn_zero_pre_hash_cm3"), "#/cm³", precision=0),
            unit="#/cm³",
            result=_result_leq(_get_float(metrics, "pn_zero_pre_hash_cm3"), PN_ZERO_HASH_CM3_MAX),
        ),
        _criterion(
            ident="pn:zero-post",
            section=section,
            clause="§4.6",
            description="PN post-test zero",
            limit="≤ 5000 #/cm³",
            measured=_format_numeric(_get_float(metrics, "pn_zero_post_hash_cm3"), "#/cm³", precision=0),
            unit="#/cm³",
            result=_result_leq(_get_float(metrics, "pn_zero_post_hash_cm3"), PN_ZERO_HASH_CM3_MAX),
        ),
    ]


def _build_span_coverage(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "Span Gas Coverage"
    clause = "§6.3"
    between_pct = _get_float(metrics, "co2_span_mid_points_pct")
    exceed_count = _get_int(metrics, "co2_span_over_limit_count")
    return [
        _criterion(
            ident="span:co2-coverage",
            section=section,
            clause=clause,
            description="CO₂ span coverage",
            limit="≥ 90 % of operative range",
            measured=_format_numeric(_get_float(metrics, "co2_span_coverage_pct"), "%", precision=1),
            unit="%",
            result=_result_geq(_get_float(metrics, "co2_span_coverage_pct"), SPAN_COVERAGE_MIN),
        ),
        _criterion(
            ident="span:co2-mid-band",
            section=section,
            clause=clause,
            description="CO₂ points between span and 2×span",
            limit="≤ 1 % of samples",
            measured=_format_numeric(between_pct, "%", precision=2),
            unit="%",
            result=_result_leq(between_pct, SPAN_TWO_X_BAND_MAX_PERCENT),
        ),
        _criterion(
            ident="span:co2-over-span",
            section=section,
            clause=clause,
            description="CO₂ > 2×span events",
            limit="0 occurrences",
            measured=str(exceed_count) if exceed_count is not None else "n/a",
            unit="events",
            result=
            PassFail.NA
            if exceed_count is None
            else PassFail.PASS if exceed_count <= SPAN_EXCEED_MAX_COUNT else PassFail.FAIL,
        ),
        _criterion(
            ident="span:co-coverage",
            section=section,
            clause=clause,
            description="CO span coverage",
            limit="≥ 90 %",
            measured=_format_numeric(_get_float(metrics, "co_span_coverage_pct"), "%", precision=1),
            unit="%",
            result=_result_geq(_get_float(metrics, "co_span_coverage_pct"), SPAN_COVERAGE_MIN),
        ),
        _criterion(
            ident="span:nox-coverage",
            section=section,
            clause=clause,
            description="NOx span coverage",
            limit="≥ 90 %",
            measured=_format_numeric(_get_float(metrics, "nox_span_coverage_pct"), "%", precision=1),
            unit="%",
            result=_result_geq(_get_float(metrics, "nox_span_coverage_pct"), SPAN_COVERAGE_MIN),
        ),
    ]


def _format_preconditioning_times(urban: float | None, expressway: float | None) -> str:
    if urban is None and expressway is None:
        return "n/a"
    parts = []
    if urban is not None:
        parts.append(f"Urban {urban:.0f} min")
    if expressway is not None:
        parts.append(f"Expressway {expressway:.0f} min")
    return " / ".join(parts)


def _format_cold_start(temp: float | None, applied: bool | None) -> str:
    if temp is None and applied is None:
        return "n/a"
    temp_text = f"Temp: {temp:.1f} °C" if temp is not None else "Temp: n/a"
    applied_text = "Applied: yes" if applied else "Applied: no"
    return f"{temp_text} | {applied_text}"


def _build_preconditioning(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "Vehicle Preconditioning & Soak"
    urban = _get_float(metrics, "preconditioning_time_urban_min")
    expressway = _get_float(metrics, "preconditioning_time_expressway_min")
    soak_hours = _get_float(metrics, "soak_time_hours")
    soak_temp = _get_float(metrics, "soak_temperature_c")
    last_temp = _get_float(metrics, "cold_start_last3h_temp_c")
    multiplier_applied = _get_bool(metrics, "cold_start_multiplier_applied")
    return [
        _criterion(
            ident="preconditioning:operation-time",
            section=section,
            clause="§8.3.2",
            description="Preconditioning time per operation type",
            limit="≥ 10 min each",
            measured=_format_preconditioning_times(urban, expressway),
            unit="min",
            result=
            PassFail.NA
            if urban is None or expressway is None
            else PassFail.PASS
            if (urban >= PRECONDITIONING_OPERATION_MIN_MINUTES and expressway >= PRECONDITIONING_OPERATION_MIN_MINUTES)
            else PassFail.FAIL,
        ),
        _criterion(
            ident="preconditioning:soak-duration",
            section=section,
            clause="§10.6",
            description="Soak time",
            limit="6–72 h",
            measured=_format_numeric(soak_hours, "h", precision=1),
            unit="h",
            result=_result_range(soak_hours, *SOAK_DURATION_HOURS_RANGE),
        ),
        _criterion(
            ident="preconditioning:soak-temperature",
            section=section,
            clause="§10.6",
            description="Soak temperature",
            limit="-7 to 38 °C",
            measured=_format_numeric(soak_temp, "°C", precision=1),
            unit="°C",
            result=_result_range(soak_temp, *SOAK_TEMPERATURE_C_RANGE),
        ),
        _criterion(
            ident="preconditioning:cold-start-multiplier",
            section=section,
            clause="§10.6",
            description="Cold-start multiplier",
            limit="×1.6 if extended [-7–0, 35–38] °C",
            measured=_format_cold_start(last_temp, multiplier_applied),
            unit="°C",
            result=PassFail.NA,
        ),
    ]


def _build_start_end(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "Start/End (ICE)"
    recorded = _get_bool(metrics, "start_end_logged")
    if recorded is None:
        result = PassFail.NA
        measured = "n/a"
    else:
        result = PassFail.PASS if recorded else PassFail.FAIL
        measured = "Recorded" if recorded else "Missing"
    return [
        _criterion(
            ident="start-end:compliance",
            section=section,
            clause="§8.1",
            description="Start/End rules complied",
            limit="Documentation available",
            measured=measured,
            unit=None,
            result=result,
        )
    ]


def _build_cold_start_window(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "Cold-Start Window"
    return [
        _criterion(
            ident="cold-start:avg-speed",
            section=section,
            clause="§9.3.4",
            description="Average speed during cold-start",
            limit="15–40 km/h",
            measured=_format_numeric(_get_float(metrics, "cold_start_avg_speed_kmh"), "km/h", precision=1),
            unit="km/h",
            result=_result_range(_get_float(metrics, "cold_start_avg_speed_kmh"), *COLD_START_AVG_SPEED_RANGE_KMH),
        ),
        _criterion(
            ident="cold-start:max-speed",
            section=section,
            clause="§3.8.2",
            description="Max speed during cold-start",
            limit="≤ 60 km/h",
            measured=_format_numeric(_get_float(metrics, "cold_start_max_speed_kmh"), "km/h", precision=1),
            unit="km/h",
            result=_result_leq(_get_float(metrics, "cold_start_max_speed_kmh"), COLD_START_MAX_SPEED_KMH_MAX),
        ),
        _criterion(
            ident="cold-start:movement",
            section=section,
            clause="§3.8.2",
            description="Vehicle moves within",
            limit="≤ 15 s",
            measured=_format_numeric(_get_float(metrics, "cold_start_move_within_s"), "s", precision=0),
            unit="s",
            result=_result_leq(_get_float(metrics, "cold_start_move_within_s"), COLD_START_MOVE_WITHIN_SECONDS_MAX),
        ),
        _criterion(
            ident="cold-start:stops",
            section=section,
            clause="§3.8.2",
            description="Total stops during cold-start",
            limit="≤ 90 s",
            measured=_format_numeric(_get_float(metrics, "cold_start_stop_total_s"), "s", precision=0),
            unit="s",
            result=_result_leq(_get_float(metrics, "cold_start_stop_total_s"), COLD_START_TOTAL_STOP_SECONDS_MAX),
        ),
    ]


def _format_trip_order(order: Iterable[str] | None) -> str:
    if not order:
        return "n/a"
    return " → ".join(str(part).title() for part in order)


def _sequence_pass(order: Iterable[str] | None) -> PassFail:
    if not order:
        return PassFail.NA
    lowered = [str(item).strip().lower() for item in order]
    expected = list(TRIP_ORDER_SEQUENCE)
    return PassFail.PASS if lowered[: len(expected)] == list(expected) else PassFail.FAIL


def _format_elevation(trip: float | None, urban: float | None) -> str:
    if trip is None and urban is None:
        return "n/a"
    parts = []
    if trip is not None:
        parts.append(f"Trip {trip:.0f}")
    if urban is not None:
        parts.append(f"Urban {urban:.0f}")
    return " / ".join(parts)


def _build_trip_composition(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "Trip Composition & Timing"
    order = metrics.get("trip_order")
    if isinstance(order, str):
        order_parts: list[str] = [part.strip() for part in order.split(">") if part.strip()]
    elif isinstance(order, Iterable):
        order_parts = list(order)
    else:
        order_parts = []

    extended_active = _get_bool(metrics, "extended_conditions_active")
    extended_valid = _get_bool(metrics, "extended_conditions_emissions_valid")

    return [
        _criterion(
            ident="trip:order",
            section=section,
            clause="§9.2",
            description="Order: urban → rural → expressway",
            limit="Sequential",
            measured=_format_trip_order(order_parts),
            unit=None,
            result=_sequence_pass(order_parts),
        ),
        _criterion(
            ident="trip:urban-distance",
            section=section,
            clause="§9.2",
            description="Urban distance",
            limit="≥ 16 km",
            measured=_format_numeric(_get_float(metrics, "urban_distance_km"), "km", precision=1),
            unit="km",
            result=_result_geq(_get_float(metrics, "urban_distance_km"), URBAN_DISTANCE_MIN_KM),
        ),
        _criterion(
            ident="trip:expressway-distance",
            section=section,
            clause="§9.2",
            description="Expressway distance",
            limit="≥ 16 km",
            measured=_format_numeric(_get_float(metrics, "expressway_distance_km"), "km", precision=1),
            unit="km",
            result=_result_geq(_get_float(metrics, "expressway_distance_km"), EXPRESSWAY_DISTANCE_MIN_KM),
        ),
        _criterion(
            ident="trip:urban-share",
            section=section,
            clause="§9.3.2",
            description="Urban share",
            limit="55 % ± 10 % (≥ 40 %)",
            measured=_format_numeric(_get_float(metrics, "urban_share_pct"), "%", precision=1),
            unit="%",
            result=_result_range(_get_float(metrics, "urban_share_pct"), *URBAN_SHARE_PERCENT_RANGE),
        ),
        _criterion(
            ident="trip:expressway-share",
            section=section,
            clause="§9.3.2",
            description="Expressway share",
            limit="45 % ± 10 %",
            measured=_format_numeric(_get_float(metrics, "expressway_share_pct"), "%", precision=1),
            unit="%",
            result=_result_range(_get_float(metrics, "expressway_share_pct"), *EXPRESSWAY_SHARE_PERCENT_RANGE),
        ),
        _criterion(
            ident="trip:duration",
            section=section,
            clause="§9.3.3",
            description="Trip duration",
            limit="90–120 min",
            measured=_format_numeric(_get_float(metrics, "trip_duration_min"), "min", precision=0),
            unit="min",
            result=_result_range(_get_float(metrics, "trip_duration_min"), *TRIP_DURATION_MINUTES_RANGE),
        ),
        _criterion(
            ident="trip:elevation-delta",
            section=section,
            clause="§8.1",
            description="Start–end elevation Δ",
            limit="≤ 100 m",
            measured=_format_numeric(_get_float(metrics, "start_end_elevation_delta_m"), "m", precision=0),
            unit="m",
            result=_result_leq(_get_float(metrics, "start_end_elevation_delta_m"), START_END_ELEVATION_DELTA_MAX_M),
        ),
        _criterion(
            ident="trip:cumulative-elevation",
            section=section,
            clause="§9.3.3",
            description="Cumulative +elevation (trip/urban)",
            limit="≤ 1200 m / 100 km",
            measured=_format_elevation(
                _get_float(metrics, "cumulative_elevation_trip_m_per_100km"),
                _get_float(metrics, "cumulative_elevation_urban_m_per_100km"),
            ),
            unit="m/100 km",
            result=
            PassFail.NA
            if (
                _get_float(metrics, "cumulative_elevation_trip_m_per_100km") is None
                or _get_float(metrics, "cumulative_elevation_urban_m_per_100km") is None
            )
            else PassFail.PASS
            if (
                _get_float(metrics, "cumulative_elevation_trip_m_per_100km")
                <= CUMULATIVE_ELEVATION_TRIP_MAX_M_PER_100KM
                and _get_float(metrics, "cumulative_elevation_urban_m_per_100km")
                <= CUMULATIVE_ELEVATION_URBAN_MAX_M_PER_100KM
            )
            else PassFail.FAIL,
        ),
        _criterion(
            ident="trip:extended-conditions",
            section=section,
            clause="§8.1",
            description="Extended conditions rule applied",
            limit="Only fail if limits exceeded",
            measured=(
                "Active – emissions compliant"
                if extended_active and (extended_valid is not False)
                else "Active – emissions exceeded"
                if extended_active and extended_valid is False
                else "Not triggered"
            ),
            unit=None,
            result=
            PassFail.PASS
            if extended_active and (extended_valid is not False)
            else PassFail.FAIL
            if extended_active and extended_valid is False
            else PassFail.PASS,
        ),
    ]


def _build_gps_validity(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "GPS Validity"
    return [
        _criterion(
            ident="gps:distance-delta",
            section=section,
            clause="§4.7",
            description="GPS vs ECU distance difference",
            limit="≤ ±4 %",
            measured=_format_numeric(_get_float(metrics, "gps_distance_delta_pct"), "%", precision=2),
            unit="%",
            result=_result_abs_leq(_get_float(metrics, "gps_distance_delta_pct"), GPS_DISTANCE_DELTA_ABS_PERCENT_MAX),
        ),
        _criterion(
            ident="gps:max-gap",
            section=section,
            clause="§6.5",
            description="Max GPS gap",
            limit="≤ 120 s",
            measured=_format_numeric(_get_float(metrics, "gps_max_gap_s"), "s", precision=0),
            unit="s",
            result=_result_leq(_get_float(metrics, "gps_max_gap_s"), GPS_MAX_GAP_SECONDS_MAX),
        ),
        _criterion(
            ident="gps:total-gaps",
            section=section,
            clause="§6.5",
            description="Total GPS gaps",
            limit="≤ 300 s",
            measured=_format_numeric(_get_float(metrics, "gps_total_gaps_s"), "s", precision=0),
            unit="s",
            result=_result_leq(_get_float(metrics, "gps_total_gaps_s"), GPS_TOTAL_GAPS_SECONDS_MAX),
        ),
    ]


def _build_dynamics(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "Dynamics & MAW"
    return [
        _criterion(
            ident="dynamics:accel-urban",
            section=section,
            clause="§3.1.3.1",
            description="Points with a > 0.1 m/s² (urban)",
            limit="≥ 100 points",
            measured=str(_get_int(metrics, "accel_points_urban") or "n/a"),
            unit="points",
            result=
            PassFail.NA
            if _get_int(metrics, "accel_points_urban") is None
            else PassFail.PASS
            if _get_int(metrics, "accel_points_urban") >= ACCELERATION_EVENTS_MIN
            else PassFail.FAIL,
        ),
        _criterion(
            ident="dynamics:accel-expressway",
            section=section,
            clause="§3.1.3.1",
            description="Points with a > 0.1 m/s² (expressway)",
            limit="≥ 100 points",
            measured=str(_get_int(metrics, "accel_points_expressway") or "n/a"),
            unit="points",
            result=
            PassFail.NA
            if _get_int(metrics, "accel_points_expressway") is None
            else PassFail.PASS
            if _get_int(metrics, "accel_points_expressway") >= ACCELERATION_EVENTS_MIN
            else PassFail.FAIL,
        ),
        _criterion(
            ident="dynamics:va-pos95-urban",
            section=section,
            clause="§4.1.1",
            description="va_pos,95 (urban)",
            limit="≤ 18.741 m²/s³",
            measured=_format_numeric(_get_float(metrics, "va_pos95_urban_m2s3"), "m²/s³", precision=3),
            unit="m²/s³",
            result=_result_leq(_get_float(metrics, "va_pos95_urban_m2s3"), VA_POS95_URBAN_MAX),
        ),
        _criterion(
            ident="dynamics:va-pos95-expressway",
            section=section,
            clause="§4.1.2",
            description="va_pos,95 (expressway)",
            limit="≤ 24.708 m²/s³",
            measured=_format_numeric(_get_float(metrics, "va_pos95_expressway_m2s3"), "m²/s³", precision=3),
            unit="m²/s³",
            result=_result_leq(_get_float(metrics, "va_pos95_expressway_m2s3"), VA_POS95_EXPRESSWAY_MAX),
        ),
        _criterion(
            ident="dynamics:rpa-urban",
            section=section,
            clause="§4.1.1",
            description="RPA (urban)",
            limit="≥ 0.125 m/s²",
            measured=_format_numeric(_get_float(metrics, "rpa_urban_ms2"), "m/s²", precision=3),
            unit="m/s²",
            result=_result_geq(_get_float(metrics, "rpa_urban_ms2"), RPA_URBAN_MIN),
        ),
        _criterion(
            ident="dynamics:rpa-expressway",
            section=section,
            clause="§4.1.2",
            description="RPA (expressway)",
            limit="≥ 0.052 m/s²",
            measured=_format_numeric(_get_float(metrics, "rpa_expressway_ms2"), "m/s²", precision=3),
            unit="m/s²",
            result=_result_geq(_get_float(metrics, "rpa_expressway_ms2"), RPA_EXPRESSWAY_MIN),
        ),
    ]


def _build_co2_windows(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "CO₂ Characteristic Windows (MAW)"
    return [
        _criterion(
            ident="co2:low-speed",
            section=section,
            clause="App. 8 §4.5.1.2",
            description="Low-speed windows in tolerance",
            limit="≥ 50 %",
            measured=_format_numeric(_get_float(metrics, "maw_low_speed_valid_pct"), "%", precision=1),
            unit="%",
            result=_result_geq(_get_float(metrics, "maw_low_speed_valid_pct"), MAW_LOW_SPEED_VALID_PERCENT_MIN),
        ),
        _criterion(
            ident="co2:high-speed",
            section=section,
            clause="App. 8 §4.5.1.2",
            description="High-speed windows in tolerance",
            limit="≥ 50 %",
            measured=_format_numeric(_get_float(metrics, "maw_high_speed_valid_pct"), "%", precision=1),
            unit="%",
            result=_result_geq(_get_float(metrics, "maw_high_speed_valid_pct"), MAW_HIGH_SPEED_VALID_PERCENT_MIN),
        ),
    ]


def _format_emission(value: float | None, unit: str, precision: int = 1) -> str:
    if value is None:
        return "n/a"
    if abs(value) >= 1e6:
        text = f"{value:.2e}".replace("e+", "e")
    else:
        text = f"{value:.{precision}f}".rstrip("0").rstrip(".")
    return f"{text} {unit}"


def _build_emissions_summary(emissions: EmissionSummary) -> list[Criterion]:
    section = "Emissions Summary"
    return [
        _criterion(
            ident="emissions:nox-urban",
            section=section,
            clause="§3.1",
            description="NOx per km (urban)",
            limit="Reported",
            measured=_format_emission(emissions.urban.NOx_mg_km, "mg/km", precision=1),
            unit="mg/km",
            result=PassFail.NA,
        ),
        _criterion(
            ident="emissions:nox-trip",
            section=section,
            clause="§3.1",
            description="NOx per km (trip)",
            limit="Reported",
            measured=_format_emission(emissions.trip.NOx_mg_km, "mg/km", precision=1),
            unit="mg/km",
            result=PassFail.NA,
        ),
        _criterion(
            ident="emissions:pn-urban",
            section=section,
            clause="§3.1",
            description="PN per km (urban)",
            limit="Reported",
            measured=_format_emission(emissions.urban.PN_hash_km, "#/km", precision=1),
            unit="#/km",
            result=PassFail.NA,
        ),
        _criterion(
            ident="emissions:pn-trip",
            section=section,
            clause="§3.1",
            description="PN per km (trip)",
            limit="Reported",
            measured=_format_emission(emissions.trip.PN_hash_km, "#/km", precision=1),
            unit="#/km",
            result=PassFail.NA,
        ),
    ]


def _build_final_conformity() -> list[Criterion]:
    section = "Final Conformity"
    return [
        _criterion(
            ident="conformity:nox",
            section=section,
            clause="App. 11 §4",
            description="Final NOx",
            limit="≤ 60 mg/km",
            measured="n/a",
            unit="mg/km",
            result=PassFail.NA,
        ),
        _criterion(
            ident="conformity:pn",
            section=section,
            clause="App. 11 §4",
            description="Final PN",
            limit="≤ 6.0e11 #/km",
            measured="n/a",
            unit="#/km",
            result=PassFail.NA,
        ),
        _criterion(
            ident="conformity:co",
            section=section,
            clause="App. 11 §4",
            description="Final CO",
            limit="≤ 1000 mg/km",
            measured="n/a",
            unit="mg/km",
            result=PassFail.NA,
        ),
    ]


def _build_leak_checks(metrics: Mapping[str, Any]) -> list[Criterion]:
    section = "Leak Checks & Device Errors"
    leak_rate = _get_float(metrics, "gas_pems_leak_rate_pct")
    dilute_rise = _get_float(metrics, "pn_dilute_pressure_rise_mbar")
    sample_rise = _get_float(metrics, "pn_sample_pressure_rise_mbar")
    error_count = _get_int(metrics, "device_error_count")
    return [
        _criterion(
            ident="leak:gas-pems",
            section=section,
            clause="§6.5",
            description="Gas PEMS leak rate",
            limit="≤ 0.5 % of flow",
            measured=_format_numeric(leak_rate, "%", precision=2),
            unit="%",
            result=_result_leq(leak_rate, GAS_PEMS_LEAK_RATE_PERCENT_MAX),
        ),
        _criterion(
            ident="leak:pn-dilute",
            section=section,
            clause="§4.6",
            description="PN dilute path pressure rise",
            limit="≤ 30 mbar / 10 s",
            measured=_format_numeric(dilute_rise, "mbar", precision=1),
            unit="mbar",
            result=_result_leq(dilute_rise, PN_DILUTE_PRESSURE_RISE_MBAR_MAX),
        ),
        _criterion(
            ident="leak:pn-sample",
            section=section,
            clause="§4.6",
            description="PN sample path pressure rise",
            limit="≤ 30 mbar / 10 s",
            measured=_format_numeric(sample_rise, "mbar", precision=1),
            unit="mbar",
            result=_result_leq(sample_rise, PN_SAMPLE_PRESSURE_RISE_MBAR_MAX),
        ),
        _criterion(
            ident="leak:device-errors",
            section=section,
            clause="§6.5",
            description="Device main errors",
            limit="0",
            measured=str(error_count) if error_count is not None else "n/a",
            unit="count",
            result=
            PassFail.NA
            if error_count is None
            else PassFail.PASS if error_count <= DEVICE_ERROR_COUNT_MAX else PassFail.FAIL,
        ),
    ]


def _build_criteria(metrics: Mapping[str, Any], emissions: EmissionSummary) -> list[Criterion]:
    criteria: list[Criterion] = []
    criteria.extend(_build_pre_post_checks(metrics))
    criteria.extend(_build_span_coverage(metrics))
    criteria.extend(_build_preconditioning(metrics))
    criteria.extend(_build_start_end(metrics))
    criteria.extend(_build_cold_start_window(metrics))
    criteria.extend(_build_trip_composition(metrics))
    criteria.extend(_build_gps_validity(metrics))
    criteria.extend(_build_dynamics(metrics))
    criteria.extend(_build_co2_windows(metrics))
    criteria.extend(_build_emissions_summary(emissions))
    criteria.extend(_build_final_conformity())
    criteria.extend(_build_leak_checks(metrics))
    return criteria


def _build_trip_meta(meta: Mapping[str, Any]) -> TripMeta:
    fallback_time = _iso_now()
    velocity_source = (meta.get("velocity_source") or meta.get("velocitySource") or "GPS").upper()
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


def _build_device(source: Mapping[str, Any]) -> DeviceInfo:
    devices = source.get("device")
    if not isinstance(devices, Mapping):
        devices = source.get("devices") if isinstance(source.get("devices"), Mapping) else {}
    return DeviceInfo(
        gasPEMS=str(devices.get("gas_pems") or devices.get("gasPEMS") or "AVL GAS 601"),
        pnPEMS=str(devices.get("pn_pems") or devices.get("pnPEMS") or "AVL PN PEMS 483"),
        efm=(devices.get("efm") if isinstance(devices.get("efm"), str) else None),
    )


def _extract_metrics(source: Mapping[str, Any]) -> Mapping[str, Any]:
    metrics = source.get("metrics")
    if isinstance(metrics, Mapping):
        return metrics
    return source


def _build_emission_block(label: str, payload: Mapping[str, Any] | None) -> EmissionBlock:
    payload = payload or {}
    co2 = payload.get("CO2_g_km")
    co = payload.get("CO_mg_km")
    nox = payload.get("NOx_mg_km")
    pn = payload.get("PN_hash_km")
    return EmissionBlock(
        label=label,
        CO2_g_km=float(co2) if isinstance(co2, (int, float)) else None,
        CO_mg_km=float(co) if isinstance(co, (int, float)) else None,
        NOx_mg_km=float(nox) if isinstance(nox, (int, float)) else None,
        PN_hash_km=float(pn) if isinstance(pn, (int, float)) else None,
    )


def _build_emissions(source: Mapping[str, Any]) -> EmissionSummary:
    emissions_payload = source.get("emissions") if isinstance(source.get("emissions"), Mapping) else {}
    urban_payload = emissions_payload.get("urban") if isinstance(emissions_payload.get("urban"), Mapping) else {}
    trip_payload = emissions_payload.get("trip") if isinstance(emissions_payload.get("trip"), Mapping) else {}
    if not trip_payload:
        trip_payload = urban_payload
    return EmissionSummary(
        urban=_build_emission_block("Urban", urban_payload),
        trip=_build_emission_block("Trip", trip_payload),
    )


def _clamp_non_negative(value: float | None) -> float | None:
    if value is None:
        return None
    return value if value >= 0 else 0.0


def _update_conformity_criteria(criteria: Iterable[Criterion], emissions: EmissionSummary, limits: FinalLimitsEU7LD) -> None:
    mapping = {
        "conformity:nox": (emissions.trip.NOx_mg_km, limits.NOx_mg_km_RDE, "mg/km"),
        "conformity:pn": (emissions.trip.PN_hash_km, limits.PN_hash_km_RDE, "#/km"),
        "conformity:co": (emissions.trip.CO_mg_km, limits.CO_mg_km_WLTP, "mg/km"),
    }
    for item in criteria:
        if item.id in mapping:
            value, limit, unit = mapping[item.id]
            clamped = _clamp_non_negative(value)
            if clamped is not None:
                item.measured = _format_emission(clamped, unit, precision=1)
            else:
                item.measured = "n/a"
            if item.id == "conformity:pn":
                limit_text = f"≤ {FINAL_PN_HASH_KM_LIMIT:.1e}".replace("e+", "e") + " #/km"
                item.limit = limit_text
            item.result = (
                PassFail.NA
                if clamped is None or limit is None
                else PassFail.PASS if clamped <= limit else PassFail.FAIL
            )


def _extended_temperature(temp: float | None) -> bool:
    if temp is None:
        return False
    low_min, low_max = COLD_START_EXTENDED_LOW_RANGE
    high_min, high_max = COLD_START_EXTENDED_HIGH_RANGE
    return low_min <= temp <= low_max or high_min <= temp <= high_max


def _apply_cold_start_multiplier(report: ReportData) -> None:
    multiplier = 1.0
    criterion = None
    temp = None
    applied = None
    for item in report.criteria:
        if item.id == "preconditioning:cold-start-multiplier":
            criterion = item
            temp = _parse_numeric(item.measured)
            applied = "applied: yes" in (item.measured or "").lower()
            break

    if criterion is None:
        return

    if applied:
        multiplier = 1.0
    else:
        multiplier = 1.6 if _extended_temperature(temp) else 1.0

    if multiplier != 1.0 and not applied:
        if report.emissions.trip.NOx_mg_km is not None:
            report.emissions.trip.NOx_mg_km *= multiplier
        if report.emissions.trip.PN_hash_km is not None:
            report.emissions.trip.PN_hash_km *= multiplier
        if report.emissions.urban.NOx_mg_km is not None:
            report.emissions.urban.NOx_mg_km *= multiplier
        if report.emissions.urban.PN_hash_km is not None:
            report.emissions.urban.PN_hash_km *= multiplier
        criterion.measured = _format_cold_start(temp, True)
        criterion.result = PassFail.PASS if multiplier != 1.0 else PassFail.NA
    else:
        if applied is None and not _extended_temperature(temp):
            criterion.result = PassFail.NA
        elif applied:
            criterion.result = PassFail.PASS
        elif not applied and _extended_temperature(temp):
            criterion.result = PassFail.FAIL


def _apply_guardrails_inplace(report: ReportData) -> None:
    _apply_cold_start_multiplier(report)
    report.emissions.trip.NOx_mg_km = _clamp_non_negative(report.emissions.trip.NOx_mg_km)
    report.emissions.trip.PN_hash_km = _clamp_non_negative(report.emissions.trip.PN_hash_km)
    report.emissions.trip.CO_mg_km = _clamp_non_negative(report.emissions.trip.CO_mg_km)
    report.emissions.urban.NOx_mg_km = _clamp_non_negative(report.emissions.urban.NOx_mg_km)
    report.emissions.urban.PN_hash_km = _clamp_non_negative(report.emissions.urban.PN_hash_km)
    report.emissions.urban.CO_mg_km = _clamp_non_negative(report.emissions.urban.CO_mg_km)
    _update_conformity_criteria(report.criteria, report.emissions, report.limits)


def build_report_data(source: Mapping[str, Any]) -> ReportData:
    if {"meta", "limits", "criteria", "emissions", "device"}.issubset(source.keys()):
        report = ReportData.model_validate(source)
        _apply_guardrails_inplace(report)
        return report

    meta_block = source.get("meta") if isinstance(source.get("meta"), Mapping) else {}
    metrics = _extract_metrics(source)
    emissions = _build_emissions(source)
    criteria = _build_criteria(metrics, emissions)
    report = ReportData(
        meta=_build_trip_meta(meta_block),
        limits=DEFAULT_LIMITS,
        criteria=criteria,
        emissions=emissions,
        device=_build_device(source),
    )
    _apply_guardrails_inplace(report)
    return report


def apply_guardrails(report: ReportData) -> ReportData:
    data = report.model_copy(deep=True)
    _apply_guardrails_inplace(data)
    return data


def group_criteria_by_section(criteria: Iterable[Criterion]) -> dict[str, list[Criterion]]:
    grouped: dict[str, list[Criterion]] = defaultdict(list)
    for item in criteria:
        grouped[item.section].append(item)
    return dict(grouped)


def save_report_json(report: ReportData, *, report_dir: Path | None = None) -> Path:
    directory = report_dir or _ensure_report_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{report.meta.testId}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(report.model_dump(mode="json"), handle, indent=2, sort_keys=True)
    return path


def load_report(test_id: str, *, report_dir: Path | None = None) -> ReportData:
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
