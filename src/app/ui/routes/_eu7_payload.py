"""Utilities for preparing EU7 results payloads for the UI."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from src.app.reporting.eu7ld_report import build_report_data, group_criteria_by_section
from src.app.reporting.schemas import Criterion, PassFail, ReportData
from src.app.utils.payload import ensure_results_payload_defaults
from src.app.regulation.eu7ld_un168_limits import (
    NOX_RDE_FINAL_MG_PER_KM,
    PN10_RDE_FINAL_PER_KM,
    CO2_SPAN_DRIFT_PPM_MAX,
    CO2_ZERO_DRIFT_PPM_MAX,
    CO_SPAN_DRIFT_PPM_MAX,
    CO_ZERO_DRIFT_PPM_MAX,
    CUMULATIVE_ELEVATION_TRIP_MAX_M_PER_100KM,
    CUMULATIVE_ELEVATION_URBAN_MAX_M_PER_100KM,
    DEVICE_ERROR_COUNT_MAX,
    EXPRESSWAY_DISTANCE_MIN_KM,
    EXPRESSWAY_SHARE_PERCENT_RANGE,
    GAS_PEMS_LEAK_RATE_PERCENT_MAX,
    GPS_DISTANCE_DELTA_ABS_PERCENT_MAX,
    GPS_MAX_GAP_SECONDS_MAX,
    GPS_TOTAL_GAPS_SECONDS_MAX,
    MAW_HIGH_SPEED_VALID_PERCENT_MIN,
    MAW_LOW_SPEED_VALID_PERCENT_MIN,
    NOX_SPAN_DRIFT_PPM_MAX,
    NOX_ZERO_DRIFT_PPM_MAX,
    PRECONDITIONING_OPERATION_MIN_MINUTES,
    PN_DILUTE_PRESSURE_RISE_MBAR_MAX,
    PN_SAMPLE_PRESSURE_RISE_MBAR_MAX,
    PN_ZERO_HASH_CM3_MAX,
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

SECTION_TITLES: tuple[str, ...] = (
    "Pre/Post Checks (Zero/Span)",
    "Span Gas Coverage",
    "Trip Composition & Timing",
    "Cold-Start Window",
    "GPS Validity",
    "Dynamics & MAW",
    "CO₂ Characteristic Windows (MAW)",
    "Emissions Summary",
    "Final Conformity",
)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            try:
                return float(stripped.replace(",", ""))
            except ValueError:
                return None
    return None


def _as_int(value: Any) -> int | None:
    number = _as_float(value)
    if number is None:
        return None
    return int(round(number))


def _bool_from_result(result: PassFail | str | None) -> bool | None:
    if result is None:
        return None
    value = str(result).lower()
    if value == "pass":
        return True
    if value == "fail":
        return False
    return None


def _criterion_row(item: Criterion) -> dict[str, Any]:
    result_literal = str(item.result)
    computed_value: Any = item.value
    measured_text = item.measured
    if measured_text in {None, ""}:
        measured_text = computed_value
    return {
        "id": item.id,
        "section": item.section,
        "ref": item.clause or item.id,
        "clause": item.clause,
        "criterion": item.description,
        "description": item.description,
        "condition": item.limit,
        "limit": item.limit,
        "value": computed_value,
        "measured": measured_text,
        "unit": item.unit,
        "result": result_literal,
        "pass": _bool_from_result(result_literal),
    }


def _build_sections(criteria: Mapping[str, Sequence[Criterion]]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for title in SECTION_TITLES:
        rows = criteria.get(title, [])
        sections.append({"title": title, "criteria": [_criterion_row(item) for item in rows]})
    return sections


def _build_final_block(criteria: Sequence[Criterion]) -> dict[str, Any]:
    rows = [_criterion_row(item) for item in criteria]
    pass_values = [row["pass"] for row in rows if row.get("pass") is not None]
    overall = bool(pass_values) and all(pass_values)
    return {"pass": overall, "pollutants": rows}


def _clamp_non_negative(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric >= 0.0 else 0.0


def _build_final_conformity(
    *,
    emissions: Mapping[str, Any] | None,
    limits: Mapping[str, Any] | None,
    final_block: Mapping[str, Any] | None,
) -> dict[str, Any]:
    emissions = emissions or {}
    limits = limits or {}

    trip = emissions.get("trip") if isinstance(emissions.get("trip"), Mapping) else {}

    def _limit(key: str, default: float) -> float:
        raw = limits.get(key)
        try:
            return float(raw)
        except (TypeError, ValueError):
            return float(default)

    final_conformity: dict[str, Any] = {}

    final_nox = _clamp_non_negative(trip.get("NOx_mg_km"))
    nox_limit = _limit("NOx_mg_km_RDE", NOX_RDE_FINAL_MG_PER_KM)
    if final_nox is not None:
        final_conformity["NOx_mg_km"] = {
            "value": final_nox,
            "limit": nox_limit,
            "pass": final_nox <= nox_limit,
        }

    final_pn = _clamp_non_negative(trip.get("PN10_hash_km") or trip.get("PN_hash_km"))
    pn_limit = _limit("PN10_hash_km_RDE", PN10_RDE_FINAL_PER_KM)
    if final_pn is not None:
        final_conformity["PN10_hash_km"] = {
            "value": final_pn,
            "limit": pn_limit,
            "pass": final_pn <= pn_limit,
        }

    final_co = _clamp_non_negative(trip.get("CO_mg_km"))
    if final_co is not None:
        final_conformity["CO_mg_km"] = {
            "value": final_co,
            "limit": None,
            "pass": True,
        }

    if final_conformity or not isinstance(final_block, Mapping):
        return final_conformity

    fallback_rows = final_block.get("pollutants")
    if isinstance(fallback_rows, Iterable):
        for item in fallback_rows:
            if not isinstance(item, Mapping):
                continue
            name = str(item.get("name") or item.get("criterion") or item.get("id") or "")
            if not name:
                continue
            value = _clamp_non_negative(item.get("value"))
            if value is None:
                continue
            limit: float | None
            if "NOx" in name:
                limit = nox_limit
                key = "NOx_mg_km"
            elif "PN" in name:
                limit = pn_limit
                key = "PN10_hash_km"
            elif "CO" in name:
                limit = None
                key = "CO_mg_km"
            else:
                continue
            final_conformity[key] = {
                "value": value,
                "limit": limit,
                "pass": True if limit is None else value <= limit,
            }

    return final_conformity


def _timestamp_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def enrich_payload(
    payload: Mapping[str, Any] | None,
    *,
    visual_data: Mapping[str, Any] | None = None,
    row_counts: Mapping[str, int] | None = None,
    meta_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply UI defaults (meta, visual, KPIs) to the raw engine *payload*."""

    base = dict(payload or {})

    visual = dict(base.get("visual") or {})
    incoming_visual = dict(visual_data or {})

    map_payload = dict(visual.get("map") or {})
    map_payload.update(incoming_visual.get("map", {}))
    visual["map"] = map_payload

    chart_payload = dict(visual.get("chart") or {})
    chart_payload.update(incoming_visual.get("chart", {}))
    visual["chart"] = chart_payload

    if "distance_km" in incoming_visual:
        visual["distance_km"] = incoming_visual.get("distance_km", 0.0)
    if "avg_speed_m_s" in incoming_visual:
        visual["avg_speed_m_s"] = incoming_visual.get("avg_speed_m_s", 0.0)

    base["visual"] = visual

    kpi_numbers = list(base.get("kpi_numbers") or [])

    def _upsert_kpi(key: str, label: str, value: float | int | None, unit: str) -> None:
        if value is None:
            return
        for entry in kpi_numbers:
            if entry.get("key") == key or entry.get("label") == label:
                entry.update({"key": key, "label": label, "value": value, "unit": unit})
                return
        kpi_numbers.append({"key": key, "label": label, "value": value, "unit": unit})

    distance_km = incoming_visual.get("distance_km") if incoming_visual else None
    if distance_km is not None:
        _upsert_kpi("total_distance_km", "Distance (km)", round(float(distance_km), 2), "km")

    avg_speed = incoming_visual.get("avg_speed_m_s") if incoming_visual else None
    if avg_speed is not None:
        _upsert_kpi(
            "avg_speed_kmh",
            "Avg speed (km/h)",
            round(float(avg_speed) * 3.6, 1),
            "km/h",
        )

    base["kpi_numbers"] = kpi_numbers

    meta = dict(base.get("meta") or {})
    overrides = dict(meta_overrides or {})

    meta.setdefault("legislation", "EU7 Light-Duty")
    meta.setdefault("test_id", overrides.get("test_id", "demo-run"))
    meta.setdefault("engine", overrides.get("engine", "WLTP-ICE 2.0L"))
    meta.setdefault("propulsion", overrides.get("propulsion", "ICE"))
    meta.setdefault("velocity_source", overrides.get("velocity_source", "GPS"))

    timestamp = overrides.get("timestamp") or _timestamp_iso()
    meta.setdefault("test_start", timestamp)
    meta.setdefault("printout", timestamp)

    if "co_mg_per_km" in overrides:
        meta["co_mg_per_km"] = overrides["co_mg_per_km"]
    else:
        meta.setdefault("co_mg_per_km", 0.0)

    devices = dict(meta.get("devices") or {})
    device_overrides = overrides.get("devices") or {}
    devices.setdefault("gas_pems", device_overrides.get("gas_pems", "AVL GAS 601"))
    devices.setdefault("pn_pems", device_overrides.get("pn_pems", "AVL PN PEMS 483"))
    if device_overrides.get("efm"):
        devices.setdefault("efm", device_overrides["efm"])
    meta["devices"] = devices

    sources = dict(row_counts or {})
    if sources:
        meta["sources"] = {
            "pems_rows": int(sources.get("pems_rows", 0)),
            "gps_rows": int(sources.get("gps_rows", 0)),
            "ecu_rows": int(sources.get("ecu_rows", 0)),
        }

    base["meta"] = meta

    emissions_meta = {
        "urban": {"label": "Urban"},
        "trip": {
            "label": "Trip",
            "NOx_mg_km": meta.get("nox_mg_per_km"),
            "PN_hash_km": meta.get("pn_per_km"),
            "CO_mg_km": meta.get("co_mg_per_km"),
        },
    }
    base["emissions"] = emissions_meta

    return base


def build_normalised_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return the canonical EU7 payload structure consumed by the UI."""

    normalised = ensure_results_payload_defaults(payload)

    report: ReportData = build_report_data(normalised)
    grouped = group_criteria_by_section(report.criteria)

    canonical = report.model_dump(mode="json")

    metrics_block = dict(normalised.get("metrics") or {})

    def mfloat(key: str) -> float | None:
        return _as_float(metrics_block.get(key))

    def mint(key: str) -> int | None:
        return _as_int(metrics_block.get(key))

    def mbool(key: str) -> bool | None:
        value = metrics_block.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "y"}:
                return True
            if lowered in {"0", "false", "no", "n"}:
                return False
        return None

    def _default(value: Any, fallback: Any) -> Any:
        return value if value not in {None, ""} else fallback

    meta_block = dict(canonical.get("meta") or {})
    original_meta = normalised.get("meta") if isinstance(normalised.get("meta"), dict) else {}
    if original_meta and isinstance(original_meta, dict):
        sources = original_meta.get("sources")
        if isinstance(sources, dict):
            meta_block["sources"] = {
                "pems_rows": int(sources.get("pems_rows", 0)),
                "gps_rows": int(sources.get("gps_rows", 0)),
                "ecu_rows": int(sources.get("ecu_rows", 0)),
            }

    output: dict[str, Any] = dict(normalised)
    output["meta"] = meta_block

    limits_block = dict(canonical.get("limits") or {})
    limits_block.setdefault("PN_hash_km_RDE", PN10_RDE_FINAL_PER_KM)
    limits_block["PN10_hash_km_RDE"] = PN10_RDE_FINAL_PER_KM
    output["limits"] = limits_block
    output["criteria"] = canonical.get("criteria", [])
    output["emissions"] = canonical.get("emissions", {})
    output["device"] = canonical.get("device", {})

    sections = _build_sections(grouped)
    output["sections"] = sections

    final_section = grouped.get("Final Conformity", [])
    output["final"] = _build_final_block(final_section)
    output["final_conformity"] = _build_final_conformity(
        emissions=output.get("emissions"),
        limits=output.get("limits"),
        final_block=output.get("final"),
    )

    emissions_block = output.get("emissions") if isinstance(output.get("emissions"), Mapping) else {}
    emissions: dict[str, Any] = {str(key): dict(value) for key, value in dict(emissions_block).items()}

    def _ensure_phase(name: str, label: str) -> None:
        phase = emissions.get(name)
        if not isinstance(phase, Mapping):
            phase = {}
        emissions[name] = {
            "label": phase.get("label") or label,
            "NOx_mg_km": _default(phase.get("NOx_mg_km"), "n/a"),
            "PN10_hash_km": _default(phase.get("PN10_hash_km") or phase.get("PN_hash_km"), "n/a"),
            "CO_mg_km": _default(phase.get("CO_mg_km"), "n/a"),
            "CO2_g_km": phase.get("CO2_g_km"),
        }

    _ensure_phase("urban", "Urban")
    _ensure_phase("rural", "Rural")
    _ensure_phase("motorway", "Motorway")
    _ensure_phase("trip", "Trip")

    output["emissions"] = emissions

    def _seconds_from_minutes(value: float | None) -> float | None:
        if value is None:
            return None
        return float(value) * 60.0

    urban_preconditioning_min = mfloat("preconditioning_time_urban_min")
    rural_preconditioning_min = mfloat("preconditioning_time_rural_min")
    motorway_preconditioning_min = mfloat("preconditioning_time_expressway_min")
    soak_duration_h = mfloat("soak_time_hours")
    soak_temperature_c = mfloat("soak_temperature_c")
    soak_temp_min_c = _default(mfloat("soak_temperature_min_c"), soak_temperature_c)
    soak_temp_max_c = _default(mfloat("soak_temperature_max_c"), soak_temperature_c)

    drive_ok = True
    for minutes in (urban_preconditioning_min, rural_preconditioning_min, motorway_preconditioning_min):
        if minutes is None:
            continue
        drive_ok = drive_ok and minutes >= float(PRECONDITIONING_OPERATION_MIN_MINUTES)

    precon_soak = {
        "drive_10min_each": {
            "urban_min_s": _seconds_from_minutes(urban_preconditioning_min),
            "rural_min_s": _seconds_from_minutes(rural_preconditioning_min),
            "motorway_min_s": _seconds_from_minutes(motorway_preconditioning_min),
            "ok": drive_ok if any(v is not None for v in (urban_preconditioning_min, rural_preconditioning_min, motorway_preconditioning_min)) else None,
        },
        "soak_duration_h": soak_duration_h,
        "soak_temp_min_c": soak_temp_min_c,
        "soak_temp_max_c": soak_temp_max_c,
        "soak_duration_range": list(SOAK_DURATION_HOURS_RANGE),
        "soak_temperature_range": list(SOAK_TEMPERATURE_C_RANGE),
        "extended_temp_flag": bool(mbool("cold_start_multiplier_applied")),
        "extended_last_temp_c": mfloat("cold_start_last3h_temp_c"),
    }

    cold_start = {
        "start_end_logged": bool(mbool("start_end_logged")),
        "avg_speed_kmh": mfloat("cold_start_avg_speed_kmh"),
        "max_speed_kmh": mfloat("cold_start_max_speed_kmh"),
        "move_within_s": mfloat("cold_start_move_within_s"),
        "stop_total_s": mfloat("cold_start_stop_total_s"),
        "multiplier_applied": bool(mbool("cold_start_multiplier_applied")),
        "ambient_temp_c": mfloat("cold_start_last3h_temp_c"),
        "duration_limit_s": 120.0,
    }

    urban_km = mfloat("urban_distance_km")
    motorway_km = mfloat("expressway_distance_km")
    rural_km = mfloat("rural_distance_km")
    if rural_km is None and urban_km is not None and motorway_km is not None:
        total = urban_km + motorway_km
        rural_share_pct = mfloat("rural_share_pct")
        if rural_share_pct is not None:
            rural_km = total * (rural_share_pct / max(1.0, (mfloat("urban_share_pct") or 0.0) + (mfloat("expressway_share_pct") or 0.0) + rural_share_pct))
        else:
            rural_km = max(0.0, total - (urban_km or 0.0) - (motorway_km or 0.0))

    urban_share_pct = mfloat("urban_share_pct")
    motorway_share_pct = mfloat("expressway_share_pct")
    rural_share_pct = mfloat("rural_share_pct")
    if rural_share_pct is None and urban_share_pct is not None and motorway_share_pct is not None:
        remainder = 100.0 - urban_share_pct - motorway_share_pct
        rural_share_pct = round(max(0.0, remainder), 1)

    trip_shares = {
        "order": list(metrics_block.get("trip_order") or []),
        "duration_min": mfloat("trip_duration_min"),
        "urban_km": urban_km,
        "rural_km": rural_km,
        "motorway_km": motorway_km,
        "urban_share_pct": urban_share_pct,
        "rural_share_pct": rural_share_pct,
        "motorway_share_pct": motorway_share_pct,
        "order_expected": list(TRIP_ORDER_SEQUENCE),
        "distance_min_km": {
            "urban": URBAN_DISTANCE_MIN_KM,
            "motorway": EXPRESSWAY_DISTANCE_MIN_KM,
        },
        "share_ranges": {
            "urban": list(URBAN_SHARE_PERCENT_RANGE),
            "motorway": list(EXPRESSWAY_SHARE_PERCENT_RANGE),
        },
        "duration_range": list(TRIP_DURATION_MINUTES_RANGE),
    }

    start_end_delta = mfloat("start_end_elevation_delta_m")
    elevation = {
        "start_end_delta_m": start_end_delta,
        "start_end_abs_m": abs(start_end_delta) if start_end_delta is not None else None,
        "trip_cumulative_m_per_100km": mfloat("cumulative_elevation_trip_m_per_100km"),
        "urban_cumulative_m_per_100km": mfloat("cumulative_elevation_urban_m_per_100km"),
        "trip_limit_m_per_100km": CUMULATIVE_ELEVATION_TRIP_MAX_M_PER_100KM,
        "urban_limit_m_per_100km": CUMULATIVE_ELEVATION_URBAN_MAX_M_PER_100KM,
        "start_end_limit_m": START_END_ELEVATION_DELTA_MAX_M,
        "extended_active": bool(mbool("extended_conditions_active")),
        "extended_emissions_valid": bool(mbool("extended_conditions_emissions_valid")),
    }

    gps_source = normalised.get("gps")
    if not isinstance(gps_source, Mapping):
        gps_source = {}

    gps = {
        "distance_delta_pct": mfloat("gps_distance_delta_pct"),
        "max_gap_s": mfloat("gps_max_gap_s"),
        "total_gaps_s": mfloat("gps_total_gaps_s"),
        "max_gap_limit_s": GPS_MAX_GAP_SECONDS_MAX,
        "total_gaps_limit_s": GPS_TOTAL_GAPS_SECONDS_MAX,
        "delta_limit_pct": GPS_DISTANCE_DELTA_ABS_PERCENT_MAX,
        "gaps": list(gps_source.get("gaps", [])),
    }

    span_zero = {
        "zero": {
            "co2_ppm": mfloat("co2_zero_drift_ppm"),
            "co_ppm": mfloat("co_zero_drift_ppm"),
            "nox_ppm": mfloat("nox_zero_drift_ppm"),
            "pn_hash_cm3": mfloat("pn_zero_pre_hash_cm3"),
        },
        "span": {
            "co2_ppm": mfloat("co2_span_drift_ppm"),
            "co_ppm": mfloat("co_span_drift_ppm"),
            "nox_ppm": mfloat("nox_span_drift_ppm"),
        },
        "coverage": {
            "co2_pct": mfloat("co2_span_coverage_pct"),
            "co_pct": mfloat("co_span_coverage_pct"),
            "nox_pct": mfloat("nox_span_coverage_pct"),
            "co2_mid_pct": mfloat("co2_span_mid_points_pct"),
            "co2_over_limit": mint("co2_span_over_limit_count"),
        },
        "limits": {
            "co2_zero_ppm": CO2_ZERO_DRIFT_PPM_MAX,
            "co_zero_ppm": CO_ZERO_DRIFT_PPM_MAX,
            "nox_zero_ppm": NOX_ZERO_DRIFT_PPM_MAX,
            "pn_zero_hash_cm3": PN_ZERO_HASH_CM3_MAX,
            "co2_span_ppm": CO2_SPAN_DRIFT_PPM_MAX,
            "co_span_ppm": CO_SPAN_DRIFT_PPM_MAX,
            "nox_span_ppm": NOX_SPAN_DRIFT_PPM_MAX,
            "coverage_min_pct": SPAN_COVERAGE_MIN,
            "two_x_pct": SPAN_TWO_X_BAND_MAX_PERCENT,
            "exceed_count": SPAN_EXCEED_MAX_COUNT,
        },
    }

    devices_block = output.get("device") if isinstance(output.get("device"), Mapping) else {}
    devices_meta = meta_block.get("devices") if isinstance(meta_block.get("devices"), Mapping) else {}
    devices = {
        "gas_pems": devices_meta.get("gas_pems") or devices_block.get("gasPEMS"),
        "pn_pems": devices_meta.get("pn_pems") or devices_block.get("pnPEMS"),
        "efm": devices_meta.get("efm") or devices_block.get("efm"),
        "leak_rate_pct": mfloat("gas_pems_leak_rate_pct"),
        "pn_dilute_pressure_mbar": mfloat("pn_dilute_pressure_rise_mbar"),
        "pn_sample_pressure_mbar": mfloat("pn_sample_pressure_rise_mbar"),
        "device_errors": mint("device_error_count"),
        "limits": {
            "leak_rate_pct": GAS_PEMS_LEAK_RATE_PERCENT_MAX,
            "pn_dilute_pressure_mbar": PN_DILUTE_PRESSURE_RISE_MBAR_MAX,
            "pn_sample_pressure_mbar": PN_SAMPLE_PRESSURE_RISE_MBAR_MAX,
            "device_errors": DEVICE_ERROR_COUNT_MAX,
        },
    }

    dynamics = {
        "urban": {
            "va_pos95": mfloat("va_pos95_urban_m2s3"),
            "rpa": mfloat("rpa_urban_ms2"),
            "accel_points": mint("accel_points_urban"),
        },
        "motorway": {
            "va_pos95": mfloat("va_pos95_expressway_m2s3"),
            "rpa": mfloat("rpa_expressway_ms2"),
            "accel_points": mint("accel_points_expressway"),
        },
        "limits": {
            "va_pos95": {"urban": VA_POS95_URBAN_MAX, "motorway": VA_POS95_EXPRESSWAY_MAX},
            "rpa_min": {"urban": RPA_URBAN_MIN, "motorway": RPA_EXPRESSWAY_MIN},
        },
        "avg_speeds_kmh": {
            "urban": mfloat("avg_speed_urban_kmh"),
            "motorway": mfloat("avg_speed_expressway_kmh"),
        },
    }

    maw_coverage = {
        "low_pct": mfloat("maw_low_speed_valid_pct"),
        "high_pct": mfloat("maw_high_speed_valid_pct"),
        "low_limit_pct": MAW_LOW_SPEED_VALID_PERCENT_MIN,
        "high_limit_pct": MAW_HIGH_SPEED_VALID_PERCENT_MIN,
        "windows": list(metrics_block.get("maw_windows") or []),
    }

    emissions_summary = {
        "phases": emissions,
        "limits": output.get("limits"),
    }

    regressions = dict(normalised.get("regressions") or {})
    final_trace = dict(normalised.get("final_trace") or {})

    output.update(
        {
            "precon_soak": precon_soak,
            "cold_start": cold_start,
            "trip_shares": trip_shares,
            "elevation": elevation,
            "gps": gps,
            "span_zero": span_zero,
            "devices": devices,
            "dynamics": dynamics,
            "maw_coverage": maw_coverage,
            "emissions_summary": emissions_summary,
            "regressions": regressions,
            "final_trace": final_trace,
        }
    )

    return output


__all__ = ["SECTION_TITLES", "build_normalised_payload", "enrich_payload"]
