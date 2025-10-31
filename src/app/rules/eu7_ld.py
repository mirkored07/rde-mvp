"""EU7 Light-Duty regulation metrics and report assembly."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

_REF_TAG = "§ EU7-LD"


def _crit(ref, criterion, condition, value, unit, passed: bool):
    """Return a normalised criteria row dictionary."""

    return {
        "ref": ref,
        "criterion": criterion,
        "condition": condition,
        "value": value,
        "unit": unit,
        "pass": bool(passed),
    }


def _format_le(limit: float | None, unit: str) -> str:
    if limit is None:
        return "Limit pending"
    suffix = f" {unit}" if unit else ""
    return f"≤ {limit:g}{suffix}"


def _format_range(bounds: tuple[float | None, float | None], unit: str) -> str:
    lower, upper = bounds
    if lower is not None and upper is not None:
        return f"{lower:g}–{upper:g} {unit}".strip()
    if lower is not None:
        return f"≥ {lower:g} {unit}".strip()
    if upper is not None:
        return f"≤ {upper:g} {unit}".strip()
    return "Requirement pending"


def _percent(part: float | None, total: float | None) -> float | None:
    if part is None or total in (None, 0):
        return None
    return (part / total) * 100.0


def _value_display(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def _as_number(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _range_tuple(entry: Any) -> tuple[float | None, float | None]:
    if isinstance(entry, (list, tuple)) and len(entry) == 2:
        lower = _as_number(entry[0])
        upper = _as_number(entry[1])
        return lower, upper
    return None, None


def compute_zero_span(data, spec):
    zero_spec = spec.get("zero_span", {}) if isinstance(spec, Mapping) else {}
    limit = zero_spec.get("pn_zero_max_per_cm3")
    unit = "#/cm³"
    measured_pre = _as_number(data.get("pn_zero_pre"))
    measured_post = _as_number(data.get("pn_zero_post"))
    rows = [
        _crit(
            "EU7-ZS-01",
            "PN zero (pre-test)",
            _format_le(limit, unit),
            _value_display(measured_pre),
            unit,
            True if limit is None else (measured_pre is not None and measured_pre <= limit),
        ),
        _crit(
            "EU7-ZS-02",
            "PN zero (post-test)",
            _format_le(limit, unit),
            _value_display(measured_post),
            unit,
            True if limit is None else (measured_post is not None and measured_post <= limit),
        ),
    ]
    return {"title": "Pre/Post Checks (Zero/Span)", "criteria": rows}


def compute_trip_composition(data, spec):
    trip_spec = spec.get("trip_composition", {}) if isinstance(spec, Mapping) else {}
    unit = "km"
    urban = _as_number(data.get("urban_km"))
    expressway = _as_number(data.get("expressway_km"))
    rural = _as_number(data.get("rural_km"))
    total = _as_number(data.get("total_km"))
    if total is None:
        components = [v for v in (urban, expressway, rural) if isinstance(v, (int, float))]
        total = sum(components) if components else None

    duration_min = _as_number(data.get("duration_minutes"))
    share_unit = "%"
    urban_share = _percent(urban, total)
    express_share = _percent(expressway, total)
    urban_min = _as_number(trip_spec.get("urban_min_km"))
    express_min = _as_number(trip_spec.get("expressway_min_km"))
    urban_range = _range_tuple(trip_spec.get("share_urban_percent_range"))
    express_range = _range_tuple(trip_spec.get("share_expressway_percent_range"))
    duration_range = _range_tuple(trip_spec.get("duration_minutes_range"))

    rows = [
        _crit(
            "EU7-TR-01",
            "Urban distance",
            _format_le(urban_min, unit).replace("≤", "≥"),
            _value_display(urban),
            unit,
            urban is not None and urban_min is not None and urban >= urban_min,
        ),
        _crit(
            "EU7-TR-02",
            "Expressway distance",
            _format_le(express_min, unit).replace("≤", "≥"),
            _value_display(expressway),
            unit,
            expressway is not None and express_min is not None and expressway >= express_min,
        ),
        _crit(
            "EU7-TR-03",
            "Urban distance share",
            _format_range(urban_range, share_unit),
            _value_display(urban_share),
            share_unit,
            _in_range(urban_share, urban_range),
        ),
        _crit(
            "EU7-TR-04",
            "Expressway distance share",
            _format_range(express_range, share_unit),
            _value_display(express_share),
            share_unit,
            _in_range(express_share, express_range),
        ),
        _crit(
            "EU7-TR-05",
            "Trip duration",
            _format_range(duration_range, "min"),
            _value_display(duration_min),
            "min",
            _in_range(duration_min, duration_range),
        ),
    ]

    start_urban_required = bool(trip_spec.get("start_urban"))
    started_in_urban = bool(data.get("start_urban", True))
    rows.append(
        _crit(
            "EU7-TR-06",
            "Trip started in urban operation",
            "Must start in urban operation" if start_urban_required else "Optional",
            "Yes" if started_in_urban else "No",
            "",
            (not start_urban_required) or started_in_urban,
        )
    )

    return {"title": "Trip Composition & Timing", "criteria": rows}


def compute_dynamics(data, spec):
    dyn_spec = spec.get("dynamics", {}) if isinstance(spec, Mapping) else {}
    avg_speed_bounds = _range_tuple(dyn_spec.get("urban_avg_speed_range_kmh"))
    stop_bounds = _range_tuple(dyn_spec.get("stop_time_share_urban_percent_range"))
    maw_low_min = _as_number(dyn_spec.get("maw_low_speed_valid_percent_min"))
    maw_high_min = _as_number(dyn_spec.get("maw_high_speed_valid_percent_min"))

    avg_speed = _as_number(data.get("avg_speed_urban_kmh"))
    stop_share = _as_number(data.get("stop_time_share_urban_percent"))
    maw_low = _as_number(data.get("maw_low_speed_valid_percent"))
    maw_high = _as_number(data.get("maw_high_speed_valid_percent"))

    rows = [
        _crit(
            "EU7-DY-01",
            "Average urban speed",
            _format_range(avg_speed_bounds, "km/h"),
            _value_display(avg_speed),
            "km/h",
            _in_range(avg_speed, avg_speed_bounds),
        ),
        _crit(
            "EU7-DY-02",
            "Urban stop time share",
            _format_range(stop_bounds, "%"),
            _value_display(stop_share),
            "%",
            _in_range(stop_share, stop_bounds),
        ),
        _crit(
            "EU7-DY-03",
            "Valid MAW windows (low speed)",
            _format_le(maw_low_min, "%").replace("≤", "≥"),
            _value_display(maw_low),
            "%",
            maw_low is not None and (maw_low_min is None or maw_low >= maw_low_min),
        ),
        _crit(
            "EU7-DY-04",
            "Valid MAW windows (high speed)",
            _format_le(maw_high_min, "%").replace("≤", "≥"),
            _value_display(maw_high),
            "%",
            maw_high is not None and (maw_high_min is None or maw_high >= maw_high_min),
        ),
    ]

    return {"title": "Dynamics / MAW", "criteria": rows}


def compute_gps_validity(data, spec):
    gps_spec = spec.get("gps", {}) if isinstance(spec, Mapping) else {}
    max_loss = _as_number(data.get("gps_max_loss_s"))
    total_loss = _as_number(data.get("gps_total_loss_s"))
    rows = [
        _crit(
            "EU7-GPS-01",
            "Maximum GPS loss",
            _format_le(gps_spec.get("max_loss_s"), "s"),
            _value_display(max_loss),
            "s",
            max_loss is not None
            and gps_spec.get("max_loss_s") is not None
            and max_loss <= gps_spec.get("max_loss_s"),
        ),
        _crit(
            "EU7-GPS-02",
            "Total GPS loss",
            _format_le(gps_spec.get("total_loss_s"), "s"),
            _value_display(total_loss),
            "s",
            total_loss is not None
            and gps_spec.get("total_loss_s") is not None
            and total_loss <= gps_spec.get("total_loss_s"),
        ),
    ]

    return {"title": "GPS/Altitude Validity", "criteria": rows}


def compute_emissions_summary(data, spec):
    limits = spec.get("limits", {}) if isinstance(spec, Mapping) else {}
    pollutants = [
        ("EU7-EM-01", "NOx", "nox_mg_per_km", "mg/km"),
        ("EU7-EM-02", "PN", "pn_per_km", "#/km"),
        ("EU7-EM-03", "CO", "co_mg_per_km", "mg/km"),
    ]
    rows = []
    for ref, label, key, unit in pollutants:
        limit = _as_number(limits.get(key))
        value = _as_number(data.get(key))
        passed = True if limit is None else (value is not None and value <= limit)
        rows.append(
            _crit(
                ref,
                f"{label} emissions",
                _format_le(limit, unit),
                _value_display(value, 3),
                unit,
                passed,
            )
        )
    return {"title": "Emissions Summary", "criteria": rows}


def compute_final_conformity(emissions_section, spec):
    limits = spec.get("limits", {}) if isinstance(spec, Mapping) else {}
    if not isinstance(limits, Mapping) or any(value is None for value in limits.values()):
        return None

    criteria = []
    if isinstance(emissions_section, Mapping):
        criteria = list(emissions_section.get("criteria") or [])

    overall_pass = all(bool(row.get("pass")) for row in criteria) if criteria else False
    return {
        "title": "Final Conformity",
        "pass": overall_pass,
        "pollutants": criteria,
    }


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text or text.upper().startswith("<TODO"):
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _value_unit(entry: Any, default_unit: str = "") -> tuple[float | None, str]:
    if isinstance(entry, Mapping):
        value = _as_float(entry.get("value"))
        unit = str(entry.get("unit") or default_unit)
        return value, unit
    return _as_float(entry), default_unit


def _format_condition_le(limit: float | None, unit: str) -> str:
    if limit is None:
        return "Limit pending"
    unit_text = f" {unit}" if unit else ""
    return f"≤ {limit:g}{unit_text}"


def _format_condition_range(bounds: tuple[float | None, float | None], unit: str) -> str:
    lower, upper = bounds
    lower_text = "" if lower is None else f"≥ {lower:g}"
    upper_text = "" if upper is None else f"≤ {upper:g}"
    if lower is not None and upper is not None:
        return f"{lower:g}–{upper:g} {unit}".strip()
    if lower is not None:
        return f"{lower_text} {unit}".strip()
    if upper is not None:
        return f"{upper_text} {unit}".strip()
    return "Requirement pending"


def _in_range(value: float | None, bounds: tuple[float | None, float | None]) -> bool:
    if value is None:
        return False
    lower, upper = bounds
    if lower is not None and value < lower:
        return False
    if upper is not None and value > upper:
        return False
    return True


def _resolve_bounds(entry: Any) -> tuple[float | None, float | None]:
    if isinstance(entry, (list, tuple)) and len(entry) == 2:
        return _as_float(entry[0]), _as_float(entry[1])
    return None, None


def _default_table() -> tuple[list[str], list[list[Any]]]:
    columns = ["criterion", "value", "unit"]
    values = [["Demo", 0, "-"]]
    return columns, values


def _round(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _safe_percent(part: float | None, total: float | None) -> float | None:
    if part is None or total in (None, 0):
        return None
    return (part / total) * 100.0


def compute_zero_span(data: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, Any]:
    """Return the zero/span block covering pre/post checks."""

    zero_limits = deepcopy(spec.get("zero_span", {}))
    zero_data = data.get("zero_span", {}) if isinstance(data.get("zero_span"), Mapping) else {}

    rows: list[dict[str, Any]] = []
    for key, limit_value in zero_limits.items():
        measured, unit = _value_unit(zero_data.get(key), "#/cm³")
        limit = _as_float(limit_value)
        passed = measured is not None and limit is not None and measured <= limit
        rows.append(
            {
                "ref": _REF_TAG,
                "criterion": key.replace("_", " ").title(),
                "condition": _format_condition_le(limit, unit),
                "value": _round(measured, 2) if measured is not None else "n/a",
                "unit": unit,
                "pass": bool(passed),
            }
        )

    return {"title": "Pre/Post Checks (Zero/Span)", "criteria": rows}


def compute_trip_composition(data: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, Any]:
    trip_spec = spec.get("trip_composition", {})
    trip_data = data.get("trip", {}) if isinstance(data.get("trip"), Mapping) else {}

    urban_km = _as_float(trip_data.get("urban_distance_km"))
    expressway_km = _as_float(trip_data.get("expressway_distance_km"))
    rural_km = _as_float(trip_data.get("rural_distance_km"))
    total_km = _as_float(trip_data.get("total_distance_km"))
    if total_km is None:
        total_km = sum(value for value in [urban_km, expressway_km, rural_km] if value is not None)
    duration_s = _as_float(trip_data.get("duration_s"))
    duration_min = duration_s / 60.0 if duration_s is not None else None

    rows: list[dict[str, Any]] = []

    def _min_row(label: str, value: float | None, threshold: Any, unit: str) -> dict[str, Any]:
        limit = _as_float(threshold)
        passed = value is not None and limit is not None and value >= limit
        return {
            "ref": _REF_TAG,
            "criterion": label,
            "condition": _format_condition_le(limit, unit).replace("≤", "≥"),
            "value": _round(value, 2) if value is not None else "n/a",
            "unit": unit,
            "pass": bool(passed),
        }

    rows.append(
        _min_row(
            "Urban distance",
            urban_km,
            trip_spec.get("urban_min_km"),
            spec.get("units", {}).get("distance", "km"),
        )
    )
    rows.append(
        _min_row(
            "Expressway distance",
            expressway_km,
            trip_spec.get("expressway_min_km"),
            spec.get("units", {}).get("distance", "km"),
        )
    )

    share_unit = "%"
    urban_share = _safe_percent(urban_km, total_km)
    expressway_share = _safe_percent(expressway_km, total_km)
    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Urban share of distance",
            "condition": _format_condition_range(
                _resolve_bounds(trip_spec.get("share_urban_percent_range")),
                share_unit,
            ),
            "value": _round(urban_share, 2) if urban_share is not None else "n/a",
            "unit": share_unit,
            "pass": _in_range(
                urban_share,
                _resolve_bounds(trip_spec.get("share_urban_percent_range")),
            ),
        }
    )
    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Expressway share of distance",
            "condition": _format_condition_range(
                _resolve_bounds(trip_spec.get("share_expressway_percent_range")),
                share_unit,
            ),
            "value": _round(expressway_share, 2) if expressway_share is not None else "n/a",
            "unit": share_unit,
            "pass": _in_range(
                expressway_share,
                _resolve_bounds(trip_spec.get("share_expressway_percent_range")),
            ),
        }
    )

    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Trip duration",
            "condition": _format_condition_range(
                _resolve_bounds(trip_spec.get("duration_minutes_range")),
                "min",
            ),
            "value": _round(duration_min, 2) if duration_min is not None else "n/a",
            "unit": "min",
            "pass": _in_range(
                duration_min,
                _resolve_bounds(trip_spec.get("duration_minutes_range")),
            ),
        }
    )

    start_expected = trip_spec.get("start_urban")
    start_phase = str(trip_data.get("start_phase") or "").lower()
    start_value = start_phase or "n/a"
    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Urban start",
            "condition": "Urban segment must start the trip" if start_expected else "Start condition not enforced",
            "value": start_value,
            "unit": "phase",
            "pass": bool(start_expected and start_phase == "urban") or not bool(start_expected),
        }
    )

    return {"title": "Trip Composition & Timing", "criteria": rows}


def compute_dynamics(data: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, Any]:
    dynamics_spec = spec.get("dynamics", {})
    dynamics_data = data.get("dynamics", {}) if isinstance(data.get("dynamics"), Mapping) else {}
    rows: list[dict[str, Any]] = []

    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Urban average speed",
            "condition": _format_condition_range(
                _resolve_bounds(dynamics_spec.get("urban_avg_speed_range_kmh")),
                spec.get("units", {}).get("speed", "km/h"),
            ),
            "value": _round(_as_float(dynamics_data.get("urban_avg_speed_kmh")), 2)
            if _as_float(dynamics_data.get("urban_avg_speed_kmh")) is not None
            else "n/a",
            "unit": spec.get("units", {}).get("speed", "km/h"),
            "pass": _in_range(
                _as_float(dynamics_data.get("urban_avg_speed_kmh")),
                _resolve_bounds(dynamics_spec.get("urban_avg_speed_range_kmh")),
            ),
        }
    )

    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Urban stop time share",
            "condition": _format_condition_range(
                _resolve_bounds(dynamics_spec.get("stop_time_share_urban_percent_range")),
                "%",
            ),
            "value": _round(_as_float(dynamics_data.get("urban_stop_time_share_percent")), 2)
            if _as_float(dynamics_data.get("urban_stop_time_share_percent")) is not None
            else "n/a",
            "unit": "%",
            "pass": _in_range(
                _as_float(dynamics_data.get("urban_stop_time_share_percent")),
                _resolve_bounds(dynamics_spec.get("stop_time_share_urban_percent_range")),
            ),
        }
    )

    va_limits = dynamics_spec.get("va_pos95_limits", {})
    va_data = dynamics_data.get("va_pos95", {}) if isinstance(dynamics_data.get("va_pos95"), Mapping) else {}
    for phase, limit in va_limits.items():
        value = _as_float(va_data.get(phase))
        unit = "m/s²"
        limit_val = _as_float(limit)
        rows.append(
            {
                "ref": _REF_TAG,
                "criterion": f"VA95 {phase.title()}",
                "condition": _format_condition_le(limit_val, unit),
                "value": _round(value, 3) if value is not None else "n/a",
                "unit": unit,
                "pass": value is not None and limit_val is not None and value <= limit_val,
            }
        )

    rpa_spec = dynamics_spec.get("rpa_min", {})
    rpa_data = dynamics_data.get("rpa", {}) if isinstance(dynamics_data.get("rpa"), Mapping) else {}
    for phase, limit in rpa_spec.items():
        value = _as_float(rpa_data.get(phase))
        limit_val = _as_float(limit)
        rows.append(
            {
                "ref": _REF_TAG,
                "criterion": f"RPA {phase.title()}",
                "condition": _format_condition_le(limit_val, "m/s²").replace("≤", "≥"),
                "value": _round(value, 3) if value is not None else "n/a",
                "unit": "m/s²",
                "pass": value is not None and limit_val is not None and value >= limit_val,
            }
        )

    maw_spec = dynamics_spec.get("maw", {})
    maw_data = dynamics_data.get("maw", {}) if isinstance(dynamics_data.get("maw"), Mapping) else {}

    low_tol = _as_float(maw_spec.get("low_speed_tolerance_percent"))
    high_tol = _as_float(maw_spec.get("high_speed_tolerance_percent"))
    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "MAW low-speed tolerance",
            "condition": _format_condition_le(low_tol, "%").replace("≤", "≥"),
            "value": _round(_as_float(maw_data.get("low_speed_tolerance_percent")), 2)
            if _as_float(maw_data.get("low_speed_tolerance_percent")) is not None
            else "n/a",
            "unit": "%",
            "pass": _as_float(maw_data.get("low_speed_tolerance_percent")) is not None
            and low_tol is not None
            and _as_float(maw_data.get("low_speed_tolerance_percent")) >= low_tol,
        }
    )
    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "MAW high-speed tolerance",
            "condition": _format_condition_le(high_tol, "%").replace("≤", "≥"),
            "value": _round(_as_float(maw_data.get("high_speed_tolerance_percent")), 2)
            if _as_float(maw_data.get("high_speed_tolerance_percent")) is not None
            else "n/a",
            "unit": "%",
            "pass": _as_float(maw_data.get("high_speed_tolerance_percent")) is not None
            and high_tol is not None
            and _as_float(maw_data.get("high_speed_tolerance_percent")) >= high_tol,
        }
    )

    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "MAW valid low-speed windows",
            "condition": _format_condition_le(
                _as_float(maw_spec.get("min_valid_low_speed_windows_percent")),
                "%",
            ).replace("≤", "≥"),
            "value": _round(_as_float(maw_data.get("valid_low_speed_windows_percent")), 2)
            if _as_float(maw_data.get("valid_low_speed_windows_percent")) is not None
            else "n/a",
            "unit": "%",
            "pass": _as_float(maw_data.get("valid_low_speed_windows_percent")) is not None
            and _as_float(maw_spec.get("min_valid_low_speed_windows_percent")) is not None
            and _as_float(maw_data.get("valid_low_speed_windows_percent"))
            >= _as_float(maw_spec.get("min_valid_low_speed_windows_percent")),
        }
    )

    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "MAW valid high-speed windows",
            "condition": _format_condition_le(
                _as_float(maw_spec.get("min_valid_high_speed_windows_percent")),
                "%",
            ).replace("≤", "≥"),
            "value": _round(_as_float(maw_data.get("valid_high_speed_windows_percent")), 2)
            if _as_float(maw_data.get("valid_high_speed_windows_percent")) is not None
            else "n/a",
            "unit": "%",
            "pass": _as_float(maw_data.get("valid_high_speed_windows_percent")) is not None
            and _as_float(maw_spec.get("min_valid_high_speed_windows_percent")) is not None
            and _as_float(maw_data.get("valid_high_speed_windows_percent"))
            >= _as_float(maw_spec.get("min_valid_high_speed_windows_percent")),
        }
    )

    return {"title": "Dynamics / MAW metrics", "criteria": rows}


def compute_gps_validity(data: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, Any]:
    gps_spec = spec.get("gps", {})
    gps_data = data.get("gps", {}) if isinstance(data.get("gps"), Mapping) else {}
    rows: list[dict[str, Any]] = []

    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Max GPS loss",
            "condition": _format_condition_le(_as_float(gps_spec.get("max_loss_s")), "s"),
            "value": _round(_as_float(gps_data.get("max_loss_s")), 2)
            if _as_float(gps_data.get("max_loss_s")) is not None
            else "n/a",
            "unit": "s",
            "pass": _as_float(gps_data.get("max_loss_s")) is not None
            and _as_float(gps_spec.get("max_loss_s")) is not None
            and _as_float(gps_data.get("max_loss_s")) <= _as_float(gps_spec.get("max_loss_s")),
        }
    )

    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Total GPS loss",
            "condition": _format_condition_le(_as_float(gps_spec.get("total_loss_s")), "s"),
            "value": _round(_as_float(gps_data.get("total_loss_s")), 2)
            if _as_float(gps_data.get("total_loss_s")) is not None
            else "n/a",
            "unit": "s",
            "pass": _as_float(gps_data.get("total_loss_s")) is not None
            and _as_float(gps_spec.get("total_loss_s")) is not None
            and _as_float(gps_data.get("total_loss_s")) <= _as_float(gps_spec.get("total_loss_s")),
        }
    )

    rows.append(
        {
            "ref": _REF_TAG,
            "criterion": "Max distance deviation",
            "condition": _format_condition_le(
                _as_float(gps_spec.get("max_distance_dev_percent")),
                "%",
            ),
            "value": _round(_as_float(gps_data.get("max_distance_dev_percent")), 2)
            if _as_float(gps_data.get("max_distance_dev_percent")) is not None
            else "n/a",
            "unit": "%",
            "pass": _as_float(gps_data.get("max_distance_dev_percent")) is not None
            and _as_float(gps_spec.get("max_distance_dev_percent")) is not None
            and _as_float(gps_data.get("max_distance_dev_percent"))
            <= _as_float(gps_spec.get("max_distance_dev_percent")),
        }
    )

    return {"title": "GPS/Altitude validity", "criteria": rows}


def _pollutant_label(key: str) -> str:
    if key.startswith("nox"):
        return "NOx"
    if key.startswith("pn"):
        return "PN"
    if key.startswith("co"):
        return "CO"
    if key.startswith("thc"):
        return "THC"
    if key.startswith("nh3"):
        return "NH3"
    return key.upper()


def _pollutant_unit(key: str, spec: Mapping[str, Any]) -> str:
    units = spec.get("units", {}) if isinstance(spec.get("units"), Mapping) else {}
    if key.startswith("pn"):
        return str(units.get("pn_per_km") or "#/km")
    return str(units.get("mass_per_km") or "mg/km")


def compute_emissions_summary(data: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, Any]:
    limits = spec.get("limits", {}) if isinstance(spec.get("limits"), Mapping) else {}
    emissions_data = data.get("emissions", {}) if isinstance(data.get("emissions"), Mapping) else {}
    corrections_data = data.get("corrections", {}) if isinstance(data.get("corrections"), Mapping) else {}

    rows: list[dict[str, Any]] = []
    pollutants: list[dict[str, Any]] = []

    for key, limit in limits.items():
        unit = _pollutant_unit(key, spec)
        label = _pollutant_label(key)
        measured_entry = emissions_data.get(key)
        value, value_unit = _value_unit(measured_entry, unit)
        corrected_value = None
        if isinstance(measured_entry, Mapping):
            corrected_value = _as_float(measured_entry.get("corrected_value"))
        display_value = corrected_value if corrected_value is not None else value
        limit_val = _as_float(limit)
        passed = (
            display_value is not None
            and limit_val is not None
            and display_value <= limit_val
        )
        rows.append(
            {
                "ref": _REF_TAG,
                "criterion": f"{label} tailpipe",
                "condition": _format_condition_le(limit_val, unit),
                "value": _round(display_value, 3) if display_value is not None else "n/a",
                "unit": value_unit or unit,
                "pass": bool(passed),
            }
        )
        pollutants.append(
            {
                "id": label,
                "value": _round(display_value, 3),
                "unit": value_unit or unit,
                "limit": limit_val,
                "pass": bool(passed),
                "raw_value": _round(value, 3),
            }
        )

    correction_notes: list[str] = []
    for key, entry in corrections_data.items():
        if not isinstance(entry, Mapping):
            continue
        applied = bool(entry.get("applied", entry.get("enabled")))
        if not applied:
            continue
        factor = entry.get("factor")
        if factor is not None:
            correction_notes.append(f"{key.upper()} factor {factor}")
        else:
            correction_notes.append(f"{key.upper()} applied")

    return {
        "title": "Emissions Summary",
        "criteria": rows,
        "pollutants": pollutants,
        "corrections": correction_notes,
    }


def compute_final_conformity(emissions: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, Any]:
    """Return the overall conformity block derived from pollutant results."""

    pollutant_entries = (
        emissions.get("pollutants", []) if isinstance(emissions, Mapping) else []
    )
    notes = list(emissions.get("corrections", [])) if isinstance(emissions, Mapping) else []

    all_pass = True
    missing_limits: list[str] = []
    for entry in pollutant_entries:
        if not isinstance(entry, Mapping):
            continue
        pollutant_pass = bool(entry.get("pass"))
        all_pass = all_pass and pollutant_pass
        if entry.get("limit") is None:
            missing_limits.append(str(entry.get("id") or "UNKNOWN"))

    if missing_limits:
        notes.append("Missing limit for: " + ", ".join(sorted(missing_limits)))

    passed = bool(all_pass) and not missing_limits

    summary_row = {
        "ref": _REF_TAG,
        "criterion": "Overall EU7-LD verdict",
        "condition": "All regulated pollutants within EU7 limits",
        "value": "PASS" if passed else "FAIL",
        "unit": "",
        "pass": passed,
    }

    return {
        "title": "Final Conformity (overall PASS/FAIL)",
        "criteria": [summary_row],
        "pass": passed,
        "pollutants": [
            dict(entry) for entry in pollutant_entries if isinstance(entry, Mapping)
        ],
        "notes": notes,
        "label": "PASS" if passed else "FAIL",
        "spec": {
            "id": spec.get("id"),
            "name": spec.get("name"),
            "version": spec.get("version"),
        },
    }


def _compute_kpis(data: Mapping[str, Any], spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    trip_data = data.get("trip", {}) if isinstance(data.get("trip"), Mapping) else {}
    total_km = _as_float(trip_data.get("total_distance_km"))
    if total_km is None:
        total_km = sum(
            value
            for value in (
                _as_float(trip_data.get("urban_distance_km")),
                _as_float(trip_data.get("expressway_distance_km")),
                _as_float(trip_data.get("rural_distance_km")),
            )
            if value is not None
        )
    duration_s = _as_float(trip_data.get("duration_s"))
    duration_h = duration_s / 3600.0 if duration_s is not None else None
    avg_speed = None
    if total_km not in (None, 0) and duration_h not in (None, 0):
        avg_speed = total_km / duration_h

    units = spec.get("units", {}) if isinstance(spec.get("units"), Mapping) else {}

    return [
        {
            "label": "Total Distance",
            "value": _round(total_km, 2) if total_km is not None else "n/a",
            "unit": units.get("distance", "km"),
        },
        {
            "label": "Trip Duration",
            "value": _round(duration_s / 60.0, 2) if duration_s is not None else "n/a",
            "unit": "min",
        },
        {
            "label": "Average Speed",
            "value": _round(avg_speed, 2) if avg_speed is not None else "n/a",
            "unit": units.get("speed", "km/h"),
        },
    ]


def _visual_block(data: Mapping[str, Any]) -> dict[str, Any]:
    visual = data.get("visual", {}) if isinstance(data.get("visual"), Mapping) else {}
    map_block = visual.get("map") if isinstance(visual.get("map"), Mapping) else {}
    chart_block = visual.get("chart") if isinstance(visual.get("chart"), Mapping) else {}
    return {"map": dict(map_block), "chart": dict(chart_block)}


def build_report(data: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, Any]:
    """Assemble the EU7-LD payload from raw harmonised inputs."""

    zero_span_block = compute_zero_span(data, spec)
    trip_block = compute_trip_composition(data, spec)
    dynamics_block = compute_dynamics(data, spec)
    gps_block = compute_gps_validity(data, spec)
    emissions_block = compute_emissions_summary(data, spec)
    final_block = compute_final_conformity(emissions_block, spec)

    columns = data.get("columns") if isinstance(data.get("columns"), list) else None
    values = data.get("values") if isinstance(data.get("values"), list) else None
    if columns is None or values is None:
        columns, values = _default_table()

    sections = [
        zero_span_block,
        trip_block,
        dynamics_block,
        gps_block,
        emissions_block,
        final_block,
    ]

    payload = {
        "id": spec.get("id"),
        "name": spec.get("name"),
        "version": spec.get("version"),
        "meta": {"legislation": spec.get("name") or "EU7 Light-Duty"},
        "kpi_numbers": _compute_kpis(data, spec),
        "sections": sections,
        "emissions": emissions_block,
        "final": {
            "pass": final_block.get("pass", False),
            "pollutants": list(final_block.get("pollutants", [])),
            "notes": list(final_block.get("notes", [])),
            "label": final_block.get("label"),
        },
        "visual": _visual_block(data),
        "columns": columns,
        "values": values,
    }
    payload["map"] = payload["visual"].get("map", {})
    payload["chart"] = payload["visual"].get("chart", {})
    return payload


def build_default_inputs(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Provide a deterministic demo dataset for the EU7-LD report."""

    units = spec.get("units", {}) if isinstance(spec.get("units"), Mapping) else {}

    corrections_spec = spec.get("corrections") if isinstance(spec.get("corrections"), Mapping) else {}
    extc_spec = corrections_spec.get("extc") if isinstance(corrections_spec.get("extc"), Mapping) else {}

    data = {
        "zero_span": {"pn_zero_max_per_cm3": {"value": 3200, "unit": "#/cm³"}},
        "trip": {
            "urban_distance_km": 18.0,
            "expressway_distance_km": 22.0,
            "rural_distance_km": 12.0,
            "total_distance_km": 52.0,
            "duration_s": 5400.0,
            "start_phase": "urban",
        },
        "dynamics": {
            "urban_avg_speed_kmh": 28.0,
            "urban_stop_time_share_percent": 22.0,
            "va_pos95": {"urban": 1.6, "expressway": 1.9},
            "rpa": {"urban": 0.18, "expressway": 0.14},
            "maw": {
                "low_speed_tolerance_percent": 90.0,
                "high_speed_tolerance_percent": 85.0,
                "valid_low_speed_windows_percent": 88.0,
                "valid_high_speed_windows_percent": 87.0,
            },
        },
        "gps": {
            "max_loss_s": 3.0,
            "total_loss_s": 8.0,
            "max_distance_dev_percent": 3.0,
        },
        "emissions": {
            "nox_mg_per_km": {"value": 42.0, "unit": units.get("mass_per_km", "mg/km")},
            "pn_per_km": {"value": 4.2e11, "unit": units.get("pn_per_km", "#/km")},
            "co_mg_per_km": {"value": 600.0, "unit": units.get("mass_per_km", "mg/km")},
            "thc_mg_per_km": {"value": 18.0, "unit": units.get("mass_per_km", "mg/km")},
            "nh3_mg_per_km": {"value": 8.0, "unit": units.get("mass_per_km", "mg/km")},
        },
        "corrections": {
            "extc": {"applied": True, "factor": extc_spec.get("factor")},
            "ki": {"applied": True},
        },
        "visual": {
            "map": {
                "center": {"lat": 48.2082, "lon": 16.3738, "zoom": 9},
                "latlngs": [
                    {"lat": 48.2082, "lon": 16.3738},
                    {"lat": 48.2500, "lon": 16.4000},
                    {"lat": 48.3000, "lon": 16.4500},
                ],
            },
            "chart": {
                "series": [
                    {"key": "NOx", "values": [120.0, 90.0, 60.0]},
                    {"key": "PN", "values": [3.0e11, 3.2e11, 4.0e11]},
                ],
                "labels": ["Urban", "Rural", "Expressway"],
            },
        },
        "columns": ["criterion", "value", "unit"],
        "values": [
            ["NOx tailpipe", 42.0, units.get("mass_per_km", "mg/km")],
            ["PN tailpipe", 4.2e11, units.get("pn_per_km", "#/km")],
        ],
    }
    return data


__all__ = [
    "build_default_inputs",
    "build_report",
    "compute_zero_span",
    "compute_trip_composition",
    "compute_dynamics",
    "compute_gps_validity",
    "compute_emissions_summary",
    "compute_final_conformity",
]
