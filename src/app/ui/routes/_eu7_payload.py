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

    return output


__all__ = ["SECTION_TITLES", "build_normalised_payload", "enrich_payload"]
