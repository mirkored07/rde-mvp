"""Entry point for generating regulation-specific report payloads."""

from __future__ import annotations

import pathlib
from copy import deepcopy
from typing import Any, Dict, Mapping

try:  # pragma: no cover - optional dependency guard
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - used in stripped CI environments
    yaml = None  # type: ignore

from . import eu7_ld

SPEC_DIR = pathlib.Path(__file__).resolve().parent / "specs"
_DEFAULT_LEGISLATION = "eu7_ld"

_FALLBACK_EU7_SPEC: Dict[str, Any] = {
    "limits": {
        "nox_mg_per_km": 6000,
        "pn_per_km": 12_000_000,
        "co_mg_per_km": 1000,
    },
    "zero_span": {"pn_zero_max_per_cm3": 5000},
    "trip_composition": {
        "urban_min_km": 10,
        "expressway_min_km": 10,
        "share_urban_percent_range": [40, 65],
        "share_expressway_percent_range": [35, 60],
        "duration_minutes_range": [90, 120],
        "start_urban": True,
    },
    "dynamics": {
        "urban_avg_speed_range_kmh": [15, 40],
        "stop_time_share_urban_percent_range": [6, 30],
        "maw_low_speed_valid_percent_min": 50,
        "maw_high_speed_valid_percent_min": 50,
    },
    "gps": {"max_loss_s": 120, "total_loss_s": 300},
}


def _load_yaml(name: str) -> Dict[str, Any]:
    if yaml is None:
        # fall back to an embedded spec when PyYAML is unavailable
        if name in {"eu7_ld", "eu7_ld.yaml"}:
            return deepcopy(_FALLBACK_EU7_SPEC)
        raise RuntimeError("PyYAML is required to load legislation specifications.")

    with open(SPEC_DIR / name, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):  # pragma: no cover - defensive
        raise ValueError(f"Specification '{name}' must be a mapping.")
    return data


def load_spec(name: str = _DEFAULT_LEGISLATION) -> Dict[str, Any]:
    filename = name if name.endswith(".yaml") else f"{name}.yaml"
    return _load_yaml(filename)


def _merge_dict(base: Dict[str, Any], patch: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in patch.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, Mapping):
            _merge_dict(base[key], value)
        else:
            base[key] = dict(value) if isinstance(value, Mapping) else value
    return base


def render_report(
    legislation: str = _DEFAULT_LEGISLATION,
    data: Mapping[str, Any] | None = None,
    *,
    spec_override: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    if legislation.lower() != _DEFAULT_LEGISLATION:
        raise ValueError(f"Unsupported legislation '{legislation}'.")

    base_inputs = dict(data or {})
    merged_spec = None
    if spec_override:
        base_spec = load_spec(legislation)
        merged_spec = _merge_dict(base_spec, spec_override)
    return evaluate_eu7_ld(base_inputs, spec_override=merged_spec)


def build_results_payload(
    legislation: str = _DEFAULT_LEGISLATION,
    data: Mapping[str, Any] | None = None,
    *,
    spec_override: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    return render_report(legislation, data, spec_override=spec_override)


def evaluate_eu7_ld(
    raw_inputs: Mapping[str, Any] | None = None,
    *,
    spec_override: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    raw_inputs = dict(raw_inputs or {})
    spec = load_spec("eu7_ld")
    if spec_override:
        spec = _merge_dict(spec, spec_override)

    data = {
        "pn_zero_pre": raw_inputs.get("pn_zero_pre", 0),
        "urban_km": raw_inputs.get("urban_km", 11.0),
        "expressway_km": raw_inputs.get("expressway_km", 32.0),
        "avg_speed_urban_kmh": raw_inputs.get("avg_speed_urban_kmh", 28.0),
        "gps_max_loss_s": raw_inputs.get("gps_max_loss_s", 1),
        "gps_total_loss_s": raw_inputs.get("gps_total_loss_s", 3),
        "nox_mg_per_km": raw_inputs.get("nox_mg_per_km", 9236.0),
        "pn_per_km": raw_inputs.get("pn_per_km", 1.055e7),
        "co_mg_per_km": raw_inputs.get("co_mg_per_km", 0.0),
    }

    sections = [
        eu7_ld.compute_zero_span(data, spec),
        eu7_ld.compute_trip_composition(data, spec),
        eu7_ld.compute_dynamics(data, spec),
        eu7_ld.compute_gps_validity(data, spec),
        eu7_ld.compute_emissions_summary(data, spec),
    ]
    final = eu7_ld.compute_final_conformity(sections[-1], spec)

    visual = {
        "map": {"center": {"lat": 47.07, "lon": 15.44, "zoom": 10}, "latlngs": []},
        "chart": {"series": []},
    }
    kpi_numbers = [
        {"label": "NOx (mg/km)", "value": data["nox_mg_per_km"]},
        {"label": "PN (#/km)", "value": data["pn_per_km"]},
    ]

    return {
        "visual": visual,
        "kpi_numbers": kpi_numbers,
        "sections": sections,
        "final": final,
        "meta": {"legislation": "EU7 Light-Duty"},
    }


__all__ = ["build_results_payload", "load_spec", "render_report", "evaluate_eu7_ld"]
