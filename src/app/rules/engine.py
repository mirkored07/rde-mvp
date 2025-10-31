"""Entry point for generating regulation-specific report payloads."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

try:  # pragma: no cover - optional dependency guard
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - raised in tests if missing
    yaml = None  # type: ignore

from src.app.utils.payload import ensure_results_payload_defaults

from . import eu7_ld

_SPEC_DIR = Path(__file__).resolve().parent / "specs"
_DEFAULT_LEGISLATION = "eu7_ld"


SPEC_DIR = _SPEC_DIR


def _load_yaml(name: str) -> Mapping[str, Any]:
    path = SPEC_DIR / name
    if yaml is None:
        raise RuntimeError("PyYAML is required to load legislation specifications.")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, Mapping):
        raise ValueError(f"Specification '{name}' must be a mapping.")
    return data


def _deep_update(target: MutableMapping[str, Any], patch: Mapping[str, Any]) -> MutableMapping[str, Any]:
    """Merge *patch* into *target* recursively and return the mutated mapping."""

    for key, value in patch.items():
        if (
            key in target
            and isinstance(target[key], MutableMapping)
            and isinstance(value, Mapping)
        ):
            _deep_update(target[key], value)
        else:
            target[key] = deepcopy(value)
    return target


@lru_cache(maxsize=8)
def load_spec(name: str = _DEFAULT_LEGISLATION) -> Mapping[str, Any]:
    """Load and cache the YAML specification for a supported legislation."""

    spec_name = name.lower()
    path = _SPEC_DIR / f"{spec_name}.yaml"
    if not path.exists():  # pragma: no cover - defensive guard
        raise ValueError(f"Unknown legislation spec '{name}'.")

    if yaml is None:
        raise RuntimeError("PyYAML is required to load legislation specifications.")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"Specification '{name}' must evaluate to a mapping.")
    return raw


def render_report(
    legislation: str = _DEFAULT_LEGISLATION,
    data: Mapping[str, Any] | None = None,
    *,
    spec_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return an un-normalised results payload for *legislation*.

    Parameters
    ----------
    legislation:
        Currently only ``"eu7_ld"`` is supported.
    data:
        Harmonised analysis inputs. If omitted a deterministic demo payload is used.
    spec_override:
        Optional mapping merged on top of the static YAML specification. Handy for
        tests that want to stub TODO limits.
    """

    key = legislation.lower()
    if key != "eu7_ld":  # pragma: no cover - future extension guard
        raise ValueError(f"Unsupported legislation '{legislation}'.")

    spec_mapping = deepcopy(dict(load_spec("eu7_ld")))
    if spec_override:
        _deep_update(spec_mapping, spec_override)

    inputs = data if data is not None else eu7_ld.build_default_inputs(spec_mapping)
    return eu7_ld.build_report(inputs, spec_mapping)


def build_results_payload(
    legislation: str = _DEFAULT_LEGISLATION,
    data: Mapping[str, Any] | None = None,
    *,
    spec_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Render and normalise a results payload for the requested legislation."""

    raw_payload = render_report(
        legislation,
        data,
        spec_override=spec_override,
    )
    return ensure_results_payload_defaults(raw_payload)


def evaluate_eu7_ld(raw_inputs: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Evaluate the EU7 Light-Duty ruleset for the provided *raw_inputs*."""

    raw_inputs = raw_inputs or {}
    spec = dict(_load_yaml("eu7_ld.yaml"))

    def _num(key: str, default: float) -> float:
        value = raw_inputs.get(key, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    total_override = raw_inputs.get("total_km")
    try:
        total_distance_override = float(total_override) if total_override is not None else None
    except (TypeError, ValueError):
        total_distance_override = None

    start_value = raw_inputs.get("start_urban", True)
    if isinstance(start_value, str):
        start_value_normalised = start_value.strip().lower()
        start_urban = start_value_normalised in {"1", "true", "yes"}
    else:
        start_urban = bool(start_value)

    data = {
        "pn_zero_pre": _num("pn_zero_pre", 3200.0),
        "pn_zero_post": _num("pn_zero_post", 3400.0),
        "urban_km": _num("urban_km", 18.5),
        "expressway_km": _num("expressway_km", 27.2),
        "rural_km": _num("rural_km", 12.3),
        "total_km": total_distance_override,
        "duration_minutes": _num("duration_minutes", 102.0),
        "start_urban": start_urban,
        "avg_speed_urban_kmh": _num("avg_speed_urban_kmh", 28.0),
        "stop_time_share_urban_percent": _num("stop_time_share_urban_percent", 12.5),
        "maw_low_speed_valid_percent": _num("maw_low_speed_valid_percent", 92.0),
        "maw_high_speed_valid_percent": _num("maw_high_speed_valid_percent", 88.0),
        "gps_max_loss_s": _num("gps_max_loss_s", 8.0),
        "gps_total_loss_s": _num("gps_total_loss_s", 45.0),
        "nox_mg_per_km": _num("nox_mg_per_km", 9.236e3),
        "pn_per_km": _num("pn_per_km", 1.055e7),
        "co_mg_per_km": _num("co_mg_per_km", 350.0),
    }

    if data.get("total_km") is None:
        total_distance = sum(
            value for value in (data.get("urban_km"), data.get("expressway_km"), data.get("rural_km")) if isinstance(value, (int, float))
        )
        data["total_km"] = total_distance

    sections = [
        eu7_ld.compute_zero_span(data, spec),
        eu7_ld.compute_trip_composition(data, spec),
        eu7_ld.compute_dynamics(data, spec),
        eu7_ld.compute_gps_validity(data, spec),
        eu7_ld.compute_emissions_summary(data, spec),
    ]
    final_block = eu7_ld.compute_final_conformity(sections[-1], spec)

    visual = {
        "map": {"center": {"lat": 47.07, "lon": 15.44, "zoom": 10}, "latlngs": []},
        "chart": {"series": [], "labels": []},
    }
    kpis = [
        {"label": "NOx (mg/km)", "value": data["nox_mg_per_km"]},
        {"label": "PN (#/km)", "value": data["pn_per_km"]},
        {"label": "CO (mg/km)", "value": data["co_mg_per_km"]},
    ]

    payload = {
        "meta": {"legislation": spec.get("name", "EU7 Light-Duty"), "version": spec.get("version")},
        "sections": sections,
        "final": final_block,
        "visual": visual,
        "kpi_numbers": kpis,
    }

    payload = ensure_results_payload_defaults(payload)
    payload.setdefault("meta", {}).setdefault("legislation", spec.get("name", "EU7 Light-Duty"))
    payload.setdefault("meta", {}).setdefault("version", spec.get("version"))
    payload["chart"] = payload.get("visual", {}).get("chart")
    payload["map"] = payload.get("visual", {}).get("map")
    return payload


__all__ = ["build_results_payload", "load_spec", "render_report", "evaluate_eu7_ld"]
