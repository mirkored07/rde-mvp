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
    """Evaluate the EU7-LD report ensuring UI-friendly defaults."""

    spec_mapping = deepcopy(dict(load_spec("eu7_ld")))
    data = (
        dict(raw_inputs)
        if isinstance(raw_inputs, Mapping)
        else eu7_ld.build_default_inputs(spec_mapping)
    )

    payload = eu7_ld.build_report(data, spec_mapping)

    visual_block = payload.get("visual") if isinstance(payload.get("visual"), Mapping) else {}
    map_block = visual_block.get("map") if isinstance(visual_block, Mapping) else {}
    if not isinstance(map_block, Mapping):
        map_block = {}
    chart_block = (
        visual_block.get("chart")
        if isinstance(visual_block, Mapping)
        else {}
    )
    if not isinstance(chart_block, Mapping):
        chart_block = {}
    payload["visual"] = {"map": dict(map_block), "chart": dict(chart_block)}

    sections = payload.get("sections")
    if not isinstance(sections, list):
        sections = []
    ordered_titles = [
        "Pre/Post Checks (Zero/Span)",
        "Trip Composition & Timing",
        "Dynamics / MAW metrics",
        "GPS/Altitude validity",
        "Emissions Summary",
        "Final Conformity (overall PASS/FAIL)",
    ]
    block_lookup = {
        block.get("title"): block
        for block in sections
        if isinstance(block, Mapping)
    }
    ordered_sections: list[dict[str, Any]] = []
    for title in ordered_titles:
        block = block_lookup.get(title)
        if isinstance(block, Mapping):
            ordered_sections.append(dict(block))
    for block in sections:
        if not isinstance(block, Mapping):
            continue
        if block.get("title") in ordered_titles:
            continue
        ordered_sections.append(dict(block))
    payload["sections"] = ordered_sections

    if not isinstance(payload.get("kpi_numbers"), list):
        payload["kpi_numbers"] = []

    final_block = payload.get("final") if isinstance(payload.get("final"), Mapping) else {}
    payload["final"] = {
        "pass": bool(final_block.get("pass")),
        "pollutants": list(final_block.get("pollutants", [])),
        "notes": list(final_block.get("notes", [])),
        "label": final_block.get("label") or ("PASS" if final_block.get("pass") else "FAIL"),
    }

    meta = dict(payload.get("meta") or {})
    meta.setdefault("legislation", "EU7 Light-Duty")
    payload["meta"] = meta

    # Mirror top-level conveniences used by legacy UI helpers
    payload["map"] = payload.get("visual", {}).get("map", {})
    payload["chart"] = payload.get("visual", {}).get("chart", {})

    normalised = ensure_results_payload_defaults(payload)
    # restore fields that ensure_results_payload_defaults may normalise away
    normalised["meta"] = payload.get("meta", {})
    normalised["sections"] = payload.get("sections", [])
    normalised["final"] = payload.get("final", {})
    normalised["emissions"] = payload.get("emissions", {})
    normalised["id"] = payload.get("id")
    normalised["name"] = payload.get("name")
    normalised["version"] = payload.get("version")
    normalised["columns"] = payload.get("columns")
    normalised["values"] = payload.get("values")

    return normalised


__all__ = ["build_results_payload", "load_spec", "render_report", "evaluate_eu7_ld"]
