"""Utilities for normalising results payload structures."""

from __future__ import annotations

__all__ = ["ensure_results_payload_defaults"]


def ensure_results_payload_defaults(payload: dict | None) -> dict:
    """Return a defensive copy of *payload* with visual/KPI defaults applied."""

    raw = payload
    if hasattr(raw, "model_dump") and callable(raw.model_dump):  # type: ignore[attr-defined]
        raw = raw.model_dump()  # type: ignore[assignment]
    elif hasattr(raw, "dict") and callable(raw.dict):  # Support Pydantic v1 style
        raw = raw.dict()  # type: ignore[assignment]

    p = dict(raw or {})

    visual = dict(p.get("visual") or {})

    default_map = {
        "center": {"lat": 48.2082, "lon": 16.3738, "zoom": 8},
        "latlngs": [],
    }
    map_payload = visual.get("map")
    if not isinstance(map_payload, dict):
        map_payload = {}
    merged_map = {**default_map, **map_payload}
    visual["map"] = merged_map

    chart_payload = visual.get("chart")
    if not isinstance(chart_payload, dict):
        chart_payload = {}
    visual["chart"] = {**{"series": [], "labels": []}, **chart_payload}

    p["visual"] = visual

    # Mirror legacy 'kpis' into 'kpi_numbers' if needed
    if p.get("kpi_numbers") is None and p.get("kpis") is not None:
        p["kpi_numbers"] = p["kpis"]

    if p.get("kpi_numbers") is None:
        p["kpi_numbers"] = [
            {"label": "Trips", "value": 0},
            {"label": "Distance [km]", "value": 0},
            {"label": "Avg Speed [km/h]", "value": 0},
        ]

    return p
