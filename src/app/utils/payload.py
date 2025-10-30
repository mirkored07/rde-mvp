"""Utilities for normalising results payload structures."""

from __future__ import annotations

from typing import Any, Mapping

__all__ = ["ensure_results_payload_defaults"]


def ensure_results_payload_defaults(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return a copy of ``payload`` with required UI defaults filled in."""

    if payload is None:
        p: dict[str, Any] = {}
    elif isinstance(payload, dict):
        p = payload
    else:
        p = dict(payload)

    # visual defaults
    visual = p.get("visual") or {}
    if not isinstance(visual, dict):
        visual = dict(visual) if isinstance(visual, Mapping) else {}
    visual.setdefault(
        "map",
        {"center": {"lat": 48.2082, "lon": 16.3738, "zoom": 8}, "latlngs": []},
    )
    visual.setdefault("chart", {"series": [], "labels": []})
    p["visual"] = visual

    # KPI defaults (support both keys but ensure kpi_numbers is present)
    if p.get("kpi_numbers") is None and p.get("kpis") is not None:
        p["kpi_numbers"] = p["kpis"]

    if p.get("kpi_numbers") is None:
        p["kpi_numbers"] = [
            {"label": "Trips", "value": 0},
            {"label": "Distance [km]", "value": 0},
            {"label": "Avg Speed [km/h]", "value": 0},
        ]

    return p
