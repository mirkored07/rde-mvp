"""Utilities for normalising results payload structures."""

from __future__ import annotations

from typing import Any, Mapping

__all__ = ["ensure_results_payload_defaults"]


def ensure_results_payload_defaults(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return a copy of ``payload`` with required UI defaults filled in."""

    if payload is None:
        p: dict[str, Any] = {}
    elif isinstance(payload, dict):
        p = dict(payload)
    else:
        p = dict(payload)

    # visual defaults
    visual_raw = p.get("visual") or {}
    if isinstance(visual_raw, dict):
        visual = dict(visual_raw)
    elif isinstance(visual_raw, Mapping):
        visual = dict(visual_raw)
    else:
        visual = {}
    visual.setdefault(
        "map",
        {"center": {"lat": 48.2082, "lon": 16.3738, "zoom": 8}, "latlngs": []},
    )
    visual.setdefault("chart", {"series": [], "labels": []})
    p["visual"] = visual

    # KPI defaults (support both keys but ensure kpi_numbers is present)
    kpi_numbers = p.get("kpi_numbers")
    if kpi_numbers is None and p.get("kpis") is not None:
        kpi_numbers = p["kpis"]

    if kpi_numbers is None:
        kpi_numbers_list: list[dict[str, Any]] = [
            {"label": "Trips", "value": 0},
            {"label": "Distance [km]", "value": 0},
            {"label": "Avg Speed [km/h]", "value": 0},
        ]
    elif isinstance(kpi_numbers, list):
        kpi_numbers_list = [dict(item) if isinstance(item, Mapping) else item for item in kpi_numbers]
    else:
        kpi_numbers_list = [kpi_numbers]

    p["kpi_numbers"] = kpi_numbers_list

    return p
