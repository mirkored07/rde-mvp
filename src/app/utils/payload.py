"""Utilities for normalising results payload structures."""

from __future__ import annotations

__all__ = ["ensure_results_payload_defaults"]


def ensure_results_payload_defaults(payload: dict | None) -> dict:
    p = dict(payload or {})

    visual = dict(p.get("visual") or {})
    visual.setdefault(
        "map",
        {
            "center": {"lat": 48.2082, "lon": 16.3738, "zoom": 8},
            "latlngs": [],
        },
    )
    visual.setdefault("chart", {"series": [], "labels": []})
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
