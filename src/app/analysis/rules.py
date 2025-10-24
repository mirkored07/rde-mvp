"""Helpers for loading analysis rules shared by UI and API layers."""

from __future__ import annotations

import functools
import pathlib
from typing import Any, Mapping

from src.app.data.analysis import AnalysisRules, load_rules

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
_DEFAULT_RULES_PATH = _PROJECT_ROOT / "data" / "rules" / "demo_rules.json"

_DEFAULT_RULES_CONFIG: Mapping[str, Any] = {
    "speed_bins": [
        {"name": "urban", "max_kmh": 60},
        {"name": "rural", "min_kmh": 60, "max_kmh": 90},
        {"name": "motorway", "min_kmh": 90},
    ],
    "min_distance_km_per_bin": 5.0,
    "min_time_s_per_bin": 300,
    "completeness": {"max_gap_s": 3},
    "kpi_defs": {
        "NOx_mg_per_km": {"numerator": "nox_mg_s", "denominator": "veh_speed_m_s"},
        "PN_1_per_km": {"numerator": "pn_1_s", "denominator": "veh_speed_m_s"},
    },
}


@functools.lru_cache(maxsize=1)
def load_analysis_rules() -> AnalysisRules:
    """Load analysis rules from the demo configuration or bundled defaults."""

    if _DEFAULT_RULES_PATH.exists():
        return load_rules(_DEFAULT_RULES_PATH)
    return load_rules(_DEFAULT_RULES_CONFIG)


__all__ = ["load_analysis_rules", "AnalysisRules"]
