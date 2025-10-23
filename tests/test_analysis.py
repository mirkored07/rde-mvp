from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pathlib import Path

from app.data.analysis import AnalysisEngine, load_rules


def _build_sample_df() -> pd.DataFrame:
    base = pd.Timestamp("2024-01-01T00:00:00Z")
    timestamps = base + pd.to_timedelta(np.arange(9), unit="s")
    speed_m_s = np.array([10.0] * 3 + [20.0] * 3 + [28.0] * 3)
    nox_mg_s = np.full_like(speed_m_s, 100.0)
    pn_1_s = np.full_like(speed_m_s, 200.0)
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "veh_speed_m_s": speed_m_s,
            "nox_mg_s": nox_mg_s,
            "pn_1_s": pn_1_s,
        }
    )


def test_analysis_engine_computes_kpis_and_summary() -> None:
    rules = load_rules(
        {
            "speed_bins": [
                {"name": "urban", "max_kmh": 60},
                {"name": "rural", "min_kmh": 60, "max_kmh": 90},
                {"name": "motorway", "min_kmh": 90},
            ],
            "min_distance_km_per_bin": 0.01,
            "min_time_s_per_bin": 2,
            "completeness": {"max_gap_s": 3},
            "kpi_defs": {
                "NOx_mg_per_km": {
                    "numerator": "nox_mg_s",
                    "denominator": "veh_speed_m_s",
                },
                "PN_1_per_km": {
                    "numerator": "pn_1_s",
                    "denominator": "veh_speed_m_s",
                },
            },
        }
    )

    df = _build_sample_df()
    engine = AnalysisEngine(rules)
    result = engine.analyze(df)

    derived = result.derived
    assert "cumulative_distance_km" in derived.columns
    assert "rolling_mean_speed_m_s" in derived.columns
    assert "acceleration_m_s2" in derived.columns
    assert derived["bin_mask__urban"].sum() == 3
    assert derived["bin_mask__motorway"].sum() == 3

    analysis = result.analysis
    assert analysis["overall"]["valid"] is True
    bins = analysis["bins"]
    assert bins["urban"]["valid"] is True
    assert bins["motorway"]["valid"] is True

    urban_kpi = bins["urban"]["kpis"]["NOx_mg_per_km"]
    assert urban_kpi == pytest.approx(10000.0, rel=1e-3)

    assert "Overall status" in result.summary_md
    assert "KPIs" in result.summary_md


def test_load_rules_from_json(tmp_path: Path) -> None:
    config = tmp_path / "rules.json"
    config.write_text(
        """
{
  \"speed_bins\": [{\"name\": \"demo\", \"max_kmh\": 50}],
  \"completeness\": {\"max_gap_s\": 2}
}
""".strip()
    )
    rules = load_rules(config)
    assert rules.speed_bins[0].name == "demo"
    assert rules.completeness_max_gap_s == 2
