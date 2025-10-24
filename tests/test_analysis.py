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


def _build_full_pollutant_df() -> pd.DataFrame:
    base = pd.Timestamp("2024-02-01T12:00:00Z")
    timestamps = base + pd.to_timedelta(np.arange(6), unit="s")
    speed_m_s = np.array([10.0, 10.0, 10.0, 25.0, 25.0, 25.0])

    data = {
        "timestamp": timestamps,
        "veh_speed_m_s": speed_m_s,
        "nox_mg_s": np.full(6, 100.0),
        "pn_1_s": np.full(6, 200.0),
        "co_mg_s": np.full(6, 50.0),
        "co2_g_s": np.full(6, 2.0),
        "thc_mg_s": np.full(6, 5.0),
        "nh3_mg_s": np.full(6, 1.0),
        "n2o_mg_s": np.full(6, 0.8),
        "pm_mg_s": np.full(6, 0.2),
    }

    return pd.DataFrame(data)


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

    kpis = analysis.get("kpis") or {}
    assert "NOx_mg_per_km" in kpis
    nox_total = kpis["NOx_mg_per_km"]["total"]["value"]
    pn_total = kpis["PN_1_per_km"]["total"]["value"]

    df = _build_sample_df()
    dt = df["timestamp"].diff().dt.total_seconds().fillna(0.0)
    distance_km = (df["veh_speed_m_s"] * dt).sum() / 1000.0
    expected_nox_total = (df["nox_mg_s"] * dt).sum() / distance_km
    expected_pn_total = (df["pn_1_s"] * dt).sum() / distance_km

    assert nox_total == pytest.approx(expected_nox_total, rel=1e-6)
    assert pn_total == pytest.approx(expected_pn_total, rel=1e-6)
    assert analysis["overall"]["kpis"]["NOx_mg_per_km"] == pytest.approx(expected_nox_total, rel=1e-6)

    assert "Overall status" in result.summary_md
    assert "KPIs" in result.summary_md


def test_analysis_engine_normalizes_kpi_units() -> None:
    rules = load_rules(
        {
            "speed_bins": [
                {"name": "urban", "max_kmh": 60},
                {"name": "rural", "min_kmh": 60, "max_kmh": 90},
                {"name": "motorway", "min_kmh": 90},
            ],
        }
    )

    df = _build_sample_df()
    df["nox_mg_s"] = df["nox_mg_s"] / 1000.0
    df.attrs = {"units": {"nox_mg_s": "g/s"}}

    engine = AnalysisEngine(rules)
    result = engine.analyze(df)

    bins = result.analysis["bins"]
    urban_kpi = bins["urban"]["kpis"]["NOx_mg_per_km"]
    assert urban_kpi == pytest.approx(10000.0, rel=1e-3)


def test_analysis_engine_computes_full_pollutant_kpis() -> None:
    rules = load_rules(
        {
            "speed_bins": [
                {"name": "urban", "max_kmh": 60},
                {"name": "rural", "min_kmh": 60, "max_kmh": 90},
                {"name": "motorway", "min_kmh": 90},
            ],
        }
    )

    df = _build_full_pollutant_df()
    engine = AnalysisEngine(rules)
    result = engine.analyze(df)

    analysis = result.analysis
    kpis = analysis.get("kpis") or {}

    expected_totals = {
        "NOx_mg_per_km": 5263.1578947368425,
        "PN_1_per_km": 10526.315789473685,
        "CO_mg_per_km": 2631.5789473684213,
        "CO2_g_per_km": 105.26315789473685,
        "THC_mg_per_km": 263.1578947368421,
        "NH3_mg_per_km": 52.631578947368425,
        "N2O_mg_per_km": 42.10526315789474,
        "PM_mg_per_km": 10.526315789473685,
    }

    expected_urban = {
        "NOx_mg_per_km": 10000.0,
        "PN_1_per_km": 20000.0,
        "CO_mg_per_km": 5000.0,
        "CO2_g_per_km": 200.0,
        "THC_mg_per_km": 500.0,
        "NH3_mg_per_km": 100.0,
        "N2O_mg_per_km": 80.0,
        "PM_mg_per_km": 20.0,
    }

    expected_motorway = {
        "NOx_mg_per_km": 4000.0,
        "PN_1_per_km": 8000.0,
        "CO_mg_per_km": 2000.0,
        "CO2_g_per_km": 80.0,
        "THC_mg_per_km": 200.0,
        "NH3_mg_per_km": 40.0,
        "N2O_mg_per_km": 32.0,
        "PM_mg_per_km": 8.0,
    }

    for key, expected in expected_totals.items():
        assert key in kpis
        total_entry = kpis[key]["total"]["value"]
        assert total_entry == pytest.approx(expected, rel=1e-6)
        overall_value = analysis["overall"]["kpis"][key]
        assert overall_value == pytest.approx(expected, rel=1e-6)

    bins = analysis["bins"]
    for key, expected in expected_urban.items():
        value = bins["urban"]["kpis"][key]
        assert value == pytest.approx(expected, rel=1e-6)

    for key, expected in expected_motorway.items():
        value = bins["motorway"]["kpis"][key]
        assert value == pytest.approx(expected, rel=1e-6)


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
