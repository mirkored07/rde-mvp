from __future__ import annotations

from typing import Any, Mapping

from src.app.regulation.eu7ld_un168_limits import (
    NOX_RDE_FINAL_MG_PER_KM,
    PN10_RDE_FINAL_PER_KM,
    VA_POS95_ALT_HIGH_SPEED_OFFSET,
    VA_POS95_ALT_HIGH_SPEED_SLOPE,
    VA_POS95_HIGH_SPEED_OFFSET,
    VA_POS95_HIGH_SPEED_SLOPE,
    VA_POS95_LOW_SPEED_OFFSET,
    VA_POS95_LOW_SPEED_SLOPE,
    VA_POS95_BREAK_KMH,
    RPA_LOW_SPEED_OFFSET,
    RPA_LOW_SPEED_SLOPE,
)
from src.app.rules.engine import evaluate_eu7_ld


def _payload(overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return evaluate_eu7_ld(dict(overrides or {}))


def _criteria_map(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for row in payload.get("criteria", []):
        mapping[row["id"]] = row
    return mapping


def test_evaluate_eu7_ld_default_payload() -> None:
    payload = _payload()

    criteria = payload.get("criteria")
    assert isinstance(criteria, list)
    assert len(criteria) >= 48
    assert all(
        isinstance(row.get("value"), (int, float)) or row.get("value") is None
        for row in criteria
    )

    emissions = payload.get("emissions")
    assert set(emissions.keys()) == {"urban", "rural", "motorway", "trip"}

    limits = payload.get("limits")
    assert limits["NOx_mg_km_RDE"] == NOX_RDE_FINAL_MG_PER_KM
    assert limits["PN10_hash_km_RDE"] == PN10_RDE_FINAL_PER_KM

    final_block = payload.get("final")
    assert isinstance(final_block, dict) and isinstance(final_block.get("pass"), bool)
    assert final_block["pass"] is True

    final_conformity = payload.get("final_conformity")
    assert final_conformity["NOx_mg_km"]["pass"] is True
    assert final_conformity["PN10_hash_km"]["pass"] is True

    sections = payload.get("sections")
    titles = [section["title"] for section in sections]
    assert titles == [
        "Pre/Post Checks (Zero/Span)",
        "Span Gas Coverage",
        "Trip Composition & Timing",
        "Cold-Start Window",
        "GPS Validity",
        "Dynamics & MAW",
        "CO₂ Characteristic Windows (MAW)",
        "Emissions Summary",
        "Final Conformity",
    ]


def test_trip_segment_constraints_fail_when_values_out_of_range() -> None:
    payload = _payload(
        {
            "phase_sequence": ["urban", "motorway", "rural"],
            "phases": {
                "urban": {"distance_km": 12.0},
                "motorway": {"distance_km": 10.0},
                "rural": {"distance_km": 0.0},
            },
            "trip": {
                "duration_s": 4800.0,
                "start_end_elev_delta_m": 150.0,
                "cum_pos_elev_m": 2000.0,
            },
        }
    )
    crit = _criteria_map(payload)
    assert crit["phase_order"]["result"] == "fail"
    assert crit["urban_distance"]["result"] == "fail"
    assert crit["motorway_distance"]["result"] == "fail"
    assert crit["rural_presence"]["result"] == "fail"
    assert crit["trip_duration"]["result"] == "fail"
    assert crit["elev_delta"]["result"] == "fail"
    assert crit["elev_trip"]["result"] == "fail"
    assert crit["elev_urban"]["result"] == "fail"


def test_dynamics_thresholds_cover_all_branches() -> None:
    urban_limit = VA_POS95_LOW_SPEED_SLOPE * 70.0 + VA_POS95_LOW_SPEED_OFFSET
    rural_rpa_limit = RPA_LOW_SPEED_SLOPE * 60.0 + RPA_LOW_SPEED_OFFSET
    motorway_limit = VA_POS95_HIGH_SPEED_SLOPE * 110.0 + VA_POS95_HIGH_SPEED_OFFSET

    payload = _payload(
        {
            "phases": {
                "urban": {"avg_speed_kmh": 70.0, "va_pos95": urban_limit + 0.5},
                "rural": {"avg_speed_kmh": 60.0, "rpa": rural_rpa_limit - 0.05},
                "motorway": {
                    "avg_speed_kmh": 110.0,
                    "va_pos95": motorway_limit + 0.5,
                    "rpa": 0.02,
                    "dynamic_events": 90,
                },
            },
        }
    )
    crit = _criteria_map(payload)
    assert crit["va95_u"]["result"] == "fail"
    assert crit["rpa_r"]["result"] == "fail"
    assert crit["va95_m"]["result"] == "fail"
    assert crit["rpa_m"]["result"] == "fail"
    assert crit["dyn_m"]["result"] == "fail"

    # Alt line (≤44 W/kg) applies a stricter slope; verify failure when enabled.
    high_speed = VA_POS95_BREAK_KMH + 20.0
    default_limit = VA_POS95_HIGH_SPEED_SLOPE * high_speed + VA_POS95_HIGH_SPEED_OFFSET
    alt_limit = VA_POS95_ALT_HIGH_SPEED_SLOPE * high_speed + VA_POS95_ALT_HIGH_SPEED_OFFSET
    assert alt_limit < default_limit
    payload_alt = _payload(
        {
            "low_power_vehicle": True,
            "phases": {
                "motorway": {
                    "avg_speed_kmh": high_speed,
                    "va_pos95": (alt_limit + default_limit) / 2.0,
                }
            },
        }
    )
    crit_alt = _criteria_map(payload_alt)
    assert crit_alt["va95_m"]["result"] == "fail"


def test_maw_coverage_and_final_limits_drive_failures() -> None:
    payload = _payload(
        {
            "maw_windows": {
                "low": [
                    {"distance_km": 12.0, "nox_mg": 600.0, "pn_count": 5.0e12, "valid": False},
                    {"distance_km": 5.0, "nox_mg": 600.0, "pn_count": 8.0e12, "valid": True},
                ],
                "high": [
                    {"distance_km": 8.0, "nox_mg": 480.0, "pn_count": 6.0e12, "valid": True},
                ],
            }
        }
    )
    crit = _criteria_map(payload)
    assert crit["maw_cov_low"]["result"] == "fail"
    assert crit["final_nox"]["result"] == "fail"
    assert crit["final_pn"]["result"] == "fail"

    final_block = payload["final"]
    assert final_block["pass"] is False
    assert payload["final_conformity"]["NOx_mg_km"]["pass"] is False
    assert payload["final_conformity"]["PN10_hash_km"]["pass"] is False


def test_payload_emissions_and_values_numeric() -> None:
    payload = _payload()
    emissions = payload["emissions"]
    for phase in ("urban", "rural", "motorway", "trip"):
        block = emissions[phase]
        assert set(block.keys()) == {"label", "NOx_mg_km", "PN10_hash_km", "CO_mg_km"}
        assert isinstance(block["NOx_mg_km"], float)
        assert isinstance(block["PN10_hash_km"], float)
        assert isinstance(block["CO_mg_km"], float)

    values = [row["value"] for row in payload["criteria"] if row["value"] is not None]
    assert all(isinstance(value, (int, float)) for value in values)

