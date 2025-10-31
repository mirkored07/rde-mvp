from __future__ import annotations

import copy
from typing import Any

from src.app.rules import build_results_payload, load_spec
from src.app.rules.eu7_ld import build_default_inputs


def _stubbed_spec() -> dict[str, Any]:
    spec = copy.deepcopy(dict(load_spec()))
    limits = spec.setdefault("limits", {})
    limits.update(
        {
            "nox_mg_per_km": 80.0,
            "pn_per_km": 6.0e11,
            "co_mg_per_km": 1000.0,
            "thc_mg_per_km": 50.0,
            "nh3_mg_per_km": 20.0,
        }
    )

    trip = spec.setdefault("trip_composition", {})
    trip.update(
        {
            "urban_min_km": 10.0,
            "expressway_min_km": 10.0,
            "share_urban_percent_range": [30.0, 55.0],
            "share_expressway_percent_range": [30.0, 60.0],
            "duration_minutes_range": [60.0, 120.0],
            "start_urban": True,
        }
    )

    dynamics = spec.setdefault("dynamics", {})
    dynamics.update(
        {
            "urban_avg_speed_range_kmh": [25.0, 35.0],
            "stop_time_share_urban_percent_range": [10.0, 35.0],
            "va_pos95_limits": {"urban": 2.0, "expressway": 2.2},
            "rpa_min": {"urban": 0.15, "expressway": 0.12},
            "maw": {
                "low_speed_tolerance_percent": 85.0,
                "high_speed_tolerance_percent": 80.0,
                "min_valid_low_speed_windows_percent": 50.0,
                "min_valid_high_speed_windows_percent": 50.0,
            },
        }
    )

    gps = spec.setdefault("gps", {})
    gps.update(
        {
            "max_loss_s": 10.0,
            "total_loss_s": 30.0,
            "max_distance_dev_percent": 5.0,
        }
    )

    corrections = spec.setdefault("corrections", {})
    corrections.setdefault("extc", {})["factor"] = 1.1
    corrections.setdefault("ki", {})["enabled"] = True

    return spec


def test_eu7_ld_payload_sections_and_final_pass() -> None:
    spec = _stubbed_spec()
    sample_inputs = build_default_inputs(spec)
    payload = build_results_payload("eu7_ld", sample_inputs, spec_override=spec)

    assert isinstance(payload.get("kpi_numbers"), list)
    assert payload["kpi_numbers"]

    sections = payload.get("sections") or []
    assert sections, "sections missing from payload"
    for section in sections:
        criteria = section.get("criteria") or []
        assert criteria, f"section {section.get('title')} is empty"
        for row in criteria:
            assert isinstance(row.get("pass"), bool)

    final_block = payload.get("final") or {}
    assert final_block.get("pass") is True
    pollutants = final_block.get("pollutants") or []
    assert pollutants
    assert all("pass" in entry for entry in pollutants)

    visual = payload.get("visual") or {}
    assert visual.get("map") is not None
    assert visual.get("chart") is not None

    # Ensure defaults also mirror onto top-level helpers used by templates.
    assert payload.get("map") is not None
    assert payload.get("chart") is not None


def test_eu7_ld_payload_detects_failures() -> None:
    spec = _stubbed_spec()
    failing_inputs = build_default_inputs(spec)
    failing_inputs = copy.deepcopy(failing_inputs)
    failing_inputs["emissions"]["pn_per_km"]["value"] = 8.0e11
    failing_inputs["zero_span"]["pn_zero_max_per_cm3"]["value"] = 9000

    payload = build_results_payload("eu7_ld", failing_inputs, spec_override=spec)
    final_block = payload.get("final") or {}
    assert final_block.get("pass") is False

    failing_rows = [
        row
        for section in payload.get("sections", [])
        for row in section.get("criteria", [])
        if isinstance(row, dict)
    ]
    assert any(not row.get("pass") for row in failing_rows)

    # Visual defaults should still be present when failures occur.
    assert payload.get("visual", {}).get("map") is not None
    assert payload.get("visual", {}).get("chart") is not None
