import pandas as pd

from src.app.quality import run_diagnostics


def _find_check(diagnostics, check_id):
    return next((check for check in diagnostics.checks if check.id == check_id), None)


def test_gap_detection_and_repair():
    fused = pd.DataFrame(
        {
            "timestamp": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:00:01Z",
                "2024-01-01T00:00:04Z",
            ],
            "veh_speed_m_s": [10.0, 10.5, 11.0],
        }
    )

    repaired, diagnostics = run_diagnostics(
        fused,
        {},
        gap_threshold_s=2.0,
        repair_small_gaps=True,
        repair_threshold_s=3.0,
    )

    gap_check = _find_check(diagnostics, "fused_gaps")
    assert gap_check is not None
    assert gap_check.level == "warn"
    assert diagnostics.repaired_spans, "expected repaired span for small gap"
    assert len(repaired) > len(fused), "repaired dataframe should include inserted rows"


def test_gps_jump_detection():
    fused = pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z"],
            "lat": [0.0, 0.0],
            "lon": [0.0, 0.002],
        }
    )

    _, diagnostics = run_diagnostics(
        fused,
        {},
        repair_small_gaps=False,
        gps_teleport_m=100.0,
    )

    jump_check = _find_check(diagnostics, "fused_gps_teleport")
    assert jump_check is not None
    assert jump_check.level == "warn"
    assert jump_check.count >= 1


def test_speed_spike_detection():
    fused = pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z"],
            "veh_speed_m_s": [10.0, 80.0],
        }
    )

    _, diagnostics = run_diagnostics(
        fused,
        {},
        repair_small_gaps=False,
        speed_spike_ms=65.0,
    )

    spike_check = _find_check(diagnostics, "fused_speed_spikes")
    assert spike_check is not None
    assert spike_check.level == "warn"
    assert spike_check.count == 1
