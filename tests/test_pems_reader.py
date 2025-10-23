from __future__ import annotations

import pandas as pd
import pytest

from src.app.data.ingestion import PEMSReader


def test_pems_reader_converts_units(tmp_path):
    frame = pd.DataFrame(
        {
            "Time": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z"],
            "ExhFlow_g_s": [350.0, 360.0],
            "NOx_ug_s": [120_000.0, 140_000.0],
            "AmbientTemp_K": [300.0, 310.0],
        }
    )
    path = tmp_path / "pems.csv"
    frame.to_csv(path, index=False)

    columns = {
        "timestamp": "Time",
        "exhaust_flow_kg_s": "ExhFlow_g_s",
        "nox_mg_s": "NOx_ug_s",
        "amb_temp_c": "AmbientTemp_K",
    }
    units = {
        "exhaust_flow_kg_s": "g/s",
        "nox_mg_s": "ug/s",
        "amb_temp_c": "K",
    }

    normalized = PEMSReader.from_csv(str(path), columns=columns, units=units)

    assert list(normalized.columns) == [
        "timestamp",
        "exhaust_flow_kg_s",
        "nox_mg_s",
        "amb_temp_c",
    ]
    assert normalized["exhaust_flow_kg_s"].tolist() == pytest.approx([0.35, 0.36])
    assert normalized["nox_mg_s"].tolist() == pytest.approx([120.0, 140.0])
    assert normalized["amb_temp_c"].tolist() == pytest.approx([26.85, 36.85], rel=1e-4)


def test_pems_reader_rejects_ppm_to_massflow(tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z"],
            "exhaust_flow_kg_s": [0.35, 0.36],
            "NOx_ppm": [300.0, 310.0],
        }
    )
    path = tmp_path / "pems_ppm.csv"
    frame.to_csv(path, index=False)

    columns = {"nox_mg_s": "NOx_ppm"}
    units = {"nox_mg_s": "ppm"}

    with pytest.raises(ValueError) as excinfo:
        PEMSReader.from_csv(str(path), columns=columns, units=units)

    message = str(excinfo.value)
    assert "ppm" in message
    assert "temperature" in message
