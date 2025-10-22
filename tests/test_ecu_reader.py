from __future__ import annotations

import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from src.app.data.ingestion.ecu_reader import ECUReader


def test_csv_normalization_orders_and_types(tmp_path):
    data = pd.DataFrame(
        {
            "time": [
                "2024-01-01T00:00:02Z",
                "2024-01-01T00:00:00",
                "2024-01-01T01:00:01+01:00",
            ],
            "vehicle_speed": [20.0, "5", "15.5"],
            "engine_speed": ["2500", "1500", "1750"],
            "load_pct": ["75", 65, "70"],
            "ignored": [1, 2, 3],
        }
    )
    csv_path = tmp_path / "ecu.csv"
    data.to_csv(csv_path, index=False)

    mapping = {
        "timestamp": "time",
        "veh_speed_m_s": "vehicle_speed",
        "engine_speed_rpm": "engine_speed",
        "engine_load_pct": "load_pct",
    }

    df = ECUReader.from_csv(str(csv_path), mapping=mapping)

    assert df.columns.tolist() == ["timestamp", "veh_speed_m_s", "engine_speed_rpm", "engine_load_pct"]
    assert str(df["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert df["timestamp"].is_monotonic_increasing
    assert df["veh_speed_m_s"].tolist() == pytest.approx([5.0, 15.5, 20.0])
    assert df["engine_speed_rpm"].tolist() == pytest.approx([1500.0, 1750.0, 2500.0])
    assert df["engine_load_pct"].tolist() == pytest.approx([65.0, 70.0, 75.0])


def test_mdf_requires_optional_dependency(monkeypatch):
    with monkeypatch.context() as m:
        m.setitem(sys.modules, "asammdf", None)
        with pytest.raises(ImportError):
            ECUReader.from_mdf("dummy.mf4")


def test_mdf_normalization_when_dependency_available(monkeypatch):
    asammdf = pytest.importorskip("asammdf")

    class DummyMDF:
        def __init__(self, path: str):
            self.path = path
            self.header = SimpleNamespace(start_time=pd.Timestamp("2024-01-01T00:00:00Z").to_pydatetime())

        def to_dataframe(self, time_from_zero: bool = False):
            return pd.DataFrame(
                {
                    "time": [0.0, 1.0, 2.0],
                    "veh_speed": [0.0, 1.5, 2.5],
                    "rpm": [1000, 1100, 1200],
                }
            )

        def close(self):
            self.closed = True

    monkeypatch.setattr(asammdf, "MDF", DummyMDF)

    mapping = {
        "timestamp": "time",
        "veh_speed_m_s": "veh_speed",
        "engine_speed_rpm": "rpm",
    }

    df = ECUReader.from_mdf("dummy.mf4", mapping=mapping)

    assert df.columns.tolist() == ["timestamp", "veh_speed_m_s", "engine_speed_rpm"]
    assert df["timestamp"].is_monotonic_increasing
    assert df["veh_speed_m_s"].tolist() == pytest.approx([0.0, 1.5, 2.5])
    assert df["engine_speed_rpm"].tolist() == pytest.approx([1000.0, 1100.0, 1200.0])
