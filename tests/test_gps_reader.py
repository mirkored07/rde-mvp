from __future__ import annotations

import math
from decimal import Decimal

import pandas as pd
import pynmea2
import pytest

from src.app.data.ingestion.gps_reader import ORDERED, GPSReader


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def test_csv_normalization_and_anti_teleport(tmp_path):
    timestamps = [
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:00:01Z",
        "2024-01-01T00:00:02Z",
        "2024-01-01T00:00:03Z",
    ]
    data = pd.DataFrame(
        {
            "time": timestamps,
            "lat": [0.0, 0.0, 0.0, 0.0],
            "lon": [0.0, 0.0000899, 0.0001798, 1.0],
            "alt_m": [10.0, 10.5, 11.0, 12.0],
            "hdop": [0.7, 0.8, 0.9, 1.0],
        }
    )
    csv_path = tmp_path / "trace.csv"
    data.to_csv(csv_path, index=False)

    df = GPSReader.from_csv(str(csv_path))

    assert list(df.columns) == ORDERED
    assert str(df["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert df["timestamp"].is_monotonic_increasing

    expected_speed = _haversine(0.0, 0.0, 0.0, 0.0000899)
    for idx in range(3):
        assert df.loc[idx, "speed_m_s"] == pytest.approx(expected_speed, rel=1e-3)
    assert math.isnan(df.loc[3, "speed_m_s"])
    assert df["fix_ok"].tolist() == [True, True, True, False]
    assert df["alt_m"].tolist() == [10.0, 10.5, 11.0, 12.0]
    assert df["hdop"].tolist() == [0.7, 0.8, 0.9, 1.0]


def test_nmea_reader_parses_rmc(tmp_path):
    msg1 = pynmea2.RMC(
        "GP",
        "RMC",
        (
            "120000",
            "A",
            "4807.038",
            "N",
            "01131.000",
            "E",
            "22.4",
            "084.4",
            "010203",
            "",
            "",
            "A",
        ),
    )
    msg2 = pynmea2.RMC(
        "GP",
        "RMC",
        (
            "120001",
            "A",
            "4807.050",
            "N",
            "01131.100",
            "E",
            "10.0",
            "084.4",
            "010203",
            "",
            "",
            "A",
        ),
    )
    nmea_path = tmp_path / "track.nmea"
    nmea_path.write_text(f"{msg1}\n{msg2}\n", encoding="utf-8")

    df = GPSReader.from_nmea(str(nmea_path))

    assert len(df) == 2
    assert df["fix_ok"].all()
    assert df["hdop"].isna().all()
    assert df["alt_m"].isna().all()

    expected_lat = 48 + 7.038 / 60
    expected_lon = 11 + 31.0 / 60
    assert df.loc[0, "lat"] == pytest.approx(expected_lat, rel=1e-6)
    assert df.loc[0, "lon"] == pytest.approx(expected_lon, rel=1e-6)

    speed_knots = [Decimal("22.4"), Decimal("10.0")]
    speed_ms = [float(val * Decimal("0.514444")) for val in speed_knots]
    smoothed = sum(speed_ms) / len(speed_ms)
    assert df["speed_m_s"].tolist() == pytest.approx([smoothed, smoothed], rel=1e-9)


def test_gpx_reader_normalizes_points(tmp_path):
    gpx_content = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<gpx version=\"1.1\" creator=\"pytest\">
  <trk>
    <name>Example</name>
    <trkseg>
      <trkpt lat=\"48.0\" lon=\"11.0\">
        <ele>600.0</ele>
        <time>2024-01-01T00:00:00Z</time>
      </trkpt>
      <trkpt lat=\"48.0001\" lon=\"11.0001\">
        <ele>601.0</ele>
        <time>2024-01-01T00:00:10Z</time>
      </trkpt>
    </trkseg>
  </trk>
</gpx>
"""
    gpx_path = tmp_path / "track.gpx"
    gpx_path.write_text(gpx_content, encoding="utf-8")

    df = GPSReader.from_gpx(str(gpx_path))

    assert len(df) == 2
    assert df["fix_ok"].all()
    assert df["hdop"].isna().all()

    distance = _haversine(48.0, 11.0, 48.0001, 11.0001)
    expected_speed = distance / 10.0
    assert df["speed_m_s"].tolist() == pytest.approx([expected_speed, expected_speed], rel=1e-6)
    assert df["alt_m"].tolist() == pytest.approx([600.0, 601.0], rel=1e-6)
