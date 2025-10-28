"""Generate aligned demo CSV files for the RDE UI."""
from __future__ import annotations

import math
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd

N_SAMPLES = 5400
PHASE_LENGTH = 1800
START_TIMESTAMP = "2024-01-01T08:00:00Z"

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "samples"

rng = np.random.default_rng(seed=42)


def generate_timeline() -> Tuple[pd.DatetimeIndex, np.ndarray]:
    """Create a 1 Hz timeline of UTC timestamps."""
    index = pd.date_range(start=START_TIMESTAMP, periods=N_SAMPLES, freq="S", tz="UTC")
    timestamps = index.strftime("%Y-%m-%dT%H:%M:%SZ").to_numpy()
    return index, timestamps


def _apply_stop_go(speed: np.ndarray, probability: float = 0.04) -> None:
    """Inject stop/go behaviour by setting random segments to near-zero speed."""
    i = 0
    size = speed.size
    while i < size:
        if rng.random() < probability:
            duration = int(rng.integers(5, 25))
            end = min(size, i + duration)
            speed[i:end] = np.maximum(0.0, speed[i:end] - rng.uniform(6.0, 12.0))
            i = end
        else:
            i += 1


def generate_speed_profile() -> np.ndarray:
    """Generate a smooth speed profile across urban, rural, and motorway phases."""
    t = np.arange(N_SAMPLES)
    speed = np.empty(N_SAMPLES)

    # Urban phase
    urban_idx = slice(0, PHASE_LENGTH)
    time_urban = t[urban_idx]
    urban_speed = (
        6.0
        + 4.0 * np.sin(time_urban / 45.0)
        + 3.0 * np.sin(time_urban / 12.0 + 1.5)
        + rng.normal(0, 2.5, size=time_urban.size)
    )
    _apply_stop_go(urban_speed)

    # Rural phase
    rural_idx = slice(PHASE_LENGTH, 2 * PHASE_LENGTH)
    time_rural = t[rural_idx]
    rural_speed = (
        20.0
        + 3.5 * np.sin(time_rural / 220.0)
        + 1.5 * np.sin(time_rural / 45.0)
        + rng.normal(0, 1.5, size=time_rural.size)
    )

    # Motorway phase
    motorway_idx = slice(2 * PHASE_LENGTH, N_SAMPLES)
    time_motorway = t[motorway_idx]
    motorway_speed = (
        30.0
        + 2.0 * np.sin(time_motorway / 360.0)
        + 1.0 * np.sin(time_motorway / 75.0 + 0.5)
        + rng.normal(0, 1.0, size=time_motorway.size)
    )

    speed[urban_idx] = urban_speed
    speed[rural_idx] = rural_speed
    speed[motorway_idx] = motorway_speed

    speed = np.clip(speed, 0.0, None)

    smoothed = pd.Series(speed).rolling(window=5, min_periods=1, center=True).mean().to_numpy()
    return smoothed


def generate_heading() -> np.ndarray:
    """Create a slowly drifting heading angle (radians)."""
    increments = rng.normal(0, 0.01, size=N_SAMPLES)
    base_heading = rng.uniform(0, 2 * math.pi)
    heading = np.cumsum(increments) + base_heading
    return heading


def integrate_gps(speed_m_s: np.ndarray, heading: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    lat = np.empty(N_SAMPLES)
    lon = np.empty(N_SAMPLES)
    alt = 35.0 + rng.normal(0, 1.5, size=N_SAMPLES)

    lat[0] = 48.8566
    lon[0] = 2.3522

    for i in range(1, N_SAMPLES):
        distance = speed_m_s[i - 1]  # meters in the previous second
        lat_rad = math.radians(lat[i - 1])
        lat[i] = lat[i - 1] + (distance * math.cos(heading[i - 1])) / 111_111
        denom = max(1e-6, 111_111 * math.cos(lat_rad))
        lon[i] = lon[i - 1] + (distance * math.sin(heading[i - 1])) / denom

    return lat, lon, alt


def compute_accel(speed_m_s: np.ndarray) -> np.ndarray:
    diff = np.diff(speed_m_s, prepend=speed_m_s[0])
    accel = np.maximum(diff, 0.0)
    return accel


def generate_pems_signals(speed_m_s: np.ndarray, accel: np.ndarray) -> pd.DataFrame:
    load_factor = 0.05 * speed_m_s + 0.6 * accel
    exhaust_flow = 0.3 + 0.02 * speed_m_s + rng.normal(0, 0.01, size=N_SAMPLES)
    nox = 50.0 + 2.5 * speed_m_s + 8.0 * load_factor + rng.normal(0, 5.0, size=N_SAMPLES)
    pn = 1e5 + 8e4 * load_factor + 5e4 * accel + rng.normal(0, 1e4, size=N_SAMPLES)

    df = pd.DataFrame(
        {
            "exhaust_flow_kg_s": np.clip(exhaust_flow, 0.0, None),
            "nox_mg_s": np.clip(nox, 0.0, None),
            "pn_1_s": np.clip(pn, 0.0, None),
            "veh_speed_m_s": speed_m_s,
        }
    )
    return df


def generate_ecu_signals(speed_m_s: np.ndarray, accel: np.ndarray) -> pd.DataFrame:
    engine_speed = 800.0 + 55.0 * speed_m_s + 120.0 * accel + rng.normal(0, 40.0, size=N_SAMPLES)
    engine_speed = np.clip(engine_speed, 650.0, 3200.0)

    engine_load = 20.0 + 1.6 * speed_m_s + 18.0 * accel + rng.normal(0, 5.0, size=N_SAMPLES)
    engine_load = np.clip(engine_load, 5.0, 100.0)

    throttle = 8.0 + 1.2 * speed_m_s + 25.0 * accel + rng.normal(0, 4.0, size=N_SAMPLES)
    throttle = np.clip(throttle, 0.0, 100.0)

    df = pd.DataFrame(
        {
            "veh_speed_m_s": speed_m_s,
            "engine_speed_rpm": engine_speed,
            "engine_load_pct": engine_load,
            "throttle_pct": throttle,
        }
    )
    return df


def write_demo_data() -> None:
    _, timestamps = generate_timeline()
    speed_m_s = generate_speed_profile()
    heading = generate_heading()
    lat, lon, alt = integrate_gps(speed_m_s, heading)
    accel = compute_accel(speed_m_s)

    df_gps = pd.DataFrame(
        {
            "timestamp": timestamps,
            "lat": np.round(lat, 6),
            "lon": np.round(lon, 6),
            "alt_m": np.round(alt, 3),
            "speed_m_s": np.round(speed_m_s, 3),
        }
    )

    df_pems = generate_pems_signals(speed_m_s, accel)
    df_pems.insert(0, "timestamp", timestamps)
    df_pems["exhaust_flow_kg_s"] = df_pems["exhaust_flow_kg_s"].round(4)
    df_pems["nox_mg_s"] = df_pems["nox_mg_s"].round(2)
    df_pems["pn_1_s"] = df_pems["pn_1_s"].round().astype(int)
    df_pems["veh_speed_m_s"] = df_pems["veh_speed_m_s"].round(3)

    df_ecu = generate_ecu_signals(speed_m_s, accel)
    df_ecu.insert(0, "timestamp", timestamps)
    df_ecu["veh_speed_m_s"] = df_ecu["veh_speed_m_s"].round(3)
    df_ecu["engine_speed_rpm"] = df_ecu["engine_speed_rpm"].round(1)
    df_ecu["engine_load_pct"] = df_ecu["engine_load_pct"].round(1)
    df_ecu["throttle_pct"] = df_ecu["throttle_pct"].round(1)

    # UNCOMMENT TO TEST GAP HANDLING (misaligned timestamps)
    # df_ecu = df_ecu.drop(df_ecu.index[1234]).reset_index(drop=True)

    assert len(df_pems) == len(df_gps) == len(df_ecu)
    assert np.array_equal(df_pems["timestamp"].to_numpy(), df_gps["timestamp"].to_numpy())
    assert np.array_equal(df_pems["timestamp"].to_numpy(), df_ecu["timestamp"].to_numpy())

    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    df_pems.to_csv(SAMPLES_DIR / "pems_demo.csv", index=False)
    df_gps.to_csv(SAMPLES_DIR / "gps_demo.csv", index=False)
    df_ecu.to_csv(SAMPLES_DIR / "ecu_demo.csv", index=False)

    report_statistics(speed_m_s)
    print("Wrote:")
    print(f"  {SAMPLES_DIR / 'pems_demo.csv'}")
    print(f"  {SAMPLES_DIR / 'gps_demo.csv'}")
    print(f"  {SAMPLES_DIR / 'ecu_demo.csv'}")
    print("Alignment OK.")


def report_statistics(speed_m_s: np.ndarray) -> None:
    total_duration_s = N_SAMPLES
    total_minutes = total_duration_s / 60.0
    print(f"Generated {N_SAMPLES} samples ({total_minutes:.1f} min @ 1 Hz)")

    phases = [
        ("Urban", slice(0, PHASE_LENGTH)),
        ("Rural", slice(PHASE_LENGTH, 2 * PHASE_LENGTH)),
        ("Motorway", slice(2 * PHASE_LENGTH, N_SAMPLES)),
    ]

    for name, s in phases:
        segment = speed_m_s[s]
        duration_s = segment.size
        duration_min = duration_s / 60.0
        distance_km = segment.sum() / 1000.0
        avg_kmh = segment.mean() * 3.6
        print(f"{name}: {duration_min:.1f} min, dist ~{distance_km:.1f} km, avg ~{avg_kmh:.1f} km/h")


def main() -> None:
    write_demo_data()


if __name__ == "__main__":
    main()
