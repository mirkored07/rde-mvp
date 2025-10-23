"""Unit tests for the fusion helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from app.data.fusion import (
    FusionEngine,
    StreamSpec,
    estimate_offset_by_correlation,
    synthesize_timestamps,
)


def test_synthesize_timestamps_from_counter() -> None:
    df = pd.DataFrame({"sample": range(5)})
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    spec = StreamSpec(
        df=df,
        counter_col="sample",
        rate_hz=2.0,
        t0=start,
        clock_offset_s=0.5,
        name="ecu",
    )

    ts = synthesize_timestamps(spec)

    expected = pd.Series(
        start + pd.to_timedelta(np.arange(5) / 2.0 + 0.5, unit="s"),
        name=ts.name,
    )
    pd.testing.assert_series_equal(ts.reset_index(drop=True), expected.reset_index(drop=True))


def test_synthesize_timestamps_accepts_dataframe() -> None:
    timeline = [
        pd.Timestamp("2023-01-01T00:00:00Z"),
        pd.Timestamp("2023-01-01T00:00:01Z"),
    ]
    df = pd.DataFrame({"timestamp": timeline, "speed_m_s": [10.0, 10.5]})

    ts = synthesize_timestamps(df)

    expected = pd.Series(timeline, name="timestamp", dtype="datetime64[ns, UTC]")
    pd.testing.assert_series_equal(ts.reset_index(drop=True), expected.reset_index(drop=True))


def test_estimate_offset_by_correlation() -> None:
    base = pd.Timestamp("2023-01-01T00:00:00Z")
    samples = 200
    dt = 0.1
    timeline = base + pd.to_timedelta(np.arange(samples) * dt, unit="s")
    freq = 0.25
    time_s = np.arange(samples) * dt
    signal = np.sin(2 * np.pi * freq * time_s)

    gps_df = pd.DataFrame({"timestamp": timeline, "speed_m_s": signal})
    gps_spec = StreamSpec(df=gps_df, ts_col="timestamp", name="gps")

    offset = 0.3
    shifted_signal = np.sin(2 * np.pi * freq * (time_s - offset))
    ecu_df = pd.DataFrame({
        "timestamp": timeline,
        "veh_speed_m_s": shifted_signal,
    })
    ecu_spec = StreamSpec(df=ecu_df, ts_col="timestamp", name="ecu")

    estimated = estimate_offset_by_correlation(
        ecu_spec,
        gps_spec,
        cols=("veh_speed_m_s", "speed_m_s"),
        grid_hz=10.0,
        max_lag_s=1.0,
    )

    assert estimated == pytest.approx(offset, abs=0.11)


def test_estimate_offset_accepts_dataframe_inputs() -> None:
    base = pd.Timestamp("2023-01-01T00:00:00Z")
    samples = 200
    dt = 0.1
    timeline = base + pd.to_timedelta(np.arange(samples) * dt, unit="s")
    freq = 0.25
    time_s = np.arange(samples) * dt
    signal = np.sin(2 * np.pi * freq * time_s)

    gps_df = pd.DataFrame({"timestamp": timeline, "speed_m_s": signal})

    offset = 0.3
    shifted_signal = np.sin(2 * np.pi * freq * (time_s - offset))
    ecu_df = pd.DataFrame({
        "timestamp": timeline,
        "veh_speed_m_s": shifted_signal,
    })

    estimated = estimate_offset_by_correlation(
        ecu_df,
        gps_df,
        cols=(["veh_speed_m_s"], ["speed_m_s"]),
        grid_hz=10.0,
        max_lag_s=1.0,
    )

    assert estimated == pytest.approx(offset, abs=0.11)


def test_fusion_engine_estimates_offset() -> None:
    base = pd.Timestamp("2023-01-01T00:00:00Z")
    samples = 120
    rate = 10.0
    times = base + pd.to_timedelta(np.arange(samples) / rate, unit="s")
    time_s = np.arange(samples) / rate
    signal = np.sin(time_s) + 20.0

    gps_df = pd.DataFrame({"timestamp": times, "speed_m_s": signal})
    gps_spec = StreamSpec(
        df=gps_df,
        ts_col="timestamp",
        name="gps",
        ref_cols=["speed_m_s"],
    )

    offset = 0.4
    counter = np.arange(samples)
    ecu_signal = np.sin(time_s - offset) + 20.0
    ecu_df = pd.DataFrame({
        "sample": counter,
        "veh_speed_m_s": ecu_signal,
    })
    ecu_spec = StreamSpec(
        df=ecu_df,
        counter_col="sample",
        rate_hz=rate,
        ref_cols=["veh_speed_m_s"],
        name="ecu",
    )

    engine = FusionEngine(gps_spec, [ecu_spec])
    fused = engine.fuse(estimate_offsets=True, correlation_cols=("veh_speed_m_s", "speed_m_s"), grid_hz=rate)

    assert "speed_m_s" in fused.columns
    assert "veh_speed_m_s_ecu" in fused.columns

    diff = fused["speed_m_s"] - fused["veh_speed_m_s_ecu"]
    assert diff.abs().median() < 0.2

