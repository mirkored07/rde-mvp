from __future__ import annotations

import pandas as pd

from src.app.data.fusion.time_alignment import StreamSpec, synthesize_timestamps


def test_synthesize_timestamps_normalizes_existing_column() -> None:
    df = pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2023-01-01T00:00:00-05:00"),
                pd.Timestamp("2023-01-01T01:00:00-05:00"),
            ],
            "value": [1, 2],
        }
    )

    spec = StreamSpec(
        name="test",
        df=df,
        ts_col="ts",
        clock_offset_s=2.0,
    )

    result = synthesize_timestamps(spec)

    expected_ts = pd.Series(
        [
            pd.Timestamp("2023-01-01T05:00:02Z"),
            pd.Timestamp("2023-01-01T06:00:02Z"),
        ],
        name="timestamp",
        dtype="datetime64[ns, UTC]",
    )

    pd.testing.assert_series_equal(result["timestamp"], expected_ts)
    assert "ts" not in result.columns


def test_synthesize_timestamps_from_counter_stream() -> None:
    df = pd.DataFrame({"counter": [0, 1, 2]})

    spec = StreamSpec(
        name="counter-stream",
        df=df,
        counter_col="counter",
        rate_hz=2.0,
        t0=pd.Timestamp("2023-01-01T00:00:00"),
        clock_offset_s=0.5,
    )

    result = synthesize_timestamps(spec)

    expected_ts = pd.Series(
        [
            pd.Timestamp("2023-01-01T00:00:00.500000Z"),
            pd.Timestamp("2023-01-01T00:00:01Z"),
            pd.Timestamp("2023-01-01T00:00:01.500000Z"),
        ],
        name="timestamp",
        dtype="datetime64[ns, UTC]",
    )

    pd.testing.assert_series_equal(result["timestamp"], expected_ts)
