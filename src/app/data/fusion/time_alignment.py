"""Helpers for timestamp synthesis and alignment across streams."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.app.utils.time import to_utc_series


@dataclass
class StreamSpec:
    """Specification describing how to obtain timestamps for a stream."""

    name: str
    df: pd.DataFrame
    ts_col: str | None = None
    counter_col: str | None = None
    rate_hz: float | None = None
    t0: Any | None = None
    clock_offset_s: float | None = 0.0


def synthesize_timestamps(spec: StreamSpec) -> pd.DataFrame:
    """Return a copy of ``spec.df`` with a UTC ``timestamp`` column."""

    df = spec.df.copy()

    # Case 1: timestamps already present -> normalize to UTC and apply clock_offset
    if spec.ts_col and spec.ts_col in df.columns:
        if spec.ts_col != "timestamp":
            df = df.rename(columns={spec.ts_col: "timestamp"})
        ts = to_utc_series(df["timestamp"])
        offset = float(spec.clock_offset_s or 0.0)
        if offset:
            ts = ts + pd.Timedelta(seconds=offset)
        df = df.assign(timestamp=ts)
        return df

    # Case 2: synthesize from counter + rate
    if spec.counter_col is None or spec.rate_hz is None:
        raise ValueError(
            f"{spec.name}: missing timestamps and no (counter_col, rate_hz) provided"
        )
    if spec.t0 is None:
        raise ValueError(
            f"{spec.name}: cannot synthesize timestamps without t0; provide t0 or use correlation"
        )

    # Ensure t0 is tz-aware UTC
    t0 = pd.Timestamp(spec.t0)
    if getattr(t0, "tzinfo", None) is None:
        t0 = t0.tz_localize("UTC")
    else:
        t0 = t0.tz_convert("UTC")

    counter = df[spec.counter_col].to_numpy()
    offset = float(spec.clock_offset_s or 0.0)
    td = pd.to_timedelta(counter / float(spec.rate_hz) + offset, unit="s")
    # Directly assign the tz-aware DatetimeIndex; no pd.to_datetime() call here
    ts = t0 + td
    df = df.assign(timestamp=ts)
    return df
