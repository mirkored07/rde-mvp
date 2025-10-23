"""Stream specifications and utilities for sensor fusion."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence, Union

import numpy as np
import pandas as pd


def _ensure_utc(timestamp: pd.Timestamp) -> pd.Timestamp:
    """Return a timezone aware timestamp in UTC."""

    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


@dataclass(kw_only=True)
class StreamSpec:
    """Describe an input data stream used during fusion."""

    df: pd.DataFrame
    ts_col: Optional[str] = "timestamp"
    counter_col: Optional[str] = None
    rate_hz: Optional[float] = None
    t0: Optional[pd.Timestamp] = None
    clock_offset_s: float = 0.0
    ref_cols: Sequence[str] = field(default_factory=list)
    name: str = "stream"
    generated_ts: bool = False


SpecLike = Union[StreamSpec, pd.DataFrame]


def as_spec(obj: SpecLike, *, default_name: str = "stream") -> StreamSpec:
    """Coerce a :class:`StreamSpec` or raw dataframe into a :class:`StreamSpec`."""

    if isinstance(obj, StreamSpec):
        return obj
    if isinstance(obj, pd.DataFrame):
        return StreamSpec(df=obj, ts_col="timestamp", name=default_name)
    raise TypeError(f"Unsupported type for spec: {type(obj)}")


def synthesize_timestamps(spec: SpecLike) -> pd.Series:
    """Ensure a stream contains a UTC timestamp column.

    Parameters
    ----------
    spec:
        Stream specification providing metadata about the stream. The
        dataframe referenced by ``spec`` is modified in-place.

    Returns
    -------
    pandas.Series
        The UTC timestamp column.
    """

    stream = as_spec(spec)

    if stream.ts_col is None:
        stream.ts_col = "timestamp"

    df = stream.df
    ts_col = stream.ts_col

    has_existing = ts_col in df and df[ts_col].notna().any()

    if has_existing and not stream.generated_ts:
        timestamps = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        if timestamps.isna().any():
            raise ValueError(f"{stream.name}: unable to parse existing timestamps")
        stream.generated_ts = False
    else:
        if not stream.counter_col or stream.rate_hz is None:
            raise ValueError(
                f"{stream.name}: missing timestamp column and counter/rate information"
            )
        if stream.counter_col not in df:
            raise KeyError(
                f"{stream.name}: counter column '{stream.counter_col}' not in dataframe"
            )

        counter = pd.to_numeric(df[stream.counter_col], errors="coerce")
        if counter.isna().any():
            raise ValueError(f"{stream.name}: counter column contains non numeric values")

        counter0 = counter.iloc[0]
        elapsed = (counter - counter0) / float(stream.rate_hz)

        if stream.t0 is None:
            origin = pd.Timestamp(0, unit="s", tz="UTC")
        else:
            origin = _ensure_utc(stream.t0)

        timestamps = origin + pd.to_timedelta(elapsed, unit="s")
        stream.generated_ts = True

    if stream.clock_offset_s:
        timestamps = timestamps + pd.to_timedelta(stream.clock_offset_s, unit="s")

    timestamps = timestamps.rename(ts_col)
    df[ts_col] = timestamps

    return timestamps


def _aggregate_reference_signal(
    spec: StreamSpec, columns: Iterable[str]
) -> pd.Series:
    """Create a normalized reference signal for correlation."""

    ts = synthesize_timestamps(spec)
    df = spec.df.copy()
    df[ts.name] = ts
    df = df.sort_values(ts.name)
    df = df.set_index(ts.name)

    series_list: list[pd.Series] = []
    for col in columns:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        series = series.dropna()
        if series.empty:
            continue
        std = series.std()
        if std == 0 or np.isnan(std):
            continue
        normalized = (series - series.mean()) / std
        series_list.append(normalized)

    if not series_list:
        raise ValueError(f"{spec.name}: no usable reference columns for correlation")

    combined = pd.concat(series_list, axis=1)
    aggregated = combined.mean(axis=1, skipna=True)
    aggregated = aggregated.dropna()
    aggregated = aggregated[~aggregated.index.duplicated(keep="first")]

    if aggregated.size < 2:
        raise ValueError(f"{spec.name}: insufficient data for correlation")

    return aggregated


def estimate_offset_by_correlation(
    spec_a: SpecLike,
    spec_b: SpecLike,
    cols: Sequence[Sequence[str] | str] | tuple[str, str] = (
        "veh_speed_m_s",
        "speed_m_s",
    ),
    grid_hz: float = 10.0,
    max_lag_s: float = 5.0,
) -> float:
    """Estimate the time offset between two streams using correlation.

    Returns the lag (in seconds) of ``spec_a`` relative to ``spec_b``. A positive
    value indicates that ``spec_a`` should be advanced (i.e. its clock offset
    reduced) to align with ``spec_b``.
    """

    if grid_hz <= 0:
        raise ValueError("grid_hz must be positive")
    if max_lag_s < 0:
        raise ValueError("max_lag_s must be non-negative")

    stream_a = as_spec(spec_a, default_name="stream_a")
    stream_b = as_spec(spec_b, default_name="stream_b")

    if cols is None:
        cols = (stream_a.ref_cols, stream_b.ref_cols)

    if len(cols) != 2:
        raise ValueError("cols must provide two column specifications")

    def _as_iterable(value: Sequence[str] | str) -> Sequence[str]:
        if isinstance(value, str):
            return [value]
        return list(value)

    cols_a = _as_iterable(cols[0]) if cols[0] else list(stream_a.ref_cols)
    cols_b = _as_iterable(cols[1]) if cols[1] else list(stream_b.ref_cols)

    if not cols_a:
        raise ValueError(f"{stream_a.name}: no reference columns provided")
    if not cols_b:
        raise ValueError(f"{stream_b.name}: no reference columns provided")

    signal_a = _aggregate_reference_signal(stream_a, cols_a)
    signal_b = _aggregate_reference_signal(stream_b, cols_b)

    # Convert to relative seconds for interpolation.
    times_a = (signal_a.index - signal_a.index[0]).total_seconds()
    times_b = (signal_b.index - signal_b.index[0]).total_seconds()

    duration = min(times_a[-1], times_b[-1])
    if duration <= 0:
        raise ValueError("Streams do not share overlapping duration for correlation")

    step = 1.0 / float(grid_hz)
    grid = np.arange(0.0, duration + step, step)
    if grid.size < 2:
        raise ValueError("Not enough samples for correlation analysis")

    interp_a = np.interp(grid, times_a, signal_a.to_numpy(dtype=float))
    interp_b = np.interp(grid, times_b, signal_b.to_numpy(dtype=float))

    interp_a -= interp_a.mean()
    interp_b -= interp_b.mean()

    std_a = interp_a.std()
    std_b = interp_b.std()
    if std_a == 0 or np.isnan(std_a):
        raise ValueError(f"{stream_a.name}: interpolated signal has zero variance")
    if std_b == 0 or np.isnan(std_b):
        raise ValueError(f"{stream_b.name}: interpolated signal has zero variance")

    interp_a /= std_a
    interp_b /= std_b

    max_shift = int(round(max_lag_s * grid_hz))
    max_shift = min(max_shift, grid.size - 1)

    best_lag = 0
    best_corr = -np.inf

    for lag in range(-max_shift, max_shift + 1):
        if lag > 0:
            x = interp_a[lag:]
            y = interp_b[: grid.size - lag]
        elif lag < 0:
            shift = -lag
            x = interp_a[: grid.size - shift]
            y = interp_b[shift:]
        else:
            x = interp_a
            y = interp_b

        if x.size < 2 or y.size < 2:
            continue

        corr_matrix = np.corrcoef(x, y)
        corr = corr_matrix[0, 1]
        if np.isnan(corr):
            continue

        if corr > best_corr or (np.isclose(corr, best_corr) and abs(lag) < abs(best_lag)):
            best_corr = corr
            best_lag = lag

    return best_lag / float(grid_hz)


__all__ = [
    "StreamSpec",
    "SpecLike",
    "as_spec",
    "synthesize_timestamps",
    "estimate_offset_by_correlation",
]

