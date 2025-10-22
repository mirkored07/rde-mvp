"""Utilities to ingest ECU/vehicle signals into a normalized DataFrame."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64tz_dtype, is_numeric_dtype

ORDERED = [
    "timestamp",
    "veh_speed_m_s",
    "engine_speed_rpm",
    "engine_load_pct",
    "throttle_pct",
]

DEFAULT_MAPPING: Mapping[str, str] = {
    "timestamp": "timestamp",
    "veh_speed_m_s": "veh_speed_m_s",
    "engine_speed_rpm": "engine_speed_rpm",
    "engine_load_pct": "engine_load_pct",
    "throttle_pct": "throttle_pct",
}


def _merge_mapping(mapping: Mapping[str, str] | None) -> dict[str, str]:
    merged = dict(DEFAULT_MAPPING)
    if mapping:
        merged.update(mapping)
    return merged


def _apply_mapping(df: pd.DataFrame, mapping: Mapping[str, str]) -> pd.DataFrame:
    rename: dict[str, str] = {}
    for canonical, raw in mapping.items():
        if raw in df.columns:
            rename[raw] = canonical
    if rename:
        df = df.rename(columns=rename)
    return df


def _mdf_start_time(mdf: Any) -> pd.Timestamp:
    header = getattr(mdf, "header", None)
    start_time = getattr(header, "start_time", None)
    if start_time is None:
        return pd.Timestamp(0, unit="s", tz="UTC")
    ts = pd.Timestamp(start_time)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def _ensure_timestamp(df: pd.DataFrame, *, start: pd.Timestamp) -> pd.DataFrame:
    if "timestamp" in df.columns:
        ts = df["timestamp"]
        if is_numeric_dtype(ts):
            df = df.copy()
            offsets = pd.to_timedelta(ts.to_numpy(), unit="s")
            df["timestamp"] = start + offsets
        return df

    # Try common column names before falling back to the index.
    for candidate in ("time", "Time", "timestamps", "Timestamp"):
        if candidate in df.columns:
            return df.rename(columns={candidate: "timestamp"})

    index = df.index
    if isinstance(index, pd.DatetimeIndex):
        df = df.reset_index()
        index_name = index.name or "index"
        df = df.rename(columns={index_name: "timestamp"})
        return df

    if isinstance(index, pd.TimedeltaIndex):
        df = df.reset_index(drop=True)
        df["timestamp"] = start + index
        return df

    if np.issubdtype(index.dtype, np.number):
        df = df.reset_index(drop=True)
        offsets = pd.to_timedelta(index.to_numpy(), unit="s")
        df["timestamp"] = start + offsets
        return df

    raise ValueError("Timestamp column could not be inferred from MDF data.")


def _to_utc(ts: pd.Series) -> pd.Series:
    converted = pd.to_datetime(ts, utc=True, errors="raise")
    if not is_datetime64tz_dtype(converted.dtype):
        converted = converted.dt.tz_localize("UTC")
    return converted


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" not in df.columns:
        raise ValueError("ECU data requires a 'timestamp' column.")

    df = df.copy()
    df["timestamp"] = _to_utc(df["timestamp"])
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)

    available: list[str] = ["timestamp"]
    for column in ORDERED[1:]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
            available.append(column)

    return df[available].copy()


class ECUReader:
    """ECU ingestion utilities (CSV always, MDF optional)."""

    @staticmethod
    def from_csv(path: str, mapping: Mapping[str, str] | None = None) -> pd.DataFrame:
        df = pd.read_csv(path)
        merged_mapping = _merge_mapping(mapping)
        df = _apply_mapping(df, merged_mapping)
        return _normalize(df)

    @staticmethod
    def from_mdf(path: str, mapping: Mapping[str, str] | None = None) -> pd.DataFrame:
        try:
            from asammdf import MDF  # type: ignore import-not-found
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on optional dep
            raise ImportError(
                "asammdf is required for MDF ingestion; install it to enable this feature."
            ) from exc

        merged_mapping = _merge_mapping(mapping)
        mdf = MDF(path)
        try:
            start = _mdf_start_time(mdf)
            try:
                df = mdf.to_dataframe(time_from_zero=False)
            except TypeError:
                df = mdf.to_dataframe()
        finally:
            close = getattr(mdf, "close", None)
            if callable(close):
                close()

        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        df = _apply_mapping(df, merged_mapping)
        df = _ensure_timestamp(df, start=start)

        # Re-apply mapping for any columns that may have been introduced after ensuring timestamp.
        df = _apply_mapping(df, merged_mapping)
        return _normalize(df)


__all__ = ["ECUReader", "ORDERED"]

