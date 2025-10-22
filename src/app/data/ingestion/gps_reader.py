"""Utilities to ingest GPS traces into a normalized pandas DataFrame."""

from __future__ import annotations

import datetime as dt
import math
from typing import Mapping

import gpxpy
import numpy as np
import pandas as pd
import pynmea2
from pandas.api.types import is_datetime64tz_dtype

ORDERED = ["timestamp", "lat", "lon", "alt_m", "speed_m_s", "hdop", "fix_ok"]


def _to_utc(ts: pd.Series) -> pd.Series:
    """Coerce timestamps to timezone-aware UTC datetimes."""

    converted = pd.to_datetime(ts, utc=True, errors="raise")
    if not is_datetime64tz_dtype(converted.dtype):
        converted = converted.dt.tz_localize("UTC")
    return converted


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance between two WGS84 coordinates in metres."""

    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _derive_speed(df: pd.DataFrame) -> pd.Series:
    """Derive ground speed (m/s) from consecutive GPS fixes."""

    timestamps = df["timestamp"].view("int64") / 1e9
    lat = df["lat"].to_numpy()
    lon = df["lon"].to_numpy()
    speed = np.full(len(df), np.nan, dtype=float)
    if len(df) >= 2:
        distances = [
            _haversine(lat[i - 1], lon[i - 1], lat[i], lon[i])
            for i in range(1, len(df))
        ]
        deltas = np.diff(timestamps)
        with np.errstate(divide="ignore", invalid="ignore"):
            velocity = np.divide(distances, np.where(deltas == 0, np.nan, deltas))
        speed[1:] = velocity
    return pd.Series(speed, index=df.index, dtype="float64")


def _anti_teleport(df: pd.DataFrame, threshold_m: float = 150.0) -> pd.Series:
    """Flag implausible jumps where the implied speed exceeds ``threshold_m`` m/s."""

    timestamps = df["timestamp"].view("int64") / 1e9
    lat = df["lat"].to_numpy()
    lon = df["lon"].to_numpy()
    jump = np.zeros(len(df), dtype=bool)
    if len(df) >= 2:
        for i in range(1, len(df)):
            dt_s = max(1e-6, timestamps[i] - timestamps[i - 1])
            dist = _haversine(lat[i - 1], lon[i - 1], lat[i], lon[i])
            jump[i] = (dist / dt_s) > threshold_m
    return pd.Series(jump, index=df.index)


def _coerce_bool(val: object) -> float | bool:
    if pd.isna(val):
        return np.nan
    if isinstance(val, (bool, np.bool_)):
        return bool(val)
    if isinstance(val, (int, np.integer)):
        return bool(val)
    if isinstance(val, str):
        lowered = val.strip().lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return True
        if lowered in {"false", "f", "0", "no", "n"}:
            return False
    return bool(val)


class GPSReader:
    """GPS ingestion (MVP): NMEA/CSV/GPX -> normalized DataFrame (UTC)."""

    @staticmethod
    def from_csv(path: str, mapping: Mapping[str, str] | None = None) -> pd.DataFrame:
        """Load a CSV file into the normalized GPS schema."""

        df = pd.read_csv(path)
        mapping = mapping or {
            "timestamp": "time",
            "lat": "lat",
            "lon": "lon",
            "alt_m": "alt_m",
            "speed_m_s": "speed",
            "hdop": "hdop",
            "fix_ok": "fix_ok",
        }
        df = df.rename(columns={v: k for k, v in mapping.items() if v in df.columns})
        return _normalize(df)

    @staticmethod
    def from_nmea(path: str) -> pd.DataFrame:
        """Parse an NMEA file into the normalized schema."""

        rows: list[dict[str, object]] = []
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    message = pynmea2.parse(line)
                except pynmea2.nmea.ParseError:
                    continue
                except Exception:
                    continue
                if message.sentence_type not in {"GGA", "RMC"}:
                    continue
                row: dict[str, object] = {}
                timestamp = getattr(message, "timestamp", None)
                if timestamp is not None:
                    datestamp = getattr(message, "datestamp", None)
                    if isinstance(timestamp, dt.time):
                        date_part = datestamp or pd.Timestamp.utcnow().date()
                        combined = dt.datetime.combine(date_part, timestamp)
                    else:
                        combined = pd.Timestamp(timestamp).to_pydatetime()
                    ts = pd.Timestamp(combined)
                    if ts.tzinfo is None:
                        ts = ts.tz_localize("UTC")
                    else:
                        ts = ts.tz_convert("UTC")
                    row["timestamp"] = ts
                if hasattr(message, "latitude"):
                    row["lat"] = float(message.latitude) if message.latitude else np.nan
                if hasattr(message, "longitude"):
                    row["lon"] = float(message.longitude) if message.longitude else np.nan
                if hasattr(message, "altitude"):
                    try:
                        row["alt_m"] = float(message.altitude)
                    except (TypeError, ValueError):
                        row["alt_m"] = np.nan
                if hasattr(message, "spd_over_grnd"):
                    try:
                        row["speed_m_s"] = float(message.spd_over_grnd) * 0.514444
                    except (TypeError, ValueError):
                        row["speed_m_s"] = np.nan
                if hasattr(message, "horizontal_dil"):
                    try:
                        row["hdop"] = float(message.horizontal_dil)
                    except (TypeError, ValueError):
                        row["hdop"] = np.nan
                if row:
                    rows.append(row)
        return _normalize(pd.DataFrame(rows))

    @staticmethod
    def from_gpx(path: str) -> pd.DataFrame:
        """Parse a GPX track file."""

        with open(path, "r", encoding="utf-8") as handle:
            gpx = gpxpy.parse(handle)
        rows: list[dict[str, object]] = []
        for track in gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
                    rows.append(
                        {
                            "timestamp": point.time,
                            "lat": point.latitude,
                            "lon": point.longitude,
                            "alt_m": point.elevation,
                        }
                    )
        return _normalize(pd.DataFrame(rows))


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw GPS data into the standard schema."""

    if df.empty:
        return pd.DataFrame(
            {
                "timestamp": pd.Series(dtype="datetime64[ns, UTC]"),
                "lat": pd.Series(dtype="float64"),
                "lon": pd.Series(dtype="float64"),
                "alt_m": pd.Series(dtype="float64"),
                "speed_m_s": pd.Series(dtype="float64"),
                "hdop": pd.Series(dtype="float64"),
                "fix_ok": pd.Series(dtype=bool),
            }
        )[ORDERED].copy()

    required = {"timestamp", "lat", "lon"}
    if not required.issubset(df.columns):
        missing = ", ".join(sorted(required - set(df.columns)))
        raise ValueError(f"Required GPS fields missing: {missing}.")

    df = df.copy()
    df["timestamp"] = _to_utc(df["timestamp"])
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)

    numeric_cols = ["lat", "lon", "alt_m", "speed_m_s", "hdop"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan

    if "fix_ok" in df.columns:
        df["fix_ok"] = df["fix_ok"].apply(_coerce_bool)
    else:
        df["fix_ok"] = np.nan

    if df["speed_m_s"].isna().all():
        df["speed_m_s"] = _derive_speed(df)

    jumps = _anti_teleport(df)
    df["speed_m_s"] = df["speed_m_s"].mask(jumps)
    df["speed_m_s"] = df["speed_m_s"].rolling(window=3, min_periods=1, center=True).mean()
    df.loc[jumps, "speed_m_s"] = np.nan

    df["fix_ok"] = df["fix_ok"].fillna(True) & (~jumps)
    df["fix_ok"] = df["fix_ok"].astype(bool)

    return df[ORDERED].copy()
