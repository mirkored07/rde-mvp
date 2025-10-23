from __future__ import annotations

"""Data quality diagnostics for fused telemetry streams."""

from dataclasses import dataclass, asdict
import math
from typing import Dict, List, Mapping, Optional

import numpy as np
import pandas as pd

__all__ = ["CheckResult", "Diagnostics", "run_diagnostics", "to_dict"]


@dataclass(slots=True)
class CheckResult:
    """Outcome of a single diagnostic check."""

    id: str
    level: str  # "pass" | "warn" | "fail"
    title: str
    details: str
    count: int = 0
    subject: Optional[str] = None


@dataclass(slots=True)
class Diagnostics:
    """Container for computed diagnostic checks and repair metadata."""

    checks: List[CheckResult]
    summary: Dict[str, int]
    repaired_spans: List[Dict[str, object]]


def _coerce_timestamps(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, utc=True, errors="coerce")
    return ts


def _to_iso(timestamp: pd.Timestamp | None) -> str:
    if timestamp is None or pd.isna(timestamp):
        return ""
    ts = timestamp
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def _estimate_rate_s(ts: pd.Series) -> float:
    if len(ts) < 3:
        return float("nan")
    diffs = ts.sort_values().diff().dt.total_seconds().dropna()
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return float("nan")
    return float(diffs.median())


def _gaps(ts: pd.Series, threshold_s: float) -> List[tuple[pd.Timestamp, pd.Timestamp, float]]:
    if ts.empty:
        return []
    diffs = ts.diff().dt.total_seconds()
    if diffs.empty:
        return []
    indices = np.where(diffs > threshold_s)[0]
    spans: List[tuple[pd.Timestamp, pd.Timestamp, float]] = []
    for idx in indices:
        start = ts.iloc[idx - 1] if idx > 0 else pd.NaT
        end = ts.iloc[idx]
        delta = float(diffs.iloc[idx])
        spans.append((start, end, delta))
    return spans


def _count_outliers(diffs: pd.Series, median: float, tolerance: float) -> int:
    if not np.isfinite(median) or median <= 0:
        return 0
    deviations = np.abs(diffs - median)
    return int((deviations > tolerance * median).sum())


def _repair_small_gaps(
    df: pd.DataFrame,
    ts_col: str,
    expected_step_s: float,
    max_span_s: float,
) -> tuple[pd.DataFrame, List[Dict[str, object]]]:
    if not np.isfinite(expected_step_s) or expected_step_s <= 0:
        return df, []

    ts = df[ts_col]
    repaired_rows: List[dict] = []
    repaired_spans: List[Dict[str, object]] = []

    for idx in range(1, len(ts)):
        previous = ts.iloc[idx - 1]
        current = ts.iloc[idx]
        if pd.isna(previous) or pd.isna(current):
            continue
        delta_s = (current - previous).total_seconds()
        if not np.isfinite(delta_s) or delta_s <= expected_step_s:
            continue
        if delta_s > max_span_s:
            continue

        insert_count = int(max(0, math.ceil(delta_s / expected_step_s) - 1))
        if insert_count <= 0:
            continue

        base_row = df.iloc[idx - 1].to_dict()
        inserted = 0
        for step in range(1, insert_count + 1):
            new_timestamp = previous + pd.to_timedelta(expected_step_s * step, unit="s")
            if new_timestamp >= current:
                break
            new_row = dict(base_row)
            new_row[ts_col] = new_timestamp
            repaired_rows.append(new_row)
            inserted += 1

        if inserted:
            repaired_spans.append(
                {
                    "start": _to_iso(previous),
                    "end": _to_iso(current),
                    "seconds": float(delta_s),
                    "inserted": inserted,
                }
            )

    if not repaired_rows:
        return df, []

    repaired_df = pd.concat([df, pd.DataFrame(repaired_rows)], ignore_index=True, sort=False)
    repaired_df = repaired_df.sort_values(ts_col).reset_index(drop=True)
    return repaired_df, repaired_spans


def _check_stream_timeline(
    name: str,
    df: pd.DataFrame,
    *,
    ts_col: str = "timestamp",
    tolerance: float = 0.35,
) -> List[CheckResult]:
    subject = name.upper()
    if df is None or df.empty or ts_col not in df.columns:
        return [
            CheckResult(
                id=f"stream_missing_{name}",
                level="warn",
                title=f"{subject} stream unavailable",
                details="No samples found for this stream.",
                subject=subject,
            )
        ]

    ts = _coerce_timestamps(df[ts_col])
    checks: List[CheckResult] = []
    invalid = int(ts.isna().sum())
    if invalid:
        checks.append(
            CheckResult(
                id=f"stream_ts_invalid_{name}",
                level="fail",
                title=f"Invalid timestamps ({subject})",
                details=f"{invalid} samples could not be parsed as timestamps.",
                count=invalid,
                subject=subject,
            )
        )

    if not ts.empty:
        if ts.is_monotonic_increasing:
            checks.append(
                CheckResult(
                    id=f"stream_ts_monotonic_{name}",
                    level="pass",
                    title=f"Monotonic timeline ({subject})",
                    details="Timestamps are sorted in ascending order.",
                    subject=subject,
                )
            )
        else:
            checks.append(
                CheckResult(
                    id=f"stream_ts_non_monotonic_{name}",
                    level="fail",
                    title=f"Non-monotonic timeline ({subject})",
                    details="Detected out-of-order timestamps.",
                    subject=subject,
                )
            )

        diffs = ts.diff().dt.total_seconds().dropna()
        diffs = diffs[diffs > 0]
        if diffs.empty:
            checks.append(
                CheckResult(
                    id=f"stream_sampling_{name}",
                    level="warn",
                    title=f"Sampling rate unknown ({subject})",
                    details="Not enough samples to estimate frequency.",
                    subject=subject,
                )
            )
        else:
            median = float(np.median(diffs))
            checks.append(
                CheckResult(
                    id=f"stream_sampling_{name}",
                    level="pass",
                    title=f"Sampling rate ~{median:.3f} s ({subject})",
                    details="Median interval computed from stream timestamps.",
                    subject=subject,
                )
            )
            outliers = _count_outliers(diffs, median, tolerance)
            if outliers:
                checks.append(
                    CheckResult(
                        id=f"stream_uniformity_{name}",
                        level="warn",
                        title=f"Irregular sampling ({subject})",
                        details=(
                            f"{outliers} intervals deviate more than {int(tolerance * 100)}% from the median {median:.3f}s."
                        ),
                        count=outliers,
                        subject=subject,
                    )
                )
            else:
                checks.append(
                    CheckResult(
                        id=f"stream_uniformity_{name}",
                        level="pass",
                        title=f"Uniform sampling ({subject})",
                        details=f"Intervals within ±{int(tolerance * 100)}% of median {median:.3f}s.",
                        subject=subject,
                    )
                )

        duplicates = int(ts.duplicated().sum())
        if duplicates:
            checks.append(
                CheckResult(
                    id=f"stream_duplicates_{name}",
                    level="warn",
                    title=f"Duplicate timestamps ({subject})",
                    details=f"{duplicates} duplicate timestamps detected.",
                    count=duplicates,
                    subject=subject,
                )
            )
        else:
            checks.append(
                CheckResult(
                    id=f"stream_duplicates_{name}",
                    level="pass",
                    title=f"No duplicates ({subject})",
                    details="No repeated timestamps found.",
                    subject=subject,
                )
            )

    return checks


def run_diagnostics(
    fused: pd.DataFrame,
    source: Mapping[str, pd.DataFrame] | None = None,
    *,
    ts_col: str = "timestamp",
    gap_threshold_s: float = 2.0,
    repair_small_gaps: bool = True,
    repair_threshold_s: float = 3.0,
    speed_spike_ms: float = 65.0,
    gps_teleport_m: float = 120.0,
) -> tuple[pd.DataFrame, Diagnostics]:
    """Run quality diagnostics over the fused dataframe and optional source streams."""

    if ts_col not in fused.columns:
        raise KeyError(f"Missing '{ts_col}' column in fused dataframe")

    working = fused.copy()
    working[ts_col] = _coerce_timestamps(working[ts_col])
    timeline = working[ts_col]
    original_timeline = timeline.copy()

    checks: List[CheckResult] = []

    invalid = int(original_timeline.isna().sum())
    if invalid:
        checks.append(
            CheckResult(
                id="fused_ts_invalid",
                level="fail",
                title="Invalid timestamps",
                details=f"{invalid} rows contain unparseable timestamps.",
                count=invalid,
                subject="Fused",
            )
        )

    if original_timeline.is_monotonic_increasing:
        checks.append(
            CheckResult(
                id="fused_ts_monotonic",
                level="pass",
                title="Monotonic timeline",
                details="Fused timestamps are in ascending order.",
                subject="Fused",
            )
        )
    else:
        checks.append(
            CheckResult(
                id="fused_ts_non_monotonic",
                level="fail",
                title="Non-monotonic timeline",
                details="Detected out-of-order timestamps in fused dataframe.",
                subject="Fused",
            )
        )

    rate_s = _estimate_rate_s(original_timeline.dropna())
    if np.isfinite(rate_s):
        checks.append(
            CheckResult(
                id="fused_sampling_rate",
                level="pass",
                title=f"Sampling rate ~{rate_s:.3f} s",
                details="Median interval across fused samples.",
                subject="Fused",
            )
        )
        diffs = original_timeline.diff().dt.total_seconds().dropna()
        diffs = diffs[diffs > 0]
        outliers = _count_outliers(diffs, rate_s, 0.4)
        if outliers:
            checks.append(
                CheckResult(
                    id="fused_sampling_irregular",
                    level="warn",
                    title="Irregular sampling cadence",
                    details=(
                        f"{outliers} intervals deviate more than 40% from the median {rate_s:.3f}s."
                    ),
                    count=outliers,
                    subject="Fused",
                )
            )
        else:
            checks.append(
                CheckResult(
                    id="fused_sampling_uniform",
                    level="pass",
                    title="Consistent sampling cadence",
                    details=f"Intervals within ±40% of median {rate_s:.3f}s.",
                    subject="Fused",
                )
            )
    else:
        checks.append(
            CheckResult(
                id="fused_sampling_rate",
                level="warn",
                title="Sampling rate unknown",
                details="Not enough fused samples to estimate cadence.",
                subject="Fused",
            )
        )

    gap_spans = _gaps(original_timeline, gap_threshold_s)
    if gap_spans:
        checks.append(
            CheckResult(
                id="fused_gaps",
                level="warn",
                title="Timeline gaps detected",
                details=f"Found {len(gap_spans)} gaps exceeding {gap_threshold_s:.1f} s.",
                count=len(gap_spans),
                subject="Fused",
            )
        )
    else:
        checks.append(
            CheckResult(
                id="fused_gaps_none",
                level="pass",
                title="No large gaps",
                details="No gaps exceeded the configured threshold.",
                subject="Fused",
            )
        )

    duplicates = int(original_timeline.duplicated().sum())
    if duplicates:
        checks.append(
            CheckResult(
                id="fused_duplicates",
                level="warn",
                title="Duplicate timestamps",
                details=f"{duplicates} duplicate timestamp rows detected.",
                count=duplicates,
                subject="Fused",
            )
        )
    else:
        checks.append(
            CheckResult(
                id="fused_duplicates_none",
                level="pass",
                title="No duplicate timestamps",
                details="All fused timestamps are unique.",
                subject="Fused",
            )
        )

    repaired_spans: List[Dict[str, object]] = []
    if repair_small_gaps and np.isfinite(rate_s) and rate_s > 0:
        working, repaired_spans = _repair_small_gaps(
            working,
            ts_col,
            expected_step_s=rate_s,
            max_span_s=repair_threshold_s,
        )
        timeline = working[ts_col]
    else:
        timeline = working[ts_col]

    if "veh_speed_m_s" in working.columns:
        speed = pd.to_numeric(working["veh_speed_m_s"], errors="coerce")
    else:
        speed = pd.to_numeric(working.get("speed_m_s"), errors="coerce")
    spikes = int((speed > speed_spike_ms).sum()) if speed is not None else 0
    if spikes:
        checks.append(
            CheckResult(
                id="fused_speed_spikes",
                level="warn",
                title="Speed spikes detected",
                details=f"{spikes} samples exceed {speed_spike_ms:.1f} m/s.",
                count=spikes,
                subject="Fused",
            )
        )
    else:
        checks.append(
            CheckResult(
                id="fused_speed_ok",
                level="pass",
                title="No excessive speed spikes",
                details=f"All samples below {speed_spike_ms:.1f} m/s.",
                subject="Fused",
            )
        )

    if {"lat", "lon"}.issubset(working.columns):
        lat = pd.to_numeric(working["lat"], errors="coerce").to_numpy()
        lon = pd.to_numeric(working["lon"], errors="coerce").to_numpy()
        teleport_count = 0
        timeline_np = timeline.to_numpy()
        for idx in range(1, len(lat)):
            ts_prev = timeline_np[idx - 1]
            ts_curr = timeline_np[idx]
            if pd.isna(ts_prev) or pd.isna(ts_curr):
                continue
            dt_s = (ts_curr - ts_prev).total_seconds()
            if not np.isfinite(dt_s) or dt_s <= 0:
                continue
            distance = _haversine(lat[idx - 1], lon[idx - 1], lat[idx], lon[idx])
            if distance > gps_teleport_m:
                teleport_count += 1
        if teleport_count:
            checks.append(
                CheckResult(
                    id="fused_gps_teleport",
                    level="warn",
                    title="GPS jumps detected",
                    details=f"{teleport_count} intervals exceed {gps_teleport_m:.0f} m.",
                    count=teleport_count,
                    subject="Fused",
                )
            )
        else:
            checks.append(
                CheckResult(
                    id="fused_gps_ok",
                    level="pass",
                    title="No GPS jumps",
                    details=f"All GPS transitions within {gps_teleport_m:.0f} m.",
                    subject="Fused",
                )
            )

    if source:
        for name, df in source.items():
            stream_checks = _check_stream_timeline(name, df)
            checks.extend(stream_checks)

    summary = {
        "pass": sum(1 for check in checks if check.level == "pass"),
        "warn": sum(1 for check in checks if check.level == "warn"),
        "fail": sum(1 for check in checks if check.level == "fail"),
    }

    diagnostics = Diagnostics(checks=checks, summary=summary, repaired_spans=repaired_spans)
    return working, diagnostics


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def to_dict(diagnostics: Diagnostics) -> dict:
    return {
        "checks": [asdict(check) for check in diagnostics.checks],
        "summary": dict(diagnostics.summary),
        "repaired_spans": list(diagnostics.repaired_spans),
    }
