"""Computation of KPIs and validity checks for fused telemetry streams."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .rules import AnalysisRules, SpeedBin

_SPEED_COL_CANDIDATES = ("veh_speed_m_s", "speed_m_s")


@dataclass(slots=True)
class AnalysisResult:
    """Container holding derived dataframe, analysis payload and summary text."""

    derived: pd.DataFrame
    analysis: Mapping[str, Any]
    summary_md: str


class AnalysisEngine:
    """Apply rule based analysis on fused telemetry data."""

    def __init__(
        self,
        rules: AnalysisRules,
        *,
        timestamp_col: str = "timestamp",
        rolling_window_s: int = 5,
    ) -> None:
        self.rules = rules
        self.timestamp_col = timestamp_col
        self.rolling_window_s = rolling_window_s

    def _resolve_speed_column(self, df: pd.DataFrame) -> str:
        for candidate in _SPEED_COL_CANDIDATES:
            if candidate in df.columns:
                return candidate
        raise KeyError(
            "No speed column found. Expected one of: " + ", ".join(_SPEED_COL_CANDIDATES)
        )

    def _sanitize_bin_name(self, bin_name: str) -> str:
        safe = [
            ch if ch.isalnum() or ch in ("_", "-") else "_"
            for ch in bin_name.strip().lower()
        ]
        return "bin_mask__" + "".join(safe)

    def analyze(self, fused: pd.DataFrame) -> AnalysisResult:
        """Compute KPIs, validity checks and a Markdown summary."""

        if self.timestamp_col not in fused.columns:
            raise KeyError(f"Missing '{self.timestamp_col}' column in fused dataframe")

        if fused.empty:
            derived = fused.copy()
            analysis: Mapping[str, Any] = {
                "overall": {
                    "total_time_s": 0.0,
                    "total_distance_km": 0.0,
                    "completeness": {
                        "max_gap_s": self.rules.completeness_max_gap_s,
                        "largest_gap_s": 0.0,
                        "ok": True,
                    },
                    "valid": False,
                },
                "bins": {},
            }
            summary = "# Analysis Summary\n\nNo data available."
            return AnalysisResult(derived=derived, analysis=analysis, summary_md=summary)

        df = fused.copy()
        df = df.sort_values(self.timestamp_col).reset_index(drop=True)

        ts = pd.to_datetime(df[self.timestamp_col], utc=True, errors="coerce")
        if ts.isna().any():
            raise ValueError("Timestamp column contains non-convertible values")

        df[self.timestamp_col] = ts

        dt = ts.diff().dt.total_seconds().fillna(0.0)
        dt = dt.mask(dt < 0, 0.0)
        df["delta_time_s"] = dt
        total_time_s = float(dt.sum())

        speed_col = self._resolve_speed_column(df)
        speed = df[speed_col].fillna(0.0)

        distance_m = speed * dt
        df["distance_increment_m"] = distance_m
        df["cumulative_distance_km"] = distance_m.cumsum() / 1000.0

        window = max(int(self.rolling_window_s), 1)
        df["rolling_mean_speed_m_s"] = speed.rolling(window=window, min_periods=1).mean()

        speed_diff = speed.diff().fillna(0.0)
        dt_nonzero = dt.replace(0, np.nan)
        acceleration = speed_diff / dt_nonzero
        acceleration = acceleration.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        df["acceleration_m_s2"] = acceleration

        speed_kmh = speed * 3.6
        bin_payload: dict[str, Any] = {}
        bin_valid_flags: list[bool] = []

        total_distance_km = float(distance_m.sum() / 1000.0)

        for bin_spec in self.rules.speed_bins:
            mask = self._compute_bin_mask(speed_kmh, bin_spec)
            mask_col = self._sanitize_bin_name(bin_spec.name)
            df[mask_col] = mask

            time_s = float((dt * mask).sum())
            distance_km = float((distance_m * mask).sum() / 1000.0)

            meets_distance = True
            if self.rules.min_distance_km_per_bin is not None:
                meets_distance = distance_km >= self.rules.min_distance_km_per_bin

            meets_time = True
            if self.rules.min_time_s_per_bin is not None:
                meets_time = time_s >= self.rules.min_time_s_per_bin

            kpis = self._compute_bin_kpis(df, mask, speed_col)

            valid = bool(meets_distance and meets_time)
            bin_valid_flags.append(valid)
            bin_payload[bin_spec.name] = {
                "time_s": time_s,
                "distance_km": distance_km,
                "meets_min_distance": meets_distance,
                "meets_min_time": meets_time,
                "valid": valid,
                "kpis": kpis,
            }

        largest_gap_s = float(dt.iloc[1:].max() if len(dt) > 1 else 0.0)
        completeness_ok = True
        if self.rules.completeness_max_gap_s is not None:
            completeness_ok = largest_gap_s <= self.rules.completeness_max_gap_s

        overall_valid = completeness_ok and all(bin_valid_flags) if bin_valid_flags else completeness_ok

        analysis_payload: Mapping[str, Any] = {
            "overall": {
                "total_time_s": total_time_s,
                "total_distance_km": total_distance_km,
                "completeness": {
                    "max_gap_s": self.rules.completeness_max_gap_s,
                    "largest_gap_s": largest_gap_s,
                    "ok": completeness_ok,
                },
                "valid": overall_valid,
            },
            "bins": bin_payload,
        }

        summary_md = self._build_summary(overall_valid, analysis_payload)

        return AnalysisResult(derived=df, analysis=analysis_payload, summary_md=summary_md)

    def _compute_bin_mask(self, speed_kmh: pd.Series, spec: SpeedBin) -> pd.Series:
        mask = pd.Series(True, index=speed_kmh.index)
        if spec.min_kmh is not None:
            mask &= speed_kmh >= spec.min_kmh
        if spec.max_kmh is not None:
            mask &= speed_kmh < spec.max_kmh
        return mask

    def _compute_bin_kpis(
        self,
        df: pd.DataFrame,
        mask: pd.Series,
        speed_col: str,
    ) -> Mapping[str, float | None]:
        results: dict[str, float | None] = {}
        if not self.rules.kpi_defs:
            return results

        dt = df["delta_time_s"]
        for name, definition in self.rules.kpi_defs.items():
            numerator_key = definition.get("numerator")
            if numerator_key is None or numerator_key not in df:
                continue
            numerator_series = df[numerator_key].fillna(0.0)
            numerator_total = float(((numerator_series * dt) * mask).sum())

            denominator_key = definition.get("denominator")
            if denominator_key is None:
                results[name] = numerator_total
                continue
            if denominator_key not in df:
                continue
            denominator_series = df[denominator_key].fillna(0.0)
            denominator_total = float(((denominator_series * dt) * mask).sum())

            if denominator_key == speed_col:
                distance_km = denominator_total / 1000.0
                if distance_km > 0:
                    results[name] = numerator_total / distance_km
                else:
                    results[name] = None
            else:
                if denominator_total != 0:
                    results[name] = numerator_total / denominator_total
                else:
                    results[name] = None

        return results

    def _build_summary(
        self,
        overall_valid: bool,
        payload: Mapping[str, Any],
    ) -> str:
        lines = ["# Analysis Summary"]
        overall = payload.get("overall", {})
        completeness = overall.get("completeness", {})

        status = "PASS" if overall_valid else "FAIL"
        lines.append("")
        lines.append(f"Overall status: **{status}**")
        total_distance = overall.get("total_distance_km")
        if total_distance is not None:
            lines.append(f"Total distance: **{total_distance:.2f} km**")
        total_time = overall.get("total_time_s")
        if total_time is not None:
            lines.append(f"Total time: **{total_time:.1f} s**")

        if completeness:
            comp_status = "PASS" if completeness.get("ok") else "FAIL"
            max_gap = completeness.get("max_gap_s")
            largest_gap = completeness.get("largest_gap_s")
            if isinstance(max_gap, (int, float)):
                max_gap_text = f" {max_gap:.1f}s"
            else:
                max_gap_text = ""
            if isinstance(largest_gap, (int, float)):
                largest_gap_text = f"{largest_gap:.1f}s"
            else:
                largest_gap_text = "n/a"
            lines.append(
                "Completeness (max gap"
                + max_gap_text
                + f"): **{comp_status}** (largest gap {largest_gap_text})"
            )

        lines.append("")
        lines.append("## Speed bin coverage")
        lines.append("")
        lines.append("| Bin | Time (s) | Distance (km) | Status |")
        lines.append("| --- | --- | --- | --- |")

        bins = payload.get("bins", {})
        for name, info in bins.items():
            bin_status = "PASS" if info.get("valid") else "FAIL"
            time_s = info.get("time_s", 0.0)
            distance_km = info.get("distance_km", 0.0)
            lines.append(
                f"| {name} | {time_s:.1f} | {distance_km:.3f} | {bin_status} |"
            )

        for name, info in bins.items():
            kpis = info.get("kpis") or {}
            if not kpis:
                continue
            lines.append("")
            lines.append(f"### KPIs – {name}")
            for kpi_name, value in kpis.items():
                if value is None:
                    formatted = "n/a"
                else:
                    formatted = f"{value:.3f}"
                lines.append(f"- **{kpi_name}**: {formatted}")

        return "\n".join(lines)


__all__ = ["AnalysisEngine", "AnalysisResult"]
