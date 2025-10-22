"""Fusion engine for aligning heterogeneous telemetry streams."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence

import pandas as pd

from .specs import (
    StreamSpec,
    estimate_offset_by_correlation,
    synthesize_timestamps,
)


@dataclass
class FusionEngine:
    """Fuse multiple :class:`~StreamSpec` objects into a unified dataframe."""

    gps: StreamSpec
    streams: Sequence[StreamSpec] = field(default_factory=list)

    def _prepare(self, spec: StreamSpec) -> pd.DataFrame:
        ts = synthesize_timestamps(spec)
        df = spec.df.copy()
        df[ts.name] = ts
        return df.sort_values(ts.name)

    def _apply_offset_estimate(
        self,
        spec: StreamSpec,
        reference: StreamSpec,
        cols: Sequence[Sequence[str] | str] | tuple[str, str] | None,
        grid_hz: float,
        max_lag_s: float,
    ) -> None:
        offset = estimate_offset_by_correlation(
            spec,
            reference,
            cols=cols,
            grid_hz=grid_hz,
            max_lag_s=max_lag_s,
        )
        spec.clock_offset_s += offset

        if spec.t0 is None:
            gps_ts = synthesize_timestamps(reference)
            if not gps_ts.empty:
                spec.t0 = gps_ts.iloc[0] - pd.to_timedelta(spec.clock_offset_s, unit="s")

    def fuse(
        self,
        additional_streams: Iterable[StreamSpec] | None = None,
        *,
        estimate_offsets: bool = False,
        correlation_cols: Sequence[Sequence[str] | str] | tuple[str, str] | None = None,
        grid_hz: float = 10.0,
        max_lag_s: float = 5.0,
        merge_direction: str = "nearest",
        tolerance: str | pd.Timedelta | None = None,
    ) -> pd.DataFrame:
        """Fuse GPS with other streams into a single dataframe."""

        reference = self.gps
        synthesize_timestamps(reference)
        fused = self._prepare(reference)
        ts_col = reference.ts_col or "timestamp"

        other_specs: list[StreamSpec] = list(self.streams)
        if additional_streams:
            other_specs.extend(additional_streams)

        for spec in other_specs:
            if estimate_offsets:
                self._apply_offset_estimate(
                    spec,
                    reference,
                    cols=correlation_cols,
                    grid_hz=grid_hz,
                    max_lag_s=max_lag_s,
                )

            right = self._prepare(spec)
            right_ts_col = spec.ts_col or "timestamp"

            suffix = f"_{spec.name}" if spec.name else ""
            if suffix:
                right[f"{right_ts_col}{suffix}"] = right[right_ts_col]
                rename_map = {
                    col: f"{col}{suffix}"
                    for col in right.columns
                    if col not in {right_ts_col, f"{right_ts_col}{suffix}"}
                }
                if rename_map:
                    right = right.rename(columns=rename_map)

            fused = pd.merge_asof(
                fused,
                right,
                left_on=ts_col,
                right_on=right_ts_col,
                direction=merge_direction,
                tolerance=tolerance,
                suffixes=("", suffix),
            )

        return fused


__all__ = ["FusionEngine"]

