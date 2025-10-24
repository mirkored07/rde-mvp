from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Mapping

import numpy as np
import pandas as pd

UnitKind = Literal["mass_rate", "count_rate"]
# canonical column names in fused df:
# - distance_m (per-sample incremental distance)
# - <pollutant column> in SI if possible (mg/s for mass, 1/s for counts). We'll scale if not.


@dataclass(frozen=True)
class MetricDef:
    col: str  # fused column name for rate
    kind: UnitKind  # how to normalize
    out_key: str  # kpis output key stem
    si_unit: str  # expected SI rate unit


# Registry maps pollutant → rate column
REGISTRY: Dict[str, MetricDef] = {
    "NOx": MetricDef(col="nox_mg_s", kind="mass_rate", out_key="NOx_mg_per_km", si_unit="mg/s"),
    "PN": MetricDef(col="pn_1_s", kind="count_rate", out_key="PN_1_per_km", si_unit="1/s"),
    "CO": MetricDef(col="co_mg_s", kind="mass_rate", out_key="CO_mg_per_km", si_unit="mg/s"),
    "CO2": MetricDef(col="co2_g_s", kind="mass_rate", out_key="CO2_g_per_km", si_unit="g/s"),
    "THC": MetricDef(col="thc_mg_s", kind="mass_rate", out_key="THC_mg_per_km", si_unit="mg/s"),
    "NH3": MetricDef(col="nh3_mg_s", kind="mass_rate", out_key="NH3_mg_per_km", si_unit="mg/s"),
    "N2O": MetricDef(col="n2o_mg_s", kind="mass_rate", out_key="N2O_mg_per_km", si_unit="mg/s"),
    "PM": MetricDef(col="pm_mg_s", kind="mass_rate", out_key="PM_mg_per_km", si_unit="mg/s"),
}


def _ensure_column(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns


def normalize_unit_series(s: pd.Series, from_unit: str, to_unit: str) -> pd.Series:
    # minimalist scaler; integrate with pint later if available
    if from_unit == to_unit:
        return s
    scale = 1.0
    # g/s ↔ mg/s ↔ ug/s
    if from_unit == "g/s" and to_unit == "mg/s":
        scale = 1000.0
    elif from_unit == "mg/s" and to_unit == "g/s":
        scale = 1 / 1000.0
    elif from_unit == "mg/s" and to_unit == "ug/s":
        scale = 1000.0
    elif from_unit == "ug/s" and to_unit == "mg/s":
        scale = 1 / 1000.0
    else:
        # leave unchanged if unknown; caller should ensure SI on ingest
        scale = 1.0
    return s * scale


def mass_rate_to_per_km(rate: pd.Series, dist_m: pd.Series, dt_s: pd.Series, out_unit: str) -> pd.Series:
    # rate[unit/s] * dt [s] = mass unit; divide by distance [km]
    km = dist_m.cumsum() / 1000.0
    mass = (rate * dt_s).cumsum()  # same unit as rate's numerator
    # avoid divide by zero
    km = km.where(km > 0, pd.NA)
    return mass / km


def count_rate_to_per_km(rate: pd.Series, dist_m: pd.Series, dt_s: pd.Series) -> pd.Series:
    km = dist_m.cumsum() / 1000.0
    counts = (rate * dt_s).cumsum()
    km = km.where(km > 0, pd.NA)
    return counts / km


def _mask_to_weights(mask: pd.Series | np.ndarray | None, index: pd.Index) -> pd.Series:
    if mask is None:
        return pd.Series(1.0, index=index)
    if isinstance(mask, pd.Series):
        series = mask.reindex(index, fill_value=False)
    else:
        series = pd.Series(mask, index=index)
    series = series.fillna(False)
    if series.dtype != bool:
        # treat as numeric weights already
        weights = pd.to_numeric(series, errors="coerce").fillna(0.0)
    else:
        weights = series.astype(float)
    return weights


def _per_km_value(rate: pd.Series, dt_s: pd.Series, dist_m: pd.Series, mask: pd.Series | np.ndarray | None) -> float | None:
    weights = _mask_to_weights(mask, rate.index)
    effective_dt = dt_s * weights
    effective_dist = dist_m * weights
    total_distance_km = float(effective_dist.sum() / 1000.0)
    if total_distance_km <= 0:
        return None
    numerator = float((rate * effective_dt).sum())
    value = numerator / total_distance_km
    if np.isnan(value):
        return None
    return value


def _output_unit(defn: MetricDef) -> str:
    numerator, *_ = defn.si_unit.split("/")
    return f"{numerator}/km"


def compute_distance_normalized_kpis(
    df: pd.DataFrame,
    *,
    dt_col: str = "delta_time_s",
    dist_col: str = "distance_increment_m",
    rate_units: Mapping[str, str] | None = None,
    bin_masks: Mapping[str, pd.Series] | None = None,
) -> dict[str, dict[str, Any]]:
    """Compute distance-normalized KPIs for all registered pollutants."""

    if dt_col not in df.columns or dist_col not in df.columns:
        return {}

    dt = pd.to_numeric(df[dt_col], errors="coerce").fillna(0.0)
    dist = pd.to_numeric(df[dist_col], errors="coerce").fillna(0.0)
    units_map = dict(rate_units or {})
    masks = dict(bin_masks or {})

    results: dict[str, dict[str, Any]] = {}
    for pollutant, definition in REGISTRY.items():
        if not _ensure_column(df, definition.col):
            continue

        rate = pd.to_numeric(df[definition.col], errors="coerce").fillna(0.0)
        from_unit = units_map.get(definition.col, definition.si_unit)
        rate_si = normalize_unit_series(rate, from_unit, definition.si_unit)
        unit = _output_unit(definition)
        entry: dict[str, Any] = {
            "label": f"{pollutant} ({unit})",
            "unit": unit,
        }

        total_value = _per_km_value(rate_si, dt, dist, None)
        entry["total"] = {"value": total_value}

        for bin_name, mask in masks.items():
            entry[bin_name] = {"value": _per_km_value(rate_si, dt, dist, mask)}

        results[definition.out_key] = entry

    return results


__all__ = [
    "MetricDef",
    "REGISTRY",
    "compute_distance_normalized_kpis",
    "count_rate_to_per_km",
    "mass_rate_to_per_km",
    "normalize_unit_series",
]
