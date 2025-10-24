"""Utilities to ingest PEMS measurements into a normalized DataFrame."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
from pandas.api.types import is_numeric_dtype
from pint.errors import DimensionalityError, UndefinedUnitError

from src.app.data.schemas import (
    ALLOWED,
    AUX_OPTIONAL,
    CORE_REQUIRED,
    GASES_OPTIONAL,
    PARTICLE_OPTIONAL,
    PEMSConfig,
)
from src.app.utils import (
    normalize_exhaust_flow,
    normalize_massflow,
    normalize_temperature,
    to_utc_series,
)

ORDERED: list[str] = list(CORE_REQUIRED) + list(GASES_OPTIONAL) + list(PARTICLE_OPTIONAL) + list(AUX_OPTIONAL)

_TEMPERATURE_COLUMNS = {"temp_c", "exhaust_temp_c", "amb_temp_c"}
_MASSFLOW_COLUMNS = {
    "nox_mg_s",
    "no_mg_s",
    "no2_mg_s",
    "co_mg_s",
    "co2_mg_s",
    "thc_mg_s",
    "nmhc_mg_s",
    "ch4_mg_s",
    "nh3_mg_s",
    "pm_mg_s",
}
_EXHAUST_FLOW_COLUMNS = {"exhaust_flow_kg_s"}


def _validated_mapping(mapping: Mapping[str, str] | PEMSConfig | None) -> dict[str, str]:
    if mapping is None:
        return {}
    if isinstance(mapping, PEMSConfig):
        return dict(mapping.columns)
    return dict(PEMSConfig(columns=mapping).columns)


def _validated_units(units: Mapping[str, str] | None) -> dict[str, str]:
    if not units:
        return {}
    unknown = set(units.keys()) - ALLOWED
    if unknown:
        raise ValueError(
            f"Units mapping contains unknown normalized columns: {sorted(unknown)}"
        )
    return dict(units)


def _apply_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    if not mapping:
        return df
    rename: dict[str, str] = {}
    for canonical, raw in mapping.items():
        if raw in df.columns:
            rename[raw] = canonical
    if rename:
        df = df.rename(columns=rename)
    return df


def _convert_series(
    series: pd.Series,
    *,
    unit: str,
    column: str,
    converter,
) -> pd.Series:
    mask = series.notna()
    if not mask.any():
        return series
    converted = series.copy()
    converted.loc[mask] = [converter(float(value), unit) for value in series.loc[mask]]
    return converted


def _convert_temperature(series: pd.Series, *, unit: str, column: str) -> pd.Series:
    return _convert_series(series, unit=unit, column=column, converter=normalize_temperature)


def _convert_massflow(series: pd.Series, *, unit: str, column: str) -> pd.Series:
    try:
        return _convert_series(series, unit=unit, column=column, converter=normalize_massflow)
    except (DimensionalityError, UndefinedUnitError) as exc:  # pragma: no cover
        raise ValueError(
            (
                f"Column '{column}' with unit '{unit}' cannot be converted to mg/s. "
                "Convert concentration units (e.g. ppm) to mass flow using exhaust flow, "
                "temperature, pressure, and molar mass before ingesting."
            )
        ) from exc


def _convert_exhaust_flow(series: pd.Series, *, unit: str, column: str) -> pd.Series:
    return _convert_series(series, unit=unit, column=column, converter=normalize_exhaust_flow)


def _apply_units(df: pd.DataFrame, units: Mapping[str, str]) -> pd.DataFrame:
    if not units:
        return df

    df = df.copy()
    for column, unit in units.items():
        if column not in df.columns:
            continue
        if column in _TEMPERATURE_COLUMNS:
            df[column] = _convert_temperature(df[column], unit=unit, column=column)
        elif column in _MASSFLOW_COLUMNS:
            df[column] = _convert_massflow(df[column], unit=unit, column=column)
        elif column in _EXHAUST_FLOW_COLUMNS:
            df[column] = _convert_exhaust_flow(df[column], unit=unit, column=column)
        else:
            raise ValueError(
                f"Units normalization for column '{column}' is not supported."
            )
    return df


def _normalize(
    df: pd.DataFrame,
    *,
    mapping: Mapping[str, str] | PEMSConfig | None,
    units: Mapping[str, str] | None,
) -> pd.DataFrame:
    if df.empty:
        raise ValueError("PEMS data must contain at least one row.")

    mapping_dict = _validated_mapping(mapping)
    units_dict = _validated_units(units)

    df = _apply_mapping(df, mapping_dict)

    missing_core = [column for column in CORE_REQUIRED if column not in df.columns]
    if missing_core:
        missing_text = ", ".join(sorted(missing_core))
        raise ValueError(f"PEMS data is missing required columns: {missing_text}.")

    df = df.copy()
    df["timestamp"] = to_utc_series(df["timestamp"])
    if df["timestamp"].isna().any():
        raise ValueError("PEMS timestamps could not be parsed into UTC datetimes.")

    for column in df.columns:
        if column == "timestamp":
            continue
        series = df[column]
        if not is_numeric_dtype(series):
            df[column] = pd.to_numeric(series, errors="coerce")

    df = _apply_units(df, units_dict)

    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)

    ordered = [column for column in ORDERED if column in df.columns]
    if not ordered:
        return df
    return df[ordered].copy()


class PEMSReader:
    """PEMS ingestion (MVP): CSV -> normalized DataFrame with SI units."""

    @staticmethod
    def from_csv(
        path: str,
        *,
        columns: Mapping[str, str] | PEMSConfig | None = None,
        units: Mapping[str, str] | None = None,
        **read_csv_kwargs,
    ) -> pd.DataFrame:
        frame = pd.read_csv(path, **read_csv_kwargs)
        return _normalize(frame, mapping=columns, units=units)


__all__ = ["PEMSReader", "ORDERED"]
