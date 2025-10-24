from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

# Canonical series plus friendly fallbacks if canonical is absent
# (extend as needed)
_SERIES: List[Tuple[str, str, str, Tuple[str, ...]]] = [
    ("NOx", "nox_mg_s", "mg/s", ("nox_ppm", "nox")),  # prefer mg/s, fallback to ppm/raw
    ("PN", "pn_1_s", "1/s", ("pn",)),
    ("CO", "co_mg_s", "mg/s", ("co",)),
    ("CO2", "co2_g_s", "g/s", ("co2",)),
    ("THC", "thc_mg_s", "mg/s", ("thc",)),
    ("NH3", "nh3_mg_s", "mg/s", ("nh3",)),
    ("N2O", "n2o_mg_s", "mg/s", ("n2o",)),
    ("PM", "pm_mg_s", "mg/s", ("pm",)),
]


def _pick_column(
    df: pd.DataFrame, canonical: str, mapping: Optional[dict], fallbacks: Tuple[str, ...]
) -> Optional[str]:
    # 1) canonical present?
    if canonical in df.columns:
        return canonical
    # 2) mapped source defined?
    if mapping:
        src = (mapping.get("pems") or {}).get(canonical)
        if isinstance(src, str) and src in df.columns:
            return src
    # 3) name-based fallbacks
    for f in fallbacks:
        if f in df.columns:
            return f
    return None


def build_pollutant_chart(df: pd.DataFrame, mapping: Optional[dict] = None) -> Dict[str, list]:
    """
    Returns: {"pollutants": [ {key, unit, t:[], y:[]}, ... ] }
    - t: ISO8601 strings (UTC)
    - y: float values (NaN -> None)
    Includes series if canonical, mapped-source, or fallback names exist.
    """

    if "timestamp" not in df.columns or len(df) == 0:
        return {"pollutants": []}

    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    t_iso = ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist()

    out: List[Dict] = []
    for key, canonical, unit, falls in _SERIES:
        col = _pick_column(df, canonical, mapping, falls)
        if col:
            y = pd.to_numeric(df[col], errors="coerce").astype(np.float64)
            y = y.where(y.notna(), None).tolist()
            out.append({"key": key, "unit": unit, "t": t_iso, "y": y})

    return {"pollutants": out}
