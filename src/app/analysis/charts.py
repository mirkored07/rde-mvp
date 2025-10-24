from __future__ import annotations

from typing import Dict, List

import pandas as pd

# Canonical column ↔ display mapping (extend as needed)
_SERIES = [
    ("NOx", "nox_mg_s", "mg/s"),
    ("PN", "pn_1_s", "1/s"),
    ("CO", "co_mg_s", "mg/s"),
    ("CO2", "co2_g_s", "g/s"),
    ("THC", "thc_mg_s", "mg/s"),
    ("NH3", "nh3_mg_s", "mg/s"),
    ("N2O", "n2o_mg_s", "mg/s"),
    ("PM", "pm_mg_s", "mg/s"),
]


def build_pollutant_chart(df: pd.DataFrame) -> Dict[str, list]:
    """Build pollutant time-series payload from a dataframe."""

    if "timestamp" not in df.columns or len(df) == 0:
        return {"pollutants": []}

    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    t_iso = ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist()

    out: List[Dict] = []
    for key, col, unit in _SERIES:
        if col in df.columns:
            y = (
                pd.to_numeric(df[col], errors="coerce")
                .astype(float)
                .where(lambda s: s.notna(), None)
                .tolist()
            )
            out.append({"key": key, "unit": unit, "t": t_iso, "y": y})

    return {"pollutants": out}
