from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd


def to_utc_series(ts: pd.Series | Iterable[object]) -> pd.Series:
    """
    Robustly convert a pandas Series of timestamps to tz-aware UTC datetimes.
    Accepts:
      - strings with or without trailing 'Z'
      - strings with 'T' or space separator
      - tz-aware datetimes (converted to UTC)
      - naive datetimes (assumed UTC)
    Any unparsable element becomes NaT.
    """

    if not isinstance(ts, pd.Series):
        ts = pd.Series(ts)

    # Fast path: already datetime dtype
    if pd.api.types.is_datetime64_any_dtype(ts):
        s = ts.copy()
        try:
            # tz-aware -> convert, tz-naive -> localize
            tz = getattr(s.dt, "tz", None)
            if tz is None:
                return s.dt.tz_localize("UTC")
            return s.dt.tz_convert("UTC")
        except Exception:  # pragma: no cover - fallback handles edge cases
            pass  # fall through to per-element parsing

    def _one(x: object) -> pd.Timestamp:
        if x is None:
            return pd.NaT
        if isinstance(x, (float, np.floating)) and pd.isna(x):
            return pd.NaT
        try:
            v = pd.to_datetime(x, utc=False, errors="raise")
        except Exception:
            try:
                v = pd.Timestamp(x)
            except Exception:
                return pd.NaT
        # localize or convert to UTC
        if getattr(v, "tzinfo", None) is None:
            try:
                return v.tz_localize("UTC")
            except Exception:
                return pd.NaT
        try:
            return v.tz_convert("UTC")
        except Exception:
            return pd.NaT

    s = ts.map(_one)
    # Ensure dtype is datetime64[ns, UTC]
    if not pd.api.types.is_datetime64tz_dtype(s):
        s = pd.to_datetime(s, utc=True, errors="coerce")
    return s
