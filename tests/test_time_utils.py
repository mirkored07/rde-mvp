from __future__ import annotations

import numpy as np
import pandas as pd

from src.app.utils import to_utc_series


def test_to_utc_series_handles_mixed_inputs() -> None:
    series = pd.Series(
        [
            "2023-01-01T00:00:00Z",
            "2023-01-01 01:00:00",
            pd.Timestamp("2023-01-01T02:00:00-05:00"),
            pd.Timestamp("2023-01-01T03:00:00"),
            None,
            np.nan,
            "invalid",
        ]
    )

    result = to_utc_series(series)

    assert str(result.dtype) == "datetime64[ns, UTC]"
    expected = pd.Series(
        [
            pd.Timestamp("2023-01-01T00:00:00Z"),
            pd.Timestamp("2023-01-01T01:00:00Z"),
            pd.Timestamp("2023-01-01T07:00:00Z"),
            pd.Timestamp("2023-01-01T03:00:00Z"),
            pd.NaT,
            pd.NaT,
            pd.NaT,
        ],
        dtype="datetime64[ns, UTC]",
    )

    pd.testing.assert_series_equal(result, expected)
