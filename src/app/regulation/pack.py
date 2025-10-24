"""Convenience helpers for loading and evaluating regulation packs."""

from __future__ import annotations

import functools
import pathlib
from typing import Any, Mapping

from src.app.data.analysis import AnalysisResult
from src.app.data.regulation import (
    PackEvaluation,
    RegulationPack,
    evaluate_pack as _evaluate_pack,
    load_pack,
)

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
_DEFAULT_PACK_PATH = _PROJECT_ROOT / "data" / "regpacks" / "eu7_demo.json"


@functools.lru_cache(maxsize=1)
def load_regulation_pack(
    source: str | pathlib.Path | Mapping[str, Any] | None = None,
) -> RegulationPack:
    """Load the default regulation pack, optionally overriding the source."""

    target = source if source is not None else _DEFAULT_PACK_PATH
    return load_pack(target)


def evaluate_pack(
    result: AnalysisResult | Mapping[str, Any],
    pack: RegulationPack | None = None,
) -> PackEvaluation:
    """Evaluate an analysis payload against a regulation pack."""

    payload: Mapping[str, Any]
    if isinstance(result, AnalysisResult):
        payload = result.analysis
    else:
        payload = result

    regulation = pack if pack is not None else load_regulation_pack()
    return _evaluate_pack(payload, regulation)


__all__ = ["evaluate_pack", "load_regulation_pack", "PackEvaluation", "RegulationPack"]
