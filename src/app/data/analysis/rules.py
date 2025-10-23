"""Rule configuration helpers for the analysis engine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore


@dataclass(frozen=True)
class SpeedBin:
    """Specification for a speed bin used when aggregating KPIs."""

    name: str
    min_kmh: float | None = None
    max_kmh: float | None = None


@dataclass(frozen=True)
class AnalysisRules:
    """Container for analysis rules loaded from JSON/YAML configuration."""

    speed_bins: tuple[SpeedBin, ...]
    min_distance_km_per_bin: float | None = None
    min_time_s_per_bin: float | None = None
    completeness_max_gap_s: float | None = None
    kpi_defs: Mapping[str, Mapping[str, Any]] | None = None

    @classmethod
    def from_mapping(cls, config: Mapping[str, Any]) -> "AnalysisRules":
        speed_bins_cfg = config.get("speed_bins") or []
        bins: Iterable[SpeedBin] = (
            SpeedBin(
                name=bin_cfg["name"],
                min_kmh=bin_cfg.get("min_kmh"),
                max_kmh=bin_cfg.get("max_kmh"),
            )
            for bin_cfg in speed_bins_cfg
        )

        completeness = config.get("completeness") or {}
        max_gap = completeness.get("max_gap_s")

        return cls(
            speed_bins=tuple(bins),
            min_distance_km_per_bin=config.get("min_distance_km_per_bin"),
            min_time_s_per_bin=config.get("min_time_s_per_bin"),
            completeness_max_gap_s=max_gap,
            kpi_defs=config.get("kpi_defs") or {},
        )


def _load_mapping_from_file(path: Path) -> Mapping[str, Any]:
    text = path.read_text()

    # JSON is a subset of YAML, so try JSON first for clearer error messages.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        if yaml is None:
            raise ValueError(
                "YAML configuration requires the optional 'pyyaml' dependency"
            ) from None
        data = yaml.safe_load(text)
        if not isinstance(data, Mapping):
            raise TypeError("YAML configuration must evaluate to a mapping")
        return data


def load_rules(config: str | Path | Mapping[str, Any]) -> AnalysisRules:
    """Load :class:`AnalysisRules` from a mapping or configuration file."""

    if isinstance(config, Mapping):
        mapping = config
    else:
        path = Path(config)
        mapping = _load_mapping_from_file(path)

    return AnalysisRules.from_mapping(mapping)


__all__ = ["AnalysisRules", "SpeedBin", "load_rules"]
