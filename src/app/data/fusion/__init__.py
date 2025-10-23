"""Fusion helpers for aligning ECU and GPS data streams."""

from .engine import FusionEngine
from .specs import (
    StreamSpec,
    SpecLike,
    as_spec,
    estimate_offset_by_correlation,
    synthesize_timestamps,
)

__all__ = [
    "StreamSpec",
    "SpecLike",
    "as_spec",
    "synthesize_timestamps",
    "estimate_offset_by_correlation",
    "FusionEngine",
]
