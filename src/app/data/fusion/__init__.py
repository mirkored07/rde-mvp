"""Fusion helpers for aligning ECU and GPS data streams."""

from .specs import (
    StreamSpec,
    estimate_offset_by_correlation,
    synthesize_timestamps,
)
from .engine import FusionEngine

__all__ = [
    "StreamSpec",
    "synthesize_timestamps",
    "estimate_offset_by_correlation",
    "FusionEngine",
]

"""Data fusion helpers."""

from .time_alignment import StreamSpec, synthesize_timestamps

__all__ = ["StreamSpec", "synthesize_timestamps"]
