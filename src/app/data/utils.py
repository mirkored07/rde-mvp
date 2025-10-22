"""Utility helpers for working with PEMS ingestion metadata."""

from __future__ import annotations

from typing import Iterable, Mapping

from .schemas import (
    AUX_OPTIONAL,
    CORE_REQUIRED,
    GASES_OPTIONAL,
    PARTICLE_OPTIONAL,
    PEMSConfig,
)

_GROUPS: tuple[tuple[str, Iterable[str]], ...] = (
    ("Core required", CORE_REQUIRED),
    ("Gas analyzers", GASES_OPTIONAL),
    ("Particle measurements", PARTICLE_OPTIONAL),
    ("Auxiliary signals", AUX_OPTIONAL),
)


def summarize_columns(mapping: Mapping[str, str] | PEMSConfig) -> str:
    """Return a human readable summary of normalized column mappings.

    Parameters
    ----------
    mapping:
        Either a plain ``Mapping`` of normalized names to source column labels or
        an already validated :class:`~.schemas.PEMSConfig` instance.

    Returns
    -------
    str
        A multi-line string grouping the present mappings by measurement type and
        highlighting any required fields that are missing. This is intended for
        quick inspection in notebooks, debug logs, or CLI tools.
    """

    if isinstance(mapping, PEMSConfig):
        columns = mapping.columns
    else:
        columns = mapping

    lines: list[str] = []
    for label, keys in _GROUPS:
        present = [key for key in keys if key in columns]
        if not present:
            continue
        lines.append(f"{label} ({len(present)}):")
        for key in present:
            lines.append(f"  - {key} -> {columns[key]}")

    missing_required = [key for key in CORE_REQUIRED if key not in columns]
    if missing_required:
        lines.append(
            "Missing required columns: " + ", ".join(sorted(missing_required))
        )

    if not lines:
        return "No known PEMS columns mapped."

    return "\n".join(lines)


__all__ = ["summarize_columns"]
