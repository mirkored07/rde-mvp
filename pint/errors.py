"""Minimal subset of :mod:`pint` error classes for offline testing."""

from __future__ import annotations


class PintError(Exception):
    """Base exception for the lightweight pint stub."""


class UndefinedUnitError(PintError):
    """Raised when a unit string cannot be resolved."""


class DimensionalityError(PintError):
    """Raised when attempting to convert between incompatible units."""

    def __init__(self, *args):
        super().__init__(*args)
