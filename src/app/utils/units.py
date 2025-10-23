"""Unit handling utilities built on top of :mod:`pint`."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import pint


@lru_cache(maxsize=1)
def ureg() -> pint.UnitRegistry:
    """Return a process-wide :class:`~pint.UnitRegistry` instance."""

    registry = pint.UnitRegistry(autoconvert_offset_to_baseunit=True)
    # Aliases we commonly see in CSV exports (avoid unicode micro symbol).
    registry.define("degC = kelvin; offset: 273.15 = celsius")
    registry.define("ug = microgram")
    return registry


def to_quantity(value: Any, unit: str) -> pint.Quantity:
    """Create a :class:`~pint.Quantity` from ``value`` and ``unit``."""

    return float(value) * ureg()(unit)


def convert_value(value: float, src_unit: str, dst_unit: str) -> float:
    """Convert ``value`` from ``src_unit`` to ``dst_unit``."""

    quantity = float(value) * ureg()(src_unit)
    return quantity.to(dst_unit).magnitude


def normalize_temperature(value: float, unit: str, dst: str = "degC") -> float:
    """Normalize a temperature measurement to degrees Celsius."""

    return convert_value(value, unit, dst)


def normalize_massflow(value: float, unit: str, dst: str = "mg/second") -> float:
    """Normalize a mass (or equivalent) flow rate to milligrams per second."""

    return convert_value(value, unit, dst)


def normalize_exhaust_flow(value: float, unit: str, dst: str = "kg/second") -> float:
    """Normalize an exhaust flow measurement to kilograms per second."""

    return convert_value(value, unit, dst)


__all__ = [
    "ureg",
    "to_quantity",
    "convert_value",
    "normalize_temperature",
    "normalize_massflow",
    "normalize_exhaust_flow",
]
