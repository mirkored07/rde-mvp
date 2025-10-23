"""Extremely small subset of :mod:`pint` used for unit conversions in tests."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import DimensionalityError, UndefinedUnitError


_MASS_FLOW_FACTORS = {
    "kg/second": 1_000_000.0,  # -> mg/s
    "g/second": 1_000.0,
    "mg/second": 1.0,
    "ug/second": 0.001,
}

_TEMPERATURE_UNITS = {"kelvin", "degC"}


def _clean_unit(unit: str) -> str:
    return unit.replace("°", "").strip()


@dataclass
class Quantity:
    magnitude: float
    unit: str
    _registry: "UnitRegistry"

    def to(self, target: str) -> "Quantity":
        new_value = self._registry.convert(self.magnitude, self.unit, target)
        return Quantity(new_value, target, self._registry)


class _Unit:
    def __init__(self, registry: "UnitRegistry", unit: str) -> None:
        self._registry = registry
        self._unit = unit

    def __rmul__(self, magnitude: float) -> Quantity:
        return Quantity(float(magnitude), self._unit, self._registry)


class UnitRegistry:
    """Very small stub mimicking :class:`pint.UnitRegistry`."""

    def __init__(self, autoconvert_offset_to_baseunit: bool = False) -> None:
        self.autoconvert_offset_to_baseunit = autoconvert_offset_to_baseunit
        self._aliases: dict[str, str] = {}
        self._init_defaults()

    def _init_defaults(self) -> None:
        aliases = {
            "kg/second": "kg/second",
            "kg/s": "kg/second",
            "kilogram/second": "kg/second",
            "g/second": "g/second",
            "g/s": "g/second",
            "gram/second": "g/second",
            "mg/second": "mg/second",
            "mg/s": "mg/second",
            "milligram/second": "mg/second",
            "ug/second": "ug/second",
            "ug/s": "ug/second",
            "microgram/second": "ug/second",
            "µg/s": "ug/second",
            "kelvin": "kelvin",
            "k": "kelvin",
            "degc": "degC",
            "degC": "degC",
            "celsius": "degC",
        }
        self._aliases.update(aliases)

    def define(self, definition: str) -> None:
        # Support the simple aliases used in tests.
        text = definition.strip()
        if text.startswith("degC"):
            self._aliases["degc"] = "degC"
            self._aliases["degC"] = "degC"
            self._aliases["celsius"] = "degC"
        elif text.startswith("ug"):
            self._aliases["ug"] = "ug"
            self._aliases["microgram"] = "ug"

    def __call__(self, unit: str) -> _Unit:
        canonical = self._normalize_unit(unit)
        return _Unit(self, canonical)

    def convert(self, magnitude: float, src: str, dst: str) -> float:
        src_unit = self._normalize_unit(src)
        dst_unit = self._normalize_unit(dst)
        if src_unit == dst_unit:
            return float(magnitude)
        if src_unit in _MASS_FLOW_FACTORS and dst_unit in _MASS_FLOW_FACTORS:
            base = float(magnitude) * _MASS_FLOW_FACTORS[src_unit]
            return base / _MASS_FLOW_FACTORS[dst_unit]
        if src_unit in _TEMPERATURE_UNITS and dst_unit in _TEMPERATURE_UNITS:
            return self._convert_temperature(float(magnitude), src_unit, dst_unit)
        raise DimensionalityError(src, dst)

    def _normalize_unit(self, unit: str) -> str:
        key = _clean_unit(unit)
        if key in self._aliases:
            return self._aliases[key]
        lowered = key.lower()
        if lowered in self._aliases:
            return self._aliases[lowered]
        raise UndefinedUnitError(unit)

    def _convert_temperature(self, value: float, src: str, dst: str) -> float:
        if src == dst:
            return value
        if src == "kelvin" and dst == "degC":
            return value - 273.15
        if src == "degC" and dst == "kelvin":
            return value + 273.15
        raise DimensionalityError(src, dst)


__all__ = ["UnitRegistry", "Quantity", "DimensionalityError", "UndefinedUnitError"]
