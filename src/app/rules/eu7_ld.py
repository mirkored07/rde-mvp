"""EU7 Light-Duty regulation evaluation utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping

from src.app.regulation.eu7ld_un168_limits import (
    ACCEL_EVENTS_MIN,
    CO2_SPAN_PPM_MAX,
    CO2_ZERO_PPM_MAX,
    CO_SPAN_PPM_MAX,
    CO_WLTP_LIMIT_MG_PER_KM,
    CO_ZERO_PPM_MAX,
    COLD_START_AVG_SPEED_MAX_KMH,
    COLD_START_AVG_SPEED_MIN_KMH,
    COLD_START_EXTENDED_FACTOR,
    COLD_START_MAX_SPEED_KMH,
    COLD_START_MOVE_TIME_MAX_S,
    COLD_START_TOTAL_STOP_MAX_S,
    COLD_START_EXTENDED_TEMP_HIGH_C,
    COLD_START_EXTENDED_TEMP_LOW_C,
    CUM_POS_ELEV_TRIP_M_PER_100KM_MAX,
    CUM_POS_ELEV_URBAN_M_PER_100KM_MAX,
    GPS_ECU_DISTANCE_DIFF_PCT_MAX,
    GPS_SINGLE_GAP_S_MAX,
    GPS_TOTAL_GAPS_S_MAX,
    MOTORWAY_MIN_DISTANCE_KM,
    NOX_RDE_FINAL_MG_PER_KM,
    NOX_SPAN_PPM_MAX,
    NOX_ZERO_PPM_MAX,
    PN10_RDE_FINAL_PER_KM,
    PN_ZERO_POST_MAX_PER_CM3,
    PN_ZERO_PRE_MAX_PER_CM3,
    RPA_BREAK_KMH,
    RPA_HIGH_SPEED_MIN,
    RPA_LOW_SPEED_OFFSET,
    RPA_LOW_SPEED_SLOPE,
    SPAN_ABOVE_TWO_X_MAX_COUNT,
    SPAN_COVERAGE_MIN_PCT,
    SPAN_TWO_X_BAND_MAX_PCT,
    START_END_ELEV_ABS_M_MAX,
    TRIP_DURATION_MAX_MIN,
    TRIP_DURATION_MIN_MIN,
    URBAN_MIN_DISTANCE_KM,
    VA_POS95_ALT_HIGH_SPEED_OFFSET,
    VA_POS95_ALT_HIGH_SPEED_SLOPE,
    VA_POS95_BREAK_KMH,
    VA_POS95_HIGH_SPEED_OFFSET,
    VA_POS95_HIGH_SPEED_SLOPE,
    VA_POS95_LOW_SPEED_OFFSET,
    VA_POS95_LOW_SPEED_SLOPE,
)

_SECTION_ZERO_SPAN = "Pre/Post Checks (Zero/Span)"
_SECTION_SPAN_COVERAGE = "Span Gas Coverage"
_SECTION_TRIP = "Trip Composition & Timing"
_SECTION_COLD = "Cold-Start Window"
_SECTION_GPS = "GPS Validity"
_SECTION_DYNAMICS = "Dynamics & MAW"
_SECTION_MAW = "CO₂ Characteristic Windows (MAW)"
_SECTION_EMISSIONS = "Emissions Summary"
_SECTION_FINAL = "Final Conformity"

_PHASE_ORDER = ["urban", "rural", "motorway"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _deep_merge(base: Dict[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, Mapping):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


@dataclass
class _SpanMetrics:
    zero: float
    span: float
    coverage_pct: float
    between_pct: float
    above_two_count: int


@dataclass
class _PhaseData:
    name: str
    distance_km: float
    duration_s: float
    avg_speed_kmh: float
    nox_mg_km: float
    pn_per_km: float
    co_mg_km: float
    cum_pos_elev_m: float
    va_pos95: float
    rpa: float
    dynamic_events: int

    @classmethod
    def from_mapping(cls, name: str, payload: Mapping[str, Any], defaults: Mapping[str, Any]) -> "_PhaseData":
        merged = dict(defaults)
        merged.update({k: v for k, v in payload.items() if v is not None})
        return cls(
            name=name,
            distance_km=float(merged.get("distance_km", 0.0)),
            duration_s=float(merged.get("duration_s", 0.0)),
            avg_speed_kmh=float(merged.get("avg_speed_kmh", 0.0)),
            nox_mg_km=float(merged.get("nox_mg_km", 0.0)),
            pn_per_km=float(merged.get("pn_per_km", 0.0)),
            co_mg_km=float(merged.get("co_mg_km", 0.0)),
            cum_pos_elev_m=float(merged.get("cum_pos_elev_m", 0.0)),
            va_pos95=float(merged.get("va_pos95", 0.0)),
            rpa=float(merged.get("rpa", 0.0)),
            dynamic_events=int(float(merged.get("dynamic_events", 0))),
        )

    @property
    def duration_min(self) -> float:
        return self.duration_s / 60.0 if self.duration_s else 0.0

    @property
    def nox_mass_mg(self) -> float:
        return max(0.0, self.nox_mg_km) * max(self.distance_km, 0.0)

    @property
    def pn_count(self) -> float:
        return max(0.0, self.pn_per_km) * max(self.distance_km, 0.0)

    @property
    def co_mass_mg(self) -> float:
        return max(0.0, self.co_mg_km) * max(self.distance_km, 0.0)

    @property
    def cumulative_positive_elevation_per_100km(self) -> float:
        distance = max(self.distance_km, 1e-9)
        return (self.cum_pos_elev_m / distance) * 100.0


@dataclass
class _TripData:
    duration_s: float
    start_end_delta_m: float
    cum_pos_elev_m: float

    @property
    def duration_min(self) -> float:
        return self.duration_s / 60.0 if self.duration_s else 0.0

    def cumulative_positive_elevation_per_100km(self, distance_km: float) -> float:
        dist = max(distance_km, 1e-9)
        return (self.cum_pos_elev_m / dist) * 100.0


@dataclass
class _ColdStartData:
    avg_speed_kmh: float
    max_speed_kmh: float
    move_time_s: float
    max_stop_s: float
    extended_required: bool

    @property
    def correction_factor(self) -> float:
        return COLD_START_EXTENDED_FACTOR if self.extended_required else 1.0


@dataclass
class _GPSData:
    max_gap_s: float
    total_gap_s: float
    distance_diff_pct: float


@dataclass
class _MAWWindow:
    distance_km: float
    nox_mg: float
    pn_count: float
    valid: bool
    cold_share: float

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "_MAWWindow":
        return cls(
            distance_km=float(payload.get("distance_km", 0.0)),
            nox_mg=float(payload.get("nox_mg", payload.get("NOx_mg", 0.0))),
            pn_count=float(payload.get("pn_count", payload.get("PN_count", 0.0))),
            valid=bool(payload.get("valid", True)),
            cold_share=max(0.0, min(1.0, float(payload.get("cold_share", 0.0)))),
        )

    def corrected_values(self, factor: float) -> tuple[float | None, float | None]:
        if not self.valid or self.distance_km <= 0.0:
            return None, None
        multiplier = 1.0 + max(0.0, factor - 1.0) * self.cold_share
        distance = max(self.distance_km, 1e-9)
        nox_mg = max(0.0, self.nox_mg) * multiplier
        pn = max(0.0, self.pn_count) * multiplier
        return nox_mg / distance, pn / distance


@dataclass
class _MAWData:
    low_windows: list[_MAWWindow]
    high_windows: list[_MAWWindow]

    def iter_valid(self, factor: float) -> Iterable[tuple[float, float]]:
        for window in self.low_windows + self.high_windows:
            nox, pn = window.corrected_values(factor)
            if nox is not None and pn is not None:
                yield max(0.0, nox), max(0.0, pn)

    def coverage_percent(self, factor: float, *, high: bool) -> float:
        windows = self.high_windows if high else self.low_windows
        total = sum(max(w.distance_km, 0.0) for w in windows)
        valid = sum(max(w.distance_km, 0.0) for w in windows if w.valid)
        if total <= 0.0:
            return 0.0
        return (valid / total) * 100.0

    def valid_counts(self, *, high: bool) -> int:
        windows = self.high_windows if high else self.low_windows
        return sum(1 for w in windows if w.valid and w.distance_km > 0.0)


class _Inputs:
    def __init__(self, raw: Mapping[str, Any] | None) -> None:
        self.raw = _deep_merge(_default_inputs(), dict(raw or {}))
        phases_payload = self.raw.get("phases", {})
        defaults = self.raw.get("phase_defaults", {})
        self.phases = {
            name: _PhaseData.from_mapping(name, phases_payload.get(name, {}), defaults.get(name, {}))
            for name in _PHASE_ORDER
        }
        trip_payload = self.raw.get("trip", {})
        duration_s = float(trip_payload.get("duration_s") or sum(p.duration_s for p in self.phases.values()))
        self.trip = _TripData(
            duration_s=duration_s,
            start_end_delta_m=float(trip_payload.get("start_end_elev_delta_m", 0.0)),
            cum_pos_elev_m=float(trip_payload.get("cum_pos_elev_m", 0.0)),
        )
        cold_payload = self.raw.get("cold_start", {})
        self.cold = _ColdStartData(
            avg_speed_kmh=float(cold_payload.get("avg_speed_kmh", 0.0)),
            max_speed_kmh=float(cold_payload.get("max_speed_kmh", 0.0)),
            move_time_s=float(cold_payload.get("move_time_s", 0.0)),
            max_stop_s=float(cold_payload.get("max_stop_s", 0.0)),
            extended_required=bool(cold_payload.get("extended_temperature", False)),
        )
        gps_payload = self.raw.get("gps", {})
        self.gps = _GPSData(
            max_gap_s=float(gps_payload.get("max_gap_s", 0.0)),
            total_gap_s=float(gps_payload.get("total_gap_s", 0.0)),
            distance_diff_pct=float(gps_payload.get("distance_diff_pct", 0.0)),
        )
        span_payload = self.raw.get("span_checks", {})
        self.span = {
            "co2": _SpanMetrics(
                zero=float(span_payload.get("co2", {}).get("zero_ppm", CO2_ZERO_PPM_MAX)),
                span=float(span_payload.get("co2", {}).get("span_ppm", CO2_SPAN_PPM_MAX)),
                coverage_pct=float(span_payload.get("co2", {}).get("coverage_pct", 100.0)),
                between_pct=float(span_payload.get("co2", {}).get("between_pct", 0.0)),
                above_two_count=int(float(span_payload.get("co2", {}).get("above_two_count", 0))),
            ),
            "co": _SpanMetrics(
                zero=float(span_payload.get("co", {}).get("zero_ppm", CO_ZERO_PPM_MAX)),
                span=float(span_payload.get("co", {}).get("span_ppm", CO_SPAN_PPM_MAX)),
                coverage_pct=float(span_payload.get("co", {}).get("coverage_pct", 100.0)),
                between_pct=float(span_payload.get("co", {}).get("between_pct", 0.0)),
                above_two_count=int(float(span_payload.get("co", {}).get("above_two_count", 0))),
            ),
            "nox": _SpanMetrics(
                zero=float(span_payload.get("nox", {}).get("zero_ppm", NOX_ZERO_PPM_MAX)),
                span=float(span_payload.get("nox", {}).get("span_ppm", NOX_SPAN_PPM_MAX)),
                coverage_pct=float(span_payload.get("nox", {}).get("coverage_pct", 100.0)),
                between_pct=float(span_payload.get("nox", {}).get("between_pct", 0.0)),
                above_two_count=int(float(span_payload.get("nox", {}).get("above_two_count", 0))),
            ),
        }
        pn_payload = span_payload.get("pn", {})
        self.pn_pre_zero = float(pn_payload.get("pre_zero_cm3", PN_ZERO_PRE_MAX_PER_CM3))
        self.pn_post_zero = float(pn_payload.get("post_zero_cm3", PN_ZERO_POST_MAX_PER_CM3))
        maw_payload = self.raw.get("maw_windows", {})
        self.maw = _MAWData(
            low_windows=[_MAWWindow.from_mapping(item) for item in maw_payload.get("low", [])],
            high_windows=[_MAWWindow.from_mapping(item) for item in maw_payload.get("high", [])],
        )
        self.phase_sequence: list[str] = list(self.raw.get("phase_sequence", _PHASE_ORDER))
        self.low_power_vehicle = bool(self.raw.get("low_power_vehicle", False))

    @property
    def total_distance_km(self) -> float:
        return sum(phase.distance_km for phase in self.phases.values())

    @property
    def total_duration_min(self) -> float:
        return self.trip.duration_min

    def average_speed_kmh(self) -> float:
        distance = self.total_distance_km
        duration_h = self.trip.duration_min / 60.0
        if duration_h <= 0.0:
            return 0.0
        return distance / duration_h


def _default_inputs() -> Dict[str, Any]:
    return {
        "phase_defaults": {
            "urban": {
                "distance_km": 18.0,
                "duration_s": 2400.0,
                "avg_speed_kmh": 27.0,
                "nox_mg_km": 45.0,
                "pn_per_km": 3.6e11,
                "co_mg_km": 320.0,
                "cum_pos_elev_m": 150.0,
                "va_pos95": 16.0,
                "rpa": 0.18,
                "dynamic_events": 180,
            },
            "rural": {
                "distance_km": 24.0,
                "duration_s": 1800.0,
                "avg_speed_kmh": 48.0,
                "nox_mg_km": 38.0,
                "pn_per_km": 3.0e11,
                "co_mg_km": 280.0,
                "cum_pos_elev_m": 200.0,
                "va_pos95": 18.0,
                "rpa": 0.16,
                "dynamic_events": 180,
            },
            "motorway": {
                "distance_km": 32.0,
                "duration_s": 1800.0,
                "avg_speed_kmh": 64.0,
                "nox_mg_km": 42.0,
                "pn_per_km": 3.4e11,
                "co_mg_km": 300.0,
                "cum_pos_elev_m": 250.0,
                "va_pos95": 24.0,
                "rpa": 0.035,
                "dynamic_events": 160,
            },
        },
        "trip": {
            "duration_s": 6000.0,
            "start_end_elev_delta_m": 45.0,
            "cum_pos_elev_m": 600.0,
        },
        "cold_start": {
            "avg_speed_kmh": 28.0,
            "max_speed_kmh": 55.0,
            "move_time_s": 8.0,
            "max_stop_s": 50.0,
            "extended_temperature": False,
        },
        "gps": {
            "max_gap_s": 45.0,
            "total_gap_s": 120.0,
            "distance_diff_pct": 1.5,
        },
        "span_checks": {
            "co2": {
                "zero_ppm": 1200.0,
                "span_ppm": 3600.0,
                "coverage_pct": 99.8,
                "between_pct": 0.6,
                "above_two_count": 0,
            },
            "co": {
                "zero_ppm": 40.0,
                "span_ppm": 800.0,
                "coverage_pct": 99.6,
                "between_pct": 0.5,
                "above_two_count": 0,
            },
            "nox": {
                "zero_ppm": 1.5,
                "span_ppm": 120.0,
                "coverage_pct": 99.7,
                "between_pct": 0.4,
                "above_two_count": 0,
            },
            "pn": {
                "pre_zero_cm3": 1500.0,
                "post_zero_cm3": 1800.0,
            },
        },
        "maw_windows": {
            "low": [
                {"distance_km": 8.0, "nox_mg": 320.0, "pn_count": 3.0e12, "valid": True, "cold_share": 0.2},
                {"distance_km": 7.5, "nox_mg": 285.0, "pn_count": 2.1e12, "valid": True, "cold_share": 0.1},
            ],
            "high": [
                {"distance_km": 10.0, "nox_mg": 450.0, "pn_count": 4.0e12, "valid": True, "cold_share": 0.0},
                {"distance_km": 9.0, "nox_mg": 420.0, "pn_count": 3.5e12, "valid": True, "cold_share": 0.0},
            ],
        },
        "phase_sequence": list(_PHASE_ORDER),
    }


def _crit(
    *,
    ident: str,
    section: str,
    clause: str,
    description: str,
    condition: str,
    value: float | int | None,
    unit: str,
    result: str,
) -> Dict[str, Any]:
    pass_bool: bool | None
    if result == "n/a":
        pass_bool = None
    else:
        pass_bool = result == "pass"
    return {
        "id": ident,
        "section": section,
        "clause": clause,
        "description": description,
        "condition": condition,
        "limit": condition,
        "value": value,
        "measured": None,
        "unit": unit,
        "result": result,
        "pass": pass_bool,
    }


def _result(passed: bool | None) -> str:
    if passed is None:
        return "n/a"
    return "pass" if passed else "fail"


def _build_zero_span(inputs: _Inputs) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    rows: list[Dict[str, Any]] = []
    coverage_rows: list[Dict[str, Any]] = []
    clauses = "UN R168 Annex 7 App.2 Table 4"
    rows.append(
        _crit(
            ident="co2_zero",
            section=_SECTION_ZERO_SPAN,
            clause=clauses,
            description="CO₂ zero drift",
            condition=f"≤ {CO2_ZERO_PPM_MAX:.0f} ppm",
            value=inputs.span["co2"].zero,
            unit="ppm",
            result=_result(inputs.span["co2"].zero <= CO2_ZERO_PPM_MAX),
        )
    )
    rows.append(
        _crit(
            ident="co2_span",
            section=_SECTION_ZERO_SPAN,
            clause=clauses,
            description="CO₂ span drift",
            condition=f"≤ {CO2_SPAN_PPM_MAX:.0f} ppm",
            value=inputs.span["co2"].span,
            unit="ppm",
            result=_result(inputs.span["co2"].span <= CO2_SPAN_PPM_MAX),
        )
    )
    rows.append(
        _crit(
            ident="co_zero",
            section=_SECTION_ZERO_SPAN,
            clause=clauses,
            description="CO zero drift",
            condition=f"≤ {CO_ZERO_PPM_MAX:.0f} ppm",
            value=inputs.span["co"].zero,
            unit="ppm",
            result=_result(inputs.span["co"].zero <= CO_ZERO_PPM_MAX),
        )
    )
    rows.append(
        _crit(
            ident="co_span",
            section=_SECTION_ZERO_SPAN,
            clause=clauses,
            description="CO span drift",
            condition=f"≤ {CO_SPAN_PPM_MAX:.1f} ppm",
            value=inputs.span["co"].span,
            unit="ppm",
            result=_result(inputs.span["co"].span <= CO_SPAN_PPM_MAX),
        )
    )
    rows.append(
        _crit(
            ident="nox_zero",
            section=_SECTION_ZERO_SPAN,
            clause=clauses,
            description="NOx zero drift",
            condition=f"≤ {NOX_ZERO_PPM_MAX:.1f} ppm",
            value=inputs.span["nox"].zero,
            unit="ppm",
            result=_result(inputs.span["nox"].zero <= NOX_ZERO_PPM_MAX),
        )
    )
    rows.append(
        _crit(
            ident="nox_span",
            section=_SECTION_ZERO_SPAN,
            clause=clauses,
            description="NOx span drift",
            condition=f"≤ {NOX_SPAN_PPM_MAX:.1f} ppm",
            value=inputs.span["nox"].span,
            unit="ppm",
            result=_result(inputs.span["nox"].span <= NOX_SPAN_PPM_MAX),
        )
    )
    rows.append(
        _crit(
            ident="pn_zero_pre",
            section=_SECTION_ZERO_SPAN,
            clause="UN R168 Annex 7 App.2 §6.5.2",
            description="PN pre-zero concentration",
            condition=f"≤ {PN_ZERO_PRE_MAX_PER_CM3:.0f} #/cm³",
            value=inputs.pn_pre_zero,
            unit="#/cm³",
            result=_result(inputs.pn_pre_zero <= PN_ZERO_PRE_MAX_PER_CM3),
        )
    )
    rows.append(
        _crit(
            ident="pn_zero_post",
            section=_SECTION_ZERO_SPAN,
            clause="UN R168 Annex 7 App.2 §6.5.2",
            description="PN post-zero concentration",
            condition=f"≤ {PN_ZERO_POST_MAX_PER_CM3:.0f} #/cm³",
            value=inputs.pn_post_zero,
            unit="#/cm³",
            result=_result(inputs.pn_post_zero <= PN_ZERO_POST_MAX_PER_CM3),
        )
    )

    for pollutant, label in (("co2", "CO₂"), ("co", "CO"), ("nox", "NOx")):
        metrics = inputs.span[pollutant]
        coverage_rows.append(
            _crit(
                ident=f"{pollutant}_span_cov",
                section=_SECTION_SPAN_COVERAGE,
                clause="UN R168 Annex 7 App.2 §5.5",
                description=f"{label} span coverage",
                condition=f"≥ {SPAN_COVERAGE_MIN_PCT:.0f} %",
                value=metrics.coverage_pct,
                unit="%",
                result=_result(metrics.coverage_pct >= SPAN_COVERAGE_MIN_PCT),
            )
        )
        coverage_rows.append(
            _crit(
                ident=f"{pollutant}_span_band",
                section=_SECTION_SPAN_COVERAGE,
                clause="UN R168 Annex 7 App.2 §5.5",
                description=f"{label} points within (span, 2×span]",
                condition=f"≤ {SPAN_TWO_X_BAND_MAX_PCT:.0f} %",
                value=metrics.between_pct,
                unit="%",
                result=_result(metrics.between_pct <= SPAN_TWO_X_BAND_MAX_PCT),
            )
        )
        coverage_rows.append(
            _crit(
                ident=f"{pollutant}_span_exceed",
                section=_SECTION_SPAN_COVERAGE,
                clause="UN R168 Annex 7 App.2 §5.5",
                description=f"{label} points > 2× span",
                condition=f"≤ {SPAN_ABOVE_TWO_X_MAX_COUNT} occurrences",
                value=metrics.above_two_count,
                unit="count",
                result=_result(metrics.above_two_count <= SPAN_ABOVE_TWO_X_MAX_COUNT),
            )
        )

    return rows, coverage_rows


def _build_trip_rows(inputs: _Inputs) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    order_ok = [name for name in inputs.phase_sequence] == _PHASE_ORDER
    rows.append(
        _crit(
            ident="phase_order",
            section=_SECTION_TRIP,
            clause="UN R168 Annex 7 App.2 §6.2",
            description="Phase order urban→rural→motorway",
            condition="= 1 (order valid)",
            value=1.0 if order_ok else 0.0,
            unit="flag",
            result=_result(order_ok),
        )
    )
    urban = inputs.phases["urban"]
    motorway = inputs.phases["motorway"]
    rural = inputs.phases["rural"]
    rows.append(
        _crit(
            ident="urban_distance",
            section=_SECTION_TRIP,
            clause="UN R168 Annex 7 App.2 §6.2.1",
            description="Urban distance",
            condition=f"≥ {URBAN_MIN_DISTANCE_KM:.0f} km",
            value=urban.distance_km,
            unit="km",
            result=_result(urban.distance_km >= URBAN_MIN_DISTANCE_KM),
        )
    )
    rows.append(
        _crit(
            ident="motorway_distance",
            section=_SECTION_TRIP,
            clause="UN R168 Annex 7 App.2 §6.2.3",
            description="Motorway distance",
            condition=f"≥ {MOTORWAY_MIN_DISTANCE_KM:.0f} km",
            value=motorway.distance_km,
            unit="km",
            result=_result(motorway.distance_km >= MOTORWAY_MIN_DISTANCE_KM),
        )
    )
    rows.append(
        _crit(
            ident="rural_presence",
            section=_SECTION_TRIP,
            clause="UN R168 Annex 7 App.2 §6.2.2",
            description="Rural phase distance",
            condition="> 0 km",
            value=rural.distance_km,
            unit="km",
            result=_result(rural.distance_km > 0.0),
        )
    )
    duration_min = inputs.trip.duration_min
    rows.append(
        _crit(
            ident="trip_duration",
            section=_SECTION_TRIP,
            clause="UN R168 Annex 7 App.2 §6.3.1",
            description="Trip duration",
            condition=f"{TRIP_DURATION_MIN_MIN:.0f}–{TRIP_DURATION_MAX_MIN:.0f} min",
            value=duration_min,
            unit="min",
            result=_result(TRIP_DURATION_MIN_MIN <= duration_min <= TRIP_DURATION_MAX_MIN),
        )
    )
    rows.append(
        _crit(
            ident="elev_delta",
            section=_SECTION_TRIP,
            clause="UN R168 Annex 7 App.2 §6.4.1",
            description="Start/end elevation difference",
            condition=f"≤ {START_END_ELEV_ABS_M_MAX:.0f} m",
            value=abs(inputs.trip.start_end_delta_m),
            unit="m",
            result=_result(abs(inputs.trip.start_end_delta_m) <= START_END_ELEV_ABS_M_MAX),
        )
    )
    rows.append(
        _crit(
            ident="elev_trip",
            section=_SECTION_TRIP,
            clause="UN R168 Annex 7 App.2 §6.4.3",
            description="Cumulative positive elevation (trip)",
            condition=f"≤ {CUM_POS_ELEV_TRIP_M_PER_100KM_MAX:.0f} m/100 km",
            value=inputs.trip.cumulative_positive_elevation_per_100km(inputs.total_distance_km),
            unit="m/100 km",
            result=_result(
                inputs.trip.cumulative_positive_elevation_per_100km(inputs.total_distance_km)
                <= CUM_POS_ELEV_TRIP_M_PER_100KM_MAX
            ),
        )
    )
    rows.append(
        _crit(
            ident="elev_urban",
            section=_SECTION_TRIP,
            clause="UN R168 Annex 7 App.2 §6.4.3",
            description="Cumulative positive elevation (urban)",
            condition=f"≤ {CUM_POS_ELEV_URBAN_M_PER_100KM_MAX:.0f} m/100 km",
            value=urban.cumulative_positive_elevation_per_100km(),
            unit="m/100 km",
            result=_result(
                urban.cumulative_positive_elevation_per_100km() <= CUM_POS_ELEV_URBAN_M_PER_100KM_MAX
            ),
        )
    )
    return rows


def _build_cold_start_rows(inputs: _Inputs) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    rows.append(
        _crit(
            ident="cs_avg_speed",
            section=_SECTION_COLD,
            clause="UN R168 Annex 7 App.2 §6.6.4",
            description="Cold-start average speed",
            condition=f"{COLD_START_AVG_SPEED_MIN_KMH:.0f}–{COLD_START_AVG_SPEED_MAX_KMH:.0f} km/h",
            value=inputs.cold.avg_speed_kmh,
            unit="km/h",
            result=_result(
                COLD_START_AVG_SPEED_MIN_KMH
                <= inputs.cold.avg_speed_kmh
                <= COLD_START_AVG_SPEED_MAX_KMH
            ),
        )
    )
    rows.append(
        _crit(
            ident="cs_max_speed",
            section=_SECTION_COLD,
            clause="UN R168 Annex 7 App.2 §6.6.4",
            description="Cold-start max speed",
            condition=f"≤ {COLD_START_MAX_SPEED_KMH:.0f} km/h",
            value=inputs.cold.max_speed_kmh,
            unit="km/h",
            result=_result(inputs.cold.max_speed_kmh <= COLD_START_MAX_SPEED_KMH),
        )
    )
    rows.append(
        _crit(
            ident="cs_move_time",
            section=_SECTION_COLD,
            clause="UN R168 Annex 7 App.2 §6.6.2",
            description="Vehicle movement after start",
            condition=f"≤ {COLD_START_MOVE_TIME_MAX_S:.0f} s",
            value=inputs.cold.move_time_s,
            unit="s",
            result=_result(inputs.cold.move_time_s <= COLD_START_MOVE_TIME_MAX_S),
        )
    )
    rows.append(
        _crit(
            ident="cs_stop_time",
            section=_SECTION_COLD,
            clause="UN R168 Annex 7 App.2 §6.6.4",
            description="Cumulative stops in cold-start",
            condition=f"≤ {COLD_START_TOTAL_STOP_MAX_S:.0f} s",
            value=inputs.cold.max_stop_s,
            unit="s",
            result=_result(inputs.cold.max_stop_s <= COLD_START_TOTAL_STOP_MAX_S),
        )
    )
    rows.append(
        _crit(
            ident="cs_correction",
            section=_SECTION_COLD,
            clause="UN R168 Annex 7 App.2 §6.6.6",
            description="Cold-start correction factor",
            condition="1.0 or ×1.6 when extended temps",
            value=inputs.cold.correction_factor,
            unit="factor",
            result=_result(True),
        )
    )
    return rows


def _build_gps_rows(inputs: _Inputs) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    rows.append(
        _crit(
            ident="gps_gap_single",
            section=_SECTION_GPS,
            clause="UN R168 Annex 7 App.2 §4.6.3",
            description="Max GPS gap",
            condition=f"≤ {GPS_SINGLE_GAP_S_MAX:.0f} s",
            value=inputs.gps.max_gap_s,
            unit="s",
            result=_result(inputs.gps.max_gap_s <= GPS_SINGLE_GAP_S_MAX),
        )
    )
    rows.append(
        _crit(
            ident="gps_gap_total",
            section=_SECTION_GPS,
            clause="UN R168 Annex 7 App.2 §4.6.3",
            description="Total GPS gaps",
            condition=f"≤ {GPS_TOTAL_GAPS_S_MAX:.0f} s",
            value=inputs.gps.total_gap_s,
            unit="s",
            result=_result(inputs.gps.total_gap_s <= GPS_TOTAL_GAPS_S_MAX),
        )
    )
    rows.append(
        _crit(
            ident="gps_distance_diff",
            section=_SECTION_GPS,
            clause="UN R168 Annex 7 App.2 §4.6.3",
            description="GPS vs ECU distance",
            condition=f"≤ {GPS_ECU_DISTANCE_DIFF_PCT_MAX:.0f} %",
            value=inputs.gps.distance_diff_pct,
            unit="%",
            result=_result(abs(inputs.gps.distance_diff_pct) <= GPS_ECU_DISTANCE_DIFF_PCT_MAX),
        )
    )
    return rows


def _va_pos95_limit(avg_speed: float, low_power: bool) -> float:
    if avg_speed <= VA_POS95_BREAK_KMH:
        return VA_POS95_LOW_SPEED_SLOPE * avg_speed + VA_POS95_LOW_SPEED_OFFSET
    if low_power:
        return VA_POS95_ALT_HIGH_SPEED_SLOPE * avg_speed + VA_POS95_ALT_HIGH_SPEED_OFFSET
    return VA_POS95_HIGH_SPEED_SLOPE * avg_speed + VA_POS95_HIGH_SPEED_OFFSET


def _rpa_limit(avg_speed: float) -> float:
    if avg_speed <= RPA_BREAK_KMH:
        return RPA_LOW_SPEED_SLOPE * avg_speed + RPA_LOW_SPEED_OFFSET
    return RPA_HIGH_SPEED_MIN


def _build_dynamics_rows(inputs: _Inputs) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    clause = "Com_Regulation_EU_2023_443_am_2017-1151_Euro6e.pdf Annex IIIA App.7"
    for phase_key in _PHASE_ORDER:
        phase = inputs.phases[phase_key]
        limit = _va_pos95_limit(phase.avg_speed_kmh, inputs.low_power_vehicle)
        high_clause = (
            f"≤ {VA_POS95_ALT_HIGH_SPEED_SLOPE:.4f}·v̄+{VA_POS95_ALT_HIGH_SPEED_OFFSET:.3f}"
            if inputs.low_power_vehicle
            else f"≤ {VA_POS95_HIGH_SPEED_SLOPE:.4f}·v̄+{VA_POS95_HIGH_SPEED_OFFSET:.3f}"
        )
        condition = f"if v̄ ≤ 74.6 then ≤ 0.136·v̄+14.44; else {high_clause}"
        rows.append(
            _crit(
                ident=f"va95_{phase_key[0]}",
                section=_SECTION_DYNAMICS,
                clause=f"{clause} Table 2",
                description=f"va_pos95 ({phase.name})",
                condition=condition,
                value=phase.va_pos95,
                unit="m²/s³",
                result=_result(phase.va_pos95 <= limit),
            )
        )
        rows.append(
            _crit(
                ident=f"vbar_{phase_key[0]}",
                section=_SECTION_DYNAMICS,
                clause=f"{clause} §3.3",
                description=f"Average speed ({phase.name})",
                condition="Reported",
                value=phase.avg_speed_kmh,
                unit="km/h",
                result=_result(True),
            )
        )
        rpa_limit = _rpa_limit(phase.avg_speed_kmh)
        rpa_condition = (
            "if v̄ ≤ 94.05 then ≥ -0.0016·v̄+0.1755; else ≥ 0.025"
        )
        rows.append(
            _crit(
                ident=f"rpa_{phase_key[0]}",
                section=_SECTION_DYNAMICS,
                clause=f"{clause} §3.4",
                description=f"RPA ({phase.name})",
                condition=rpa_condition,
                value=phase.rpa,
                unit="m/s²",
                result=_result(phase.rpa >= rpa_limit),
            )
        )
        rows.append(
            _crit(
                ident=f"dyn_{phase_key[0]}",
                section=_SECTION_DYNAMICS,
                clause=f"{clause} §3.3",
                description=f"Dynamic points ({phase.name})",
                condition=f"≥ {ACCEL_EVENTS_MIN} events",
                value=phase.dynamic_events,
                unit="count",
                result=_result(phase.dynamic_events >= ACCEL_EVENTS_MIN),
            )
        )
    return rows


def _build_maw_rows(inputs: _Inputs) -> tuple[list[Dict[str, Any]], float, float]:
    rows: list[Dict[str, Any]] = []
    factor = inputs.cold.correction_factor
    low_cov = inputs.maw.coverage_percent(factor, high=False)
    high_cov = inputs.maw.coverage_percent(factor, high=True)
    rows.append(
        _crit(
            ident="maw_cov_low",
            section=_SECTION_MAW,
            clause="UN R168 Annex 7 App.2 §6.7",
            description="Low-speed window coverage",
            condition="≥ 50 %",
            value=low_cov,
            unit="%",
            result=_result(low_cov >= 50.0),
        )
    )
    rows.append(
        _crit(
            ident="maw_cov_high",
            section=_SECTION_MAW,
            clause="UN R168 Annex 7 App.2 §6.7",
            description="High-speed window coverage",
            condition="≥ 50 %",
            value=high_cov,
            unit="%",
            result=_result(high_cov >= 50.0),
        )
    )
    nox_values: list[float] = []
    pn_values: list[float] = []
    for nox, pn in inputs.maw.iter_valid(factor):
        nox_values.append(nox)
        pn_values.append(pn)
    avg_nox = sum(nox_values) / len(nox_values) if nox_values else 0.0
    avg_pn = sum(pn_values) / len(pn_values) if pn_values else 0.0
    return rows, avg_nox, avg_pn


def _build_emissions_summary_rows(
    inputs: _Inputs,
    final_nox: float,
    final_pn: float,
    co_trip: float,
) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for key, label in (("urban", "Urban"), ("rural", "Rural"), ("motorway", "Motorway")):
        phase = inputs.phases[key]
        rows.append(
            _crit(
                ident=f"nox_{key}",
                section=_SECTION_EMISSIONS,
                clause="Reported",
                description=f"NOx ({label})",
                condition="Reported",
                value=phase.nox_mg_km,
                unit="mg/km",
                result=_result(True),
            )
        )
        rows.append(
            _crit(
                ident=f"pn_{key}",
                section=_SECTION_EMISSIONS,
                clause="Reported",
                description=f"PN10 ({label})",
                condition="Reported",
                value=phase.pn_per_km,
                unit="#/km",
                result=_result(True),
            )
        )
        rows.append(
            _crit(
                ident=f"co_{key}",
                section=_SECTION_EMISSIONS,
                clause="Reported",
                description=f"CO ({label})",
                condition="Reported",
                value=phase.co_mg_km,
                unit="mg/km",
                result=_result(True),
            )
        )
    rows.append(
        _crit(
            ident="nox_trip",
            section=_SECTION_EMISSIONS,
            clause="Reported",
            description="NOx (trip)",
            condition="Reported",
            value=final_nox,
            unit="mg/km",
            result=_result(True),
        )
    )
    rows.append(
        _crit(
            ident="pn_trip",
            section=_SECTION_EMISSIONS,
            clause="Reported",
            description="PN10 (trip)",
            condition="Reported",
            value=final_pn,
            unit="#/km",
            result=_result(True),
        )
    )
    rows.append(
        _crit(
            ident="co_trip",
            section=_SECTION_EMISSIONS,
            clause="Reported",
            description="CO (trip)",
            condition="Reported",
            value=co_trip,
            unit="mg/km",
            result=_result(True),
        )
    )
    return rows


def _build_emission_rows(
    final_nox: float,
    final_pn: float,
) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    rows.append(
        _crit(
            ident="final_nox",
            section=_SECTION_FINAL,
            clause="EU 2025/1706 Annex III Table 1",
            description="Final NOx (mg/km)",
            condition=f"≤ {NOX_RDE_FINAL_MG_PER_KM:.0f} mg/km",
            value=final_nox,
            unit="mg/km",
            result=_result(final_nox <= NOX_RDE_FINAL_MG_PER_KM),
        )
    )
    rows.append(
        _crit(
            ident="final_pn",
            section=_SECTION_FINAL,
            clause="EU 2025/1706 Annex III Table 1",
            description="Final PN10 (#/km)",
            condition=f"≤ {PN10_RDE_FINAL_PER_KM:.1e} #/km",
            value=final_pn,
            unit="#/km",
            result=_result(final_pn <= PN10_RDE_FINAL_PER_KM),
        )
    )
    return rows


def _round(value: float, digits: int = 3) -> float:
    return round(float(value), digits)


def build_payload(raw_inputs: Mapping[str, Any] | None) -> Dict[str, Any]:
    inputs = _Inputs(raw_inputs)
    zero_rows, coverage_rows = _build_zero_span(inputs)
    trip_rows = _build_trip_rows(inputs)
    cold_rows = _build_cold_start_rows(inputs)
    gps_rows = _build_gps_rows(inputs)
    dynamics_rows = _build_dynamics_rows(inputs)
    maw_rows, final_nox, final_pn = _build_maw_rows(inputs)

    total_distance = inputs.total_distance_km
    co_trip = 0.0
    if total_distance > 0.0:
        co_trip = sum(phase.co_mass_mg for phase in inputs.phases.values()) / total_distance

    emissions_rows = _build_emissions_summary_rows(inputs, final_nox, final_pn, co_trip)
    final_rows = _build_emission_rows(final_nox, final_pn)

    criteria = (
        zero_rows
        + coverage_rows
        + trip_rows
        + cold_rows
        + gps_rows
        + dynamics_rows
        + maw_rows
        + emissions_rows
        + final_rows
    )

    section_order = [
        _SECTION_ZERO_SPAN,
        _SECTION_SPAN_COVERAGE,
        _SECTION_TRIP,
        _SECTION_COLD,
        _SECTION_GPS,
        _SECTION_DYNAMICS,
        _SECTION_MAW,
        _SECTION_EMISSIONS,
        _SECTION_FINAL,
    ]
    sections = [
        {"title": title, "criteria": [r for r in criteria if r["section"] == title]}
        for title in section_order
    ]

    emissions = {
        "urban": {
            "label": "Urban",
            "NOx_mg_km": _round(inputs.phases["urban"].nox_mg_km, 3),
            "PN10_hash_km": _round(inputs.phases["urban"].pn_per_km, 3),
            "CO_mg_km": _round(inputs.phases["urban"].co_mg_km, 3),
        },
        "rural": {
            "label": "Rural",
            "NOx_mg_km": _round(inputs.phases["rural"].nox_mg_km, 3),
            "PN10_hash_km": _round(inputs.phases["rural"].pn_per_km, 3),
            "CO_mg_km": _round(inputs.phases["rural"].co_mg_km, 3),
        },
        "motorway": {
            "label": "Motorway",
            "NOx_mg_km": _round(inputs.phases["motorway"].nox_mg_km, 3),
            "PN10_hash_km": _round(inputs.phases["motorway"].pn_per_km, 3),
            "CO_mg_km": _round(inputs.phases["motorway"].co_mg_km, 3),
        },
        "trip": {
            "label": "Trip",
            "NOx_mg_km": _round(final_nox, 3),
            "PN10_hash_km": _round(final_pn, 3),
            "CO_mg_km": _round(co_trip, 3),
        },
    }

    limits = {
        "NOx_mg_km_RDE": NOX_RDE_FINAL_MG_PER_KM,
        "PN10_hash_km_RDE": PN10_RDE_FINAL_PER_KM,
        "CO_mg_km_WLTP": CO_WLTP_LIMIT_MG_PER_KM,
    }

    final_block = {
        "NOx_mg_km": _round(final_nox, 3),
        "limit": NOX_RDE_FINAL_MG_PER_KM,
        "pass": final_nox <= NOX_RDE_FINAL_MG_PER_KM,
        "PN10_hash_km": _round(final_pn, 3),
        "pn_limit": PN10_RDE_FINAL_PER_KM,
        "pn_pass": final_pn <= PN10_RDE_FINAL_PER_KM,
    }

    kpi_numbers = [
        {"key": "nox_final", "label": "NOx (mg/km)", "value": _round(final_nox, 3), "unit": "mg/km"},
        {"key": "pn10_final", "label": "PN10 (#/km)", "value": _round(final_pn, 3), "unit": "#/km"},
        {
            "key": "distance_km",
            "label": "Distance (km)",
            "value": _round(inputs.total_distance_km, 3),
            "unit": "km",
        },
        {
            "key": "duration_min",
            "label": "Duration (min)",
            "value": _round(inputs.total_duration_min, 3),
            "unit": "min",
        },
    ]

    meta = {
        "legislation": "EU7 Light-Duty",
        "testId": raw_inputs.get("test_id", "demo-run") if isinstance(raw_inputs, Mapping) else "demo-run",
        "engine": raw_inputs.get("engine", "WLTP-ICE 2.0L") if isinstance(raw_inputs, Mapping) else "WLTP-ICE 2.0L",
        "propulsion": raw_inputs.get("propulsion", "ICE") if isinstance(raw_inputs, Mapping) else "ICE",
        "testStart": raw_inputs.get("test_start", _now_iso()) if isinstance(raw_inputs, Mapping) else _now_iso(),
        "printout": raw_inputs.get("printout", _now_iso()) if isinstance(raw_inputs, Mapping) else _now_iso(),
        "velocitySource": raw_inputs.get("velocity_source", "GPS") if isinstance(raw_inputs, Mapping) else "GPS",
        "total_distance_km": _round(inputs.total_distance_km, 3),
        "total_time_min": _round(inputs.total_duration_min, 3),
    }

    visual = {
        "map": {"center": {"lat": 47.07, "lon": 15.44, "zoom": 10}, "latlngs": []},
        "chart": {"series": []},
    }

    device = {
        "gasPEMS": raw_inputs.get("gas_pems", "AVL GAS 601") if isinstance(raw_inputs, Mapping) else "AVL GAS 601",
        "pnPEMS": raw_inputs.get("pn_pems", "AVL PN PEMS 483") if isinstance(raw_inputs, Mapping) else "AVL PN PEMS 483",
        "efm": raw_inputs.get("efm") if isinstance(raw_inputs, Mapping) else None,
    }

    final_conformity = {
        "NOx_mg_km": {
            "value": _round(final_nox, 3),
            "limit": NOX_RDE_FINAL_MG_PER_KM,
            "pass": final_nox <= NOX_RDE_FINAL_MG_PER_KM,
        },
        "PN10_hash_km": {
            "value": _round(final_pn, 3),
            "limit": PN10_RDE_FINAL_PER_KM,
            "pass": final_pn <= PN10_RDE_FINAL_PER_KM,
        },
    }

    final_section_rows = [r for r in criteria if r["section"] == _SECTION_FINAL]
    final_overall_pass = all((row.get("pass") is not False) for row in final_section_rows if row.get("pass") is not None)
    final_block = {"pass": final_overall_pass, "pollutants": final_section_rows}

    payload = {
        "meta": meta,
        "limits": limits,
        "criteria": criteria,
        "sections": sections,
        "emissions": emissions,
        "final": final_block,
        "final_conformity": final_conformity,
        "visual": visual,
        "kpi_numbers": kpi_numbers,
        "device": device,
        "maw": {
            "low_windows": len(inputs.maw.low_windows),
            "high_windows": len(inputs.maw.high_windows),
        },
    }
    return payload


__all__ = ["build_payload"]
