"""EU7 Light-Duty regulation metrics and report assembly."""

from __future__ import annotations

from typing import Any, Dict


def _crit(ref: str, criterion: str, condition: str, value: Any, unit: str, passed: bool) -> Dict[str, Any]:
    return {
        "ref": ref,
        "criterion": criterion,
        "condition": condition,
        "value": value,
        "unit": unit,
        "pass": bool(passed),
    }


def compute_zero_span(data: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    zmax = spec["zero_span"]["pn_zero_max_per_cm3"]
    v = data.get("pn_zero_pre", 0)
    return {
        "title": "Pre/Post Checks (Zero/Span)",
        "criteria": [
            _crit(
                "EU7-LD",
                "PN pre-zero limit",
                f"≤ {zmax}",
                v,
                "#/cm³",
                v <= zmax,
            )
        ],
    }


def compute_trip_composition(data: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    s = spec["trip_composition"]
    uk = data.get("urban_km", 0.0)
    ek = data.get("expressway_km", 0.0)
    return {
        "title": "Trip Composition & Timing",
        "criteria": [
            _crit(
                "EU7-LD",
                "Minimum urban coverage",
                f"≥ {s['urban_min_km']} km",
                uk,
                "km",
                uk >= s["urban_min_km"],
            ),
            _crit(
                "EU7-LD",
                "Minimum expressway coverage",
                f"≥ {s['expressway_min_km']} km",
                ek,
                "km",
                ek >= s["expressway_min_km"],
            ),
        ],
    }


def compute_dynamics(data: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    d = spec["dynamics"]
    vavg = data.get("avg_speed_urban_kmh", 0.0)
    lo, hi = d["urban_avg_speed_range_kmh"]
    return {
        "title": "Dynamics & MAW",
        "criteria": [
            _crit(
                "EU7-LD",
                "Urban avg speed",
                f"{lo}–{hi} km/h",
                vavg,
                "km/h",
                lo <= vavg <= hi,
            ),
        ],
    }


def compute_gps_validity(data: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    g = spec["gps"]
    mx = data.get("gps_max_loss_s", 0)
    tot = data.get("gps_total_loss_s", 0)
    return {
        "title": "GPS Validity",
        "criteria": [
            _crit(
                "EU7-LD",
                "Max GPS gap",
                f"≤ {g['max_loss_s']} s",
                mx,
                "s",
                mx <= g["max_loss_s"],
            ),
            _crit(
                "EU7-LD",
                "Total GPS gaps",
                f"≤ {g['total_loss_s']} s",
                tot,
                "s",
                tot <= g["total_loss_s"],
            ),
        ],
    }


def compute_emissions_summary(data: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    lim = spec["limits"]
    nox = data.get("nox_mg_per_km", 0.0)
    pn = data.get("pn_per_km", 0.0)
    co = data.get("co_mg_per_km", 0.0)
    rows = []
    if lim.get("nox_mg_per_km") is not None:
        rows.append(
            _crit(
                "EU7-LD",
                "NOx per km",
                f"≤ {lim['nox_mg_per_km']} mg/km",
                nox,
                "mg/km",
                nox <= lim["nox_mg_per_km"],
            )
        )
    if lim.get("pn_per_km") is not None:
        rows.append(
            _crit(
                "EU7-LD",
                "PN per km",
                f"≤ {lim['pn_per_km']} #/km",
                pn,
                "#/km",
                pn <= lim["pn_per_km"],
            )
        )
    if lim.get("co_mg_per_km") is not None:
        rows.append(
            _crit(
                "EU7-LD",
                "CO per km",
                f"≤ {lim['co_mg_per_km']} mg/km",
                co,
                "mg/km",
                co <= lim["co_mg_per_km"],
            )
        )
    return {"title": "Emissions Summary", "criteria": rows}


def compute_final_conformity(emissions_block: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    rows = list((emissions_block or {}).get("criteria", []) or [])
    if not rows:
        return {
            "title": "Final Conformity",
            "pass": False,
            "pollutants": [],
            "notes": [],
        }
    ok = all(bool(r.get("pass")) for r in rows)
    return {
        "title": "Final Conformity",
        "pass": bool(ok),
        "pollutants": rows,
        "notes": [],
    }
