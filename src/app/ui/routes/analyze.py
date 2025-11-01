"""UI analysis endpoint that accepts demo CSV uploads and returns EU7 results."""

from __future__ import annotations

import statistics
from datetime import datetime
from typing import Any, Iterable, Sequence

from fastapi import APIRouter, File, Request, UploadFile
from starlette.templating import Jinja2Templates

from src.app.data.ingestion.ecu_reader import read_ecu_csv
from src.app.data.ingestion.gps_reader import read_gps_csv
from src.app.data.ingestion.pems_reader import read_pems_csv
from src.app.rules.engine import evaluate_eu7_ld
from src.app.ui.responses import respond_success
from src.app.utils.payload import ensure_results_payload_defaults

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


async def _as_text(upload: UploadFile | None) -> str | None:
    """Read *upload* and return decoded text."""

    if not upload:
        return None

    data = await upload.read()
    try:  # reset stream for potential reuse in other middlewares
        await upload.seek(0)
    except Exception:  # pragma: no cover - UploadFile.seek may be sync depending on backend
        try:
            upload.file.seek(0)  # type: ignore[attr-defined]
        except Exception:
            pass

    if not data:
        return ""
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="ignore")
    return str(data)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(raw: Any) -> datetime | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _mean(values: Iterable[float | None]) -> float | None:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return statistics.mean(cleaned)


def _compute_distance_km(rows: Sequence[dict[str, Any]]) -> tuple[float, list[datetime | None]]:
    timestamps: list[datetime | None] = []
    speeds: list[float | None] = []
    for row in rows:
        timestamps.append(_parse_timestamp(row.get("timestamp")))
        speeds.append(_safe_float(row.get("veh_speed_m_s") or row.get("speed_m_s")))

    distance_m = 0.0
    for idx in range(1, len(timestamps)):
        current = timestamps[idx]
        previous = timestamps[idx - 1]
        if current is None or previous is None:
            continue
        delta_s = max(0.0, (current - previous).total_seconds())
        if delta_s == 0:
            continue
        speed = speeds[idx - 1] if speeds[idx - 1] is not None else speeds[idx]
        if speed is None:
            continue
        distance_m += speed * delta_s

    return distance_m / 1000.0, timestamps


def _gps_latlngs(
    rows: Sequence[dict[str, Any]],
    limit: int = 500,
) -> tuple[list[dict[str, float]], dict[str, float], list[list[float]]]:
    points: list[dict[str, float]] = []
    for row in rows:
        lat = _safe_float(row.get("lat"))
        lon = _safe_float(row.get("lon"))
        if lat is None or lon is None:
            continue
        points.append({"lat": lat, "lon": lon})
        if len(points) >= limit:
            break

    if not points:
        center = {"lat": 48.2082, "lon": 16.3738}
        return points, center, []

    lats = [p["lat"] for p in points]
    lons = [p["lon"] for p in points]
    center = {"lat": statistics.mean(lats), "lon": statistics.mean(lons)}
    bounds = [[min(lats), min(lons)], [max(lats), max(lons)]]
    return points, center, bounds


def _gps_loss_seconds(timestamps: Sequence[datetime | None]) -> tuple[int, int]:
    if not timestamps:
        return 0, 0

    gaps: list[int] = []
    for idx in range(1, len(timestamps)):
        current = timestamps[idx]
        previous = timestamps[idx - 1]
        if current is None or previous is None:
            continue
        gap = int(max(0.0, (current - previous).total_seconds() - 1.0))
        if gap > 0:
            gaps.append(gap)

    if not gaps:
        return 0, 0

    return max(gaps), sum(gaps)


def _prepare_inputs(
    pems_rows: Sequence[dict[str, Any]],
    gps_rows: Sequence[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    nox_values = [_safe_float(row.get("nox_mg_s")) for row in pems_rows]
    pn_values = [_safe_float(row.get("pn_1_s")) for row in pems_rows]
    speed_values = [_safe_float(row.get("veh_speed_m_s") or row.get("speed_m_s")) for row in pems_rows]

    avg_nox = _mean(nox_values)
    avg_pn = _mean(pn_values)
    avg_speed = _mean(speed_values)

    distance_km, pems_timestamps = _compute_distance_km(pems_rows)
    if distance_km <= 0:
        fallback_rows = len(pems_rows) or len(gps_rows) or 1
        distance_km = max(0.1, fallback_rows * 0.02)

    urban_km = max(10.0, round(distance_km * 0.45, 2))
    expressway_km = max(10.0, round(distance_km - urban_km, 2))
    if expressway_km < 10.0:
        expressway_km = 10.0

    avg_speed_urban = (avg_speed * 3.6) if avg_speed is not None else 28.0

    pn_zero_pre = 3200.0 / 100.0
    if pems_rows:
        pn_zero = _safe_float(pems_rows[0].get("pn_1_s"))
        if pn_zero is not None:
            pn_zero_pre = max(0.0, pn_zero / 100.0)

    mg_per_km = None
    pn_per_km = None
    if avg_nox and avg_speed and avg_speed > 0:
        mg_per_km = (avg_nox / avg_speed) * 1000
    if avg_pn and avg_speed and avg_speed > 0:
        pn_per_km = (avg_pn / avg_speed) * 1000

    gps_points, gps_center, gps_bounds = _gps_latlngs(gps_rows)
    gps_timestamps = [_parse_timestamp(row.get("timestamp")) for row in gps_rows]
    gps_max_loss, gps_total_loss = _gps_loss_seconds(gps_timestamps or pems_timestamps)

    count = len(pems_rows) or len(gps_rows) or 200
    capped = min(count, 200)
    visual = {
        "map": {
            "center": gps_center,
            "bounds": gps_bounds,
            "latlngs": gps_points,
        },
        "chart": {
            "times": [row.get("timestamp") for row in pems_rows[:capped]],
            "series": [
                {
                    "name": "Vehicle speed",
                    "unit": "m/s",
                    "values": [value or 0.0 for value in speed_values[:capped]],
                },
                {
                    "name": "NOx",
                    "unit": "mg/s",
                    "values": [value or 0.0 for value in nox_values[:capped]],
                },
            ],
        },
        "distance_km": distance_km,
        "avg_speed_m_s": avg_speed or 0.0,
    }

    if not visual["chart"]["series"]:
        visual["chart"]["series"] = [
            {"name": "Vehicle speed", "unit": "m/s", "values": []},
            {"name": "NOx", "unit": "mg/s", "values": []},
        ]

    engine_inputs = {
        "pn_zero_pre": pn_zero_pre,
        "urban_km": urban_km,
        "expressway_km": expressway_km,
        "avg_speed_urban_kmh": avg_speed_urban,
        "gps_max_loss_s": gps_max_loss,
        "gps_total_loss_s": gps_total_loss,
        "nox_mg_per_km": mg_per_km or 5500.0,
        "pn_per_km": pn_per_km or 9_500_000.0,
        "co_mg_per_km": 0.0,
    }

    return engine_inputs, visual


def _safe_readers(
    pems_txt: str | None,
    gps_txt: str | None,
    ecu_txt: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    pems_rows: list[dict[str, Any]] = []
    gps_rows: list[dict[str, Any]] = []
    ecu_rows: list[dict[str, Any]] = []

    if pems_txt:
        try:
            pems_rows = read_pems_csv(pems_txt)
        except Exception:
            pems_rows = []

    if gps_txt:
        try:
            gps_rows = read_gps_csv(gps_txt)
        except Exception:
            gps_rows = []

    if ecu_txt:
        try:
            ecu_rows = read_ecu_csv(ecu_txt)
        except Exception:
            ecu_rows = []

    return pems_rows, gps_rows, ecu_rows


@router.post("/analyze", include_in_schema=False)
async def analyze(
    request: Request,
    pems_file: UploadFile | None = File(default=None),
    gps_file: UploadFile | None = File(default=None),
    ecu_file: UploadFile | None = File(default=None),
):
    """Process uploaded demo CSV files and return an EU7 results payload."""

    accept = (request.headers.get("accept") or "").lower()

    pems_txt = await _as_text(pems_file)
    gps_txt = await _as_text(gps_file)
    ecu_txt = await _as_text(ecu_file)

    pems_rows, gps_rows, ecu_rows = _safe_readers(pems_txt, gps_txt, ecu_txt)

    engine_inputs, visual_data = _prepare_inputs(pems_rows, gps_rows)

    payload = evaluate_eu7_ld(engine_inputs)

    visual_payload = dict(payload.get("visual") or {})
    map_payload = dict(visual_payload.get("map") or {})
    map_payload.update(visual_data.get("map", {}))
    visual_payload["map"] = map_payload

    chart_payload = dict(visual_payload.get("chart") or {})
    chart_payload.update(visual_data.get("chart", {}))
    visual_payload["chart"] = chart_payload
    visual_payload["distance_km"] = visual_data.get("distance_km", 0.0)
    payload["visual"] = visual_payload

    kpi_numbers = list(payload.get("kpi_numbers") or [])

    def _upsert_kpi(key: str, label: str, value: float, unit: str) -> None:
        for entry in kpi_numbers:
            if entry.get("key") == key or entry.get("label") == label:
                entry.update({"key": key, "label": label, "value": value, "unit": unit})
                return
        kpi_numbers.append({"key": key, "label": label, "value": value, "unit": unit})

    distance_km = round(visual_data.get("distance_km", 0.0), 2)
    _upsert_kpi("total_distance_km", "Distance (km)", distance_km, "km")

    avg_speed = visual_data.get("avg_speed_m_s")
    if avg_speed is not None:
        _upsert_kpi("avg_speed_kmh", "Avg speed (km/h)", round(avg_speed * 3.6, 1), "km/h")
    payload["kpi_numbers"] = kpi_numbers

    meta = dict(payload.get("meta") or {})
    meta.setdefault("legislation", "EU7 Light-Duty")
    meta["sources"] = {
        "pems_rows": len(pems_rows),
        "gps_rows": len(gps_rows),
        "ecu_rows": len(ecu_rows),
    }
    payload["meta"] = meta

    normalised_payload = ensure_results_payload_defaults(payload)

    if "application/json" in accept:
        return respond_success(normalised_payload)

    return templates.TemplateResponse(
        "results.html",
        {"request": request, "results_payload": normalised_payload},
        media_type="text/html",
    )


__all__ = ["router"]
