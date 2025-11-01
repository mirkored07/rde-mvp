"""EU7-focused UI server routes."""

from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Mapping

from fastapi import APIRouter, HTTPException, Request, UploadFile, status
from fastapi.responses import FileResponse, JSONResponse
from starlette.responses import StreamingResponse
from starlette.templating import Jinja2Templates

from src.app.rules.engine import evaluate_eu7_ld
from src.app.ui.routes.export import ALLOWED_SAMPLE_FILES, build_samples_zip_bytes

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")
SAMPLES_DIR = Path(__file__).resolve().parents[4] / "data" / "samples"


def _read_upload(upload: UploadFile) -> str:
    data = upload.file.read() if upload.file else upload.read()
    if hasattr(upload.file, "seek"):
        upload.file.seek(0)
    if not data:
        return ""
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="ignore")
    return str(data)


def _parse_csv(text: str) -> List[Dict[str, str]]:
    if not text.strip():
        return []
    return list(csv.DictReader(io.StringIO(text)))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(raw: Any) -> datetime | None:
    if not raw:
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
    return mean(cleaned)


def _compute_distance_km(rows: List[Dict[str, str]]) -> tuple[float, list[datetime]]:
    timestamps: list[datetime] = []
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
        speed = speeds[idx - 1]
        if speed is None:
            speed = speeds[idx]
        if speed is None:
            continue
        distance_m += speed * delta_s

    return distance_m / 1000.0, timestamps


def _gps_latlngs(rows: List[Dict[str, str]], limit: int = 500) -> tuple[list[dict[str, float]], dict[str, float], list[list[float]]]:
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
        bounds: list[list[float]] = []
        return points, center, bounds

    lats = [p["lat"] for p in points]
    lons = [p["lon"] for p in points]
    center = {"lat": mean(lats), "lon": mean(lons)}
    bounds = [[min(lats), min(lons)], [max(lats), max(lons)]]
    return points, center, bounds


def _gps_loss_seconds(timestamps: list[datetime]) -> tuple[int, int]:
    if not timestamps:
        return 0, 0
    losses: list[int] = []
    for idx in range(1, len(timestamps)):
        current = timestamps[idx]
        previous = timestamps[idx - 1]
        if current is None or previous is None:
            continue
        gap = int(max(0.0, (current - previous).total_seconds() - 1.0))
        if gap > 0:
            losses.append(gap)
    if not losses:
        return 0, 0
    return max(losses), sum(losses)


def _build_engine_inputs(pems_rows: List[Dict[str, str]], gps_rows: List[Dict[str, str]]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    nox_values = [_safe_float(row.get("nox_mg_s")) for row in pems_rows]
    pn_values = [_safe_float(row.get("pn_1_s")) for row in pems_rows]
    speed_values = [_safe_float(row.get("veh_speed_m_s")) for row in pems_rows]

    avg_nox = _mean(nox_values)
    avg_pn = _mean(pn_values)
    avg_speed = _mean(speed_values)

    distance_km, timestamps = _compute_distance_km(pems_rows)
    if not distance_km:
        distance_km = max(0.1, (len(pems_rows) or 1) * 0.02)

    urban_km = max(10.0, round(distance_km * 0.45, 2))
    expressway_km = max(10.0, round(distance_km - urban_km, 2))
    if expressway_km < 10.0:
        expressway_km = 10.0

    avg_speed_urban = avg_speed * 3.6 if avg_speed else 28.0

    pn_zero_pre = max(0.0, (_safe_float(pems_rows[0].get("pn_1_s")) or 3200.0) / 100.0)

    mg_per_km = None
    pn_per_km = None
    if avg_nox and avg_speed and avg_speed > 0:
        mg_per_km = (avg_nox / avg_speed) * 1000
    if avg_pn and avg_speed and avg_speed > 0:
        pn_per_km = (avg_pn / avg_speed) * 1000

    points, center, bounds = _gps_latlngs(gps_rows)
    gps_max_loss, gps_total_loss = _gps_loss_seconds(timestamps)

    engine_inputs: Dict[str, Any] = {
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

    visual_data = {
        "map": {
            "center": center,
            "bounds": bounds,
            "latlngs": points,
        },
        "chart": {
            "times": [row.get("timestamp") for row in pems_rows[: len(points) or 200]],
            "series": [
                {
                    "name": "Vehicle speed",
                    "unit": "m/s",
                    "values": [value or 0.0 for value in speed_values[: len(points) or 200]],
                },
                {
                    "name": "NOx",
                    "unit": "mg/s",
                    "values": [value or 0.0 for value in nox_values[: len(points) or 200]],
                },
            ],
        },
        "distance_km": distance_km,
    }
    return engine_inputs, visual_data


@router.get("/", include_in_schema=False)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/samples/{filename}", include_in_schema=False)
def sample_file(filename: str) -> FileResponse:
    if filename not in ALLOWED_SAMPLE_FILES:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Sample not found.")
    target = (SAMPLES_DIR / filename).resolve()
    if target.parent != SAMPLES_DIR.resolve() or not target.exists():
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Sample not found.")
    return FileResponse(target, media_type="text/csv", filename=filename)


@router.get("/samples.zip", include_in_schema=False)
def samples_zip() -> StreamingResponse:
    blob = build_samples_zip_bytes()
    return StreamingResponse(
        io.BytesIO(blob),
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="samples.zip"'},
    )


@router.post("/analyze", include_in_schema=False)
async def analyze(request: Request):
    form = await request.form()
    uploads: dict[str, UploadFile] = {}
    for key in ("pems_file", "gps_file", "ecu_file"):
        value = form.get(key)
        if isinstance(value, UploadFile):
            uploads[key] = value
    if len(uploads) != 3:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Please provide PEMS, GPS, and ECU files to run the analysis.",
        )

    pems_text = _read_upload(uploads["pems_file"])
    gps_text = _read_upload(uploads["gps_file"])
    ecu_text = _read_upload(uploads["ecu_file"])
    if not pems_text or not gps_text or not ecu_text:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Uploaded files are empty.")

    pems_rows = _parse_csv(pems_text)
    gps_rows = _parse_csv(gps_text)
    ecu_rows = _parse_csv(ecu_text)
    if not pems_rows or not gps_rows:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Unable to parse uploaded datasets.")

    engine_inputs, visual_data = _build_engine_inputs(pems_rows, gps_rows)
    payload = evaluate_eu7_ld(engine_inputs)

    visual_block = payload.setdefault("visual", {})
    map_block = visual_block.setdefault("map", {})
    map_block.update(visual_data["map"])
    chart_block = visual_block.setdefault("chart", {})
    chart_block.update(visual_data.get("chart", {}))

    payload.setdefault("kpi_numbers", [])
    payload["kpi_numbers"].append({"label": "Distance (km)", "value": round(visual_data["distance_km"], 2)})
    payload.setdefault("meta", {}).setdefault("legislation", "EU7 Light-Duty")
    payload["meta"]["sources"] = {
        "pems_rows": len(pems_rows),
        "gps_rows": len(gps_rows),
        "ecu_rows": len(ecu_rows),
    }

    accept = (request.headers.get("accept") or "").lower()
    if "application/json" in accept:
        return JSONResponse({"results_payload": payload})

    return templates.TemplateResponse(
        "results.html",
        {"request": request, "results_payload": payload},
        status_code=status.HTTP_200_OK,
    )


__all__ = ["router"]
