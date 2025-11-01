from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Request
from starlette.templating import Jinja2Templates

from src.app.rules.engine import evaluate_eu7_ld

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


@router.get("/print_preview", include_in_schema=False)
def print_preview(request: Request):
    payload = evaluate_eu7_ld({})
    payload.setdefault("kpi_numbers", [])
    payload.setdefault("visual", {}).setdefault("map", {})
    payload.setdefault("visual", {}).setdefault("chart", {})
    meta = dict(payload.get("meta") or {})
    meta.setdefault("legislation", "EU7 Light-Duty")
    meta.setdefault("test_id", "demo-run")
    meta.setdefault("engine", "WLTP-ICE 2.0L")
    meta.setdefault("propulsion", "ICE")
    meta.setdefault("velocity_source", "GPS")
    stamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    meta.setdefault("test_start", stamp)
    meta.setdefault("printout", stamp)
    meta.setdefault("co_mg_per_km", meta.get("co_mg_per_km", 0.0))
    devices = dict(meta.get("devices") or {})
    devices.setdefault("gas_pems", "AVL GAS 601")
    devices.setdefault("pn_pems", "AVL PN PEMS 483")
    meta["devices"] = devices
    payload["meta"] = meta
    payload["emissions"] = {
        "urban": {"label": "Urban"},
        "trip": {
            "label": "Trip",
            "NOx_mg_km": meta.get("nox_mg_per_km"),
            "PN_hash_km": meta.get("pn_per_km"),
            "CO_mg_km": meta.get("co_mg_per_km"),
        },
    }
    return templates.TemplateResponse(
        request,
        "print_eu7.html",
        {"request": request, "results_payload": payload},
        media_type="text/html",
    )
