from __future__ import annotations

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
    payload.setdefault("meta", {}).setdefault("legislation", "EU7 Light-Duty")
    return templates.TemplateResponse(
        request,
        "print_eu7.html",
        {"request": request, "results_payload": payload},
        media_type="text/html",
    )
