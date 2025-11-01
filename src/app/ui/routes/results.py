"""EU7 results route wiring for the UI."""

from __future__ import annotations

from fastapi import APIRouter, Request
from starlette.templating import Jinja2Templates

from src.app.rules.engine import evaluate_eu7_ld
from src.app.ui.responses import respond_success

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


def _build_inputs_from_session_or_demo(request: Request) -> dict:
    """Extract ingested inputs for the EU7 evaluation.

    The request may carry ingestion data on ``request.state``; fall back to
    an empty mapping to ensure the engine always receives a valid payload.
    """

    data = getattr(request, "state", None)
    inputs = getattr(data, "ingested", None) if data else None
    return inputs or {}


@router.get("/results", include_in_schema=False)
def results(request: Request):
    raw_inputs = _build_inputs_from_session_or_demo(request)
    payload = evaluate_eu7_ld(raw_inputs)
    payload.setdefault("kpi_numbers", [])
    payload.setdefault("visual", {}).setdefault("map", {})
    payload.setdefault("visual", {}).setdefault("chart", {})
    payload.setdefault("meta", {}).setdefault("legislation", "EU7 Light-Duty")
    accept = (request.headers.get("accept") or "").lower()
    if "application/json" in accept:
        return respond_success(payload)
    return templates.TemplateResponse(
        request,
        "results.html",
        {"request": request, "results_payload": payload},
    )


__all__ = ["router"]
