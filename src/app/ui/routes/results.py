"""EU7 results route wiring for the UI."""

from __future__ import annotations

import json

from fastapi import APIRouter, Request
from starlette.templating import Jinja2Templates

from src.app.rules.engine import evaluate_eu7_ld
from src.app.ui.responses import respond_success
from src.app.ui.routes._eu7_payload import build_normalised_payload, enrich_payload

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
    enriched = enrich_payload(
        payload,
        visual_data=payload.get("visual") or {},
        row_counts={},
        meta_overrides={
            "co_mg_per_km": raw_inputs.get("co_mg_per_km", 0.0),
            "test_id": raw_inputs.get("test_id", "demo-run"),
        },
    )
    normalised = build_normalised_payload(enriched)

    accept = (request.headers.get("accept") or "").lower()
    if "application/json" in accept:
        return respond_success(normalised)

    payload_json = json.dumps(normalised, ensure_ascii=False)
    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "results_payload": normalised,
            "results_payload_json": payload_json,
        },
    )


__all__ = ["router"]
