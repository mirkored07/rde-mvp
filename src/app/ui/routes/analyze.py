"""EU7 analysis demo route returning computed payload JSON."""

from __future__ import annotations

from fastapi import APIRouter, Request

from src.app.rules.engine import evaluate_eu7_ld
from src.app.ui.responses import respond_success

router = APIRouter()


@router.get("/analyze", include_in_schema=False)
def analyze(request: Request):
    """Return the EU7 payload using demo inputs for the UI preview."""

    payload = evaluate_eu7_ld({})
    return respond_success(payload)


__all__ = ["router"]
