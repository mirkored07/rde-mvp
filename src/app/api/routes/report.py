"""Read-only API endpoints serving EU7-LD conformity reports."""

from __future__ import annotations

import json
from fastapi import APIRouter, HTTPException

from src.app.reporting.eu7ld_report import apply_guardrails, load_report
from src.app.reporting.schemas import ReportData


router = APIRouter(prefix="/api/report", tags=["report"])


@router.get("/{test_id}", response_model=ReportData)
def get_report(test_id: str) -> ReportData:
    """Return the conformity report associated with ``test_id``."""

    try:
        report = load_report(test_id)
    except FileNotFoundError as exc:  # pragma: no cover - simple IO guard
        raise HTTPException(status_code=404, detail="Report not found") from exc
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=422, detail="Report is invalid") from exc
    except Exception as exc:  # pragma: no cover - consistent error surface
        raise HTTPException(status_code=422, detail="Report is invalid") from exc
    return apply_guardrails(report)


__all__ = ["router"]

