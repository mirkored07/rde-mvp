"""Server-rendered EU7-LD conformity report view."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from starlette.templating import Jinja2Templates

from src.app.reporting.eu7ld_report import group_criteria_by_section, load_report


router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


@router.get("/report/{test_id}", include_in_schema=False)
def report_view(request: Request, test_id: str):
    try:
        report = load_report(test_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Report not found") from exc
    grouped = group_criteria_by_section(report.criteria)
    return templates.TemplateResponse(
        "report.html",
        {
            "request": request,
            "report": report,
            "grouped_criteria": grouped,
        },
    )


__all__ = ["router"]

