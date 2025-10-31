from __future__ import annotations

import pathlib

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from fastapi.templating import Jinja2Templates

from src.app.reporting.pdf import html_to_pdf_bytes
from src.app.rules.engine import evaluate_eu7_ld

router = APIRouter()

_TEMPLATE_DIR = pathlib.Path(__file__).resolve().parents[2] / "templates"
_templates = Jinja2Templates(directory=str(_TEMPLATE_DIR))


@router.get("/export_pdf", include_in_schema=False)
def export_pdf() -> Response:
    """Generate a printable EU7 Light-Duty PDF report."""

    payload = evaluate_eu7_ld(raw_inputs={})
    template = _templates.get_template("print_eu7.html")
    html_document = template.render({"results_payload": payload})

    try:
        pdf_bytes = html_to_pdf_bytes(html_document)
    except RuntimeError as exc:  # pragma: no cover - depends on optional dependency
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    headers = {"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)


__all__ = ["router"]
