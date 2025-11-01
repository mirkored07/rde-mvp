from __future__ import annotations

import io
from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.templating import Jinja2Templates

from src.app.rules.engine import evaluate_eu7_ld

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory="src/app/ui/templates")


class ExportIn(BaseModel):
    results_payload: dict[str, Any]


def _render_pdf(payload: dict[str, Any]) -> bytes:
    try:
        from weasyprint import HTML  # type: ignore
    except Exception:
        html = templates.get_template("print_eu7.html").render(results_payload=payload)
        return html.encode("utf-8")

    html_str = templates.get_template("print_eu7.html").render(results_payload=payload)
    return HTML(string=html_str, base_url=".").write_pdf()


@router.post("/export_pdf")
def export_pdf_post(body: ExportIn) -> StreamingResponse:
    pdf = _render_pdf(body.results_payload)
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'},
    )


@router.get("/export_pdf")
def export_pdf_get() -> StreamingResponse:
    payload = evaluate_eu7_ld(raw_inputs={})
    pdf = _render_pdf(payload)
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'},
    )


__all__ = ["router"]
