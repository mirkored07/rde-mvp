from __future__ import annotations

import io

from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import StreamingResponse
from starlette.templating import Jinja2Templates

from src.app.rules.engine import evaluate_eu7_ld

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory="src/app/ui/templates")


def _render_pdf_with_weasyprint(html_str: str) -> bytes:
    try:
        from weasyprint import HTML  # lazy import for CI
    except Exception:
        raise HTTPException(status_code=503, detail="WeasyPrint not installed")
    return HTML(string=html_str, base_url=".").write_pdf()


@router.post("/export_pdf")
def export_pdf_post(results_payload: dict = Body(..., embed=True)) -> StreamingResponse:
    # Use the provided on-page payload
    html_str = templates.get_template("print_eu7.html").render(results_payload=results_payload)
    pdf = _render_pdf_with_weasyprint(html_str)
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'},
    )


@router.get("/export_pdf")
def export_pdf_get() -> StreamingResponse:
    # Fallback: compute fresh EU7 payload and render
    payload = evaluate_eu7_ld(raw_inputs={})
    html_str = templates.get_template("print_eu7.html").render(results_payload=payload)
    pdf = _render_pdf_with_weasyprint(html_str)
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'},
    )


__all__ = ["router"]
