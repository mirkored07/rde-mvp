from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import StreamingResponse
from starlette.templating import Jinja2Templates
import io

from src.app.rules.engine import evaluate_eu7_ld

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


def _render_pdf_with_weasyprint(html_str: str) -> bytes:
    # Lazy import so CI surfaces as 503 if missing
    try:
        from weasyprint import HTML
    except Exception:
        raise HTTPException(status_code=503, detail="WeasyPrint not installed")
    return HTML(string=html_str, base_url=".").write_pdf()


@router.post("/export_pdf")
def export_pdf_post(results_payload: dict = Body(..., embed=True)):
    # Use the payload sent by the client (matches test)
    html_str = templates.get_template("print_eu7.html").render(results_payload=results_payload)
    pdf = _render_pdf_with_weasyprint(html_str)
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'}
    )


@router.get("/export_pdf")
def export_pdf_get():
    # Fallback path (no sessions): compute fresh payload
    payload = evaluate_eu7_ld(raw_inputs={})
    html_str = templates.get_template("print_eu7.html").render(results_payload=payload)
    pdf = _render_pdf_with_weasyprint(html_str)
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'}
    )
