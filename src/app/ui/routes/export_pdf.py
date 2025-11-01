from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import StreamingResponse
from starlette.templating import Jinja2Templates
import io

from src.app.rules.engine import evaluate_eu7_ld

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


def _render_pdf_with_weasyprint(html_str: str) -> bytes:
    # Lazy import: if not installed, surface as 503 (what the test expects)
    try:
        from weasyprint import HTML
    except Exception:
        raise HTTPException(status_code=503, detail="WeasyPrint not installed")
    return HTML(string=html_str, base_url=".").write_pdf()


@router.post("/export_pdf")
def export_pdf_post(
    # Accept BOTH shapes:
    # 1) {"results_payload": {...}}  (test path)
    # 2) {...} (direct payload if used elsewhere)
    body: dict = Body(...),
):
    results_payload = body.get("results_payload", body)
    html_str = templates.get_template("print_eu7.html").render(results_payload=results_payload)
    pdf = _render_pdf_with_weasyprint(html_str)
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'}
    )


@router.get("/export_pdf")
def export_pdf_get():
    payload = evaluate_eu7_ld(raw_inputs={})
    html_str = templates.get_template("print_eu7.html").render(results_payload=payload)
    pdf = _render_pdf_with_weasyprint(html_str)
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'}
    )
