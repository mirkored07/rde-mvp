from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from starlette.templating import Jinja2Templates
import io, json
from src.app.rules.engine import evaluate_eu7_ld

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")

def _render_pdf_with_weasyprint(html_str: str) -> bytes:
    # Lazy import: tests expect 503 if missing
    try:
        from weasyprint import HTML
    except Exception:
        raise HTTPException(status_code=503, detail="WeasyPrint not installed")
    return HTML(string=html_str, base_url=".").write_pdf()

async def _parse_results_payload(request: Request) -> dict | None:
    ctype = request.headers.get("content-type", "")
    if "application/json" in ctype:
        data = await request.json()
        rp = data.get("results_payload", data)
        return rp if isinstance(rp, dict) and rp else None
    if "application/x-www-form-urlencoded" in ctype or "multipart/form-data" in ctype:
        form = await request.form()
        raw = form.get("results_payload") or form.get("exportPdfPayload")
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            try:
                obj = json.loads(raw)
                return obj if isinstance(obj, dict) and obj else None
            except Exception:
                return None
    return None

@router.post("/export_pdf")
async def export_pdf_post(request: Request):
    results_payload = await _parse_results_payload(request)
    if not results_payload:
        # exact message required by the test
        raise HTTPException(status_code=400, detail="Results payload is required.")

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
