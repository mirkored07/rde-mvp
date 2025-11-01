from __future__ import annotations

import io
import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.templating import Jinja2Templates

from src.app.rules.engine import evaluate_eu7_ld

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


def _render_pdf_with_weasyprint(html_str: str) -> bytes:
    """Render the supplied HTML string to a PDF document."""

    try:
        from weasyprint import HTML  # type: ignore
    except Exception as exc:  # pragma: no cover - dependency missing in CI
        raise HTTPException(status_code=503, detail="WeasyPrint not installed") from exc

    return HTML(string=html_str, base_url=".").write_pdf()


async def _extract_results_payload(request: Request) -> dict:
    """Return a results payload from JSON or form-encoded submissions."""

    content_type = request.headers.get("content-type", "")

    if "application/json" in content_type:
        data = await request.json()
        if not isinstance(data, dict):
            return {}
        payload = data.get("results_payload", data)
        return payload if isinstance(payload, dict) else {}

    if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        form = await request.form()
        raw_payload = form.get("results_payload") or form.get("exportPdfPayload")

        if isinstance(raw_payload, (bytes, bytearray)):
            raw_payload = raw_payload.decode("utf-8", errors="ignore")

        if isinstance(raw_payload, str):
            try:
                parsed = json.loads(raw_payload)
            except Exception:
                return {}
            return parsed if isinstance(parsed, dict) else {}

    return evaluate_eu7_ld(raw_inputs={})


def _render_results_pdf(results_payload: dict) -> StreamingResponse:
    html_str = templates.get_template("print_eu7.html").render(results_payload=results_payload)
    pdf_bytes = _render_pdf_with_weasyprint(html_str)
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'},
    )


@router.post("/export_pdf")
async def export_pdf_post(request: Request) -> StreamingResponse:
    results_payload = await _extract_results_payload(request)
    return _render_results_pdf(results_payload)


@router.get("/export_pdf")
def export_pdf_get() -> StreamingResponse:
    payload = evaluate_eu7_ld(raw_inputs={})
    return _render_results_pdf(payload)
