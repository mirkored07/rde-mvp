"""PDF export endpoint for EU7 results payloads."""

from __future__ import annotations

import io
import json
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


def _render_pdf_with_weasyprint(html_str: str) -> bytes:
    """Render HTML to PDF, surfacing dependency errors as HTTP 503."""

    try:  # pragma: no cover - import guard tested via HTTP layer
        from weasyprint import HTML  # type: ignore
    except Exception as exc:  # pragma: no cover - mapped to 503 for tests
        raise HTTPException(status_code=503, detail="WeasyPrint not installed") from exc

    return HTML(string=html_str, base_url=".").write_pdf()


async def _parse_results_payload(request: Request) -> Dict[str, Any] | None:
    """Extract a dict payload from JSON or form submissions."""

    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        data = await request.json()
        candidate = data.get("results_payload", data) if isinstance(data, dict) else None
        return candidate if isinstance(candidate, dict) and candidate else None

    if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        form = await request.form()
        raw = form.get("results_payload") or form.get("exportPdfPayload")
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except Exception:
                return None
            return parsed if isinstance(parsed, dict) and parsed else None

    return None


@router.post("/export_pdf")
async def export_pdf_post(request: Request) -> StreamingResponse:
    """Generate a PDF rendition of the provided EU7 results payload."""

    results_payload = await _parse_results_payload(request)
    if not results_payload:
        raise HTTPException(status_code=400, detail="Results payload is required.")

    html_str = templates.get_template("print_eu7.html").render(results_payload=results_payload)
    pdf_bytes = _render_pdf_with_weasyprint(html_str)
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="report_eu7_ld.pdf"'},
    )
