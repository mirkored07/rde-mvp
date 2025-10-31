from __future__ import annotations

import io
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.templating import Jinja2Templates

from src.app.rules.engine import evaluate_eu7_ld

try:  # pragma: no cover - import guard for optional dependency
    from weasyprint import HTML  # type: ignore
except Exception:  # pragma: no cover - handled at runtime
    HTML = None


router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory="src/app/ui/templates")


class ExportIn(BaseModel):
    results_payload: dict[str, Any] | None = None


def _render_pdf(payload: dict[str, Any]) -> bytes:
    if HTML is None:  # pragma: no cover - depends on optional dependency
        raise HTTPException(
            status_code=503,
            detail="PDF export requires WeasyPrint. Install the 'pdf' extra.",
        )

    template = templates.get_template("print_eu7.html")
    html = template.render(results_payload=payload)
    return HTML(string=html, base_url="src/app/ui/templates").write_pdf()


def _latest_payload_from_session(request: Request) -> dict[str, Any] | None:
    try:
        session_data = request.session  # type: ignore[attr-defined]
    except RuntimeError:
        return None

    payload = session_data.get("latest_results_payload") if session_data else None
    if isinstance(payload, dict):
        return payload
    return None


@router.post("/export_pdf")
def export_pdf_post(body: ExportIn, request: Request) -> StreamingResponse:
    payload = body.results_payload
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Results payload is required.")

    response = StreamingResponse(
        io.BytesIO(_render_pdf(payload)),
        media_type="application/pdf",
    )
    response.headers["Content-Disposition"] = 'attachment; filename="report_eu7_ld.pdf"'

    try:  # pragma: no cover - depends on session middleware
        request.session["latest_results_payload"] = payload
    except RuntimeError:
        pass

    return response


@router.get("/export_pdf")
def export_pdf_get(request: Request) -> StreamingResponse:
    payload = _latest_payload_from_session(request)
    if payload is None:
        payload = evaluate_eu7_ld(raw_inputs={})

    response = StreamingResponse(
        io.BytesIO(_render_pdf(payload)),
        media_type="application/pdf",
    )
    response.headers["Content-Disposition"] = 'attachment; filename="report_eu7_ld.pdf"'
    return response


__all__ = ["router"]
