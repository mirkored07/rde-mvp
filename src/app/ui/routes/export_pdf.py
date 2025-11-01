"""PDF export endpoint for EU7 results payloads."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from starlette.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


@router.post("/export_pdf", include_in_schema=False)
async def export_pdf(request: Request) -> Response:
    content_type = (request.headers.get("content-type") or "").lower()
    data = await request.json() if "application/json" in content_type else None
    payload = (data or {}).get("results_payload") if isinstance(data, dict) else None

    if not payload:
        raise HTTPException(status_code=400, detail="Results payload is required.")

    try:
        from weasyprint import HTML  # noqa: F401
    except Exception as exc:  # pragma: no cover - dependency check via HTTP tests
        raise HTTPException(status_code=503, detail="WeasyPrint not installed") from exc

    html = templates.get_template("print_eu7.html").render({"results_payload": payload})
    from weasyprint import HTML

    pdf = HTML(string=html).write_pdf()
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=report_eu7_ld.pdf"},
    )


__all__ = ["router"]
