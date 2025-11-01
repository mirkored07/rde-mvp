"""PDF export endpoint for EU7 results payloads."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from starlette.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")

_DEV_PDF_BYTES = (
    b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 595 842]/Contents 4 0 R"
    b"/Resources<</Font<</F1 5 0 R>>>>>>endobj\n"
    b"4 0 obj<</Length 55>>stream\nBT /F1 24 Tf 72 780 Td (EU7 Report - Dev PDF) Tj ET\nendstream\nendobj\n"
    b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"xref\n0 6\n0000000000 65535 f \n0000000010 00000 n \n0000000061 00000 n \n0000000116 00000 n \n"
    b"0000000267 00000 n \n0000000371 00000 n \ntrailer<</Size 6/Root 1 0 R>>\nstartxref\n446\n%%EOF\n"
)


@router.post("/export_pdf", include_in_schema=False)
async def export_pdf(request: Request) -> Response:
    content_type = (request.headers.get("content-type") or "").lower()
    params = request.query_params
    dev_fallback = params.get("dev_fallback") in ("1", "true", "yes")

    data = await request.json() if "application/json" in content_type else {}
    payload = (data or {}).get("results_payload")

    if not payload:
        raise HTTPException(status_code=400, detail="Results payload is required.")

    try:
        from weasyprint import HTML  # noqa: F401
    except Exception:  # pragma: no cover - dependency check via HTTP tests
        if dev_fallback:
            return Response(
                content=_DEV_PDF_BYTES,
                media_type="application/pdf",
                headers={"Content-Disposition": "attachment; filename=report_eu7_ld.pdf"},
            )
        raise HTTPException(status_code=503, detail="WeasyPrint not installed")

    html = templates.get_template("print_eu7.html").render({"results_payload": payload})
    from weasyprint import HTML

    pdf = HTML(string=html).write_pdf()
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=report_eu7_ld.pdf"},
    )


__all__ = ["router"]
