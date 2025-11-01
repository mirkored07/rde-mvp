"""Legacy UI entry points (index page and fallback sample download routes)."""

from __future__ import annotations

import io

from fastapi import APIRouter, Request
from starlette.responses import StreamingResponse
from starlette.templating import Jinja2Templates

from src.app.ui.routes.export import build_samples_zip_bytes

router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


@router.get("/", include_in_schema=False)
async def index(request: Request):
    """Render the landing page that hosts the upload workflow."""

    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/samples.zip", include_in_schema=False)
def samples_zip() -> StreamingResponse:
    """Return all demo CSVs bundled into a zip archive."""

    blob = build_samples_zip_bytes()
    return StreamingResponse(
        io.BytesIO(blob),
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="samples.zip"'},
    )


__all__ = ["router"]
