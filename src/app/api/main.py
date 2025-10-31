"""FastAPI application wiring for the RDE MVP web experience."""

from __future__ import annotations

import pathlib

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from src.app.ui.routes import export_pdf_router, export_router
from src.app.ui.server import router as ui_router

app = FastAPI(title="RDE MVP")

# Session storage is used to persist the most recent analysis payload so
# export endpoints can render PDFs without requiring the frontend to resend
# the data on every GET request.
app.add_middleware(SessionMiddleware, secret_key="rde-mvp-session")


@app.get("/health")
def health() -> JSONResponse:
    """Simple liveness endpoint used by deployment probes."""

    return JSONResponse({"status": "ok"})


@app.get("/favicon.ico")
def favicon() -> Response:
    """Return an empty favicon response to silence 404 noise."""

    return Response(status_code=204)


# Register UI routes (landing page, analysis workflow).
app.include_router(ui_router)

# Lightweight export utilities.
app.include_router(export_router)
app.include_router(export_pdf_router)


# Expose static assets (CSS/JS) used by the Tailwind/HTMX UI.
static_dir = pathlib.Path(__file__).resolve().parent.parent / "ui" / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


__all__ = ["app", "health"]
