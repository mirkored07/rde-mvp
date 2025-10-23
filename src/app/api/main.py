"""FastAPI application wiring for the RDE MVP web experience."""

from __future__ import annotations

import pathlib

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from src.app.ui.server import router as ui_router

app = FastAPI(title="RDE MVP")


@app.get("/health")
def health() -> JSONResponse:
    """Simple liveness endpoint used by deployment probes."""

    return JSONResponse({"status": "ok"})


# Register UI routes (landing page, analysis workflow).
app.include_router(ui_router)


# Expose static assets (CSS/JS) used by the Tailwind/HTMX UI.
static_dir = pathlib.Path(__file__).resolve().parent.parent / "ui" / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


__all__ = ["app", "health"]
