"""Reusable UI route modules."""

from __future__ import annotations

from .analyze import router as analyze_router
from .export import router as export_router
from .export_pdf import router as export_pdf_router
from .print_preview import router as print_preview_router
from .results import router as results_router
from .samples import router as samples_router

__all__ = [
    "analyze_router",
    "export_router",
    "export_pdf_router",
    "print_preview_router",
    "results_router",
    "samples_router",
]
