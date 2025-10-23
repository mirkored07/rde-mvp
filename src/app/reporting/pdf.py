from __future__ import annotations

"""Helpers for rendering PDF reports."""

from typing import Optional  # noqa: F401  # kept for compatibility with instructions


def html_to_pdf_bytes(html: str) -> bytes:
    """Convert HTML to PDF bytes using WeasyPrint, if available."""
    try:
        from weasyprint import HTML  # type: ignore
    except Exception as e:  # pragma: no cover - library import guarded in tests
        raise RuntimeError(
            "PDF export requires WeasyPrint. Install with: 'poetry add weasyprint' "
            "and ensure system libraries (cairo, pango) are present."
        ) from e
    pdf = HTML(string=html, base_url=".").write_pdf()
    return pdf
