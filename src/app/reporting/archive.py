from __future__ import annotations

"""Helpers for producing ZIP archives of analysis results."""

import io
import json
import zipfile
from typing import Any

from .html import build_report_html

__all__ = ["build_report_archive"]


def build_report_archive(results: dict[str, Any]) -> bytes:
    """Create a ZIP archive containing the rendered report and diagnostics."""

    html_document = build_report_html(results)
    diagnostics = (
        results.get("quality")
        or results.get("diagnostics")
        or {}
    )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("index.html", html_document)
        archive.writestr("diagnostics.json", json.dumps(diagnostics, indent=2, sort_keys=True))

    buffer.seek(0)
    return buffer.getvalue()
