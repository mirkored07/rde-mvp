from __future__ import annotations

"""Helpers for producing ZIP archives of analysis results."""

import io
import json
import zipfile
from typing import Any

from src.app.reporting.eu7ld_report import apply_guardrails, build_report_data, save_report_json

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

    try:
        report = apply_guardrails(build_report_data(results))
        save_report_json(report)
    except Exception:  # pragma: no cover - diagnostics-only best effort
        report = None

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("index.html", html_document)
        archive.writestr("diagnostics.json", json.dumps(diagnostics, indent=2, sort_keys=True))
        if report is not None:
            archive.writestr(
                "report.json", json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True)
            )

    buffer.seek(0)
    return buffer.getvalue()
