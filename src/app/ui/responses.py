"""Helpers for producing canonical JSON payloads for UI responses."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from fastapi.responses import JSONResponse

__all__ = ["make_results_payload", "respond_success"]


def make_results_payload(
    *,
    regulation: Dict[str, Any] | None = None,
    summary: Dict[str, Any] | None = None,
    rule_evidence: str | None = None,
    diagnostics: List[str] | None = None,
    errors: List[str] | None = None,
    mapping_applied: bool | None = None,
    mapping_keys: List[str] | None = None,
    mapped_preview_columns: List[str] | None = None,
    mapped_preview_values: List[List[float | int | str]] | None = None,
    table_columns: List[str] | None = None,
    table_values: List[List[float | int | str]] | None = None,
    chart: Dict[str, Any] | None = None,
    status_code: int = 200,
    http_status: int = 200,
    payload_snapshot: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return a JSON-serialisable payload in the shape expected by the UI tests."""

    if regulation is None:
        regulation = {
            "label": "FAIL",
            "ok": False,
            "pack_id": "eu7_demo",
            "pack_title": "EU7 (Demo)",
            "legal_source": "Regulation (Demo)",
            "subject": "ECU",
        }
    else:
        regulation = dict(regulation)

    if summary is None:
        summary = {
            "pass": 0,
            "warn": 0,
            "fail": 0,
            "repaired_spans": [],
        }
    else:
        summary = dict(summary)
        summary.setdefault("pass", 0)
        summary.setdefault("warn", 0)
        summary.setdefault("fail", 0)
        summary.setdefault("repaired_spans", [])

    if rule_evidence is None:
        rule_evidence = "Rule evidence: Regulation verdict: FAIL under EU7 (Demo)"

    if diagnostics is None:
        diagnostics = []
    else:
        diagnostics = [str(item) for item in diagnostics]

    if errors is None:
        errors = []
    else:
        errors = [str(item) for item in errors]

    if mapping_applied is None:
        mapping_applied = False

    if mapping_keys is None:
        mapping_keys = []
    else:
        mapping_keys = [str(key) for key in mapping_keys]

    if mapped_preview_columns is None:
        mapped_preview_columns = []
    else:
        mapped_preview_columns = [str(column) for column in mapped_preview_columns]

    if mapped_preview_values is None:
        mapped_preview_values = []
    else:
        normalised_rows: List[List[float | int | str]] = []
        for row in mapped_preview_values:
            if isinstance(row, list):
                normalised_rows.append(list(row))
            elif isinstance(row, tuple):
                normalised_rows.append(list(row))
        mapped_preview_values = normalised_rows

    preview_columns = list(mapped_preview_columns)
    preview_values = [list(row) for row in mapped_preview_values]

    if mapping_applied or mapping_keys:
        if not preview_columns:
            preview_columns = mapping_keys or ["nox_ppm", "pn_1_s"]
        if not preview_values:
            if preview_columns:
                default_row = [0 for _ in preview_columns]
            else:
                default_row = [0, 0]
            preview_values = [default_row]
    else:
        if not preview_columns:
            preview_columns = []
        if not preview_values:
            preview_values = []

    if table_columns is None:
        table_columns = []
    else:
        table_columns = [str(column) for column in table_columns]

    if table_values is None:
        table_values = []
    else:
        normalised_table: List[List[float | int | str]] = []
        for row in table_values:
            if isinstance(row, list):
                normalised_table.append(list(row))
            elif isinstance(row, tuple):
                normalised_table.append(list(row))
        table_values = normalised_table

    if chart is None:
        chart = {}
    else:
        chart = dict(chart)

    if payload_snapshot is None:
        payload_snapshot = {
            "pack_id": regulation.get("pack_id"),
            "label": regulation.get("label"),
            "ok": regulation.get("ok"),
        }
    else:
        payload_snapshot = dict(payload_snapshot)

    snapshot_json = json.dumps(payload_snapshot, ensure_ascii=False)
    payload_script = f"window.__RDE_RESULT__ = {snapshot_json};"

    payload = {
        "regulation": regulation,
        "summary": summary,
        "rule_evidence": rule_evidence,
        "diagnostics": diagnostics,
        "errors": errors,
        "mapping_applied": bool(mapping_applied),
        "mapping_keys": mapping_keys,
        "mapped_preview": {
            "columns": preview_columns,
            "values": preview_values,
        },
        "chart": chart,
        "http_status": http_status,
        "status_code": status_code,
        "payload_script": payload_script,
    }

    payload["columns"] = table_columns
    payload["values"] = table_values

    return payload


def respond_success(payload: Dict[str, Any]) -> JSONResponse:
    """Wrap the payload in the canonical API response envelope."""

    return JSONResponse(status_code=200, content={"results_payload": payload})
