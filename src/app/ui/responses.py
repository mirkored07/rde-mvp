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

    return {
        "regulation": regulation,
        "summary": summary,
        "rule_evidence": rule_evidence,
        "diagnostics": diagnostics,
        "errors": errors,
        "mapping_applied": bool(mapping_applied),
        "mapping_keys": mapping_keys,
        "chart": chart,
        "http_status": http_status,
        "status_code": status_code,
        "payload_script": payload_script,
    }


def respond_success(payload: Dict[str, Any]) -> JSONResponse:
    """Wrap the payload in the canonical API response envelope."""

    return JSONResponse(status_code=200, content={"results_payload": payload})
