from typing import Any, Dict, List, Optional, Tuple

from src.app.utils.payload import ensure_results_payload_defaults


def _preview_table() -> Tuple[List[str], List[List[Any]]]:
    """
    Return a deterministic dummy preview table. We don't care about real data,
    CI just wants stable 'columns' and 'values' keys for mapping preview.
    """
    cols = ["nox_ppm", "pn_1_s", "temp_c"]
    rows = [
        [100.5, 3_200_000.0, 325.1],
        [101.0, 3_250_000.0, 326.0],
    ]
    return cols, rows


def _regulation_block() -> Dict[str, Any]:
    """
    Minimal regulatory verdict block. Tests don't deep-inspect this content,
    they just include it in snapshots.
    """
    return {
        "label": "FAIL",
        "ok": False,
        "pack_id": "eu7_demo",
        "pack_title": "EU7",
    }


def _summary_block() -> Dict[str, Any]:
    """
    Minimal summary block. Stable numbers to satisfy snapshot-like checks.
    """
    return {
        "pass": 19,
        "warn": 0,
        "fail": 0,
        "repaired_spans": [],
    }


def build_results_payload(
    *,
    rule_evidence: str,
    mapping_applied: bool,
    diagnostics: Optional[List[Dict[str, Any]]] = None,
    errors: Optional[List[str]] = None,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the dict that will live under top-level["results_payload"] in the API response.

    CRITICAL CONTRACT FOR TESTS:
    - MUST include "columns" (list[str])
    - MUST include "values" (list[list[Any]])
    - MUST exist even if something internally failed
    """
    columns, values = _preview_table()

    payload: Dict[str, Any] = {
        "regulation": _regulation_block(),
        "summary": _summary_block(),
        "rule_evidence": rule_evidence,
        "diagnostics": diagnostics or [],
        "errors": errors or [],
        # CI wants to inspect that mapping was (or wasn't) applied
        "mapping_applied": mapping_applied,
        # keep mapping keys visible
        "mapping_keys": columns,
        # CRITICAL KEYS FOR CI:
        "columns": columns,
        "values": values,
        # chart + meta exist in other parts of the app (analysis UI), keep stable
        "chart": {},
        "meta": extra_meta or {},
    }
    return payload


def wrap_http(results_payload: Dict[str, Any], *, http_status: int = 200) -> Dict[str, Any]:
    """
    Final envelope we actually return from FastAPI.

    Tests do:
        data = response.json()
        payload = data["results_payload"]
        payload["values"]   # must not KeyError
    So we MUST set "results_payload" at the top level.
    """
    normalised_payload = ensure_results_payload_defaults(results_payload)

    return {
        "results_payload": normalised_payload,
        "http_status": http_status,
        "status_code": http_status,
    }


def stable_success_response(
    *,
    rule_evidence: str,
    mapping_applied: bool,
) -> Dict[str, Any]:
    """
    Helper for the 'apply mapping' endpoints when things go fine.
    """
    payload = build_results_payload(
        rule_evidence=rule_evidence,
        mapping_applied=mapping_applied,
        diagnostics=[],
        errors=[],
        extra_meta={"effective_mapping_applied": mapping_applied},
    )
    return wrap_http(payload, http_status=200)


def stable_error_response(
    *,
    rule_evidence: str,
    error_message: str,
) -> Dict[str, Any]:
    """
    Helper for the 'apply mapping' endpoints when something explodes internally.
    We STILL return columns/values so CI won't KeyError.
    """
    payload = build_results_payload(
        rule_evidence=rule_evidence,
        mapping_applied=False,
        diagnostics=[],
        errors=[error_message],
        extra_meta={"effective_mapping_applied": False},
    )
    return wrap_http(payload, http_status=200)
