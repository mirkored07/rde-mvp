from typing import Any, Dict, List, Optional


def make_results_payload(
    *,
    regulation: Dict[str, Any],
    summary: Dict[str, Any],
    rule_evidence: str = "",
    diagnostics: Optional[List[Dict[str, Any]]] = None,
    errors: Optional[List[str]] = None,
    mapping_applied: bool = False,
    mapping_keys: Optional[List[str]] = None,
    table_columns: Optional[List[str]] = None,
    table_values: Optional[List[List[Any]]] = None,
    chart: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the 'results_payload' dict that tests read with:
        data = response.json()
        payload = data["results_payload"]

    CRITICAL: tests expect that `payload["values"]`
    and `payload["columns"]` ALWAYS exist at the TOP LEVEL.
    """

    payload: Dict[str, Any] = {
        "regulation": regulation,
        "summary": summary,
        "rule_evidence": rule_evidence,
        "diagnostics": diagnostics or [],
        "errors": errors or [],
        "mapping_applied": bool(mapping_applied),
        "mapping_keys": mapping_keys or [],
        # REQUIRED keys for tests:
        "columns": table_columns or [],
        "values": table_values or [],
    }

    if chart is not None:
        payload["chart"] = chart

    return payload


def respond_success(
    results_payload: Dict[str, Any],
    *,
    status_code: int = 200,
    http_status: int = 200,
) -> Dict[str, Any]:
    """
    Final envelope returned by FastAPI endpoints.
    The tests do:
        data = response.json()
        payload = data["results_payload"]
    so we MUST include "results_payload" here.
    """
    return {
        "results_payload": results_payload,
        "status_code": status_code,
        "http_status": http_status,
    }
