from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from src.app.reporting.schemas import ReportData
from src.main import app


client = TestClient(app)


def test_sample_report_validates() -> None:
    raw = json.loads(Path("reports/sample.json").read_text(encoding="utf-8"))
    report = ReportData.model_validate(raw)
    assert report.meta.testId == "sample"
    assert report.device.gasPEMS
    assert any(item.section == "Final Conformity" for item in report.criteria)
    assert len(report.criteria) == 53


def test_report_endpoint_returns_sample() -> None:
    response = client.get("/api/report/sample")
    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["testId"] == "sample"
    assert payload["limits"]["NOx_mg_km_RDE"] == 60.0
    assert "criteria" in payload and payload["criteria"]
    conformity = [c for c in payload["criteria"] if c["section"] == "Final Conformity"]
    assert conformity
    results = {item["id"]: item["result"] for item in conformity}
    assert results.get("conformity:nox") == "pass"
    assert len(payload["criteria"]) == 53

