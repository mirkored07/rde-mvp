"""Tests covering the Tailwind/HTMX UI endpoints."""

from __future__ import annotations

import html
import importlib.util
import io
import json
import re
import zipfile

import pytest
from fastapi.testclient import TestClient

from src.main import app

client = TestClient(app)


PEMS_SAMPLE = """timestamp,exhaust_flow_kg_s,nox_mg_s,pn_1_s,veh_speed_m_s
2024-01-01T00:00:00Z,0.35,120,200,12
2024-01-01T00:00:01Z,0.36,130,210,13
2024-01-01T00:00:02Z,0.37,140,220,14
2024-01-01T00:00:03Z,0.36,150,230,15
"""

GPS_SAMPLE = """timestamp,lat,lon,alt_m,speed_m_s
2024-01-01T00:00:00Z,48.8566,2.3522,35,12
2024-01-01T00:00:01Z,48.8567,2.3524,35,13
2024-01-01T00:00:02Z,48.8569,2.3527,35,14
2024-01-01T00:00:03Z,48.8571,2.3530,35,15
"""

ECU_SAMPLE = """timestamp,veh_speed_m_s,engine_speed_rpm,engine_load_pct,throttle_pct
2024-01-01T00:00:00Z,12,1500,30,15
2024-01-01T00:00:01Z,13,1520,32,16
2024-01-01T00:00:02Z,14,1540,34,18
2024-01-01T00:00:03Z,15,1560,35,20
"""

CUSTOM_PEMS = """Time,ExhFlow_g_s,NOx_ug_s,PN_count,Speed_m_s
2024-01-01T00:00:00Z,350,120000,200,12
2024-01-01T00:00:01Z,360,130000,210,13
2024-01-01T00:00:02Z,370,140000,220,14
2024-01-01T00:00:03Z,360,150000,230,15
"""

CUSTOM_GPS = """Time,Latitude,Longitude,Altitude_m,Speed_m_s
2024-01-01T00:00:00Z,48.8566,2.3522,35,12
2024-01-01T00:00:01Z,48.8567,2.3524,35,13
2024-01-01T00:00:02Z,48.8569,2.3527,35,14
2024-01-01T00:00:03Z,48.8571,2.3530,35,15
"""

CUSTOM_ECU = """Time,VehicleSpeed,EngineRPM,EngineLoad,Throttle
2024-01-01T00:00:00Z,12,1500,30,15
2024-01-01T00:00:01Z,13,1520,32,16
2024-01-01T00:00:02Z,14,1540,34,18
2024-01-01T00:00:03Z,15,1560,35,20
"""

WEASYPRINT_AVAILABLE = importlib.util.find_spec("weasyprint") is not None


def _post_analysis() -> str:
    response = client.post(
        "/analyze",
        files={
            "pems_file": ("pems.csv", PEMS_SAMPLE.encode("utf-8"), "text/csv"),
            "gps_file": ("gps.csv", GPS_SAMPLE.encode("utf-8"), "text/csv"),
            "ecu_file": ("ecu.csv", ECU_SAMPLE.encode("utf-8"), "text/csv"),
        },
    )
    assert response.status_code == 200
    return response.text


def _extract_results_payload(html_text: str) -> dict[str, object]:
    match = re.search(r"data-report-payload>(.*?)</script>", html_text, re.DOTALL)
    assert match is not None, "results payload script not found"
    raw_json = html.unescape(match.group(1))
    return json.loads(raw_json)


def test_index_page_renders() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "Upload PEMS, GPS, and ECU data" in response.text
    assert "Download sample files" in response.text


def test_static_assets_served() -> None:
    response = client.get("/static/css/styles.css")
    assert response.status_code == 200
    assert ".dropzone" in response.text


def test_analysis_endpoint_returns_results() -> None:
    html_text = _post_analysis()

    assert "Analysis Summary" in html_text
    assert "Regulation verdict" in html_text
    assert "Rule evidence" in html_text
    assert "Download PDF" in html_text


def test_sample_file_downloads() -> None:
    for name in ("pems_demo.csv", "gps_demo.csv", "ecu_demo.csv"):
        response = client.get(f"/samples/{name}")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")
        assert "timestamp" in response.text


def test_samples_zip_download() -> None:
    response = client.get("/samples.zip")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"

    buffer = io.BytesIO(response.content)
    with zipfile.ZipFile(buffer) as archive:
        names = sorted(archive.namelist())
        assert names == [
            "ecu_demo.csv",
            "gps_demo.csv",
            "pems_demo.csv",
        ]
        pems_csv = archive.read("pems_demo.csv").decode("utf-8")
        assert "nox_mg_s" in pems_csv


def test_analysis_applies_column_mapping() -> None:
    mapping_payload = {
        "pems": {
            "columns": {
                "timestamp": "Time",
                "exhaust_flow_kg_s": "ExhFlow_g_s",
                "nox_mg_s": "NOx_ug_s",
                "pn_1_s": "PN_count",
                "veh_speed_m_s": "Speed_m_s",
            },
            "units": {
                "exhaust_flow_kg_s": "g/s",
                "nox_mg_s": "ug/s",
            },
        },
        "gps": {
            "columns": {
                "timestamp": "Time",
                "lat": "Latitude",
                "lon": "Longitude",
                "speed_m_s": "Speed_m_s",
            }
        },
        "ecu": {
            "columns": {
                "timestamp": "Time",
                "veh_speed_m_s": "VehicleSpeed",
                "engine_speed_rpm": "EngineRPM",
            }
        },
    }
    response = client.post(
        "/analyze",
        data={"mapping_payload": json.dumps(mapping_payload)},
        files={
            "pems_file": ("pems.csv", CUSTOM_PEMS.encode("utf-8"), "text/csv"),
            "gps_file": ("gps.csv", CUSTOM_GPS.encode("utf-8"), "text/csv"),
            "ecu_file": ("ecu.csv", CUSTOM_ECU.encode("utf-8"), "text/csv"),
        },
    )
    assert response.status_code == 200
    payload = _extract_results_payload(response.text)
    chart = payload.get("analysis", {}).get("chart", {})
    traces = chart.get("traces", [])
    speed_trace = next((trace for trace in traces if trace.get("name") == "Vehicle speed (m/s)"), None)
    assert speed_trace is not None
    assert speed_trace["y"][0] == pytest.approx(12.0, rel=1e-6)


def test_analysis_reports_missing_required_column_from_mapping() -> None:
    mapping_payload = {
        "gps": {
            "columns": {
                "lat": "Latitude",
                "lon": "Longitude",
            }
        }
    }
    response = client.post(
        "/analyze",
        data={"mapping_payload": json.dumps(mapping_payload)},
        files={
            "pems_file": ("pems.csv", PEMS_SAMPLE.encode("utf-8"), "text/csv"),
            "gps_file": ("gps.csv", CUSTOM_GPS.encode("utf-8"), "text/csv"),
            "ecu_file": ("ecu.csv", ECU_SAMPLE.encode("utf-8"), "text/csv"),
        },
    )
    assert response.status_code == 400
    assert "timestamp" in response.text
    assert "GPS" in response.text


def test_export_pdf_requires_payload() -> None:
    response = client.post("/export_pdf", json={})
    assert response.status_code == 400
    assert response.json()["detail"] == "Results payload is required."


@pytest.mark.skipif(not WEASYPRINT_AVAILABLE, reason="WeasyPrint not installed")
def test_export_pdf_generates_document() -> None:
    html_text = _post_analysis()
    payload = _extract_results_payload(html_text)

    response = client.post("/export_pdf", json={"results": payload})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    assert len(response.content) > 500


@pytest.mark.skipif(WEASYPRINT_AVAILABLE, reason="WeasyPrint installed")
def test_export_pdf_reports_missing_dependency() -> None:
    html_text = _post_analysis()
    payload = _extract_results_payload(html_text)

    response = client.post("/export_pdf", json={"results": payload})
    assert response.status_code == 503
    detail = response.json().get("detail", "")
    assert "WeasyPrint" in detail
