import base64
import io
import json
import re
import zipfile
from pathlib import Path

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
try:  # pragma: no cover - optional dependency in CI
    import weasyprint  # type: ignore  # noqa: F401

    WEASYPRINT_AVAILABLE = True
except Exception:  # pragma: no cover - handled via skip
    WEASYPRINT_AVAILABLE = False


def _post_analysis_json() -> dict:
    response = client.post(
        "/analyze",
        files={
            "pems_file": ("pems.csv", PEMS_SAMPLE.encode("utf-8"), "text/csv"),
            "gps_file": ("gps.csv", GPS_SAMPLE.encode("utf-8"), "text/csv"),
            "ecu_file": ("ecu.csv", ECU_SAMPLE.encode("utf-8"), "text/csv"),
        },
        headers={"accept": "application/json"},
    )
    assert response.status_code == 200
    payload = response.json().get("results_payload")
    assert isinstance(payload, dict)
    return payload


def _post_analysis_html() -> str:
    response = client.post(
        "/analyze",
        files={
            "pems_file": ("pems.csv", PEMS_SAMPLE.encode("utf-8"), "text/csv"),
            "gps_file": ("gps.csv", GPS_SAMPLE.encode("utf-8"), "text/csv"),
            "ecu_file": ("ecu.csv", ECU_SAMPLE.encode("utf-8"), "text/csv"),
        },
        headers={"accept": "text/html"},
    )
    assert response.status_code == 200
    return response.text


def test_index_page_renders() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "Upload PEMS, GPS, and ECU" in response.text


def test_static_assets_served() -> None:
    response = client.get("/static/css/styles.css")
    assert response.status_code == 200
    assert ".dropzone" in response.text


def test_analyze_returns_eu7_payload() -> None:
    payload = _post_analysis_json()

    visual = payload.get("visual")
    assert isinstance(visual, dict)
    map_block = visual.get("map")
    assert isinstance(map_block, dict)
    assert isinstance(map_block.get("latlngs"), list)
    chart_block = visual.get("chart")
    assert isinstance(chart_block, dict)
    assert isinstance(chart_block.get("series"), list) and chart_block["series"]

    kpi_numbers = payload.get("kpi_numbers")
    assert isinstance(kpi_numbers, list) and kpi_numbers

    limits = payload.get("limits")
    assert isinstance(limits, dict)
    assert limits.get("NOx_mg_km_RDE") == 60.0
    assert limits.get("PN10_hash_km_RDE") == pytest.approx(6.0e11)
    assert limits.get("CO_mg_km_WLTP") == 1000.0

    criteria = payload.get("criteria")
    assert isinstance(criteria, list) and len(criteria) >= 50
    first = criteria[0]
    assert "section" in first and "description" in first and "result" in first
    assert sum(1 for item in criteria if item.get("value") not in (None, "", "n/a")) >= 40

    emissions = payload.get("emissions")
    assert isinstance(emissions, dict)
    assert isinstance(emissions.get("trip"), dict)
    assert isinstance(emissions.get("urban"), dict)

    device = payload.get("device")
    assert isinstance(device, dict)
    assert device.get("gasPEMS")

    sections = payload.get("sections")
    assert isinstance(sections, list) and len(sections) == 9
    expected_titles = {
        "Pre/Post Checks (Zero/Span)",
        "Span Gas Coverage",
        "Trip Composition & Timing",
        "Cold-Start Window",
        "GPS Validity",
        "Dynamics & MAW",
        "CO₂ Characteristic Windows (MAW)",
        "Emissions Summary",
        "Final Conformity",
    }
    assert {section.get("title") for section in sections} == expected_titles

    final_block = payload.get("final")
    assert isinstance(final_block, dict)
    assert isinstance(final_block.get("pass"), bool)
    pollutants = final_block.get("pollutants")
    assert isinstance(pollutants, list) and pollutants
    assert all(isinstance(p.get("value"), (int, float)) for p in pollutants)
    final_conformity = payload.get("final_conformity")
    assert final_conformity["NOx_mg_km"]["limit"] == 60.0
    assert final_conformity["PN10_hash_km"]["limit"] == pytest.approx(6.0e11)

    meta = payload.get("meta")
    assert isinstance(meta, dict)
    assert meta.get("legislation") == "EU7 Light-Duty"
    assert meta.get("testId")
    assert meta.get("velocitySource")
    sources = meta.get("sources")
    assert isinstance(sources, dict) and sources.get("pems_rows")


def test_results_template_injects_payload_before_app_js() -> None:
    html = _post_analysis_html()

    assert 'id="drive-map"' in html
    assert '<link rel="stylesheet" href="/static/leaflet/leaflet.css">' in html
    assert '<script src="/static/leaflet/leaflet.js"></script>' in html

    assert 'document.dispatchEvent(new Event("rde:payload-ready"))' in html

    payload_pos = html.find("window.results_payload")
    app_pos = html.find('/static/js/app.js')
    assert payload_pos != -1 and app_pos != -1
    assert payload_pos < app_pos


def test_results_payload_embedded_in_html() -> None:
    html = _post_analysis_html()
    match = re.search(r"window.results_payload\s*=\s*(.*?);\s*window.__RDE_RESULT__", html, re.DOTALL)
    assert match is not None
    payload = json.loads(match.group(1))

    visual = payload.get("visual")
    assert isinstance(visual, dict)
    assert isinstance(visual.get("map", {}).get("latlngs"), list)
    assert isinstance(visual.get("chart", {}).get("series"), list)

    assert isinstance(payload.get("criteria"), list) and payload["criteria"]
    assert sum(1 for item in payload["criteria"] if isinstance(item.get("value"), (int, float))) >= 40


def test_analyze_demo_route_renders_results() -> None:
    response = client.get("/analyze", params={"demo": 1})
    assert response.status_code == 200
    html = response.text
    assert "window.results_payload" in html
    assert 'document.dispatchEvent(new Event("rde:payload-ready"))' in html


def test_app_js_registers_required_listeners() -> None:
    script = Path("src/app/ui/static/js/app.js").read_text(encoding="utf-8")
    assert 'document.addEventListener("rde:payload-ready", () => {' in script
    assert 'document.addEventListener("htmx:afterSwap", (event) => {' in script
    assert 'safeInitMap(payload, document.getElementById(' in script
    assert 'window.renderSectionTable = renderSectionTable' in script


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

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert sorted(archive.namelist()) == [
            "ecu_demo.csv",
            "gps_demo.csv",
            "pems_demo.csv",
        ]


def test_export_zip_stream_download() -> None:
    response = client.get("/export_zip")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert response.headers["content-disposition"] == 'attachment; filename="rde_export.zip"'

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.namelist() == [
            "ecu_demo.csv",
            "gps_demo.csv",
            "pems_demo.csv",
        ]


def test_export_zip_json_mode() -> None:
    response = client.get("/export_zip", params={"download": 0})
    assert response.status_code == 200
    body = response.json()
    results_payload = body.get("results_payload", {})
    attachments = results_payload.get("attachments", [])
    assert attachments
    archive_info = attachments[0]
    blob = base64.b64decode(archive_info["content_base64"])
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        assert sorted(archive.namelist()) == [
            "ecu_demo.csv",
            "gps_demo.csv",
            "pems_demo.csv",
        ]


def test_export_pdf_requires_payload() -> None:
    response = client.post("/export_pdf", json={})
    assert response.status_code == 400
    assert response.json()["detail"] == "Results payload is required."


@pytest.mark.skipif(not WEASYPRINT_AVAILABLE, reason="WeasyPrint not installed")
def test_export_pdf_generates_document() -> None:
    payload = _post_analysis_json()
    response = client.post("/export_pdf", json={"results_payload": payload})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    assert response.headers["content-disposition"] == 'attachment; filename="report_eu7_ld.pdf"'
    assert response.content.startswith(b"%PDF")


@pytest.mark.skipif(WEASYPRINT_AVAILABLE, reason="WeasyPrint installed")
def test_export_pdf_missing_dependency() -> None:
    payload = _post_analysis_json()
    response = client.post("/export_pdf", json={"results_payload": payload})
    assert response.status_code == 503
    assert "WeasyPrint" in response.json().get("detail", "")


def test_results_payload_script_precedes_bundle() -> None:
    html = _post_analysis_html()
    payload_index = html.find("window.results_payload =")
    bundle_index = html.find("/static/js/app.js")
    assert payload_index != -1 and bundle_index != -1
    assert payload_index < bundle_index
