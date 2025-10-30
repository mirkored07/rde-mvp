"""Tests covering the Tailwind/HTMX UI endpoints."""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import re
import zipfile
from collections.abc import Mapping
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


def _post_analysis() -> dict[str, object]:
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
    body = response.json()
    assert isinstance(body, dict)
    assert "results_payload" in body
    return body["results_payload"]  # type: ignore[return-value]

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
    assert "Upload PEMS, GPS, and ECU data" in response.text
    assert "Download sample files" in response.text


def test_static_assets_served() -> None:
    response = client.get("/static/css/styles.css")
    assert response.status_code == 200
    assert ".dropzone" in response.text


def test_analysis_endpoint_returns_results() -> None:
    """Smoke test for the JSON contract consumed by the SPA results page."""
    # The frontend reads ``window.__RDE_RESULT__`` and expects the analysis,
    # chart, map, and KPI structures asserted below. Keep this in sync with
    # ``renderAll`` in ``src/app/ui/static/js/app.js``.

    html = _post_analysis_html()
    assert 'id="results-json"' in html
    assert 'window.dispatchEvent(new Event(''rde:payload-ready''))' in html

    payload_pos = html.find("window.__RDE_RESULT__ =")
    app_js_pos = html.find("/static/js/app.js")
    assert payload_pos != -1 and app_js_pos != -1 and payload_pos < app_js_pos

    for element_id in ("chart-speed", "chart-nox", "chart-pn", "chart-pm", "drive-map"):
        assert f'id="{element_id}"' in html

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

    body = response.json()
    assert isinstance(body, dict)
    assert "results_payload" in body

    payload = body["results_payload"]
    assert isinstance(payload, dict)

    analysis = payload.get("analysis")
    assert isinstance(analysis, dict)

    metrics = analysis.get("metrics")
    assert isinstance(metrics, list) and metrics
    total_distance = next(
        (metric for metric in metrics if metric.get("label") == "Total distance"),
        None,
    )
    assert isinstance(total_distance, dict)
    distance_value = total_distance.get("value")
    assert isinstance(distance_value, str) and distance_value != "n/a"
    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", distance_value)
    assert match is not None
    distance_numeric = float(match.group())
    assert distance_numeric > 0

    chart = analysis.get("chart")
    assert isinstance(chart, dict)
    times = chart.get("times")
    assert isinstance(times, list) and times
    speed = chart.get("speed")
    assert isinstance(speed, dict)
    speed_values = speed.get("values") or speed.get("y")
    assert isinstance(speed_values, list) and any(v is not None for v in speed_values)
    pollutants = chart.get("pollutants")
    assert isinstance(pollutants, list) and pollutants
    pollutant_with_values = next(
        (series for series in pollutants if isinstance(series, dict) and series.get("values")),
        None,
    )
    assert pollutant_with_values is not None
    assert any(value is not None for value in pollutant_with_values.get("values", []))

    map_payload = analysis.get("map")
    assert isinstance(map_payload, dict)
    points = map_payload.get("points")
    assert isinstance(points, list) and points
    first_point = points[0]
    assert isinstance(first_point, dict)
    assert "lat" in first_point and "lon" in first_point
    assert isinstance(first_point.get("lat"), (int, float))
    assert isinstance(first_point.get("lon"), (int, float))
    center = map_payload.get("center")
    if center is not None:
        assert isinstance(center, dict)
        assert center.get("lat") is not None and center.get("lon") is not None
    bounds = map_payload.get("bounds")
    if bounds:
        assert isinstance(bounds, list) and len(bounds) == 2

    kpis = analysis.get("kpis")
    assert isinstance(kpis, dict) and kpis
    nox_kpi = kpis.get("NOx_mg_per_km")
    assert nox_kpi is not None
    if isinstance(nox_kpi, Mapping):
        total_block = nox_kpi.get("total")
        if isinstance(total_block, Mapping):
            kpi_value = total_block.get("value")
        else:
            kpi_value = nox_kpi.get("value")
    else:
        kpi_value = nox_kpi
    assert kpi_value is not None
    assert float(kpi_value) > 0


def test_results_payload_has_chart_and_map_shapes() -> None:
    samples_dir = Path(__file__).resolve().parents[1] / "data" / "samples"
    pems_text = (samples_dir / "pems_demo.csv").read_text()
    gps_text = (samples_dir / "gps_demo.csv").read_text()
    ecu_text = (samples_dir / "ecu_demo.csv").read_text()

    response = client.post(
        "/analyze",
        files={
            "pems_file": ("pems_demo.csv", pems_text.encode("utf-8"), "text/csv"),
            "gps_file": ("gps_demo.csv", gps_text.encode("utf-8"), "text/csv"),
            "ecu_file": ("ecu_demo.csv", ecu_text.encode("utf-8"), "text/csv"),
        },
        headers={"accept": "text/html"},
    )
    assert response.status_code == 200

    html = response.text
    match = re.search(r"window.__RDE_RESULT__ = (.*?);\s*window.dispatchEvent", html, re.DOTALL)
    assert match is not None
    payload_json = match.group(1)
    payload = json.loads(payload_json)

    analysis = payload.get("analysis")
    assert isinstance(analysis, dict)

    chart = analysis.get("chart")
    assert isinstance(chart, dict)

    times = chart.get("times")
    assert isinstance(times, list) and len(times) > 100

    speed = chart.get("speed", {})
    speed_values = speed.get("values") or speed.get("y")
    assert isinstance(speed_values, list)
    assert len(speed_values) == len(times)

    pollutants = chart.get("pollutants")
    assert isinstance(pollutants, list)
    keys = {entry.get("key") for entry in pollutants if isinstance(entry, Mapping)}
    assert {"NOx", "PN", "PM"}.issubset(keys)

    map_payload = analysis.get("map")
    assert isinstance(map_payload, dict)
    points = map_payload.get("points")
    assert isinstance(points, list) and len(points) > 0


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


def test_export_zip_contains_diagnostics() -> None:
    payload = _post_analysis()

    response = client.post("/export_zip", json={"results": payload})
    assert response.status_code == 200

    body = response.json()
    results = body.get("results_payload", {})
    attachments = results.get("attachments", [])
    assert attachments, "expected zip attachment"

    archive_info = attachments[0]
    assert archive_info["media_type"] == "application/zip"

    archive_bytes = base64.b64decode(archive_info["content_base64"])
    buffer = io.BytesIO(archive_bytes)
    with zipfile.ZipFile(buffer) as archive:
        names = set(archive.namelist())
        assert "index.html" in names
        assert "diagnostics.json" in names
        diagnostics_payload = json.loads(archive.read("diagnostics.json").decode("utf-8"))
        assert "summary" in diagnostics_payload

    diagnostics_messages = results.get("diagnostics", [])
    assert any("Download ZIP" in entry for entry in diagnostics_messages)


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
        headers={"accept": "application/json"},
    )
    assert response.status_code == 200
    payload = response.json()["results_payload"]

    assert payload.get("mapping_applied") is True

    chart = payload.get("chart", {})
    speed = chart.get("speed", {})
    assert speed and speed.get("values")
    assert speed["values"][0] == pytest.approx(12.0, rel=1e-6)

    pollutants = chart.get("pollutants", [])
    nox_series = next((item for item in pollutants if item.get("key") == "NOx"), None)
    assert nox_series is not None
    assert nox_series["values"][0] == pytest.approx(120.0, rel=1e-6)

    kpis = payload.get("analysis", {}).get("kpis", {})
    assert "NOx_mg_per_km" in kpis
    nox_metric = kpis["NOx_mg_per_km"]
    assert isinstance(nox_metric, dict)
    assert nox_metric.get("total", {}).get("value") == pytest.approx(10000.0, rel=1e-6)


def test_analysis_accepts_inline_mapping_json() -> None:
    mapping_json = json.dumps(
        {
            "pems": {
                "timestamp": "Time",
                "exhaust_flow_kg_s": "ExhFlow_g_s",
                "nox_mg_s": "NOx_ug_s",
                "pn_1_s": "PN_count",
                "veh_speed_m_s": "Speed_m_s",
            },
            "gps": {
                "timestamp": "Time",
                "lat": "Latitude",
                "lon": "Longitude",
                "speed_m_s": "Speed_m_s",
            },
            "ecu": {
                "timestamp": "Time",
                "veh_speed_m_s": "VehicleSpeed",
                "engine_speed_rpm": "EngineRPM",
            },
            "units": {
                "nox_mg_s": "ug/s",
                "exhaust_flow_kg_s": "g/s",
            },
        }
    )

    response = client.post(
        "/analyze",
        data={"mapping_json": mapping_json},
        files={
            "pems_file": ("pems.csv", CUSTOM_PEMS.encode("utf-8"), "text/csv"),
            "gps_file": ("gps.csv", CUSTOM_GPS.encode("utf-8"), "text/csv"),
            "ecu_file": ("ecu.csv", CUSTOM_ECU.encode("utf-8"), "text/csv"),
        },
        headers={"accept": "application/json"},
    )
    assert response.status_code == 200
    payload = response.json()["results_payload"]

    chart = payload.get("chart", {})
    pollutants = chart.get("pollutants", [])
    nox_series = next((item for item in pollutants if item.get("key") == "NOx"), None)
    assert nox_series is not None
    assert nox_series["values"][0] == pytest.approx(120.0, rel=1e-6)


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
        headers={"accept": "application/json"},
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "timestamp" in detail
    assert "GPS" in detail


def test_export_pdf_requires_payload() -> None:
    response = client.post("/export_pdf", json={})
    assert response.status_code == 400
    assert response.json()["detail"] == "Results payload is required."


@pytest.mark.skipif(not WEASYPRINT_AVAILABLE, reason="WeasyPrint not installed")
def test_export_pdf_generates_document() -> None:
    payload = _post_analysis()

    response = client.post("/export_pdf", json={"results": payload})
    assert response.status_code == 200
    body = response.json()
    results = body["results_payload"]
    attachments = results.get("attachments", [])
    assert attachments, "expected pdf attachment"
    pdf_info = attachments[0]
    assert pdf_info["media_type"] == "application/pdf"
    pdf_bytes = base64.b64decode(pdf_info["content_base64"])
    assert len(pdf_bytes) > 500


@pytest.mark.skipif(WEASYPRINT_AVAILABLE, reason="WeasyPrint installed")
def test_export_pdf_reports_missing_dependency() -> None:
    payload = _post_analysis()

    response = client.post("/export_pdf", json={"results": payload})
    assert response.status_code == 503
    detail = response.json().get("detail", "")
    assert "WeasyPrint" in detail


def test_analysis_results_page_renders_html() -> None:
    html = _post_analysis_html()
    assert "data-results-payload=\"true\"" in html
    assert "id=\"charts-kpis\"" in html
    assert "id=\"drive-map\"" in html
    assert "window.__RDE_RESULT__ =" in html
    assert "/static/js/app.js" in html
    assert "window.dispatchEvent(new Event(rde:payload-ready))" in html

def test_results_page_includes_map_container() -> None:
    html = _post_analysis_html()
    assert "id=\"drive-map\"" in html


def test_payload_script_precedes_app_bundle() -> None:
    html = _post_analysis_html()
    payload_index = html.find("window.__RDE_RESULT__ =")
    bundle_index = html.find("/static/js/app.js")
    assert payload_index != -1 and bundle_index != -1
    assert payload_index < bundle_index


