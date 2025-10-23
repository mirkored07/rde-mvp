"""Tests covering the Tailwind/HTMX UI endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.main import app

client = TestClient(app)


def test_index_page_renders() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "Upload PEMS, GPS, and ECU data" in response.text


def test_static_assets_served() -> None:
    response = client.get("/static/css/styles.css")
    assert response.status_code == 200
    assert ".dropzone" in response.text


def test_analysis_endpoint_returns_results() -> None:
    pems_csv = """timestamp,exhaust_flow_kg_s,nox_mg_s,pn_1_s,veh_speed_m_s
2024-01-01T00:00:00Z,0.35,120,200,12
2024-01-01T00:00:01Z,0.36,130,210,13
2024-01-01T00:00:02Z,0.37,140,220,14
2024-01-01T00:00:03Z,0.36,150,230,15
"""
    gps_csv = """timestamp,lat,lon,alt_m,speed_m_s
2024-01-01T00:00:00Z,48.8566,2.3522,35,12
2024-01-01T00:00:01Z,48.8567,2.3524,35,13
2024-01-01T00:00:02Z,48.8569,2.3527,35,14
2024-01-01T00:00:03Z,48.8571,2.3530,35,15
"""
    ecu_csv = """timestamp,veh_speed_m_s,engine_speed_rpm,engine_load_pct,throttle_pct
2024-01-01T00:00:00Z,12,1500,30,15
2024-01-01T00:00:01Z,13,1520,32,16
2024-01-01T00:00:02Z,14,1540,34,18
2024-01-01T00:00:03Z,15,1560,35,20
"""

    response = client.post(
        "/analyze",
        files={
            "pems_file": ("pems.csv", pems_csv.encode("utf-8"), "text/csv"),
            "gps_file": ("gps.csv", gps_csv.encode("utf-8"), "text/csv"),
            "ecu_file": ("ecu.csv", ecu_csv.encode("utf-8"), "text/csv"),
        },
    )

    assert response.status_code == 200
    assert "Analysis Summary" in response.text
    assert "Overall status" in response.text
