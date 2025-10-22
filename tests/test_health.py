"""Smoke tests for the FastAPI health endpoint."""
from fastapi.testclient import TestClient

from src.main import app


def test_health_returns_ok_status() -> None:
    """Ensure the health endpoint returns a 200 status with expected payload."""
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
