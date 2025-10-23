"""FastAPI application entrypoint for the RDE MVP."""

from __future__ import annotations

from src.app.api.main import app, health

__all__ = ["app", "health"]
