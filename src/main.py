"""FastAPI application entrypoint for the RDE MVP."""
from fastapi import FastAPI

app = FastAPI(title="RDE MVP")


@app.get("/health")
def read_health() -> dict[str, str]:
    """Simple health check endpoint for uptime monitoring."""
    return {"status": "ok"}


__all__ = ["app", "read_health"]
