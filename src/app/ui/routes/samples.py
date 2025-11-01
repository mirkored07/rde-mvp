"""Serve demo CSV sample files for the UI."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter()
ROOT = Path(__file__).resolve().parents[4]
SAMPLES = {
    "pems_demo.csv": ROOT / "data" / "samples" / "pems_demo.csv",
    "gps_demo.csv": ROOT / "data" / "samples" / "gps_demo.csv",
    "ecu_demo.csv": ROOT / "data" / "samples" / "ecu_demo.csv",
}


@router.get("/samples/{name}", include_in_schema=False)
def get_sample(name: str) -> FileResponse:
    path = SAMPLES.get(name)
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(path, media_type="text/csv", filename=name)


__all__ = ["router"]
