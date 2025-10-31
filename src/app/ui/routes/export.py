"""Sample export routes returning ZIP downloads."""

from __future__ import annotations

import base64
import io
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

router = APIRouter()

REPO_ROOT = Path(__file__).resolve().parents[4]
SAMPLES_DIR = REPO_ROOT / "data" / "samples"
ALLOWED_SAMPLE_FILES = ["ecu_demo.csv", "gps_demo.csv", "pems_demo.csv"]


def build_samples_zip_bytes() -> bytes:
    """Create an in-memory ZIP archive with the whitelisted sample CSVs."""

    buffer = io.BytesIO()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        for filename in ALLOWED_SAMPLE_FILES:
            file_path = SAMPLES_DIR / filename
            if file_path.exists():
                archive.write(file_path, arcname=filename)
    return buffer.getvalue()


@router.get("/export_zip", include_in_schema=False)
def export_zip(download: int = 1):
    """Return the RDE sample ZIP either as a file download or JSON envelope."""

    blob = build_samples_zip_bytes()
    if download:
        return StreamingResponse(
            io.BytesIO(blob),
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="rde_export.zip"'},
        )

    return {
        "results_payload": {
            "diagnostics": ["Download ZIP ready"],
            "attachments": [
                {
                    "filename": "rde_export.zip",
                    "media_type": "application/zip",
                    "content_base64": base64.b64encode(blob).decode("ascii"),
                }
            ],
        }
    }


__all__ = ["router", "build_samples_zip_bytes"]
