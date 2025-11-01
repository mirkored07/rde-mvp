# RDE MVP

Minimal FastAPI scaffold for the RDE homologation app.

## Getting started

Install dependencies with [Poetry](https://python-poetry.org/):

```bash
poetry install
```

> **Offline environments.** Continuous-integration sandboxes that do not allow
> network egress will need pre-downloaded wheels for the heavy scientific stack
> used by the project (``numpy``, ``pandas``, ``fastapi`` and friends). Populate a
> local package index or vendor directory before running the test suite in such
> settings; otherwise `pytest` will fail during collection when those imports are
> missing.

Run the development server:

```bash
poetry run uvicorn src.main:app --reload
```

### EU7-LD conformity module

* The JSON reports served by the API live under `reports/` by default. Override
  the directory by exporting `REPORT_DIR=/path/to/reports` before starting the
  app.
* Generate a conformity JSON file by triggering any export action (PDF or ZIP)
  after running an analysis. The serializer writes `<testId>.json` into the
  reports directory alongside the export artifact.
* Fetch a stored report via `GET /api/report/{test_id}`. The sample dataset is
  available at `/api/report/sample` once the repo is cloned.
* Open `/report/{test_id}` in the browser to review the grouped conformity
  criteria with PASS/FAIL badges. Example: http://localhost:8000/report/sample
  while the development server is running.
* To add or refine checks without changing the schema, edit the rows generated
  in `src/app/reporting/eu7ld_report.py`. Each criterion is a dedicated entry
  in the typed `ReportData` model.

Run the test suite:

```bash
poetry run pytest
```

## CI/CD

> CI regenerates `poetry.lock` via `poetry lock --no-update` to prevent lock/pyproject drift errors.

## Quick guide: PEMS ingestion

Use :class:`src.app.data.ingestion.PEMSReader` to normalize raw exports and
enforce SI units. Provide the column mapping from your file to the repository's
canonical names plus optional units for automatic conversions:

```python
from src.app.data.ingestion import PEMSReader

columns = {
    "timestamp": "Time",
    "exhaust_flow_kg_s": "ExhFlow_g_s",
    "nox_mg_s": "NOx_ug_s",
    "amb_temp_c": "AmbientTemp_K",
}

units = {
    "exhaust_flow_kg_s": "g/s",  # convert to kg/s
    "nox_mg_s": "ug/s",          # convert to mg/s
    "amb_temp_c": "K",           # convert to degC
}

normalized = PEMSReader.from_csv("pems_raw.csv", columns=columns, units=units)
```

Converting concentration units such as ``ppm`` to mass flow requires additional
context (temperature, pressure, molar mass). Perform that transformation before
ingesting the data and supply the resulting ``mg/s`` stream to the reader.
