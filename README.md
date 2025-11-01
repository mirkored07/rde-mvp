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
