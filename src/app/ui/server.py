"""FastAPI router serving the Tailwind/HTMX powered user interface."""

from __future__ import annotations

import functools
import io
import json
import math
import pathlib
import tempfile
import zipfile
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import TemplateResponse

from src.app.data.analysis import AnalysisEngine, AnalysisResult, load_rules
from src.app.data.regulation import PackEvaluation, evaluate_pack, load_pack
from src.app.data.fusion import FusionEngine
from src.app.data.fusion.specs import StreamSpec
from src.app.data.ingestion import ECUReader, GPSReader, PEMSReader
from src.app.quality import Diagnostics, run_diagnostics, to_dict
from src.app.reporting.archive import build_report_archive
from src.app.reporting.html import build_report_html
from src.app.reporting.pdf import html_to_pdf_bytes
from src.app.schemas import UNIT_HINTS, as_payload
from src.app.utils.mappings import (
    DatasetMapping,
    MappingValidationError,
    load_mapping_from_dict,
    parse_mapping_payload,
    serialise_mapping_state,
    slugify_profile_name,
)

router = APIRouter(include_in_schema=False)

template_dir = pathlib.Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(template_dir))

BADGE_PASS = "px-2 py-1 rounded bg-green-600/10 text-green-700 dark:text-green-300 border border-green-600/20"
BADGE_FAIL = "px-2 py-1 rounded bg-red-600/10 text-red-700 dark:text-red-300 border border-red-600/20"

MAX_UPLOAD_MB = 50
GPS_EXTENSIONS = {".csv", ".nmea", ".gpx", ".txt"}
ECU_EXTENSIONS = {".csv", ".mf4", ".mdf"}
PEMS_EXTENSIONS = {".csv"}

_project_root = pathlib.Path(__file__).resolve().parents[3]
_rules_path = _project_root / "data" / "rules" / "demo_rules.json"
_regpack_path = _project_root / "data" / "regpacks" / "eu7_demo.json"
_samples_dir = _project_root / "data" / "samples"
_mappings_dir = _project_root / "data" / "mappings"
_SAMPLE_FILES: dict[str, pathlib.Path] = {
    path.name: path
    for path in _samples_dir.glob("*")
    if path.is_file()
}


def _ensure_mappings_dir() -> pathlib.Path:
    _mappings_dir.mkdir(parents=True, exist_ok=True)
    return _mappings_dir


def _canonical_schema_json() -> str:
    return json.dumps({"datasets": as_payload()}, separators=(",", ":"))


def _unit_hints_json() -> str:
    return json.dumps(UNIT_HINTS, separators=(",", ":"))


def _profile_path(slug: str) -> pathlib.Path:
    normalized = slugify_profile_name(slug)
    if not normalized:
        raise ValueError("Invalid mapping profile identifier.")
    return _ensure_mappings_dir() / f"{normalized}.json"


def _list_mapping_profiles() -> list[dict[str, str]]:
    directory = _ensure_mappings_dir()
    profiles: list[dict[str, str]] = []
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        slug = data.get("slug") or path.stem
        name = data.get("name") or slug
        if isinstance(slug, str) and isinstance(name, str):
            profiles.append({"slug": slug, "name": name})
    profiles.sort(key=lambda item: item["name"].lower())
    return profiles


def _load_mapping_profile(slug: str) -> dict[str, Any]:
    path = _profile_path(slug)
    if not path.exists():
        raise FileNotFoundError
    data = json.loads(path.read_text(encoding="utf-8"))
    mapping_payload = data.get("mapping")
    if not isinstance(mapping_payload, dict):
        mapping_payload = {}
    state = load_mapping_from_dict(mapping_payload)
    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        name = data.get("slug") or path.stem
    slug_value = data.get("slug") or slugify_profile_name(slug) or path.stem
    return {
        "name": name,
        "slug": slug_value,
        "mapping": serialise_mapping_state(state),
    }


def _save_mapping_profile(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = (name or "").strip()
    if not cleaned:
        raise ValueError("Profile name is required.")
    slug = slugify_profile_name(cleaned)
    if not slug:
        raise ValueError("Profile name must include letters or numbers.")
    state = load_mapping_from_dict(payload)
    data = {
        "name": cleaned,
        "slug": slug,
        "mapping": serialise_mapping_state(state),
    }
    path = _ensure_mappings_dir() / f"{slug}.json"
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    return data

DEFAULT_RULES_CONFIG: dict[str, Any] = {
    "speed_bins": [
        {"name": "urban", "max_kmh": 60},
        {"name": "rural", "min_kmh": 60, "max_kmh": 90},
        {"name": "motorway", "min_kmh": 90},
    ],
    "min_distance_km_per_bin": 5.0,
    "min_time_s_per_bin": 300,
    "completeness": {"max_gap_s": 3},
    "kpi_defs": {
        "NOx_mg_per_km": {"numerator": "nox_mg_s", "denominator": "veh_speed_m_s"},
        "PN_1_per_km": {"numerator": "pn_1_s", "denominator": "veh_speed_m_s"},
    },
}


def _get_sample_path(filename: str) -> pathlib.Path:
    path = _SAMPLE_FILES.get(filename)
    if path is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sample not found.")
    return path


@router.get("/samples/{filename}", include_in_schema=False)
def download_sample(filename: str) -> FileResponse:
    path = _get_sample_path(filename)
    media_type = "text/csv" if path.suffix.lower() == ".csv" else "application/octet-stream"
    return FileResponse(path, media_type=media_type, filename=path.name)


@router.get("/samples.zip", include_in_schema=False)
def download_samples_archive() -> StreamingResponse:
    if not _SAMPLE_FILES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sample not found.")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for sample in sorted(_SAMPLE_FILES.values(), key=lambda item: item.name):
            archive.write(sample, arcname=sample.name)

    buffer.seek(0)
    headers = {"Content-Disposition": "attachment; filename=samples.zip"}
    return StreamingResponse(buffer, media_type="application/zip", headers=headers)


@functools.lru_cache(maxsize=1)
def _load_analysis_rules() -> Any:
    if _rules_path.exists():
        return load_rules(_rules_path)
    return load_rules(DEFAULT_RULES_CONFIG)


@functools.lru_cache(maxsize=1)
def _load_regulation_pack() -> Any:
    if _regpack_path.exists():
        return load_pack(_regpack_path)
    raise FileNotFoundError("Default regulation pack not found.")


def _extension_for(filename: str | None) -> str:
    if not filename:
        return ""
    return pathlib.Path(filename).suffix.lower()


def _check_size_limit(path: pathlib.Path, label: str) -> None:
    size_mb = path.stat().st_size / (1024 * 1024)
    if size_mb > MAX_UPLOAD_MB:
        raise ValueError(f"{label} exceeds the {MAX_UPLOAD_MB} MB upload limit.")


def _require_extension(filename: str | None, allowed: set[str], label: str) -> None:
    ext = _extension_for(filename)
    if ext not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise ValueError(f"Unsupported {label} file type: '{ext or 'unknown'}'. Expected one of {allowed_text}.")


def _write_temp_file(
    filename: str | None,
    data: bytes,
    *,
    label: str,
    allowed: set[str],
) -> pathlib.Path:
    _require_extension(filename, allowed, label)
    suffix = pathlib.Path(filename or "").suffix
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
        handle.write(data)
        temp_path = pathlib.Path(handle.name)
    _check_size_limit(temp_path, label)
    return temp_path


def _ingest_pems(
    filename: str | None,
    data: bytes,
    mapping: "DatasetMapping" | None = None,
) -> pd.DataFrame:
    path = _write_temp_file(filename, data, label="PEMS", allowed=PEMS_EXTENSIONS)
    try:
        columns = mapping.column_mapping() if mapping else None
        units = mapping.unit_mapping() if mapping else None
        frame = PEMSReader.from_csv(str(path), columns=columns, units=units)
    except Exception as exc:  # pragma: no cover - reader surfaces detailed errors
        raise ValueError(str(exc)) from exc
    finally:
        path.unlink(missing_ok=True)
    return frame


def _ingest_gps(
    filename: str | None,
    data: bytes,
    mapping: "DatasetMapping" | None = None,
) -> pd.DataFrame:
    path = _write_temp_file(filename, data, label="GPS", allowed=GPS_EXTENSIONS)
    try:
        ext = _extension_for(filename)
        if ext == ".csv":
            columns = mapping.column_mapping() if mapping else None
            frame = GPSReader.from_csv(str(path), mapping=columns)
        elif ext in {".nmea", ".txt"}:
            frame = GPSReader.from_nmea(str(path))
        elif ext == ".gpx":
            frame = GPSReader.from_gpx(str(path))
        else:  # pragma: no cover - guarded by extension check
            columns = mapping.column_mapping() if mapping else None
            frame = GPSReader.from_csv(str(path), mapping=columns)
    finally:
        path.unlink(missing_ok=True)
    if frame.empty:
        raise ValueError("GPS file does not contain any valid fixes.")
    return frame


def _ingest_ecu(
    filename: str | None,
    data: bytes,
    mapping: "DatasetMapping" | None = None,
) -> pd.DataFrame:
    path = _write_temp_file(filename, data, label="ECU", allowed=ECU_EXTENSIONS)
    try:
        ext = _extension_for(filename)
        if ext == ".csv":
            columns = mapping.column_mapping() if mapping else None
            frame = ECUReader.from_csv(str(path), mapping=columns)
        else:
            columns = mapping.column_mapping() if mapping else None
            frame = ECUReader.from_mdf(str(path), mapping=columns)
    finally:
        path.unlink(missing_ok=True)
    if frame.empty:
        raise ValueError("ECU file does not contain any samples.")
    return frame


def _ensure_speed_column(df: pd.DataFrame) -> pd.DataFrame:
    if "veh_speed_m_s" in df.columns:
        return df
    for candidate in ("veh_speed_m_s_pems", "veh_speed_m_s_ecu", "speed_m_s", "speed_m_s_gps"):
        if candidate in df.columns and df[candidate].notna().any():
            df = df.copy()
            df["veh_speed_m_s"] = df[candidate]
            return df
    raise ValueError("Unable to locate a vehicle speed signal in the fused dataset.")


def _fuse_streams(pems: pd.DataFrame, gps: pd.DataFrame, ecu: pd.DataFrame) -> pd.DataFrame:
    gps_spec = StreamSpec(df=gps, ts_col="timestamp", name="gps", ref_cols=["speed_m_s"])
    streams: list[StreamSpec] = []
    if not pems.empty:
        streams.append(StreamSpec(df=pems, ts_col="timestamp", name="pems", ref_cols=["veh_speed_m_s"]))
    if not ecu.empty:
        streams.append(StreamSpec(df=ecu, ts_col="timestamp", name="ecu", ref_cols=["veh_speed_m_s"]))

    engine = FusionEngine(gps=gps_spec, streams=streams)
    fused = engine.fuse()
    fused = fused.sort_values("timestamp", kind="stable").reset_index(drop=True)
    fused = _ensure_speed_column(fused)
    return fused


def _reduce_rows(df: pd.DataFrame, max_points: int = 500) -> pd.DataFrame:
    if df.empty:
        return df
    if len(df) <= max_points:
        return df
    step = max(1, math.ceil(len(df) / max_points))
    return df.iloc[::step].reset_index(drop=True)


def _to_iso(timestamp: pd.Timestamp) -> str:
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat()


def _prepare_chart_payload(derived: pd.DataFrame) -> dict[str, Any]:
    if derived.empty or "timestamp" not in derived.columns:
        return {"traces": [], "layout": {}}

    working = derived[[col for col in derived.columns if col in {"timestamp", "veh_speed_m_s", "nox_mg_s", "pn_1_s", "exhaust_flow_kg_s"}]].copy()
    working = working.dropna(subset=["timestamp"])
    working = working.sort_values("timestamp", kind="stable")
    working = _reduce_rows(working)

    times = [_to_iso(ts) for ts in working["timestamp"]]
    traces: list[dict[str, Any]] = []

    if "veh_speed_m_s" in working.columns:
        traces.append(
            {
                "name": "Vehicle speed (m/s)",
                "mode": "lines",
                "x": times,
                "y": [value if pd.notna(value) else None for value in working["veh_speed_m_s"]],
                "line": {"color": "#2563eb", "width": 2.5},
                "yaxis": "y1",
            }
        )

    emission_series = [
        ("NOx (mg/s)", "nox_mg_s", "#dc2626"),
        ("PN (#/s)", "pn_1_s", "#9333ea"),
        ("Exhaust flow (kg/s)", "exhaust_flow_kg_s", "#14b8a6"),
    ]

    for label, column, color in emission_series:
        if column in working.columns and working[column].notna().any():
            traces.append(
                {
                    "name": label,
                    "mode": "lines",
                    "x": times,
                    "y": [value if pd.notna(value) else None for value in working[column]],
                    "line": {"color": color, "width": 2},
                    "yaxis": "y2",
                }
            )

    layout = {
        "margin": {"t": 32, "r": 32, "b": 40, "l": 48},
        "legend": {"orientation": "h", "y": -0.2},
        "xaxis": {"title": "Time", "showgrid": False},
        "yaxis": {"title": "Speed (m/s)", "zeroline": False},
        "yaxis2": {
            "title": "Emissions",
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "zeroline": False,
        },
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
    }

    return {"traces": traces, "layout": layout}


def _prepare_map_payload(derived: pd.DataFrame) -> dict[str, Any]:
    if derived.empty:
        return {"points": []}
    if "lat" not in derived.columns or "lon" not in derived.columns:
        return {"points": []}

    coords = derived.dropna(subset=["lat", "lon"])
    if coords.empty:
        return {"points": []}

    coords = coords[["lat", "lon"]]
    coords = coords.sort_index()
    coords = _reduce_rows(coords, max_points=750)

    points = [
        {"lat": float(row.lat), "lon": float(row.lon)}
        for row in coords.itertuples(index=False, name="Coord")
    ]

    lats = [p["lat"] for p in points]
    lons = [p["lon"] for p in points]
    bounds = [[min(lats), min(lons)], [max(lats), max(lons)]] if points else []
    center = {"lat": sum(lats) / len(lats), "lon": sum(lons) / len(lons)} if points else None

    return {"points": points, "bounds": bounds, "center": center}


def _format_metric(label: str, value: float | None, suffix: str) -> dict[str, str]:
    if value is None or pd.isna(value):
        display = "n/a"
    else:
        display = f"{value:.2f}{suffix}"
    return {"label": label, "value": display}


def _format_value(value: float | None, units: str | None, *, decimals: int | None = None) -> str:
    if value is None or pd.isna(value):
        return "n/a"

    val = float(value)
    if decimals is not None:
        formatted = f"{val:.{decimals}f}"
    else:
        abs_val = abs(val)
        if abs_val >= 1000 or (abs_val > 0 and abs_val < 0.01):
            formatted = f"{val:.3g}"
        else:
            formatted = f"{val:.3f}"

    if units:
        return f"{formatted} {units}"
    return formatted


def _format_margin(margin: float | None, units: str | None) -> str | None:
    if margin is None or pd.isna(margin):
        return None
    val = float(margin)
    abs_val = abs(val)
    if abs_val >= 1000 or (abs_val > 0 and abs_val < 0.01):
        formatted = f"{val:+.3g}"
    else:
        formatted = f"{val:+.3f}"
    if units:
        return f"{formatted} {units}"
    return formatted


def _prepare_results(
    result: AnalysisResult,
    evaluation: PackEvaluation,
    diagnostics: Diagnostics | dict[str, Any] | None = None,
) -> dict[str, Any]:
    overall = result.analysis.get("overall", {})
    completeness = overall.get("completeness") or {}
    status_ok = bool(overall.get("valid"))
    status_label = "PASS" if status_ok else "FAIL"

    total_distance = overall.get("total_distance_km")
    total_time = overall.get("total_time_s")
    largest_gap = completeness.get("largest_gap_s")

    metrics: list[dict[str, str]] = []
    metrics.append(_format_metric("Total distance", float(total_distance) if isinstance(total_distance, (int, float)) else None, " km"))
    metrics.append(
        _format_metric(
            "Duration",
            float(total_time) / 60 if isinstance(total_time, (int, float)) else None,
            " min",
        )
    )
    metrics.append(
        _format_metric(
            "Largest gap",
            float(largest_gap) if isinstance(largest_gap, (int, float)) else None,
            " s",
        )
    )

    bins_payload = result.analysis.get("bins") or {}
    bin_rows: list[dict[str, Any]] = []
    for name, info in bins_payload.items():
        time_s = info.get("time_s")
        distance_km = info.get("distance_km")
        kpis = info.get("kpis") or {}
        kpi_rows = []
        for kpi_name, value in kpis.items():
            if value is None or pd.isna(value):
                display = "n/a"
            else:
                display = f"{float(value):.3f}"
            kpi_rows.append({"name": kpi_name, "value": display})
        bin_rows.append(
            {
                "name": name,
                "valid": bool(info.get("valid")),
                "time": f"{float(time_s):.1f}" if isinstance(time_s, (int, float)) else "0.0",
                "distance": f"{float(distance_km):.3f}" if isinstance(distance_km, (int, float)) else "0.000",
                "kpis": kpi_rows,
            }
        )

    chart_payload = _prepare_chart_payload(result.derived)
    map_payload = _prepare_map_payload(result.derived)

    evidence_rows: list[dict[str, Any]] = []
    for item in evaluation.evidence:
        rule = item.rule
        requirement_value = _format_value(rule.threshold, rule.units)
        observed_value = _format_value(item.actual, rule.units)
        requirement = (
            f"{rule.comparator} {requirement_value}"
            if requirement_value != "n/a"
            else rule.comparator
        )

        context_items: list[dict[str, str]] = []
        distance = item.context.get("distance_km")
        if distance is not None and not pd.isna(distance):
            context_items.append(
                {
                    "label": "Distance in bin",
                    "value": _format_value(distance, "km", decimals=3),
                }
            )
        time_spent = item.context.get("time_s")
        if time_spent is not None and not pd.isna(time_spent):
            context_items.append(
                {
                    "label": "Time in bin",
                    "value": _format_value(time_spent, "s", decimals=1),
                }
            )
        margin_text = _format_margin(item.margin, rule.units)
        if margin_text:
            context_items.append({"label": "Margin", "value": margin_text})
        if item.bin_name:
            context_items.append({"label": "Bin", "value": item.bin_name})

        notes = [note for note in (rule.notes,) if note]

        evidence_rows.append(
            {
                "id": rule.id,
                "title": rule.title,
                "legal_source": rule.legal_source,
                "article": rule.article,
                "scope": rule.scope,
                "metric": rule.metric,
                "mandatory": rule.mandatory,
                "passed": item.passed,
                "requirement": requirement,
                "observed": observed_value,
                "context": context_items,
                "notes": notes,
                "detail": item.detail,
            }
        )

    regulation_summary = {
        "label": "PASS" if evaluation.overall_passed else "FAIL",
        "ok": evaluation.overall_passed,
        "pack_id": evaluation.pack.id,
        "pack_title": evaluation.pack.title,
        "legal_source": evaluation.pack.legal_source,
        "version": evaluation.pack.version,
        "counts": {
            "mandatory_passed": evaluation.mandatory_passed,
            "mandatory_total": evaluation.mandatory_total,
            "optional_passed": evaluation.optional_passed,
            "optional_total": evaluation.optional_total,
        },
    }

    payload: dict[str, Any] = {
        "regulation": regulation_summary,
        "evidence": evidence_rows,
        "analysis": {
            "summary_md": result.summary_md,
            "status": {"label": status_label, "ok": status_ok},
            "metrics": metrics,
            "bins": bin_rows,
            "chart": chart_payload,
            "map": map_payload,
        },
    }

    if diagnostics is not None:
        payload["diagnostics"] = to_dict(diagnostics) if isinstance(diagnostics, Diagnostics) else diagnostics

    return payload


def _build_results_context(
    results: dict[str, Any] | None,
    errors: list[str],
    *,
    include_export_controls: bool = True,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "errors": errors,
        "has_errors": bool(errors),
        "include_export_controls": include_export_controls,
        "has_results": bool(results) and not errors,
        "results": results,
    }

    if errors or results is None:
        return context

    regulation = results.get("regulation") or {}
    counts = regulation.get("counts") or {}

    def _as_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    reg_ok = bool(regulation.get("ok"))
    reg_label = str(regulation.get("label") or ("PASS" if reg_ok else "FAIL"))
    pack_title = regulation.get("pack_title") or regulation.get("pack_id") or "Regulation pack"

    regulation_view = {
        "label": reg_label,
        "ok": reg_ok,
        "status_class": BADGE_PASS if reg_ok else BADGE_FAIL,
        "ping_class": "bg-emerald-400/60" if reg_ok else "bg-rose-400/60",
        "dot_class": "bg-emerald-500" if reg_ok else "bg-rose-500",
        "pack_title": pack_title,
        "version": regulation.get("version"),
        "legal_source": regulation.get("legal_source"),
        "pack_id": regulation.get("pack_id"),
        "counts": {
            "mandatory_passed": _as_int(counts.get("mandatory_passed")),
            "mandatory_total": _as_int(counts.get("mandatory_total")),
            "optional_passed": _as_int(counts.get("optional_passed")),
            "optional_total": _as_int(counts.get("optional_total")),
        },
    }

    analysis = results.get("analysis") or {}
    analysis_status = analysis.get("status") or {}
    analysis_ok = bool(analysis_status.get("ok"))
    analysis_label = str(analysis_status.get("label", ""))

    analysis_view = {
        "status": {
            "ok": analysis_ok,
            "label": analysis_label,
            "class": BADGE_PASS if analysis_ok else BADGE_FAIL,
        },
        "metrics": analysis.get("metrics", []),
        "bins": analysis.get("bins", []),
        "summary_md": analysis.get("summary_md", ""),
        "chart": analysis.get("chart", {}),
        "map": analysis.get("map", {}),
    }

    diagnostics_data = results.get("diagnostics") or {}
    diag_checks = diagnostics_data.get("checks") or []
    diag_summary = diagnostics_data.get("summary") or {}
    diag_repaired: list[str] = []
    for span in diagnostics_data.get("repaired_spans") or []:
        parts: list[str] = []
        start_val = span.get("start")
        end_val = span.get("end")
        start_text = "" if start_val in (None, "") else str(start_val)
        end_text = "" if end_val in (None, "") else str(end_val)
        if start_text or end_text:
            parts.append(f"{start_text} → {end_text}")
        seconds = span.get("seconds")
        if isinstance(seconds, (int, float)):
            parts.append(f"{float(seconds):.2f} s")
        inserted = span.get("inserted")
        if isinstance(inserted, (int, float)):
            parts.append(f"{int(inserted)} rows")
        if parts:
            diag_repaired.append(" · ".join(parts))

    context.update(
        {
            "regulation": regulation_view,
            "analysis": analysis_view,
            "evidence": results.get("evidence", []),
            "diagnostics": {
                "checks": diag_checks,
                "summary": diag_summary,
                "repaired": diag_repaired,
            },
        }
    )

    return context



def _base_template_context(
    request: Request,
    *,
    results: dict[str, Any] | None,
    errors: list[str],
    include_export_controls: bool = True,
) -> dict[str, Any]:
    context = {
        "request": request,
        "max_upload_mb": MAX_UPLOAD_MB,
        "canonical_schema_json": _canonical_schema_json(),
        "unit_hints_json": _unit_hints_json(),
        "badge_pass": BADGE_PASS,
        "badge_fail": BADGE_FAIL,
    }
    context.update(
        _build_results_context(
            results,
            errors,
            include_export_controls=include_export_controls,
        )
    )
    return context



@router.get("/")
async def index(request: Request) -> TemplateResponse:
    context = _base_template_context(request, results=None, errors=[])
    return templates.TemplateResponse("index.html", context)


async def _extract_form_data(
    request: Request,
) -> tuple[dict[str, tuple[str | None, bytes]], dict[str, str]]:
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise ValueError("Request must be multipart/form-data.")

    boundary_token = None
    for part in content_type.split(";"):
        part = part.strip()
        if part.startswith("boundary="):
            boundary_token = part.split("=", 1)[1].strip()
            break
    if boundary_token is None:
        raise ValueError("Multipart boundary not found in Content-Type header.")
    if boundary_token.startswith(""") and boundary_token.endswith("""):
        boundary_token = boundary_token[1:-1]

    body = await request.body()
    boundary = boundary_token.encode("utf-8")
    delimiter = b"--" + boundary

    files: dict[str, tuple[str | None, bytes]] = {}
    fields: dict[str, str] = {}
    for raw_part in body.split(delimiter):
        if not raw_part or raw_part in {b"", b"--", b"--\r\n"}:
            continue
        part = raw_part.lstrip(b"\r\n")
        header_block, _, content = part.partition(b"\r\n\r\n")
        if not _:
            continue
        content = content.rstrip(b"\r\n")
        headers: dict[str, str] = {}
        for header_line in header_block.split(b"\r\n"):
            if not header_line:
                continue
            name, _, value = header_line.decode("utf-8", errors="ignore").partition(":")
            headers[name.lower().strip()] = value.strip()

        disposition = headers.get("content-disposition", "")
        if "form-data" not in disposition:
            continue
        params: dict[str, str] = {}
        for segment in disposition.split(";"):
            if "=" not in segment:
                continue
            key, _, val = segment.strip().partition("=")
            cleaned = val.strip().strip('"')
            params[key.lower()] = cleaned
        field = params.get("name")
        if not field:
            continue
        filename = params.get("filename")
        if filename is not None:
            files[field] = (filename, content)
        else:
            fields[field] = content.decode("utf-8", errors="ignore")

    return files, fields


@router.post("/analyze")
async def analyze(request: Request) -> TemplateResponse:
    errors: list[str] = []
    results_payload: dict[str, Any] | None = None

    try:
        files, fields = await _extract_form_data(request)
    except ValueError as exc:
        errors.append(str(exc))
        files = {}
        fields = {}

    mapping_state: dict[str, DatasetMapping] = {}
    if not errors:
        try:
            mapping_state = parse_mapping_payload(fields.get("mapping_payload"))
        except MappingValidationError as exc:
            errors.append(str(exc))

    if not errors:
        required = {"pems_file", "gps_file", "ecu_file"}
        missing = required - set(files)
        if missing:
            errors.append(
                "Please provide PEMS, GPS, and ECU files to run the analysis."
            )

    if not errors:
        try:
            pems_name, pems_bytes = files["pems_file"]
            gps_name, gps_bytes = files["gps_file"]
            ecu_name, ecu_bytes = files["ecu_file"]

            pems_df = _ingest_pems(pems_name, pems_bytes, mapping=mapping_state.get("pems"))
            gps_df = _ingest_gps(gps_name, gps_bytes, mapping=mapping_state.get("gps"))
            ecu_df = _ingest_ecu(ecu_name, ecu_bytes, mapping=mapping_state.get("ecu"))
            fused = _fuse_streams(pems_df, gps_df, ecu_df)
            fused, diagnostics = run_diagnostics(
                fused,
                {
                    "pems": pems_df,
                    "gps": gps_df,
                    "ecu": ecu_df,
                },
                gap_threshold_s=2.0,
                repair_small_gaps=True,
                repair_threshold_s=3.0,
            )
            engine = AnalysisEngine(_load_analysis_rules())
            analysis_result = engine.analyze(fused)
            pack = _load_regulation_pack()
            evaluation = evaluate_pack(analysis_result.analysis, pack)
            results_payload = _prepare_results(analysis_result, evaluation, diagnostics)
        except Exception as exc:  # pragma: no cover - user feedback path
            errors.append(str(exc))

    status_code = status.HTTP_400_BAD_REQUEST if errors else status.HTTP_200_OK

    if request.headers.get("hx-request") == "true":
        context = _build_results_context(
            results_payload,
            errors,
            include_export_controls=True,
        )
        context.update({
            "request": request,
            "badge_pass": BADGE_PASS,
            "badge_fail": BADGE_FAIL,
        })
        return templates.TemplateResponse(
            "results.html",
            context,
            status_code=status_code,
        )

    context = _base_template_context(
        request,
        results=results_payload,
        errors=errors,
        include_export_controls=True,
    )
    return templates.TemplateResponse("index.html", context, status_code=status_code)


@router.get("/mapping_profiles", include_in_schema=False)
async def list_mapping_profiles_endpoint() -> JSONResponse:
    try:
        profiles = _list_mapping_profiles()
    except Exception as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to list mapping profiles.",
        ) from exc
    return JSONResponse({"profiles": profiles})


@router.get("/mapping_profiles/{slug}", include_in_schema=False)
async def get_mapping_profile(slug: str) -> JSONResponse:
    try:
        profile = _load_mapping_profile(slug)
    except FileNotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Mapping profile not found.")
    except MappingValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except Exception as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to load mapping profile.",
        ) from exc
    return JSONResponse(profile)


@router.post("/mapping_profiles", include_in_schema=False)
async def save_mapping_profile(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception as exc:  # pragma: no cover - FastAPI handles parsing
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload.") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Payload must be an object.")

    name = payload.get("name")
    mapping_payload = payload.get("mapping")
    if not isinstance(mapping_payload, dict):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Mapping payload must be an object.")
    if not isinstance(name, str) or not name.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Profile name is required.")

    try:
        saved = _save_mapping_profile(name, mapping_payload)
    except MappingValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except Exception as exc:  # pragma: no cover - filesystem guard
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to save mapping profile.",
        ) from exc

    return JSONResponse({"slug": saved["slug"], "name": saved["name"]})



@router.post("/export_pdf", include_in_schema=False)
async def export_pdf(request: Request) -> Response:
    try:
        payload = await request.json()
    except Exception as exc:  # pragma: no cover - FastAPI surfaces JSON errors
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload.") from exc

    results_payload = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results_payload, dict):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Results payload is required.")

    try:
        html_document = build_report_html(results_payload)
    except Exception as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Unable to render report from supplied payload.",
        ) from exc

    try:
        pdf_bytes = html_to_pdf_bytes(html_document)
    except RuntimeError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    headers = {"Content-Disposition": "attachment; filename=analysis-report.pdf"}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)


@router.post("/export_zip", include_in_schema=False)
async def export_zip(request: Request) -> Response:
    try:
        payload = await request.json()
    except Exception as exc:  # pragma: no cover - FastAPI surfaces JSON errors
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload.") from exc

    results_payload = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results_payload, dict):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Results payload is required.")

    try:
        archive_bytes = build_report_archive(results_payload)
    except Exception as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Unable to render report from supplied payload.",
        ) from exc

    headers = {"Content-Disposition": "attachment; filename=analysis-report.zip"}
    return Response(content=archive_bytes, media_type="application/zip", headers=headers)


__all__ = ["router"]
