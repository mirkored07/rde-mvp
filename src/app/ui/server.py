"""FastAPI router serving the Tailwind/HTMX powered user interface."""

from __future__ import annotations

import functools
import html
import io
import json
import math
import pathlib
import tempfile
import zipfile
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse

from src.app.data.analysis import AnalysisEngine, AnalysisResult, load_rules
from src.app.data.regulation import PackEvaluation, evaluate_pack, load_pack
from src.app.data.fusion import FusionEngine
from src.app.data.fusion.specs import StreamSpec
from src.app.data.ingestion import ECUReader, GPSReader
from src.app.reporting.html import build_report_html
from src.app.reporting.pdf import html_to_pdf_bytes

router = APIRouter(include_in_schema=False)

template_dir = pathlib.Path(__file__).resolve().parent / "templates"
_index_template_cache: str | None = None

MAX_UPLOAD_MB = 50
GPS_EXTENSIONS = {".csv", ".nmea", ".gpx", ".txt"}
ECU_EXTENSIONS = {".csv", ".mf4", ".mdf"}
PEMS_EXTENSIONS = {".csv"}

_project_root = pathlib.Path(__file__).resolve().parents[3]
_rules_path = _project_root / "data" / "rules" / "demo_rules.json"
_regpack_path = _project_root / "data" / "regpacks" / "eu7_demo.json"
_samples_dir = _project_root / "data" / "samples"
_SAMPLE_FILES: dict[str, pathlib.Path] = {
    path.name: path
    for path in _samples_dir.glob("*")
    if path.is_file()
}
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


def _normalize_pems(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("PEMS file does not contain any rows.")
    if "timestamp" not in df.columns:
        raise ValueError("PEMS data must include a 'timestamp' column.")

    normalized = df.copy()
    normalized["timestamp"] = pd.to_datetime(
        normalized["timestamp"], utc=True, errors="coerce"
    )
    if normalized["timestamp"].isna().any():
        raise ValueError("PEMS timestamps could not be parsed into UTC datetimes.")

    for column in normalized.columns:
        if column == "timestamp":
            continue
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.sort_values("timestamp", kind="stable").reset_index(drop=True)
    return normalized


def _ingest_pems(filename: str | None, data: bytes) -> pd.DataFrame:
    path = _write_temp_file(filename, data, label="PEMS", allowed=PEMS_EXTENSIONS)
    try:
        frame = pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - pandas provides rich errors
        raise ValueError("Unable to read the PEMS CSV file.") from exc
    finally:
        path.unlink(missing_ok=True)
    return _normalize_pems(frame)


def _ingest_gps(filename: str | None, data: bytes) -> pd.DataFrame:
    path = _write_temp_file(filename, data, label="GPS", allowed=GPS_EXTENSIONS)
    try:
        ext = _extension_for(filename)
        if ext == ".csv":
            frame = GPSReader.from_csv(str(path))
        elif ext in {".nmea", ".txt"}:
            frame = GPSReader.from_nmea(str(path))
        elif ext == ".gpx":
            frame = GPSReader.from_gpx(str(path))
        else:  # pragma: no cover - guarded by extension check
            frame = GPSReader.from_csv(str(path))
    finally:
        path.unlink(missing_ok=True)
    if frame.empty:
        raise ValueError("GPS file does not contain any valid fixes.")
    return frame


def _ingest_ecu(filename: str | None, data: bytes) -> pd.DataFrame:
    path = _write_temp_file(filename, data, label="ECU", allowed=ECU_EXTENSIONS)
    try:
        ext = _extension_for(filename)
        if ext == ".csv":
            frame = ECUReader.from_csv(str(path))
        else:
            frame = ECUReader.from_mdf(str(path))
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


def _prepare_results(result: AnalysisResult, evaluation: PackEvaluation) -> dict[str, Any]:
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

    return {
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


def _render_results_html(
    results: dict[str, Any] | None,
    errors: list[str],
    *,
    include_export_controls: bool = True,
) -> str:
    if errors:
        items = "".join(f"<li>{html.escape(message)}</li>" for message in errors)
        return (
            '<div class="rounded-3xl border border-rose-200/70 bg-rose-50/70 p-6 text-rose-700 shadow-inner '
            'dark:border-rose-500/40 dark:bg-rose-500/10 dark:text-rose-100">'
            "<h3 class='text-lg font-semibold'>We couldn't analyze your files</h3>"
            f"<ul class='mt-3 list-disc space-y-2 pl-5 text-sm'>{items}</ul></div>"
        )

    if results is None:
        return (
            '<div class="rounded-3xl border border-dashed border-slate-300 bg-white/60 p-10 text-center text-slate-500 '
            'shadow-inner dark:border-slate-700 dark:bg-slate-900/40 dark:text-slate-400">'
            "<p class='text-lg font-medium'>Upload your telemetry files to see KPIs, interactive charts, and a live map of the route.</p></div>"
        )

    regulation = results.get("regulation") or {}
    analysis = results.get("analysis") or {}
    evidence = results.get("evidence") or []

    def _escape_text(value: Any) -> str:
        return html.escape(str(value)) if value not in (None, "") else ""

    reg_ok = bool(regulation.get("ok"))
    reg_label = _escape_text(regulation.get("label") or ("PASS" if reg_ok else "FAIL"))
    status_classes = (
        "bg-emerald-500/15 text-emerald-600 dark:bg-emerald-500/20 dark:text-emerald-200"
        if reg_ok
        else "bg-rose-500/15 text-rose-600 dark:bg-rose-500/20 dark:text-rose-200"
    )
    ping_color = "bg-emerald-400/60" if reg_ok else "bg-rose-400/60"
    dot_color = "bg-emerald-500" if reg_ok else "bg-rose-500"

    pack_label_raw = (
        regulation.get("pack_title")
        or regulation.get("pack_id")
        or "Regulation pack"
    )
    pack_label = _escape_text(pack_label_raw)
    version_raw = regulation.get("version")
    version_badge = (
        f'<span class="ml-2 inline-flex items-center rounded-full bg-slate-500/10 px-2 py-0.5 text-xs font-semibold '
        f'text-slate-600 dark:bg-slate-700/60 dark:text-slate-200">v{_escape_text(version_raw)}</span>'
        if version_raw not in (None, "")
        else ""
    )
    legal_source_html = _escape_text(regulation.get("legal_source"))
    pack_id_html = _escape_text(regulation.get("pack_id"))

    counts = regulation.get("counts") or {}

    def _as_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    mandatory_passed = _as_int(counts.get("mandatory_passed"))
    mandatory_total = _as_int(counts.get("mandatory_total"))
    optional_passed = _as_int(counts.get("optional_passed"))
    optional_total = _as_int(counts.get("optional_total"))

    def _rule_card(label: str, passed: int, total: int, hint: str) -> str:
        if total:
            value_text = f"{passed}/{total}"
            hint_text = hint
        else:
            value_text = "0"
            hint_text = "Not configured"
        return (
            '<div class="rounded-2xl border border-slate-200 bg-white/80 px-5 py-4 text-center shadow-sm '
            'dark:border-slate-700 dark:bg-slate-900/70">'
            f'<dt class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400 dark:text-slate-500">{html.escape(label)}</dt>'
            f'<dd class="mt-2 text-lg font-semibold text-slate-900 dark:text-white">{html.escape(value_text)}</dd>'
            f'<p class="mt-1 text-xs text-slate-500 dark:text-slate-400">{html.escape(hint_text)}</p>'
            '</div>'
        )

    regulation_cards = "".join(
        (
            _rule_card("Mandatory rules", mandatory_passed, mandatory_total, "All must pass"),
            _rule_card("Optional rules", optional_passed, optional_total, "Informational"),
        )
    )

    pack_line = (
        '<p class="mt-3 text-sm text-slate-500 dark:text-slate-400">Pack: '
        f'<span class="font-semibold text-slate-900 dark:text-white">{pack_label}</span>{version_badge}</p>'
        if pack_label
        else ""
    )
    legal_line = (
        f'<p class="text-xs text-slate-500 dark:text-slate-400">Legal source: {legal_source_html}</p>'
        if legal_source_html
        else ""
    )
    id_line = (
        f'<p class="text-xs text-slate-400 dark:text-slate-500">Identifier: <code>{pack_id_html}</code></p>'
        if pack_id_html and pack_id_html != pack_label
        else ""
    )

    regulation_block = (
        '<div class="rounded-2xl border border-slate-200 bg-white/90 p-6 shadow-sm '
        'dark:border-slate-700 dark:bg-slate-900/70">'
        '<div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">'
        '<div>'
        '<p class="text-xs font-semibold uppercase tracking-[0.4em] text-slate-500 dark:text-slate-400">Regulation verdict</p>'
        f'<span class="mt-3 inline-flex items-center gap-2 rounded-full px-4 py-2 text-sm font-semibold {status_classes}">' \
        f'<span class="relative flex h-2.5 w-2.5"><span class="absolute inline-flex h-full w-full animate-ping rounded-full {ping_color}"></span>'
        f'<span class="relative inline-flex h-2.5 w-2.5 rounded-full {dot_color}"></span></span>{reg_label}</span>'
        f'{pack_line}{legal_line}{id_line}'
        '</div>'
        f'<dl class="grid gap-4 sm:grid-cols-2 lg:gap-6">{regulation_cards}</dl>'
        '</div>'
        '</div>'
    )

    evidence_rows = []
    for entry in evidence:
        passed = bool(entry.get("passed"))
        status_badge = (
            "bg-emerald-500/15 text-emerald-600 dark:bg-emerald-500/20 dark:text-emerald-200"
            if passed
            else "bg-rose-500/15 text-rose-600 dark:bg-rose-500/20 dark:text-rose-200"
        )
        mandatory = bool(entry.get("mandatory"))
        mandatory_badge = (
            "bg-amber-500/20 text-amber-600 dark:bg-amber-500/25 dark:text-amber-200"
            if mandatory
            else "bg-slate-500/15 text-slate-600 dark:bg-slate-700/60 dark:text-slate-300"
        )
        mandatory_label = "Mandatory" if mandatory else "Optional"
        requirement = _escape_text(entry.get("requirement"))
        observed = _escape_text(entry.get("observed"))

        context_items = entry.get("context") or []
        context_html = "".join(
            f'<li><span class="font-medium text-slate-900 dark:text-white">{_escape_text(item.get("label"))}:</span> '
            f'{_escape_text(item.get("value"))}</li>'
            for item in context_items
        )
        if context_html:
            context_html = (
                "<ul class='mt-2 space-y-1 text-xs text-slate-500 dark:text-slate-400'>"
                + context_html
                + "</ul>"
            )

        detail = entry.get("detail")
        detail_html = (
            f'<p class="mt-2 text-xs text-amber-600 dark:text-amber-300">{_escape_text(detail)}</p>'
            if detail
            else ""
        )

        notes_list = entry.get("notes") or []
        notes_html = "".join(
            f'<p class="mt-2 text-xs italic text-slate-500 dark:text-slate-400">{_escape_text(note)}</p>'
            for note in notes_list
        )

        citation_parts = []
        legal_source = entry.get("legal_source")
        article = entry.get("article")
        if legal_source:
            citation_parts.append(str(legal_source))
        if article:
            citation_parts.append(str(article))
        citation_html = (
            '<div class="mt-1 text-xs text-slate-500 dark:text-slate-400">'
            + html.escape(" · ".join(citation_parts))
            + '</div>'
            if citation_parts
            else ""
        )

        metric = entry.get("metric")
        metric_html = (
            f'<div class="mt-1 text-xs text-slate-500 dark:text-slate-400">Metric: <code>{_escape_text(metric)}</code></div>'
            if metric
            else ""
        )

        scope = entry.get("scope")
        scope_badge = (
            f'<span class="ml-2 inline-flex items-center rounded-full bg-slate-500/15 px-2 py-0.5 text-[11px] font-semibold '
            f'uppercase tracking-wider text-slate-600 dark:bg-slate-700/60 dark:text-slate-300">{_escape_text(scope)}</span>'
            if scope
            else ""
        )

        evidence_rows.append(
            '<tr>'
            f'<td class="px-4 py-4 align-top">'
            f'<div class="font-semibold text-slate-900 dark:text-white">{_escape_text(entry.get("title"))}{scope_badge}</div>'
            f'{citation_html}{metric_html}{notes_html}'
            '</td>'
            f'<td class="px-4 py-4 align-top"><div class="font-semibold text-slate-900 dark:text-white">{requirement}</div></td>'
            f'<td class="px-4 py-4 align-top"><div class="font-semibold text-slate-900 dark:text-white">{observed}</div>'
            f'{context_html}{detail_html}</td>'
            f'<td class="px-4 py-4 align-top"><div class="flex flex-col gap-2">'
            f'<span class="inline-flex items-center justify-center rounded-full px-2.5 py-0.5 text-xs font-semibold {status_badge}">' \
            f'{"Pass" if passed else "Fail"}</span>'
            f'<span class="inline-flex items-center justify-center rounded-full px-2.5 py-0.5 text-[11px] font-semibold {mandatory_badge}">' \
            f'{mandatory_label}</span></div></td>'
            '</tr>'
        )

    if evidence_rows:
        evidence_table = (
            '<div class="mt-6 overflow-hidden rounded-2xl border border-slate-200 bg-white/80 shadow-sm '
            'dark:border-slate-700 dark:bg-slate-900/70">'
            '<table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">'
            '<thead class="bg-slate-50/70 text-xs font-semibold uppercase tracking-[0.3em] text-slate-500 '
            'dark:bg-slate-900/60 dark:text-slate-400"><tr>'
            '<th scope="col" class="px-4 py-3 text-left">Rule</th>'
            '<th scope="col" class="px-4 py-3 text-left">Requirement</th>'
            '<th scope="col" class="px-4 py-3 text-left">Observed</th>'
            '<th scope="col" class="px-4 py-3 text-left">Status</th>'
            '</tr></thead>'
            f'<tbody class="divide-y divide-slate-200 bg-white/80 text-sm text-slate-600 '
            f'dark:divide-slate-800 dark:bg-slate-900/60 dark:text-slate-300">{"".join(evidence_rows)}</tbody></table></div>'
        )
    else:
        evidence_table = (
            '<div class="mt-6 rounded-2xl border border-dashed border-slate-300 bg-slate-100/60 p-6 text-sm text-slate-500 '
            'dark:border-slate-700 dark:bg-slate-900/40 dark:text-slate-400">'
            'No regulation evidence available.</div>'
        )

    metrics_html = "".join(
        (
            '<div class="rounded-2xl border border-slate-200 bg-white/80 px-5 py-4 text-center shadow-sm '
            'dark:border-slate-700 dark:bg-slate-900/70">'
            f'<dt class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400 dark:text-slate-500">'
            f'{html.escape(metric.get("label", ""))}</dt>'
            f'<dd class="mt-2 text-lg font-semibold text-slate-900 dark:text-white">'
            f'{html.escape(metric.get("value", ""))}</dd></div>'
        )
        for metric in analysis.get("metrics", [])
    )

    bin_rows = []
    for bin_info in analysis.get("bins", []):
        valid = bool(bin_info.get("valid"))
        badge_class = (
            "bg-emerald-500/15 text-emerald-600 dark:bg-emerald-500/20 dark:text-emerald-200"
            if valid
            else "bg-rose-500/15 text-rose-600 dark:bg-rose-500/20 dark:text-rose-200"
        )
        kpis = bin_info.get("kpis") or []
        if kpis:
            kpi_html = "".join(
                f'<li><span class="font-medium text-slate-900 dark:text-white">{html.escape(kpi.get("name", ""))}:</span> '
                f'{html.escape(kpi.get("value", ""))}</li>'
                for kpi in kpis
            )
            kpi_block = f"<ul class='space-y-1'>{kpi_html}</ul>"
        else:
            kpi_block = (
                '<span class="text-xs uppercase tracking-[0.3em] text-slate-400 dark:text-slate-500">'
                'No KPIs configured</span>'
            )
        bin_rows.append(
            '<tr>'
            f'<td class="px-4 py-4"><span class="font-semibold text-slate-900 dark:text-white">{html.escape(bin_info.get("name", ""))}</span>'
            f'<span class="ml-2 inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold {badge_class}">' \
            f'{"Pass" if valid else "Fail"}</span></td>'
            f'<td class="px-4 py-4">{html.escape(bin_info.get("time", ""))}</td>'
            f'<td class="px-4 py-4">{html.escape(bin_info.get("distance", ""))}</td>'
            f'<td class="px-4 py-4">{kpi_block}</td>'
            '</tr>'
        )

    if bin_rows:
        bins_section = (
            '<div class="overflow-hidden rounded-2xl border border-slate-200 bg-white/80 shadow-sm '
            'dark:border-slate-700 dark:bg-slate-900/70">'
            '<table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">'
            '<thead class="bg-slate-50/70 text-xs font-semibold uppercase tracking-[0.3em] text-slate-500 '
            'dark:bg-slate-900/60 dark:text-slate-400"><tr>'
            '<th scope="col" class="px-4 py-3 text-left">Speed bin</th>'
            '<th scope="col" class="px-4 py-3 text-left">Time (s)</th>'
            '<th scope="col" class="px-4 py-3 text-left">Distance (km)</th>'
            '<th scope="col" class="px-4 py-3 text-left">KPIs</th>'
            '</tr></thead>'
            f'<tbody class="divide-y divide-slate-200 bg-white/80 text-sm text-slate-600 '
            f'dark:divide-slate-800 dark:bg-slate-900/60 dark:text-slate-300">{"".join(bin_rows)}</tbody></table></div>'
        )
    else:
        bins_section = (
            '<div class="rounded-2xl border border-dashed border-slate-300 bg-slate-100/60 p-6 text-sm text-slate-500 '
            'dark:border-slate-700 dark:bg-slate-900/40 dark:text-slate-400">'
            'Configure speed bins and KPIs in <code>data/rules/demo_rules.json</code> to populate this view.</div>'
        )

    analysis_status = analysis.get("status") or {}
    analysis_ok = bool(analysis_status.get("ok"))
    analysis_label = html.escape(str(analysis_status.get("label", "")))
    analysis_status_classes = (
        "bg-emerald-500/15 text-emerald-600 dark:bg-emerald-500/20 dark:text-emerald-200"
        if analysis_ok
        else "bg-rose-500/15 text-rose-600 dark:bg-rose-500/20 dark:text-rose-200"
    )

    summary_json = json.dumps(analysis.get("summary_md", ""))
    chart_json = json.dumps(analysis.get("chart", {}))
    map_json = json.dumps(analysis.get("map", {}))

    analysis_block = (
        '<div class="mt-10 rounded-3xl border border-slate-200/70 bg-white/95 p-8 shadow-card '
        'dark:border-slate-800/70 dark:bg-slate-900/80">'
        '<div class="flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">'
        '<div>'
        '<p class="text-xs font-semibold uppercase tracking-[0.4em] text-slate-500 dark:text-slate-400">Analysis validity</p>'
        f'<span class="mt-3 inline-flex items-center gap-2 rounded-full px-4 py-2 text-sm font-semibold {analysis_status_classes}">{analysis_label}</span>'
        '<p class="mt-3 max-w-xl text-sm text-slate-500 dark:text-slate-400">Derived metrics from fused telemetry streams. Explore KPIs, charts, and the drive route.</p>'
        '</div>'
        f'<dl class="grid gap-4 sm:grid-cols-3">{metrics_html}</dl>'
        '</div>'
        '<div class="mt-8">'
        '<div class="flex flex-wrap gap-2 border-b border-slate-200 dark:border-slate-700" role="tablist">'
        '<button type="button" class="tab-trigger is-active" data-tab-target="summary" '
        'aria-controls="analysis-tab-summary" aria-selected="true">Summary</button>'
        '<button type="button" class="tab-trigger" data-tab-target="kpis" '
        'aria-controls="analysis-tab-kpis" aria-selected="false">KPIs</button>'
        '<button type="button" class="tab-trigger" data-tab-target="charts" '
        'aria-controls="analysis-tab-charts" aria-selected="false">Charts</button>'
        '<button type="button" class="tab-trigger" data-tab-target="map" '
        'aria-controls="analysis-tab-map" aria-selected="false">Map</button>'
        '</div>'
        '<div class="mt-6 space-y-6">'
        '<div id="analysis-tab-summary" class="tab-panel" data-tab-panel="summary">'
        '<div id="analysis-summary" class="prose max-w-none prose-slate dark:prose-invert"></div></div>'
        f'<div id="analysis-tab-kpis" class="tab-panel" data-tab-panel="kpis" hidden>{bins_section}</div>'
        '<div id="analysis-tab-charts" class="tab-panel" data-tab-panel="charts" hidden>'
        '<div id="analysis-chart" class="hidden h-80 w-full rounded-2xl border border-slate-200 bg-white/70 shadow-inner '
        'dark:border-slate-700 dark:bg-slate-900/60" data-chart></div>'
        '<div data-chart-empty class="flex h-80 items-center justify-center rounded-2xl border border-dashed border-slate-300 '
        'bg-slate-100/60 text-sm text-slate-500 dark:border-slate-700 dark:bg-slate-900/40 dark:text-slate-400">'
        'Charts will appear once emissions or speed signals are available.</div></div>'
        '<div id="analysis-tab-map" class="tab-panel" data-tab-panel="map" hidden>'
        '<div id="analysis-map" class="hidden h-80 w-full overflow-hidden rounded-2xl border border-slate-200 '
        'dark:border-slate-700" data-map></div>'
        '<div data-map-empty class="flex h-80 items-center justify-center rounded-2xl border border-dashed border-slate-300 '
        'bg-slate-100/60 text-sm text-slate-500 dark:border-slate-700 dark:bg-slate-900/40 dark:text-slate-400">'
        'Upload GPS data containing latitude and longitude to visualize the drive path.</div></div></div></div>'
        f'<script id="summary-data" type="application/json">{summary_json}</script>'
        f'<script id="chart-data" type="application/json">{chart_json}</script>'
        f'<script id="map-data" type="application/json">{map_json}</script>'
        '</div>'
    )

    export_controls = ""
    if include_export_controls:
        payload_json = html.escape(json.dumps(results))
        export_controls = (
            '<div class="mb-6 rounded-3xl border border-slate-200/70 bg-white/95 px-5 py-4 shadow-sm '
            'dark:border-slate-700/60 dark:bg-slate-900/70">'
            '<div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">'
            '<div>'
            '<p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500 dark:text-slate-400">Final report</p>'
            '<p class="mt-1 text-sm text-slate-500 dark:text-slate-400">Download a portable version of the current results.</p>'
            '</div>'
            '<div class="flex flex-wrap items-center gap-2">'
            '<button type="button" '
            'class="inline-flex items-center gap-2 rounded-full bg-gradient-to-r from-slate-900 via-slate-800 to-slate-900 '
            'px-4 py-2 text-sm font-semibold text-white shadow-md transition hover:shadow-lg focus-visible:outline-none '
            'focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-slate-900 dark:from-sky-500 '
            'dark:via-indigo-500 dark:to-indigo-600" data-download-pdf>'
            'Download PDF'
            '<svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" '
            'stroke-linecap="round" stroke-linejoin="round">'
            '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>'
            '<polyline points="7 10 12 15 17 10"></polyline>'
            '<line x1="12" y1="15" x2="12" y2="3"></line>'
            '</svg>'
            '</button>'
            '</div>'
            '</div>'
            '<p data-export-error class="hidden text-sm font-medium text-rose-600 dark:text-rose-300"></p>'
            f'<script type="application/json" data-report-payload>{payload_json}</script>'
            '</div>'
        )

    return (
        '<section class="relative overflow-hidden rounded-3xl border border-slate-200/70 bg-white/95 p-8 shadow-card '
        'backdrop-blur dark:border-slate-800/70 dark:bg-slate-900/80" data-component="analysis-results">'
        f'{export_controls}'
        f'{regulation_block}'
        '<div class="mt-6">'
        '<h3 class="text-lg font-semibold text-slate-900 dark:text-white">Rule evidence</h3>'
        '<p class="mt-1 text-sm text-slate-500 dark:text-slate-400">Thresholds are demo placeholders and do not represent legal advice.</p>'
        f'{evidence_table}'
        '</div>'
        f'{analysis_block}'
        '</section>'
    )


def _render_index(results_html: str, *, max_upload_mb: int) -> str:
    global _index_template_cache
    if _index_template_cache is None:
        _index_template_cache = (template_dir / "index.html").read_text(encoding="utf-8")
    page = _index_template_cache.replace("{{RESULTS_SECTION}}", results_html)
    page = page.replace("{{MAX_UPLOAD_MB}}", str(max_upload_mb))
    return page


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    results_html = _render_results_html(None, [])
    page = _render_index(results_html, max_upload_mb=MAX_UPLOAD_MB)
    return HTMLResponse(page)


async def _extract_files(request: Request) -> dict[str, tuple[str | None, bytes]]:
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
    if boundary_token.startswith("\"") and boundary_token.endswith("\""):
        boundary_token = boundary_token[1:-1]

    body = await request.body()
    boundary = boundary_token.encode("utf-8")
    delimiter = b"--" + boundary

    files: dict[str, tuple[str | None, bytes]] = {}
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
        files[field] = (filename, content)

    return files


@router.post("/analyze", response_class=HTMLResponse)
async def analyze(request: Request) -> HTMLResponse:
    errors: list[str] = []
    results_payload: dict[str, Any] | None = None

    try:
        files = await _extract_files(request)
    except ValueError as exc:
        errors.append(str(exc))
        files = {}

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

            pems_df = _ingest_pems(pems_name, pems_bytes)
            gps_df = _ingest_gps(gps_name, gps_bytes)
            ecu_df = _ingest_ecu(ecu_name, ecu_bytes)
            fused = _fuse_streams(pems_df, gps_df, ecu_df)
            engine = AnalysisEngine(_load_analysis_rules())
            analysis_result = engine.analyze(fused)
            pack = _load_regulation_pack()
            evaluation = evaluate_pack(analysis_result.analysis, pack)
            results_payload = _prepare_results(analysis_result, evaluation)
        except Exception as exc:  # pragma: no cover - user feedback path
            errors.append(str(exc))

    results_html = _render_results_html(results_payload, errors)

    if request.headers.get("hx-request") == "true":
        status_code = status.HTTP_400_BAD_REQUEST if errors else status.HTTP_200_OK
        return HTMLResponse(results_html, status_code=status_code)

    page = _render_index(results_html, max_upload_mb=MAX_UPLOAD_MB)
    status_code = status.HTTP_400_BAD_REQUEST if errors else status.HTTP_200_OK
    return HTMLResponse(page, status_code=status_code)


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


__all__ = ["router"]
