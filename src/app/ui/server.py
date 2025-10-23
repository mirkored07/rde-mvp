"""FastAPI router serving the Tailwind/HTMX powered user interface."""

from __future__ import annotations

import functools
import html
import json
import math
import pathlib
import tempfile
from typing import Any

import pandas as pd
from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse

from src.app.data.analysis import AnalysisEngine, AnalysisResult, load_rules
from src.app.data.fusion import FusionEngine
from src.app.data.fusion.specs import StreamSpec
from src.app.data.ingestion import ECUReader, GPSReader

router = APIRouter(include_in_schema=False)

template_dir = pathlib.Path(__file__).resolve().parent / "templates"
_index_template_cache: str | None = None

MAX_UPLOAD_MB = 50
GPS_EXTENSIONS = {".csv", ".nmea", ".gpx", ".txt"}
ECU_EXTENSIONS = {".csv", ".mf4", ".mdf"}
PEMS_EXTENSIONS = {".csv"}

_project_root = pathlib.Path(__file__).resolve().parents[3]
_rules_path = _project_root / "data" / "rules" / "demo_rules.json"
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


@functools.lru_cache(maxsize=1)
def _load_analysis_rules() -> Any:
    if _rules_path.exists():
        return load_rules(_rules_path)
    return load_rules(DEFAULT_RULES_CONFIG)


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


def _prepare_results(result: AnalysisResult) -> dict[str, Any]:
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

    return {
        "summary_md": result.summary_md,
        "status": {"label": status_label, "ok": status_ok},
        "metrics": metrics,
        "bins": bin_rows,
        "chart": chart_payload,
        "map": map_payload,
    }


def _render_results_html(results: dict[str, Any] | None, errors: list[str]) -> str:
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

    status = results.get("status", {})
    status_ok = bool(status.get("ok"))
    status_label = html.escape(str(status.get("label", "")))
    status_classes = (
        "bg-emerald-500/15 text-emerald-600 dark:bg-emerald-500/20 dark:text-emerald-200"
        if status_ok
        else "bg-rose-500/15 text-rose-600 dark:bg-rose-500/20 dark:text-rose-200"
    )
    ping_color = "bg-emerald-400/60" if status_ok else "bg-rose-400/60"
    dot_color = "bg-emerald-500" if status_ok else "bg-rose-500"

    metrics_html = "".join(
        (
            '<div class="rounded-2xl border border-slate-200 bg-white/80 px-5 py-4 text-center shadow-sm '
            'dark:border-slate-700 dark:bg-slate-900/70">'
            f'<dt class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400 dark:text-slate-500">'
            f'{html.escape(metric.get("label", ""))}</dt>'
            f'<dd class="mt-2 text-lg font-semibold text-slate-900 dark:text-white">'
            f'{html.escape(metric.get("value", ""))}</dd></div>'
        )
        for metric in results.get("metrics", [])
    )

    bin_rows = []
    for bin_info in results.get("bins", []):
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

    summary_json = json.dumps(results.get("summary_md", ""))
    chart_json = json.dumps(results.get("chart", {}))
    map_json = json.dumps(results.get("map", {}))

    return (
        '<section class="relative overflow-hidden rounded-3xl border border-slate-200/70 bg-white/95 p-8 shadow-card '
        'backdrop-blur dark:border-slate-800/70 dark:bg-slate-900/80" data-component="analysis-results">'
        '<div class="flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">'
        '<div>'
        '<p class="text-xs font-semibold uppercase tracking-[0.4em] text-slate-500 dark:text-slate-400">Overall status</p>'
        f'<span class="mt-3 inline-flex items-center gap-2 rounded-full px-4 py-2 text-sm font-semibold {status_classes}">' \
        f'<span class="relative flex h-2.5 w-2.5"><span class="absolute inline-flex h-full w-full animate-ping rounded-full '
        f'{ping_color}"></span><span class="relative inline-flex h-2.5 w-2.5 rounded-full {dot_color}"></span></span>'
        f'{status_label}</span>'
        '<p class="mt-3 max-w-xl text-sm text-slate-500 dark:text-slate-400">Results generated from the fused telemetry '
        'streams. Toggle tabs to explore KPIs, charts, and the drive route.</p></div>'
        f'<dl class="grid gap-4 sm:grid-cols-3">{metrics_html}</dl></div>'
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
            results_payload = _prepare_results(analysis_result)
        except Exception as exc:  # pragma: no cover - user feedback path
            errors.append(str(exc))

    results_html = _render_results_html(results_payload, errors)

    if request.headers.get("hx-request") == "true":
        status_code = status.HTTP_400_BAD_REQUEST if errors else status.HTTP_200_OK
        return HTMLResponse(results_html, status_code=status_code)

    page = _render_index(results_html, max_upload_mb=MAX_UPLOAD_MB)
    status_code = status.HTTP_400_BAD_REQUEST if errors else status.HTTP_200_OK
    return HTMLResponse(page, status_code=status_code)


__all__ = ["router"]
