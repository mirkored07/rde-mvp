from __future__ import annotations

"""HTML rendering helpers for exportable analysis reports."""

import datetime as dt
import html
from typing import Any, Iterable

__all__ = ["build_report_html"]


_EXPORT_CSS = """
:root {
  color-scheme: light;
}
body {
  margin: 0;
  background: #f8fafc;
  font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
  color: #1f2937;
}
main.report {
  max-width: 960px;
  margin: 0 auto;
  padding: 48px 40px 64px;
  background: #ffffff;
  border-radius: 32px;
  box-shadow: 0 24px 60px -36px rgba(15, 23, 42, 0.4);
}
.report__header {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  padding-bottom: 24px;
  border-bottom: 1px solid #e2e8f0;
}
.report__eyebrow {
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.32em;
  text-transform: uppercase;
  color: #64748b;
  margin-bottom: 8px;
}
.report__timestamp {
  font-size: 13px;
  color: #64748b;
  margin-top: 6px;
}
.report__status {
  text-align: right;
}
.report__pack-title {
  font-size: 16px;
  font-weight: 600;
  color: #0f172a;
  margin-top: 14px;
}
.report__pack-meta {
  font-size: 12px;
  color: #475569;
  margin-top: 6px;
}
.badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  padding: 6px 14px;
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}
.badge--pass {
  background: rgba(16, 185, 129, 0.14);
  color: #047857;
}
.badge--fail {
  background: rgba(239, 68, 68, 0.14);
  color: #b91c1c;
}
.section {
  margin-top: 36px;
}
.section h2 {
  font-size: 18px;
  margin-bottom: 12px;
  color: #0f172a;
}
.section p {
  font-size: 14px;
  line-height: 1.6;
}
.stat-grid {
  margin-top: 18px;
  display: grid;
  gap: 14px;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
}
.stat {
  background: rgba(226, 232, 240, 0.5);
  border-radius: 18px;
  padding: 14px;
}
.stat__label {
  font-size: 11px;
  letter-spacing: 0.28em;
  text-transform: uppercase;
  color: #64748b;
  margin-bottom: 6px;
}
.stat__value {
  font-size: 16px;
  font-weight: 600;
  color: #0f172a;
}
.summary {
  margin-top: 16px;
}
.summary p {
  margin: 0 0 10px;
}
.summary ul {
  margin: 0 0 12px 18px;
  padding: 0;
}
.summary li {
  font-size: 13px;
  color: #1f2937;
}
.table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 16px;
}
.table th {
  text-transform: uppercase;
  letter-spacing: 0.28em;
  font-size: 11px;
  font-weight: 600;
  color: #64748b;
  text-align: left;
  padding: 10px 12px;
  background: #f8fafc;
}
.table td {
  padding: 12px;
  border-top: 1px solid #e2e8f0;
  font-size: 13px;
  vertical-align: top;
}
.table tr:nth-child(even) td {
  background: rgba(248, 250, 252, 0.6);
}
.tag {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 600;
  padding: 4px 10px;
  margin-left: 8px;
}
.tag--pass {
  background: rgba(16, 185, 129, 0.14);
  color: #047857;
}
.tag--fail {
  background: rgba(239, 68, 68, 0.14);
  color: #b91c1c;
}
.tag--mandatory {
  background: rgba(37, 99, 235, 0.16);
  color: #1d4ed8;
}
.tag--optional {
  background: rgba(124, 58, 237, 0.18);
  color: #6d28d9;
}
.list-inline {
  list-style: none;
  margin: 8px 0 0;
  padding: 0;
}
.list-inline li {
  font-size: 12px;
  color: #475569;
}
.list-inline li + li {
  margin-top: 4px;
}
.notes {
  font-size: 12px;
  color: #475569;
  margin-top: 8px;
}
.notes ul {
  margin: 4px 0 0 16px;
  padding: 0;
}
.notes li {
  margin: 4px 0;
}
.empty {
  font-size: 13px;
  color: #94a3b8;
  font-style: italic;
}
.status-column {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
@page {
  size: A4;
  margin: 22mm 18mm;
}
"""


def _escape(value: Any) -> str:
    """HTML-escape a value, returning an empty string for ``None``."""

    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return html.escape(str(value))


def _format_summary(text: str) -> str:
    if not text or not text.strip():
        return "<p class=\"empty\">No narrative summary provided.</p>"

    lines = text.splitlines()
    parts: list[str] = []
    paragraph: list[str] = []
    list_items: list[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            parts.append(f"<p>{' '.join(paragraph)}</p>")
            paragraph = []

    def flush_list() -> None:
        nonlocal list_items
        if list_items:
            items = "".join(f"<li>{item}</li>" for item in list_items)
            parts.append(f"<ul>{items}</ul>")
            list_items = []

    for raw in lines:
        line = raw.strip()
        if not line:
            flush_list()
            flush_paragraph()
            continue
        if line.startswith("- "):
            flush_paragraph()
            list_items.append(_escape(line[2:].strip()))
        else:
            flush_list()
            paragraph.append(_escape(line))

    flush_list()
    flush_paragraph()

    return "".join(parts) or "<p class=\"empty\">No narrative summary provided.</p>"


def _render_counts(regulation: dict[str, Any]) -> str:
    counts = regulation.get("counts") or {}
    stat_items: list[str] = []

    mandatory_passed = counts.get("mandatory_passed")
    mandatory_total = counts.get("mandatory_total")
    if mandatory_passed is not None or mandatory_total is not None:
        stat_items.append(
            "<div class=\"stat\">"
            "<div class=\"stat__label\">Mandatory rules</div>"
            f"<div class=\"stat__value\">{_escape(mandatory_passed)} / {_escape(mandatory_total)}</div>"
            "</div>"
        )

    optional_passed = counts.get("optional_passed")
    optional_total = counts.get("optional_total")
    if optional_passed is not None or optional_total is not None:
        stat_items.append(
            "<div class=\"stat\">"
            "<div class=\"stat__label\">Optional rules</div>"
            f"<div class=\"stat__value\">{_escape(optional_passed)} / {_escape(optional_total)}</div>"
            "</div>"
        )

    total_rules = None
    if mandatory_total is not None and optional_total is not None:
        total_rules = _escape((mandatory_total or 0) + (optional_total or 0))
    if total_rules is not None:
        stat_items.append(
            "<div class=\"stat\">"
            "<div class=\"stat__label\">Total rules evaluated</div>"
            f"<div class=\"stat__value\">{total_rules}</div>"
            "</div>"
        )

    if not stat_items:
        return "<p class=\"empty\">No regulation counters available.</p>"
    return f"<div class=\"stat-grid\">{''.join(stat_items)}</div>"


def _render_metrics(metrics: Iterable[dict[str, Any]]) -> str:
    items = list(metrics or [])
    if not items:
        return "<p class=\"empty\">No key metrics were computed.</p>"
    rows = "".join(
        f"<tr><th scope=\"row\">{_escape(metric.get('label'))}</th><td>{_escape(metric.get('value'))}</td></tr>"
        for metric in items
    )
    return f"<table class=\"table\"><tbody>{rows}</tbody></table>"


def _render_bins(bins: Iterable[dict[str, Any]]) -> str:
    items = list(bins or [])
    if not items:
        return "<p class=\"empty\">No speed bins were configured for this analysis.</p>"

    rows: list[str] = []
    for entry in items:
        kpis = entry.get("kpis") or []
        if kpis:
            kpi_html = "<ul class=\"list-inline\">" + "".join(
                f"<li><strong>{_escape(kpi.get('name'))}:</strong> {_escape(kpi.get('value'))}</li>" for kpi in kpis
            ) + "</ul>"
        else:
            kpi_html = "<p class=\"empty\">No KPIs available.</p>"
        status = "PASS" if entry.get("valid") else "FAIL"
        status_class = "tag--pass" if entry.get("valid") else "tag--fail"
        rows.append(
            "<tr>"
            f"<td>{_escape(entry.get('name'))}<span class=\"tag {status_class}\">{status}</span></td>"
            f"<td>{_escape(entry.get('time'))}</td>"
            f"<td>{_escape(entry.get('distance'))}</td>"
            f"<td>{kpi_html}</td>"
            "</tr>"
        )

    header = (
        "<thead><tr><th scope=\"col\">Speed bin</th><th scope=\"col\">Time (s)</th>"
        "<th scope=\"col\">Distance (km)</th><th scope=\"col\">KPIs</th></tr></thead>"
    )
    return f"<table class=\"table\">{header}<tbody>{''.join(rows)}</tbody></table>"


def _render_evidence(entries: Iterable[dict[str, Any]]) -> str:
    items = list(entries or [])
    if not items:
        return "<p class=\"empty\">No regulation evidence was produced.</p>"

    rows: list[str] = []
    for entry in items:
        title = _escape(entry.get("title") or "Regulation requirement")
        meta_parts: list[str] = []
        legal_source = entry.get("legal_source")
        if legal_source:
            meta_parts.append(_escape(legal_source))
        article = entry.get("article")
        if article:
            meta_parts.append(f"Article {_escape(article)}")
        scope = entry.get("scope")
        if scope:
            meta_parts.append(_escape(scope))
        metric_name = entry.get("metric")
        if metric_name:
            meta_parts.append(f"Metric {_escape(metric_name)}")
        meta_html = (
            f"<div class=\"notes\">{' · '.join(meta_parts)}</div>"
            if meta_parts
            else ""
        )

        notes = entry.get("notes") or []
        notes_html = (
            "<div class=\"notes\"><strong>Notes:</strong><ul>"
            + "".join(f"<li>{_escape(note)}</li>" for note in notes)
            + "</ul></div>"
            if notes
            else ""
        )

        context_items = entry.get("context") or []
        context_html = (
            "<ul class=\"list-inline\">"
            + "".join(
                f"<li><strong>{_escape(item.get('label'))}:</strong> {_escape(item.get('value'))}</li>"
                for item in context_items
                if item
            )
            + "</ul>"
            if context_items
            else ""
        )

        detail = entry.get("detail")
        detail_html = (
            f"<div class=\"notes\"><strong>Detail:</strong> {_escape(detail)}</div>"
            if detail
            else ""
        )

        passed = bool(entry.get("passed"))
        status_tag = f"<span class=\"tag {'tag--pass' if passed else 'tag--fail'}\">{'PASS' if passed else 'FAIL'}</span>"
        mandatory = bool(entry.get("mandatory"))
        mandatory_tag = (
            "<span class=\"tag tag--mandatory\">Mandatory</span>"
            if mandatory
            else "<span class=\"tag tag--optional\">Optional</span>"
        )

        rows.append(
            "<tr>"
            f"<td><div><strong>{title}</strong></div>{meta_html}{notes_html}</td>"
            f"<td>{_escape(entry.get('requirement'))}</td>"
            f"<td>{_escape(entry.get('observed'))}{context_html}{detail_html}</td>"
            f"<td class=\"status-column\">{status_tag}{mandatory_tag}</td>"
            "</tr>"
        )

    header = (
        "<thead><tr><th scope=\"col\">Rule</th><th scope=\"col\">Requirement</th>"
        "<th scope=\"col\">Observed</th><th scope=\"col\">Status</th></tr></thead>"
    )
    return f"<table class=\"table\">{header}<tbody>{''.join(rows)}</tbody></table>"


def build_report_html(results: dict[str, Any]) -> str:
    """Render a standalone HTML document for the supplied results payload."""

    regulation = results.get("regulation") or {}
    analysis = results.get("analysis") or {}

    generated_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    status_ok = bool(regulation.get("ok"))
    status_label = _escape(regulation.get("label") or ("PASS" if status_ok else "FAIL"))
    status_class = "badge--pass" if status_ok else "badge--fail"

    pack_title = _escape(regulation.get("pack_title") or "Regulation pack")
    pack_meta_parts: list[str] = []
    pack_id = regulation.get("pack_id")
    if pack_id:
        pack_meta_parts.append(f"ID {_escape(pack_id)}")
    legal_source = regulation.get("legal_source")
    if legal_source:
        pack_meta_parts.append(_escape(legal_source))
    version = regulation.get("version")
    if version:
        pack_meta_parts.append(f"Version {_escape(version)}")
    pack_meta = " · ".join(pack_meta_parts)
    pack_meta_html = f"<div class=\"report__pack-meta\">{pack_meta}</div>" if pack_meta else ""

    analysis_status = analysis.get("status") or {}
    analysis_status_label = analysis_status.get("label")
    analysis_status_html = (
        f"<p><strong>Analysis validity:</strong> <span class=\"tag {'tag--pass' if analysis_status.get('ok') else 'tag--fail'}\">{_escape(analysis_status_label)}</span></p>"
        if analysis_status_label
        else ""
    )

    summary_html = _format_summary(analysis.get("summary_md", ""))
    metrics_html = _render_metrics(analysis.get("metrics") or [])
    bins_html = _render_bins(analysis.get("bins") or [])
    evidence_html = _render_evidence(results.get("evidence") or [])
    counts_html = _render_counts(regulation)

    document = (
        "<!DOCTYPE html>"
        "<html lang=\"en\">"
        "<head>"
        "<meta charset=\"utf-8\" />"
        "<title>RDE Analysis Report</title>"
        f"<style>{_EXPORT_CSS}</style>"
        "</head>"
        "<body>"
        "<main class=\"report\">"
        "<header class=\"report__header\">"
        "<div>"
        "<div class=\"report__eyebrow\">RDE MVP</div>"
        "<h1>Regulatory analysis report</h1>"
        f"<div class=\"report__timestamp\">Generated on {generated_at}</div>"
        "</div>"
        "<div class=\"report__status\">"
        f"<span class=\"badge {status_class}\">{status_label}</span>"
        f"<div class=\"report__pack-title\">{pack_title}</div>"
        f"{pack_meta_html}"
        "</div>"
        "</header>"
        "<section class=\"section\">"
        "<h2>Regulation summary</h2>"
        f"{counts_html}"
        "</section>"
        "<section class=\"section\">"
        "<h2>Analysis overview</h2>"
        f"{analysis_status_html}"
        "<div class=\"summary\">"
        f"{summary_html}"
        "</div>"
        "<h3>Key metrics</h3>"
        f"{metrics_html}"
        "</section>"
        "<section class=\"section\">"
        "<h2>Speed bin performance</h2>"
        f"{bins_html}"
        "</section>"
        "<section class=\"section\">"
        "<h2>Regulation evidence</h2>"
        f"{evidence_html}"
        "</section>"
        "</main>"
        "</body>"
        "</html>"
    )
    return document
