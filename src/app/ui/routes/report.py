"""Server-rendered EU7-LD conformity report view."""

from __future__ import annotations

from typing import Any, Iterable

import json

from fastapi import APIRouter, HTTPException, Request
from starlette.templating import Jinja2Templates

from src.app.reporting.eu7ld_report import group_criteria_by_section, load_report
from src.app.reporting.schemas import Criterion, PassFail, ReportData
from src.app.ui.routes._eu7_payload import build_normalised_payload


router = APIRouter()
templates = Jinja2Templates(directory="src/app/ui/templates")


def _criterion_lookup(criteria: Iterable[Criterion]) -> dict[str, Criterion]:
    return {item.id: item for item in criteria}


def _overall_result(criteria: Iterable[Criterion]) -> str:
    lookup = _criterion_lookup(criteria)
    targets = [lookup.get("conformity:nox"), lookup.get("conformity:pn")]
    available = [item for item in targets if item is not None]
    if not available:
        return "pending"
    if any(item.result == PassFail.FAIL for item in available):
        return "fail"
    if all(item.result == PassFail.PASS for item in available):
        return "pass"
    return "pending"


def _value_or_na(value: Any, unit: str | None = None) -> str:
    if value is None or value == "":
        return "n/a"
    if isinstance(value, float):
        if abs(value) >= 1e6:
            text = f"{value:.2e}".replace("e+", "e")
        else:
            text = f"{value:.2f}".rstrip("0").rstrip(".")
        return f"{text} {unit}".strip() if unit else text
    if isinstance(value, int):
        return f"{value} {unit}".strip() if unit else str(value)
    return str(value)


def _card_from_criterion(criterion: Criterion | None, label: str) -> dict[str, Any]:
    if criterion is None:
        return {
            "label": label,
            "value": "n/a",
            "status": PassFail.NA.value,
            "tooltip": "",
        }
    raw_value = criterion.value if criterion.value not in {None, ""} else criterion.measured
    return {
        "label": label,
        "value": _value_or_na(raw_value, criterion.unit),
        "status": criterion.result.value,
        "tooltip": criterion.limit,
    }


def _build_quick_cards(report: ReportData) -> list[dict[str, Any]]:
    lookup = _criterion_lookup(report.criteria)
    cards: list[dict[str, Any]] = []
    cards.append(_card_from_criterion(lookup.get("conformity:pn"), "PN final"))
    cards[-1]["value"] = _value_or_na(report.emissions.trip.PN_hash_km, "#/km")
    cards[-1]["tooltip"] = "≤ 6.0e11 #/km"

    urban_distance = _card_from_criterion(lookup.get("trip:urban-distance"), "Min urban coverage")
    expressway_distance = _card_from_criterion(lookup.get("trip:expressway-distance"), "Min expressway coverage")
    cards.extend([urban_distance, expressway_distance])

    cold_start_speed = _card_from_criterion(lookup.get("cold-start:avg-speed"), "Urban avg speed")
    cards.append(cold_start_speed)

    cards.append(_card_from_criterion(lookup.get("gps:max-gap"), "Max GPS gap"))
    cards.append(_card_from_criterion(lookup.get("gps:total-gaps"), "Total GPS gaps"))

    nox_trip = _card_from_criterion(lookup.get("conformity:nox"), "NOx per km")
    nox_trip["value"] = _value_or_na(report.emissions.trip.NOx_mg_km, "mg/km")
    cards.append(nox_trip)

    pn_trip = _card_from_criterion(lookup.get("conformity:pn"), "PN per km")
    pn_trip["value"] = _value_or_na(report.emissions.trip.PN_hash_km, "#/km")
    cards.append(pn_trip)

    co_value = _value_or_na(report.emissions.trip.CO_mg_km, "mg/km")
    cards.append(
        {
            "label": "CO per km",
            "value": co_value,
            "status": PassFail.NA.value,
            "tooltip": "≤ 1000 mg/km" if report.emissions.trip.CO_mg_km is not None else "",
        }
    )

    return cards


@router.get("/report/{test_id}", include_in_schema=False)
def report_view(request: Request, test_id: str):
    try:
        report = load_report(test_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Report not found") from exc
    grouped = group_criteria_by_section(report.criteria)
    quick_cards = _build_quick_cards(report)
    overall = _overall_result(report.criteria)
    payload = build_normalised_payload(report.model_dump(mode="json"))
    payload_json = json.dumps(payload, ensure_ascii=False)
    final_block = payload.get("final", {}) if isinstance(payload.get("final"), dict) else {}
    overall_pass = final_block.get("pass") if isinstance(final_block.get("pass"), bool) else None
    return templates.TemplateResponse(
        "report.html",
        {
            "request": request,
            "report": report,
            "grouped_criteria": grouped,
            "quick_cards": quick_cards,
            "overall_result": overall,
            "results_payload": payload,
            "results_payload_json": payload_json,
            "overall_pass": overall_pass,
        },
    )


__all__ = ["router"]

