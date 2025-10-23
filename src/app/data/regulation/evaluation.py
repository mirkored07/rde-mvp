"""Evaluation engine that applies regulation rules to analysis payloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .pack import RegulationPack, RegulationRule


@dataclass(slots=True)
class RuleEvidence:
    """Result of applying a single regulation rule."""

    rule: RegulationRule
    passed: bool
    actual: float | None
    margin: float | None
    context: Mapping[str, float | None]
    detail: str | None
    bin_name: str | None


@dataclass(slots=True)
class PackEvaluation:
    """Aggregated evaluation for a full regulation pack."""

    pack: RegulationPack
    overall_passed: bool
    mandatory_passed: int
    mandatory_total: int
    optional_passed: int
    optional_total: int
    evidence: list[RuleEvidence]


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compare(actual: float | None, threshold: float | None, comparator: str) -> bool:
    if actual is None or threshold is None:
        return False
    if comparator == ">=":
        return actual >= threshold
    if comparator == ">":
        return actual > threshold
    if comparator == "<=":
        return actual <= threshold
    if comparator == "<":
        return actual < threshold
    if comparator == "==":
        return actual == threshold
    if comparator == "!=":
        return actual != threshold
    raise ValueError(f"Unsupported comparator '{comparator}'.")


def _compute_margin(actual: float | None, threshold: float | None, comparator: str) -> float | None:
    if actual is None or threshold is None:
        return None
    if comparator in {">=", ">"}:
        return actual - threshold
    if comparator in {"<=", "<"}:
        return threshold - actual
    if comparator in {"==", "!="}:
        return actual - threshold
    return None


def _resolve_metric(
    analysis: Mapping[str, Any],
    rule: RegulationRule,
) -> tuple[float | None, dict[str, float | None], str | None, str | None]:
    bins = analysis.get("bins") if isinstance(analysis, Mapping) else None
    if not isinstance(bins, Mapping):
        bins = {}

    metric = (rule.metric or "").strip()
    if not metric:
        return None, {}, "Rule metric is not defined.", None

    parts = metric.split(".")
    head = parts[0]

    if head == "kpis" and len(parts) >= 3:
        kpi_name = parts[1]
        bin_name = parts[2]
        bin_payload = bins.get(bin_name)
        if not isinstance(bin_payload, Mapping):
            return None, {}, f"Speed bin '{bin_name}' not found.", bin_name
        kpis = bin_payload.get("kpis")
        if not isinstance(kpis, Mapping):
            return None, {
                "distance_km": _as_float(bin_payload.get("distance_km")),
                "time_s": _as_float(bin_payload.get("time_s")),
            }, f"KPI '{kpi_name}' not available.", bin_name
        value = _as_float(kpis.get(kpi_name))
        context = {
            "distance_km": _as_float(bin_payload.get("distance_km")),
            "time_s": _as_float(bin_payload.get("time_s")),
        }
        detail = None if value is not None else f"KPI '{kpi_name}' not available."
        return value, context, detail, bin_name

    if head in bins:
        bin_name = head
        bin_payload = bins.get(bin_name)
        if not isinstance(bin_payload, Mapping):
            return None, {}, f"Speed bin '{bin_name}' not found.", bin_name
        current: Any = bin_payload
        for key in parts[1:]:
            if isinstance(current, Mapping):
                current = current.get(key)
            else:
                current = None
                break
        value = _as_float(current)
        context = {
            "distance_km": _as_float(bin_payload.get("distance_km")),
            "time_s": _as_float(bin_payload.get("time_s")),
        }
        detail = None if value is not None else f"Metric '{rule.metric}' not available."
        return value, context, detail, bin_name

    current: Any = analysis
    for key in parts:
        if isinstance(current, Mapping):
            current = current.get(key)
        else:
            current = None
            break
    value = _as_float(current)
    detail = None if value is not None else f"Metric '{rule.metric}' not available."
    return value, {}, detail, None


def evaluate_pack(analysis: Mapping[str, Any], pack: RegulationPack) -> PackEvaluation:
    """Evaluate the provided analysis payload against a regulation pack."""

    evidence: list[RuleEvidence] = []
    mandatory_total = 0
    mandatory_passed = 0
    optional_total = 0
    optional_passed = 0

    for rule in pack.rules:
        actual, context, detail, bin_name = _resolve_metric(analysis, rule)
        passed = _compare(actual, rule.threshold, rule.comparator)
        margin = _compute_margin(actual, rule.threshold, rule.comparator)

        if rule.mandatory:
            mandatory_total += 1
            if passed:
                mandatory_passed += 1
        else:
            optional_total += 1
            if passed:
                optional_passed += 1

        evidence.append(
            RuleEvidence(
                rule=rule,
                passed=passed,
                actual=actual,
                margin=margin,
                context=context,
                detail=detail,
                bin_name=bin_name,
            )
        )

    overall_passed = mandatory_total == mandatory_passed if mandatory_total else True

    return PackEvaluation(
        pack=pack,
        overall_passed=overall_passed,
        mandatory_passed=mandatory_passed,
        mandatory_total=mandatory_total,
        optional_passed=optional_passed,
        optional_total=optional_total,
        evidence=evidence,
    )


__all__ = ["evaluate_pack", "PackEvaluation", "RuleEvidence"]
