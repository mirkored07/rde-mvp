"""Loading of regulation packs expressed as JSON or YAML."""

from __future__ import annotations

import importlib
import importlib.util
import json
import pathlib
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

_ALLOWED_COMPARATORS = {"<", "<=", ">", ">=", "==", "!="}


def _load_yaml_module() -> Any | None:
    spec = importlib.util.find_spec("yaml")
    if spec is None:
        return None
    return importlib.import_module("yaml")


_yaml = _load_yaml_module()


@dataclass(slots=True, frozen=True)
class RegulationRule:
    """Single requirement within a regulation pack."""

    id: str
    title: str
    legal_source: str | None
    article: str | None
    scope: str
    metric: str
    comparator: str
    threshold: float | None
    units: str | None
    mandatory: bool
    notes: str | None


@dataclass(slots=True, frozen=True)
class RegulationPack:
    """Collection of regulation rules with associated metadata."""

    id: str
    title: str
    legal_source: str | None
    version: str | None
    rules: tuple[RegulationRule, ...]

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "RegulationPack":
        pack_id = str(payload.get("id") or "").strip()
        if not pack_id:
            raise ValueError("Regulation pack is missing an 'id'.")

        title = str(payload.get("title") or pack_id)
        legal_source = payload.get("legal_source")
        version = payload.get("version")

        raw_rules = payload.get("rules")
        if not isinstance(raw_rules, Sequence) or not raw_rules:
            raise ValueError("Regulation pack must define a non-empty 'rules' list.")

        rules: list[RegulationRule] = []
        for index, entry in enumerate(raw_rules):
            if not isinstance(entry, Mapping):
                raise ValueError(f"Rule at index {index} is not a mapping.")

            rule_id = str(entry.get("id") or "").strip()
            if not rule_id:
                raise ValueError(f"Rule at index {index} is missing an 'id'.")

            comparator = str(entry.get("comparator") or "").strip()
            if comparator not in _ALLOWED_COMPARATORS:
                raise ValueError(
                    f"Rule '{rule_id}' specifies unsupported comparator '{comparator}'."
                )

            threshold_value = entry.get("threshold")
            if threshold_value is None:
                threshold = None
            elif isinstance(threshold_value, (int, float)):
                threshold = float(threshold_value)
            else:
                try:
                    threshold = float(threshold_value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Rule '{rule_id}' threshold must be numeric."
                    ) from exc

            rule = RegulationRule(
                id=rule_id,
                title=str(entry.get("title") or rule_id),
                legal_source=str(entry.get("legal_source") or payload.get("legal_source") or "") or None,
                article=str(entry.get("article") or "") or None,
                scope=str(entry.get("scope") or "unspecified"),
                metric=str(entry.get("metric") or ""),
                comparator=comparator,
                threshold=threshold,
                units=str(entry.get("units") or "") or None,
                mandatory=bool(entry.get("mandatory", True)),
                notes=str(entry.get("notes") or "") or None,
            )
            rules.append(rule)

        return cls(
            id=pack_id,
            title=title,
            legal_source=str(legal_source or "") or None,
            version=str(version or "") or None,
            rules=tuple(rules),
        )


def _read_source(source: str | pathlib.Path) -> tuple[str, str]:
    path = pathlib.Path(source)
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    return text, suffix


def _parse_text(text: str, *, suffix: str) -> Mapping[str, Any]:
    if suffix in {".yaml", ".yml"}:
        if _yaml is None:
            raise ValueError("PyYAML is required to load YAML regulation packs.")
        data = _yaml.safe_load(text)
    else:
        data = json.loads(text)

    if not isinstance(data, Mapping):
        raise ValueError("Regulation pack payload must be a mapping.")
    return data


def load_pack(source: str | pathlib.Path | Mapping[str, Any]) -> RegulationPack:
    """Load a regulation pack from a path or mapping."""

    if isinstance(source, Mapping):
        payload = source
    else:
        text, suffix = _read_source(source)
        payload = _parse_text(text, suffix=suffix)

    return RegulationPack.from_mapping(payload)


__all__ = ["RegulationPack", "RegulationRule", "load_pack"]
