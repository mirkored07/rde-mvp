"""Utilities for validating and serialising telemetry column mappings."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Mapping

from src.app.schemas import CANONICAL, DatasetSchema, get_schema


class MappingValidationError(ValueError):
    """Raised when a mapping payload is invalid."""

    def __init__(self, message: str, *, dataset: str | None = None) -> None:
        self.dataset = dataset
        super().__init__(message)


@dataclass(frozen=True)
class DatasetMapping:
    """Validated mapping definition for a single dataset."""

    dataset: str
    columns: dict[str, str]
    units: dict[str, str]

    def to_payload(self) -> dict[str, Mapping[str, str]]:
        payload: dict[str, Mapping[str, str]] = {}
        if self.columns:
            payload["columns"] = dict(self.columns)
        if self.units:
            payload["units"] = dict(self.units)
        return payload

    def column_mapping(self) -> Mapping[str, str] | None:
        return self.columns or None

    def unit_mapping(self) -> Mapping[str, str] | None:
        return self.units or None


DatasetMappingState = dict[str, DatasetMapping]


def _label_for(dataset: str) -> str:
    schema = CANONICAL.get(dataset)
    return schema.label if schema else dataset.upper()


def _clean_pairs(payload: Mapping[str, Any] | None) -> dict[str, str]:
    cleaned: dict[str, str] = {}
    if not payload:
        return cleaned
    for key, value in payload.items():
        if not isinstance(key, str):
            raise ValueError("Canonical column names must be strings.")
        canonical = key.strip()
        if not canonical:
            continue
        if value is None:
            continue
        if not isinstance(value, str):
            value = str(value)
        raw = value.strip()
        if raw:
            cleaned[canonical] = raw
    return cleaned


def _validate_columns(dataset: DatasetSchema, columns: Mapping[str, str]) -> None:
    allowed = set(dataset.all_fields)
    unknown = [name for name in columns if name not in allowed]
    if unknown:
        raise MappingValidationError(
            f"Unknown canonical columns for {_label_for(dataset.key)}: {', '.join(sorted(unknown))}.",
            dataset=dataset.key,
        )


def _validate_units(dataset: DatasetSchema, units: Mapping[str, str]) -> None:
    allowed = set(dataset.all_fields)
    unknown = [name for name in units if name not in allowed]
    if unknown:
        raise MappingValidationError(
            f"Units supplied for unknown {_label_for(dataset.key)} columns: {', '.join(sorted(unknown))}.",
            dataset=dataset.key,
        )


def validate_dataset_mapping(dataset: str, payload: Mapping[str, Any] | None) -> DatasetMapping:
    schema = get_schema(dataset)
    payload = payload or {}
    if not isinstance(payload, Mapping):
        raise MappingValidationError(
            f"{_label_for(dataset)} mapping must be a JSON object.", dataset=dataset
        )

    columns = _clean_pairs(payload.get("columns") if isinstance(payload, Mapping) else None)
    units = _clean_pairs(payload.get("units") if isinstance(payload, Mapping) else None)

    _validate_columns(schema, columns)
    _validate_units(schema, units)

    return DatasetMapping(dataset=dataset, columns=columns, units=units)


def load_mapping_from_dict(payload: Mapping[str, Any]) -> DatasetMappingState:
    state: DatasetMappingState = {}
    for dataset in CANONICAL:
        section = payload.get(dataset)
        if section is None:
            continue
        if not isinstance(section, Mapping):
            raise MappingValidationError(
                f"{_label_for(dataset)} mapping must be a JSON object.", dataset=dataset
            )
        state[dataset] = validate_dataset_mapping(dataset, section)
    return state


def parse_mapping_payload(raw: str | bytes | None) -> DatasetMappingState:
    if raw is None:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    raw = raw.strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MappingValidationError("Column mapping payload is not valid JSON.") from exc
    if not isinstance(payload, Mapping):
        raise MappingValidationError("Column mapping payload must be a JSON object.")

    if "datasets" in payload and isinstance(payload["datasets"], Mapping):
        payload = payload["datasets"]

    return load_mapping_from_dict(payload)


def serialise_mapping_state(state: DatasetMappingState) -> dict[str, Any]:
    return {key: mapping.to_payload() for key, mapping in state.items()}


_SLUG_RE = re.compile(r"[^a-z0-9-]+")


def slugify_profile_name(name: str) -> str:
    slug = name.strip().lower().replace(" ", "-")
    slug = _SLUG_RE.sub("", slug)
    return slug[:64]


__all__ = [
    "DatasetMapping",
    "DatasetMappingState",
    "MappingValidationError",
    "load_mapping_from_dict",
    "parse_mapping_payload",
    "serialise_mapping_state",
    "slugify_profile_name",
    "validate_dataset_mapping",
]
