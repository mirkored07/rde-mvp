"""Entry point for generating regulation-specific report payloads."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

try:  # pragma: no cover - optional dependency guard
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - raised in tests if missing
    yaml = None  # type: ignore

from src.app.utils.payload import ensure_results_payload_defaults

from . import eu7_ld

_SPEC_DIR = Path(__file__).resolve().parent / "specs"
_DEFAULT_LEGISLATION = "eu7_ld"


def _deep_update(target: MutableMapping[str, Any], patch: Mapping[str, Any]) -> MutableMapping[str, Any]:
    """Merge *patch* into *target* recursively and return the mutated mapping."""

    for key, value in patch.items():
        if (
            key in target
            and isinstance(target[key], MutableMapping)
            and isinstance(value, Mapping)
        ):
            _deep_update(target[key], value)
        else:
            target[key] = deepcopy(value)
    return target


@lru_cache(maxsize=8)
def load_spec(name: str = _DEFAULT_LEGISLATION) -> Mapping[str, Any]:
    """Load and cache the YAML specification for a supported legislation."""

    spec_name = name.lower()
    path = _SPEC_DIR / f"{spec_name}.yaml"
    if not path.exists():  # pragma: no cover - defensive guard
        raise ValueError(f"Unknown legislation spec '{name}'.")

    if yaml is None:
        raise RuntimeError("PyYAML is required to load legislation specifications.")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"Specification '{name}' must evaluate to a mapping.")
    return raw


def render_report(
    legislation: str = _DEFAULT_LEGISLATION,
    data: Mapping[str, Any] | None = None,
    *,
    spec_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return an un-normalised results payload for *legislation*.

    Parameters
    ----------
    legislation:
        Currently only ``"eu7_ld"`` is supported.
    data:
        Harmonised analysis inputs. If omitted a deterministic demo payload is used.
    spec_override:
        Optional mapping merged on top of the static YAML specification. Handy for
        tests that want to stub TODO limits.
    """

    key = legislation.lower()
    if key != "eu7_ld":  # pragma: no cover - future extension guard
        raise ValueError(f"Unsupported legislation '{legislation}'.")

    spec_mapping = deepcopy(dict(load_spec("eu7_ld")))
    if spec_override:
        _deep_update(spec_mapping, spec_override)

    inputs = data if data is not None else eu7_ld.build_default_inputs(spec_mapping)
    return eu7_ld.build_report(inputs, spec_mapping)


def build_results_payload(
    legislation: str = _DEFAULT_LEGISLATION,
    data: Mapping[str, Any] | None = None,
    *,
    spec_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Render and normalise a results payload for the requested legislation."""

    raw_payload = render_report(
        legislation,
        data,
        spec_override=spec_override,
    )
    return ensure_results_payload_defaults(raw_payload)


__all__ = ["build_results_payload", "load_spec", "render_report"]
