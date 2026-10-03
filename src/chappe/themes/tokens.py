from __future__ import annotations

from collections.abc import Mapping
from importlib.resources import files
from typing import Any

import yaml

from chappe.core.errors import ChappeConfigError
from chappe.core.render import Tokens

STATE_KEYS = frozenset({"pending", "running", "succeeded", "failed", "skipped"})
_SECTIONS = ("icons", "labels", "extra")


def _builtin(theme_name: str) -> dict[str, Any]:
    resource = files("chappe.themes.builtin").joinpath(f"{theme_name}.yaml")
    if not resource.is_file():
        raise ChappeConfigError(f"theme {theme_name!r} has no built-in tokens; set tokens.extends")
    data = yaml.safe_load(resource.read_text())
    return {section: dict(data.get(section, {})) for section in _SECTIONS}


def resolve_tokens(theme_name: str, override: Mapping[str, Any] | None) -> Tokens:
    override = dict(override or {})
    base_name = str(override.pop("extends", theme_name))
    unknown = set(override) - set(_SECTIONS)
    if unknown:
        raise ChappeConfigError(
            f"unknown token section(s) {sorted(unknown)}; expected {list(_SECTIONS)}"
        )
    data = _builtin(base_name)
    for section in ("icons", "labels"):
        for key in override.get(section, {}):
            if key not in STATE_KEYS:
                raise ChappeConfigError(
                    f"unknown token {section}.{key}; expected one of {sorted(STATE_KEYS)}"
                )
    for section in _SECTIONS:
        data[section].update({str(k): str(v) for k, v in override.get(section, {}).items()})
    for section in ("icons", "labels"):
        missing = STATE_KEYS - set(data[section])
        if missing:
            raise ChappeConfigError(f"tokens.{section} is missing {sorted(missing)}")
    return Tokens(icons=data["icons"], labels=data["labels"], extra=data["extra"])
