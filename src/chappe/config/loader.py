# src/chappe/config/loader.py
from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from chappe.config.models import ChappeSettings, ConfigFile
from chappe.core.errors import ChappeConfigError

ENV_CONFIG = "CHAPPE_CONFIG"
ENV_ENABLED = "CHAPPE_ENABLED"


def load_settings(path: str | Path | None = None) -> ChappeSettings:
    location = path or os.environ.get(ENV_CONFIG)
    if not location:
        raise ChappeConfigError(f"no config file: pass a path or set {ENV_CONFIG}")
    file = Path(location)
    try:
        data = yaml.safe_load(file.read_text())
    except OSError as exc:
        raise ChappeConfigError(f"{file}: cannot read ({exc.strerror})") from exc
    except yaml.YAMLError as exc:
        raise ChappeConfigError(f"{file}: invalid YAML ({exc})") from exc
    try:
        return ConfigFile.model_validate(data).chappe
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}" for err in exc.errors()
        )
        raise ChappeConfigError(f"{file}: {problems}") from exc


def chappe_enabled(settings: ChappeSettings) -> bool:
    if os.environ.get(ENV_ENABLED, "").strip().lower() in {"0", "false", "no", "off"}:
        return False
    return settings.enabled
