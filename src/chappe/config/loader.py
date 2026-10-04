# src/chappe/config/loader.py
from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from chappe.config.models import ChappeSettings, ConfigFile
from chappe.config.validate import theme_problems
from chappe.core.errors import ChappeConfigError

ENV_CONFIG = "CHAPPE_CONFIG"
ENV_ENABLED = "CHAPPE_ENABLED"

ProcessError = Callable[[str, str], None]


def _read(file: Path) -> Any:
    try:
        return yaml.safe_load(file.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ChappeConfigError(f"{file}: cannot read ({exc.strerror})") from exc
    except UnicodeDecodeError as exc:
        raise ChappeConfigError(
            f"{file}: not UTF-8 (byte {exc.start}: {exc.reason}); save the file as UTF-8"
        ) from exc
    except yaml.YAMLError as exc:
        raise ChappeConfigError(f"{file}: invalid YAML ({exc})") from exc


def _process_of(loc: tuple[int | str, ...], data: Any) -> str | None:
    """The process an error belongs to (`chappe.processes.<name>...`), or None."""
    if len(loc) < 3 or loc[:2] != ("chappe", "processes"):
        return None
    name = loc[2]
    chappe = data.get("chappe") if isinstance(data, dict) else None
    processes = chappe.get("processes") if isinstance(chappe, dict) else None
    return (
        name
        if isinstance(name, str) and isinstance(processes, dict) and name in processes
        else None
    )


def _parse(file: Path, data: Any, on_process_error: ProcessError | None) -> ChappeSettings:
    try:
        return ConfigFile.model_validate(data).chappe
    except ValidationError as exc:
        errors = exc.errors()
    by_process: dict[str, list[str]] = {}
    other: list[str] = []
    for err in errors:
        text = f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}"
        name = _process_of(err["loc"], data)
        if name is None:
            other.append(text)
        else:
            by_process.setdefault(name, []).append(text)
    if other or on_process_error is None:
        problems = other + [text for texts in by_process.values() for text in texts]
        raise ChappeConfigError(f"{file}: {'; '.join(problems)}")
    for name, texts in by_process.items():
        on_process_error(name, "; ".join(texts))
    kept = {k: v for k, v in data["chappe"]["processes"].items() if k not in by_process}
    trimmed = {**data, "chappe": {**data["chappe"], "processes": kept}}
    return _parse(file, trimmed, None)


def load_settings(
    path: str | Path | None = None, *, on_process_error: ProcessError | None = None
) -> ChappeSettings:
    """Load and validate the config file. Raises ChappeConfigError.

    Strict by default (`chappe validate-config`): any error raises. With `on_process_error`
    (the runtime, spec 9.1), an error inside `processes.<name>` drops only that process and is
    reported as `on_process_error(name, problem)`; errors elsewhere still raise.
    """
    location = path or os.environ.get(ENV_CONFIG)
    if not location:
        raise ChappeConfigError(f"no config file: pass a path or set {ENV_CONFIG}")
    file = Path(location)
    settings = _parse(file, _read(file), on_process_error)
    problems = theme_problems(settings)
    if not problems:
        return settings
    if on_process_error is None or any(owner is None for owner, _ in problems):
        raise ChappeConfigError(f"{file}: {'; '.join(problem for _, problem in problems)}")
    dropped: dict[str, list[str]] = {}
    for owner, problem in problems:
        dropped.setdefault(str(owner), []).append(problem)
    for name, texts in dropped.items():
        on_process_error(name, "; ".join(texts))
    kept = {k: v for k, v in settings.processes.items() if k not in dropped}
    return settings.model_copy(update={"processes": kept})


def chappe_enabled(settings: ChappeSettings) -> bool:
    if os.environ.get(ENV_ENABLED, "").strip().lower() in {"0", "false", "no", "off"}:
        return False
    return settings.enabled
