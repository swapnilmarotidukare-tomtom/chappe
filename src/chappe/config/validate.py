# src/chappe/config/validate.py
"""Checks that need the theme registry, run by `load_settings` (spec 9.1: validated at load)."""

from __future__ import annotations

from chappe.config.models import ChappeSettings
from chappe.core.errors import ChappeConfigError


def validate_settings(settings: ChappeSettings) -> None:
    """Every configured theme exists and its token overrides resolve; else ChappeConfigError."""
    # lazy: chappe.themes imports the theme modules, which must not load with the config models
    from chappe.themes import THEMES
    from chappe.themes.tokens import resolve_tokens

    named = [("defaults.theme", settings.defaults.theme)]
    for name, process in settings.processes.items():
        named.append((f"processes.{name}.theme", settings.theme_for(process)))
    for where, theme in named:
        if theme.name not in THEMES:
            available = ", ".join(sorted(THEMES))
            raise ChappeConfigError(
                f"{where}: unknown theme {theme.name!r}; available themes: {available}"
            )
        try:
            resolve_tokens(theme.name, theme.tokens)
        except ChappeConfigError as exc:
            raise ChappeConfigError(f"{where}: {exc}") from exc
