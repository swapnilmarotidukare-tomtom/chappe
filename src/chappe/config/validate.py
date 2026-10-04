# src/chappe/config/validate.py
"""Checks that need the theme registry, run by `load_settings` (spec 9.1: validated at load)."""

from __future__ import annotations

from chappe.config.models import ChappeSettings, ThemeConfig
from chappe.core.errors import ChappeConfigError


def theme_problems(settings: ChappeSettings) -> list[tuple[str | None, str]]:
    """Every unknown theme or unresolvable token override, as (process name or None, problem).

    None marks a problem in `defaults`, which affects every process.
    """
    # lazy: chappe.themes imports the theme modules, which must not load with the config models
    from chappe.themes import THEMES
    from chappe.themes.tokens import resolve_tokens

    named: list[tuple[str | None, str, ThemeConfig]] = [
        (None, "defaults.theme", settings.defaults.theme)
    ]
    for name, process in settings.processes.items():
        named.append((name, f"processes.{name}.theme", settings.theme_for(process)))
    problems: list[tuple[str | None, str]] = []
    for owner, where, theme in named:
        if theme.name not in THEMES:
            available = ", ".join(sorted(THEMES))
            problems.append(
                (owner, f"{where}: unknown theme {theme.name!r}; available themes: {available}")
            )
            continue
        try:
            resolve_tokens(theme.name, theme.tokens)
        except ChappeConfigError as exc:
            problems.append((owner, f"{where}: {exc}"))
    return problems
