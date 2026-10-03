"""Themes and their tokens."""

from __future__ import annotations

from chappe.ports.theme import Theme
from chappe.themes.builtin.plain import PlainTheme

THEMES: dict[str, type[Theme]] = {"plain": PlainTheme}
