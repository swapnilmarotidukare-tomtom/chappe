"""Themes and their tokens."""

from __future__ import annotations

from chappe.ports.theme import Theme
from chappe.themes.builtin.plain import PlainTheme
from chappe.themes.builtin.thread import ThreadTheme

THEMES: dict[str, type[Theme]] = {"thread": ThreadTheme, "plain": PlainTheme}
