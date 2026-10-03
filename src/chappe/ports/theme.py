# src/chappe/ports/theme.py
from __future__ import annotations

from typing import ClassVar, Protocol

from chappe.core.messages import MessageSet
from chappe.core.render import RenderContext
from chappe.core.view import ProcessView


class Theme(Protocol):
    name: ClassVar[str]

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        """Pure and deterministic: the same view and context give the same message set."""
        ...
