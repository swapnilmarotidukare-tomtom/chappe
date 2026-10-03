# src/chappe/ports/source.py
from __future__ import annotations

from typing import Protocol

from chappe.core.events import ChappeEvent
from chappe.core.view import ProcessView


class Source(Protocol):
    def snapshot(self, event: ChappeEvent) -> ProcessView | None:
        """The current view of the event's process, or None when it has no milestones."""
        ...
