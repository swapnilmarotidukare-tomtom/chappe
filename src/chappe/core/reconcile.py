# src/chappe/core/reconcile.py
"""What has been sent, whether a render may be written, and what to send next."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from chappe.core.messages import Alert, MessageSet, ThreadEntry
from chappe.core.view import Watermark


@dataclass(frozen=True, slots=True)
class SentState:
    process_key: str
    parent_ref: str | None = None  # Slack ts of the canonical parent
    parent_text: str = ""  # last text this writer put on the parent
    watermark: Watermark | None = None  # newest watermark seen by any writer (merged)
    parent_written: Watermark | None = None  # watermark of the last parent write (last writer wins)
    sent_keys: frozenset[str] = frozenset()
    stale_parents: frozenset[str] = frozenset()  # losing duplicate parents still to delete
    cleared_parents: frozenset[str] = frozenset()  # parents already deleted; never resurrected
    degraded: bool = False
    updated_at: datetime | None = None  # for cleanup


@dataclass(frozen=True, slots=True)
class SendPlan:
    post_parent: bool
    update_parent: bool
    entries: tuple[ThreadEntry, ...]
    alerts: tuple[Alert, ...]

    @property
    def empty(self) -> bool:
        return not (self.post_parent or self.update_parent or self.entries or self.alerts)


def write_allowed(watermark: Watermark, sent: SentState | None) -> bool:
    """Spec 7.1: never replace newer state. Finished sorts first, so finished is sticky."""
    return sent is None or watermark.newer_than(sent.watermark)


def plan_sends(messages: MessageSet, sent: SentState | None) -> SendPlan:
    known = sent.sent_keys if sent is not None else frozenset()
    has_parent = sent is not None and sent.parent_ref is not None
    text_changed = sent is not None and messages.parent.text != sent.parent_text
    return SendPlan(
        post_parent=not has_parent,
        update_parent=has_parent and text_changed,
        entries=tuple(e for e in messages.thread if e.key not in known),
        alerts=tuple(a for a in messages.alerts if a.key not in known),
    )
