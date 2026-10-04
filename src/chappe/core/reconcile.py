# src/chappe/core/reconcile.py
"""What has been sent, whether a render may be written, and what to send next."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from chappe.core.messages import Alert, MessageSet, ThreadEntry
from chappe.core.view import Watermark

_SLACK_TS = re.compile(r"(\d+)(?:\.(\d+))?", re.ASCII)


@dataclass(frozen=True, slots=True)
class SentState:
    process_key: str
    parent_ref: str | None = None  # Slack ts of the canonical parent
    parent_text: str = ""  # last text this writer put on the parent
    watermark: Watermark | None = None  # newest watermark seen by any writer (merged)
    parent_written: Watermark | None = None  # watermark of the last parent write (last writer wins)
    # the newest watermark written to each parent (merged per ts); picks the canonical parent
    parent_wms: Mapping[str, Watermark] = field(default_factory=dict)
    sent_keys: frozenset[tuple[str, str]] = frozenset()  # (key, ts of the parent it went under)
    stale_parents: frozenset[str] = frozenset()  # losing duplicate parents still to delete
    cleared_parents: frozenset[str] = frozenset()  # parents already deleted; never resurrected
    degraded: bool = False
    updated_at: datetime | None = None  # for cleanup

    @property
    def live_keys(self) -> frozenset[str]:
        """Keys that count as sent: sent under a parent that was not deleted.

        A reply under a deleted parent is gone with it, so the next event that plans it sends it
        again under the canonical parent.
        """
        return frozenset(key for key, ts in self.sent_keys if ts not in self.cleared_parents)


@dataclass(frozen=True, slots=True)
class SendPlan:
    update_parent: bool
    entries: tuple[ThreadEntry, ...]
    alerts: tuple[Alert, ...]


def write_allowed(watermark: Watermark, sent: SentState | None) -> bool:
    """Spec 7.1: never replace newer state. Finished sorts first, so finished is sticky."""
    return sent is None or watermark.newer_than(sent.watermark)


def plan_sends(messages: MessageSet, sent: SentState | None) -> SendPlan:
    known = sent.live_keys if sent is not None else frozenset()
    has_parent = sent is not None and sent.parent_ref is not None
    text_changed = sent is not None and messages.parent.text != sent.parent_text
    return SendPlan(
        update_parent=has_parent and text_changed,
        entries=tuple(e for e in messages.thread if e.key not in known),
        alerts=tuple(a for a in messages.alerts if a.key not in known),
    )


def slack_ts_key(ts: str) -> tuple[int, int]:
    """Order Slack timestamps numerically: ``"1791050263.984869"`` -> ``(1791050263, 984869)``.

    Slack sends six fractional digits. Shorter fractions are right-padded and longer ones are cut to
    microseconds, so ``"1.5"`` and ``"1.500000"`` compare equal.
    """
    match = _SLACK_TS.fullmatch(ts.strip())
    if match is None:
        raise ValueError(f"not a Slack ts: {ts!r}")
    seconds, fraction = match.group(1), match.group(2) or ""
    return int(seconds), int((fraction + "000000")[:6])


def _newer(stored: Watermark | None, new: Watermark | None) -> Watermark | None:
    if new is not None and new.newer_than(stored):
        return new
    return stored


def _parent_rank(
    ts: str, written: Mapping[str, Watermark]
) -> tuple[bool, tuple[bool, int, int, datetime] | tuple[()], tuple[int, int]]:
    """Larger wins: the newest view written to the parent, then the lowest ts."""
    wm = written.get(ts)
    seconds, micros = slack_ts_key(ts)
    return (wm is not None, wm.order_key() if wm is not None else (), (-seconds, -micros))


def merge_sent(stored: SentState | None, new: SentState) -> SentState:
    """Merge a state about to be saved into the stored one (contract D1/D2). Pure."""
    base = stored if stored is not None else new
    cleared = base.cleared_parents | new.cleared_parents
    written = dict(base.parent_wms)
    for ts, wm in new.parent_wms.items():
        written[ts] = _newer(written.get(ts), wm) or wm
    # Every parent still in Slack is a candidate (D2): the one carrying the newest written view
    # wins, ties go to the lowest ts. The rule only reads the merged state, so all writers
    # converge. A cleared (deleted) parent never is a candidate, so when the canonical parent is
    # deleted a live duplicate or a new post takes over (spec 9.2).
    refs = {ref for ref in (base.parent_ref, new.parent_ref) if ref is not None}
    candidates = (refs | base.stale_parents | new.stale_parents) - cleared
    parent_ref = max(candidates, key=lambda ts: _parent_rank(ts, written)) if candidates else None
    stale = frozenset(candidates - {parent_ref})
    last_write = new if new.parent_written is not None else base
    return SentState(
        process_key=new.process_key,
        parent_ref=parent_ref,
        parent_text=last_write.parent_text,
        watermark=_newer(base.watermark, new.watermark),
        parent_written=last_write.parent_written,
        parent_wms=written,
        sent_keys=base.sent_keys | new.sent_keys,
        stale_parents=stale,
        cleared_parents=cleared,
        degraded=base.degraded or new.degraded,
        updated_at=new.updated_at,
    )
