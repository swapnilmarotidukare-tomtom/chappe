# src/chappe/core/engine.py
"""Event → snapshot → load → write rules → render → plan → send and save (spec 7, contract D)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from chappe.core.errors import StoreError, TransportError
from chappe.core.events import ChappeEvent, EventKind
from chappe.core.messages import MessageSet
from chappe.core.reconcile import SentState, plan_sends, slack_ts_key, write_allowed
from chappe.core.render import RenderContext
from chappe.core.view import ProcessView, Watermark
from chappe.ports.source import Source
from chappe.ports.store import Store
from chappe.ports.theme import Theme
from chappe.ports.transport import Transport

log = logging.getLogger("chappe")

MESSAGE_NOT_FOUND = "message_not_found"


class HandleResult(str, Enum):
    DISABLED = "disabled"
    NO_PROCESS = "no_process"
    SKIPPED = "skipped"
    DEGRADED = "degraded"
    YIELDED = "yielded"  # another event's newer render owns the parent
    SENT = "sent"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class EngineSettings:
    channel: str
    event_budget_s: float = 10.0
    final_budget_s: float = 30.0
    final_check_delay_s: float = 2.0


def parent_payload(process_key: str, watermark: Watermark) -> dict[str, Any]:
    """Chappe's Slack metadata payload, sent on every parent post and update."""
    occurred = None if watermark.occurred_at is None else watermark.occurred_at.isoformat()
    return {
        "v": 1,
        "process_key": process_key,
        "wm": {
            "finished": watermark.finished,
            "settled": watermark.settled_steps,
            "started": watermark.started_steps,
            "occurred_at": occurred,
        },
    }


class Engine:
    def __init__(
        self,
        *,
        source: Source,
        theme: Theme,
        fallback_theme: Theme,
        context: RenderContext,
        transport: Transport,
        store: Store,
        settings: EngineSettings,
        enabled: Callable[[], bool] = lambda: True,
        metrics: Callable[[str], None] = lambda name: None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._source = source
        self._theme = theme
        self._fallback = fallback_theme
        self._ctx = context
        self._transport = transport
        self._store = store
        self._settings = settings
        self._enabled = enabled
        self._metrics = metrics
        self._clock = clock
        self._sleep = sleep

    def handle(self, event: ChappeEvent) -> HandleResult:
        """Never raises: Chappe must not break the pipeline."""
        try:
            if not self._enabled():
                result = HandleResult.DISABLED
            else:
                final = event.kind is EventKind.RUN_FINISHED
                budget = self._settings.final_budget_s if final else self._settings.event_budget_s
                result = self._handle(event, final, self._clock() + budget)
        except Exception:
            log.exception(
                "chappe: event failed (process %s, run %s)",
                getattr(event, "process", "?"),
                getattr(event, "run_id", "?"),
            )
            result = HandleResult.ERROR
        self._metric(f"chappe.events.{result.value}")
        return result

    def _metric(self, name: str) -> None:
        """Count `name`. A failing metrics hook is logged and never changes what Chappe does."""
        try:
            self._metrics(name)
        except Exception:
            log.exception("chappe: metrics hook failed (%s)", name)

    def _handle(self, event: ChappeEvent, final: bool, deadline: float) -> HandleResult:
        view = self._source.snapshot(event)
        if view is None:
            return HandleResult.NO_PROCESS
        sent = self._store.load(view.key)
        if sent is not None and sent.degraded:
            return HandleResult.DEGRADED
        try:
            if sent is not None:
                sent = self._clear_stale(view.key, sent, deadline)
            if not write_allowed(view.watermark, sent):
                return HandleResult.SKIPPED
            messages = self._render(view)
            result = self._apply(view, messages, sent, deadline)
            if final and result is HandleResult.SENT:
                self._final_check(view, messages, deadline)
            return result
        except TransportError as exc:
            if exc.retryable:
                raise
            self._mark_degraded(view, exc)
            return HandleResult.DEGRADED

    def _render(self, view: ProcessView) -> MessageSet:
        try:
            return self._theme.render(view, self._ctx)
        except Exception:
            log.exception("chappe: theme %r failed; using the plain theme", self._theme.name)
            self._metric("chappe.theme_fallback")
            return self._fallback.render(view, self._ctx)

    def _apply(
        self, view: ProcessView, messages: MessageSet, sent: SentState | None, deadline: float
    ) -> HandleResult:
        key, wm = view.key, view.watermark
        if sent is None or sent.parent_ref is None:
            posted = self._post_parent(view, messages, deadline)
            if posted is None:
                return HandleResult.YIELDED
            state, saved = posted, True
        elif plan_sends(messages, sent).update_parent:
            state = self._write_parent(key, sent.parent_ref, messages.parent.text, wm, deadline)
            saved = True
        else:
            state, saved = sent, False

        plan = plan_sends(messages, state)
        replies = [(e.key, e.text, e.broadcast) for e in plan.entries]
        replies += [(a.key, a.text, False) for a in plan.alerts]
        for reply_key, text, broadcast in replies:
            if reply_key in state.sent_keys:
                continue  # a parallel event sent it meanwhile
            parent = state.parent_ref
            if parent is None:
                raise StoreError(f"no parent message stored for {key}")
            self._transport.post_reply(
                self._settings.channel, parent, text, broadcast=broadcast, deadline=deadline
            )
            state = self._store.save(
                key, SentState(key, watermark=wm, sent_keys=frozenset({reply_key}))
            )
            saved = True
        if not saved:
            # nothing changed in Slack; remember the watermark
            self._store.save(key, SentState(key, watermark=wm))
        return HandleResult.SENT

    def _post_parent(
        self, view: ProcessView, messages: MessageSet, deadline: float
    ) -> SentState | None:
        """Post a parent. If parallel events posted too, the lowest Slack ts wins (contract D2)."""
        key, wm, text = view.key, view.watermark, messages.parent.text
        mine = self._transport.post_parent(
            self._settings.channel, text, parent_payload(key, wm), deadline=deadline
        )
        claim = SentState(key, parent_ref=mine, parent_text=text, watermark=wm, parent_written=wm)
        state = self._store.save(key, claim)
        # verify: without compare-and-set, a parallel stale write can drop `mine`
        for _ in range(2):
            loaded = self._store.load(key)
            if loaded is not None and mine in {
                loaded.parent_ref,
                *loaded.stale_parents,
                *loaded.cleared_parents,
            }:
                state = loaded
                break
            # the merge re-adds `mine`; the lowest ts still wins
            state = self._store.save(key, claim)
        state = self._clear_stale(key, state, deadline)  # deletes `mine` when another parent won
        winner = state.parent_ref
        if winner is None or winner == mine:
            return state
        log.info("chappe: a parallel event posted the parent first; using %s", winner)
        self._metric("chappe.duplicate_parent")
        if state.watermark is not None and state.watermark.newer_than(wm):
            return None
        return self._write_parent(key, winner, text, wm, deadline)

    def _write_parent(
        self, key: str, ts: str, text: str, wm: Watermark, deadline: float
    ) -> SentState:
        payload = parent_payload(key, wm)
        try:
            self._transport.update_parent(
                self._settings.channel, ts, text, payload, deadline=deadline
            )
        except TransportError as exc:
            current = self._store.load(key) if exc.code == MESSAGE_NOT_FOUND else None
            if current is None or current.parent_ref is None or current.parent_ref == ts:
                raise
            ts = current.parent_ref  # a lower-ts parent won meanwhile and ours was deleted
            self._transport.update_parent(
                self._settings.channel, ts, text, payload, deadline=deadline
            )
        return self._store.save(
            key, SentState(key, parent_ref=ts, parent_text=text, watermark=wm, parent_written=wm)
        )

    def _clear_stale(self, key: str, state: SentState, deadline: float) -> SentState:
        """Delete losing duplicate parents and record them as cleared (contract D2)."""
        if not state.stale_parents:
            return state
        cleared: set[str] = set()
        for ts in sorted(state.stale_parents, key=slack_ts_key):
            try:
                self._transport.delete(self._settings.channel, ts, deadline=deadline)
            except TransportError as exc:
                log.warning(
                    "chappe: could not delete duplicate parent %s (%s); the next event retries",
                    ts,
                    exc.code,
                )
                continue
            cleared.add(ts)
        if not cleared:
            return state
        self._metric("chappe.duplicate_parent_deleted")
        return self._store.save(key, SentState(key, cleared_parents=frozenset(cleared)))

    def _final_check(self, view: ProcessView, messages: MessageSet, deadline: float) -> None:
        """Rule 3 (contract D3): read the store back once.

        Rewrite the parent if a late writer edited it after us.
        """
        left = deadline - self._clock()
        if left <= 0:
            log.warning("chappe: no time left to read back the final message for %s", view.key)
            return
        self._sleep(min(self._settings.final_check_delay_s, left))
        loaded = self._store.load(view.key)
        if loaded is None or loaded.parent_ref is None:
            return
        text, written = messages.parent.text, loaded.parent_written
        if loaded.parent_text == text or (
            written is not None and not view.watermark.newer_than(written)
        ):
            return
        log.warning("chappe: repairing a late overwrite of the final message for %s", view.key)
        self._metric("chappe.final_repaired")
        self._write_parent(view.key, loaded.parent_ref, text, view.watermark, deadline)

    def _mark_degraded(self, view: ProcessView, exc: TransportError) -> None:
        log.error("chappe: Slack refused (%s); stopping for process %s", exc.code, view.key)
        self._metric("chappe.degraded")
        try:
            self._store.save(view.key, SentState(view.key, degraded=True))
        except Exception:
            log.exception("chappe: could not record the degraded state for %s", view.key)
