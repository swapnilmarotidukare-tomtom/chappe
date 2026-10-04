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
from chappe.core.reconcile import SendPlan, SentState, plan_sends, slack_ts_key, write_allowed
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
    OUT_OF_TIME = "out_of_time"  # the budget ran out before a call into Airflow
    SENT = "sent"
    ERROR = "error"


class _OutOfTime(Exception):
    """The event's deadline passed before a call into Airflow (spec 9.3)."""

    def __init__(self, key: str | None, before: str) -> None:
        super().__init__(before)
        self.key = key
        self.before = before


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
        except _OutOfTime as exc:
            self._out_of_time(exc, event, final)
            result = HandleResult.OUT_OF_TIME
        except Exception:
            log.exception(
                "chappe: event failed (process %s, run %s)",
                getattr(event, "process", "?"),
                getattr(event, "run_id", "?"),
            )
            result = HandleResult.ERROR
        self._metric(f"chappe.events.{result.value}")
        return result

    def _out_of_time(self, exc: _OutOfTime, event: ChappeEvent, final: bool) -> None:
        key = exc.key if exc.key is not None else f"{event.dag_id}/{event.run_id}"
        if final:  # no event follows the final one
            log.error(
                "chappe: out of time on the final event for %s before %s; "
                "the final message may be missing or incomplete",
                key,
                exc.before,
            )
        else:
            log.warning(
                "chappe: time budget spent for %s before %s; the rest is left to the next event",
                key,
                exc.before,
            )
        self._metric("chappe.budget_spent")

    def _in_time(self, deadline: float, key: str | None, before: str) -> None:
        """Calls into Airflow have no Chappe-side timeout: never start one after the deadline."""
        if self._clock() >= deadline:
            raise _OutOfTime(key, before)

    def _load(self, key: str, deadline: float) -> SentState | None:
        self._in_time(deadline, key, "reading the store")
        return self._store.load(key)

    def _save(self, key: str, state: SentState, deadline: float) -> SentState:
        """Save what no Slack call depends on; skipped once the deadline has passed."""
        self._in_time(deadline, key, "writing the store")
        return self._store.save(key, state)

    def _record(self, key: str, state: SentState) -> SentState:
        """Save a Slack call that was made. Never skipped for time: an untracked post would be
        posted again by the next event (silence over a duplicate)."""
        return self._store.save(key, state)

    def _metric(self, name: str) -> None:
        """Count `name`. A failing metrics hook is logged and never changes what Chappe does."""
        try:
            self._metrics(name)
        except Exception:
            log.exception("chappe: metrics hook failed (%s)", name)

    def _handle(self, event: ChappeEvent, final: bool, deadline: float) -> HandleResult:
        self._in_time(deadline, None, "reading the task states")
        view = self._source.snapshot(event)
        if view is None:
            return HandleResult.NO_PROCESS
        sent = self._load(view.key, deadline)
        if sent is not None and sent.degraded:
            return HandleResult.DEGRADED
        try:
            if sent is not None:
                sent = self._clear_stale(view.key, sent, deadline)
            if sent is not None and not write_allowed(view.watermark, sent):
                return self._send_missing(view, sent, deadline, final=final)
            messages = self._render(view)
            result = self._apply(view, messages, sent, deadline, final=final)
            if final and result is HandleResult.SENT:
                self._final_check(view, messages, deadline)
            return result
        except TransportError as exc:
            if exc.retryable:
                raise
            self._mark_degraded(view, exc, deadline)
            return HandleResult.DEGRADED

    def _render(self, view: ProcessView) -> MessageSet:
        try:
            return self._theme.render(view, self._ctx)
        except Exception:
            log.exception(
                "chappe: theme %r failed for %s; using the plain theme", self._theme.name, view.key
            )
            self._metric("chappe.theme_fallback")
            return self._fallback.render(view, self._ctx)

    def _apply(
        self,
        view: ProcessView,
        messages: MessageSet,
        sent: SentState | None,
        deadline: float,
        *,
        final: bool = False,
    ) -> HandleResult:
        key, wm = view.key, view.watermark
        text = messages.parent.text
        if sent is not None and sent.parent_ref is not None:
            if not plan_sends(messages, sent).update_parent:
                state, saved = sent, False
            else:
                written = self._write_parent(key, sent.parent_ref, text, wm, deadline)
                if written is None:  # the parent was re-posted and a newer render owns it
                    return HandleResult.YIELDED
                state, saved = written, True
        else:
            posted = self._post_parent(key, text, wm, deadline)
            if posted is None:
                return HandleResult.YIELDED
            state, saved = posted, True

        if final:
            state = self._settle(key, state, deadline)
        plan = plan_sends(messages, state)
        replied, complete = self._send_replies(view, plan, state, deadline, final=final)
        if complete and not (replied or saved):
            # nothing changed in Slack; remember the watermark
            self._save(key, SentState(key, watermark=wm), deadline)
        return HandleResult.SENT

    def _settle(self, key: str, state: SentState, deadline: float) -> SentState:
        """The final event waits its check delay after writing the parent, then re-reads the store
        before posting its thread entries (spec 7, step 9).

        The last task's own callback is often posting its timed reply at this moment; waiting lets
        it land first, so the final event neither repeats it untimed nor broadcasts the result
        above it.
        """
        left = deadline - self._clock()
        if left <= 0:
            return state  # the replies log the error
        self._sleep(min(self._settings.final_check_delay_s, left))
        if self._clock() >= deadline:
            return state
        loaded = self._load(key, deadline)
        return loaded if loaded is not None else state

    def _send_missing(
        self, view: ProcessView, sent: SentState, deadline: float, *, final: bool
    ) -> HandleResult:
        """A view that is not newer never touches the parent, but while the run is unfinished it
        sends replies not yet sent.

        Parallel tasks race: a task's own callback (the only view with its times) can be older
        than a parallel event that already showed it finished. Its reply must still go out.
        Without a parent, a newer event posts it and sends the replies. Once the stored run is
        finished, nothing older is sent (finished is sticky, spec 7.1): a cleared and rerun step
        or a late duplicate final event must not add replies under the final status.
        """
        stored = sent.watermark
        if sent.parent_ref is None or (stored is not None and stored.finished):
            return HandleResult.SKIPPED
        plan = plan_sends(self._render(view), sent)
        if not (plan.entries or plan.alerts):
            return HandleResult.SKIPPED
        replied, _ = self._send_replies(view, plan, sent, deadline, final=final)
        return HandleResult.SENT if replied else HandleResult.SKIPPED

    def _send_replies(
        self,
        view: ProcessView,
        plan: SendPlan,
        state: SentState,
        deadline: float,
        *,
        final: bool,
    ) -> tuple[bool, bool]:
        """Post the entries and alerts not sent yet, saving after each.

        Returns (any sent, all sent); all sent is False when the time budget ran out.
        """
        key, wm = view.key, view.watermark
        saved = False
        replies = [(e.key, e.text, e.broadcast) for e in plan.entries]
        replies += [(a.key, a.text, False) for a in plan.alerts]
        for index, (reply_key, reply, broadcast) in enumerate(replies):
            if self._clock() >= deadline:
                # never block (spec 9.3): what was sent is saved, the next event sends the rest
                if final:  # no event follows the final one
                    log.error(
                        "chappe: out of time on the final event for %s; "
                        "%d message(s) were not sent",
                        key,
                        len(replies) - index,
                    )
                else:
                    log.warning(
                        "chappe: time budget spent for %s; %d message(s) left to the next event",
                        key,
                        len(replies) - index,
                    )
                self._metric("chappe.budget_spent")
                return saved, False
            current = self._load(key, deadline)  # a parallel event may have sent it meanwhile
            if current is not None:
                state = current
            if reply_key in state.live_keys:
                continue
            parent = state.parent_ref
            if parent is None:
                raise StoreError(f"no parent message stored for {key}")
            self._transport.post_reply(
                self._settings.channel, parent, reply, broadcast=broadcast, deadline=deadline
            )
            state = self._record(
                key, SentState(key, watermark=wm, sent_keys=frozenset({(reply_key, parent)}))
            )
            saved = True
        return saved, True

    def _post_parent(self, key: str, text: str, wm: Watermark, deadline: float) -> SentState | None:
        """Post a parent. If parallel events posted too, the parent carrying the newest written view
        wins, ties to the lowest Slack ts (contract D2).

        Returns None when a newer render owns the parent (YIELDED).
        """
        mine = self._transport.post_parent(
            self._settings.channel, text, parent_payload(key, wm), deadline=deadline
        )
        claim = SentState(
            key,
            parent_ref=mine,
            parent_text=text,
            watermark=wm,
            parent_written=wm,
            parent_wms={mine: wm},
        )
        state = self._record(key, claim)
        # verify: without compare-and-set, a parallel stale write can drop `mine`
        for _ in range(2):
            loaded = self._load(key, deadline)
            if loaded is not None and mine in {
                loaded.parent_ref,
                *loaded.stale_parents,
                *loaded.cleared_parents,
            }:
                state = loaded
                break
            # the merge re-adds `mine`; the winner rule (D2) still decides
            state = self._record(key, claim)
        state = self._clear_stale(key, state, deadline)  # deletes `mine` when another parent won
        winner = state.parent_ref
        if winner is None or winner == mine:
            return state
        log.info("chappe: a parallel event posted the parent of %s first; using %s", key, winner)
        self._metric("chappe.duplicate_parent")
        if state.watermark is not None and state.watermark.newer_than(wm):
            return None
        return self._write_parent(key, winner, text, wm, deadline)

    def _write_parent(
        self, key: str, ts: str, text: str, wm: Watermark, deadline: float
    ) -> SentState | None:
        """Edit the parent `ts`. Returns None when a newer render owns the parent: stored before
        any attempt (an attempt never overwrites a newer render, spec 7.1), or on a re-posted
        parent."""
        payload = parent_payload(key, wm)

        def still_current() -> bool:
            stored = self._load(key, deadline)
            return stored is None or not (
                stored.watermark is not None and stored.watermark.newer_than(wm)
            )

        try:
            written = self._transport.update_parent(
                self._settings.channel,
                ts,
                text,
                payload,
                deadline=deadline,
                still_current=still_current,
            )
        except TransportError as exc:
            if exc.code != MESSAGE_NOT_FOUND:
                raise
            current = self._load(key, deadline)
            if current is None:
                raise
            if current.parent_ref == ts:
                # deleted by hand (spec 9.2): clear it so it never wins again, then use a live
                # duplicate if one is left, else post a new parent
                log.warning("chappe: the parent %s of %s was deleted; replacing it", ts, key)
                self._metric("chappe.parent_replaced")
                current = self._save(key, SentState(key, cleared_parents=frozenset({ts})), deadline)
            if current.parent_ref is None:
                return self._post_parent(key, text, wm, deadline)
            # a lower-ts parent won meanwhile and ours was deleted, or a duplicate took over
            return self._write_parent(key, current.parent_ref, text, wm, deadline)
        if not written:
            log.info("chappe: a newer render of %s was stored; not editing the parent", key)
            return None
        return self._record(
            key,
            SentState(
                key,
                parent_ref=ts,
                parent_text=text,
                watermark=wm,
                parent_written=wm,
                parent_wms={ts: wm},
            ),
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
                    "chappe: could not delete duplicate parent %s of %s (%s); "
                    "the next event retries",
                    ts,
                    key,
                    exc.code,
                )
                continue
            cleared.add(ts)
        if not cleared:
            return state
        self._metric("chappe.duplicate_parent_deleted")
        # a delete that is not recorded is repeated by the next event and counts as done
        return self._save(key, SentState(key, cleared_parents=frozenset(cleared)), deadline)

    def _final_check(self, view: ProcessView, messages: MessageSet, deadline: float) -> None:
        """Rule 3 (contract D3): read the store back once, after the check delay (spent before the
        thread entries, see `_settle`).

        Rewrite the parent if a late writer edited it after us.
        """
        if self._clock() >= deadline:
            log.warning("chappe: no time left to read back the final message for %s", view.key)
            return
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

    def _mark_degraded(self, view: ProcessView, exc: TransportError, deadline: float) -> None:
        log.error("chappe: Slack refused (%s); stopping for process %s", exc.code, view.key)
        self._metric("chappe.degraded")
        if self._clock() >= deadline:
            log.warning("chappe: no time left to record the degraded state for %s", view.key)
            return
        try:
            self._store.save(view.key, SentState(view.key, degraded=True))
        except Exception:
            log.exception("chappe: could not record the degraded state for %s", view.key)
