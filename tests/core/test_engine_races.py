# tests/core/test_engine_races.py
"""Interleavings found in the final review, replayed with the in-memory fakes (no network)."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from typing import Any

import pytest

from chappe.core.engine import HandleResult
from chappe.core.errors import TransportError
from chappe.core.events import EventKind
from chappe.core.model import ProcessState, StepState
from chappe.core.reconcile import SentState
from chappe.core.view import ProcessView
from chappe.themes.builtin.thread import ThreadTheme
from chappe.transports.slack.transport import SlackTransport
from tests.support.engine import (
    CHANNEL,
    KEY,
    StepEntriesTheme,
    Writer,
    render,
    stage,
    store_for,
    writer,
)
from tests.support.fakes import FakeSlackApi, FakeVariables
from tests.support.samples import ProcessViewBuilder, default_context

S, P, R, F = StepState.SUCCEEDED, StepState.PENDING, StepState.RUNNING, StepState.FAILED
THREAD_CTX = default_context("thread")
WAIT_S = 5.0  # a generous bound: the threads below only ever wait for each other


def thread_writer(api: FakeSlackApi, variables: FakeVariables, **kwargs: Any) -> Writer:
    return writer(api, variables, theme=ThreadTheme(), context=THREAD_CTX, **kwargs)


def two_steps(
    load: StepState, *, load_timed: bool, finished: ProcessState | None = None
) -> ProcessView:
    builder = ProcessViewBuilder(key=KEY).section("Main")
    builder.step("Extract", S, timed=False)
    builder.step("Load", load, timed=load_timed)
    if finished is not None:
        builder.finished(finished)
    return builder.build()


class GatedReplies(SlackTransport):
    """Calls `gate` before its first reply reaches Slack."""

    def __init__(self, api: FakeSlackApi, gate: Callable[[], None]) -> None:
        super().__init__(api, clock=lambda: 0.0)
        self._gate: Callable[[], None] | None = gate

    def post_reply(
        self, channel: str, parent_ts: str, text: str, *, broadcast: bool, deadline: float
    ) -> str:
        if self._gate is not None:
            gate, self._gate = self._gate, None
            gate()
        return super().post_reply(channel, parent_ts, text, broadcast=broadcast, deadline=deadline)


def test_the_last_tasks_own_reply_lands_before_the_final_result_and_only_once() -> None:
    """Review I-2: the last task's callback (A) is posting its timed reply when the DAG's final
    callback (B) runs. B writes the parent, waits its check delay (A's reply lands meanwhile),
    re-reads the store and posts only what is still missing: no untimed duplicate, and the final
    broadcast comes after every step reply."""
    api, variables = FakeSlackApi(), FakeVariables()
    setup = ProcessViewBuilder(key=KEY).section("Main").step("Extract", S).step("Load", R)
    thread_writer(api, variables).handle(setup.build())

    a_posting, b_waiting, a_done = threading.Event(), threading.Event(), threading.Event()
    results: dict[str, HandleResult] = {}

    def a_waits_for_b() -> None:
        a_posting.set()
        assert b_waiting.wait(WAIT_S)

    def b_sleeps(seconds: float) -> None:
        b_waiting.set()
        assert a_done.wait(WAIT_S)

    a = thread_writer(api, variables, transport=GatedReplies(api, a_waits_for_b))
    b = thread_writer(api, variables, sleep=b_sleeps)

    def run_a() -> None:
        try:
            results["a"] = a.handle(two_steps(S, load_timed=True))
        finally:
            a_done.set()

    def run_b() -> None:
        final = two_steps(S, load_timed=False, finished=ProcessState.SUCCEEDED)
        results["b"] = b.handle(final, EventKind.RUN_FINISHED)

    thread_a = threading.Thread(target=run_a)
    thread_a.start()
    assert a_posting.wait(WAIT_S)
    thread_b = threading.Thread(target=run_b)
    thread_b.start()
    thread_a.join(WAIT_S)
    thread_b.join(WAIT_S)
    assert results == {"a": HandleResult.SENT, "b": HandleResult.SENT}

    (parent,) = api.top_level(CHANNEL)
    replies = api.replies(CHANNEL, parent.ts)
    loads = [m.text for m in replies if "*Load*" in m.text]
    assert len(loads) == 1 and "30m" in loads[0]  # A's timed reply, never an untimed duplicate
    assert replies[-1].broadcast and "*orders 2026.10.1* · Passed" in replies[-1].text


def test_a_reply_sent_by_a_parallel_event_meanwhile_is_not_sent_again() -> None:
    """Review I-2 (a): before each reply the store is read again and keys sent meanwhile are
    skipped."""
    api, variables = FakeSlackApi(), FakeVariables()
    theme_writer = thread_writer(api, variables)
    theme_writer.handle(stage(R, P, P))
    shared = store_for(variables)
    c = thread_writer(api, variables)
    ran: list[HandleResult] = []

    def parallel_event_after_our_first_reply() -> None:
        stored = shared.load(KEY)
        if ran or stored is None or not any("extract" in k for k in stored.live_keys):
            return
        ran.append(c.handle(stage(S, S, S)))  # sends Transform and Load

    a = thread_writer(api, variables, before_read=parallel_event_after_our_first_reply)
    assert a.handle(stage(S, S, R)) is HandleResult.SENT  # sends Extract, then Transform
    assert ran == [HandleResult.SENT]

    (parent,) = api.top_level(CHANNEL)
    texts = [m.text for m in api.replies(CHANNEL, parent.ts)]
    for title in ("Extract", "Transform", "Load"):
        assert sum(f"*{title}*" in text for text in texts) == 1, texts


class SlowClaim(SlackTransport):
    """Runs `meanwhile` once, right after its first parent post and before the claim is saved."""

    def __init__(self, api: FakeSlackApi, meanwhile: Callable[[], None]) -> None:
        super().__init__(api, clock=lambda: 0.0)
        self._meanwhile: Callable[[], None] | None = meanwhile

    def post_parent(
        self, channel: str, text: str, metadata: Mapping[str, Any] | None, *, deadline: float
    ) -> str:
        ts = super().post_parent(channel, text, metadata, deadline=deadline)
        if self._meanwhile is not None:
            meanwhile, self._meanwhile = self._meanwhile, None
            meanwhile()
        return ts


def test_a_slow_older_claim_does_not_replace_the_final_parent() -> None:
    """Review I-1: an older event A posts the first parent (lowest ts) but saves it late. The
    final event B runs entirely in between: it posts its own parent, the result, the alert, and
    reads back. When A saves, the parent carrying B's newer view must win; under "lowest ts
    wins" B's parent was deleted with its replies and A's "In progress" stayed forever."""
    api, variables = FakeSlackApi(), FakeVariables()
    b = writer(api, variables)
    final = stage(S, F, finished=ProcessState.FAILED)
    results: list[HandleResult] = []
    a = writer(
        api,
        variables,
        transport=SlowClaim(api, lambda: results.append(b.handle(final, EventKind.RUN_FINISHED))),
    )
    assert a.handle(stage(R, P)) is HandleResult.YIELDED
    assert results == [HandleResult.SENT]

    expected = render(final)
    (parent,) = api.top_level(CHANNEL)  # A deleted its own, losing parent
    assert parent.text == expected.parent.text
    assert [m.text for m in api.replies(CHANNEL, parent.ts)] == [
        *(e.text for e in expected.thread),
        *(a.text for a in expected.alerts),
    ]  # B's result and alert stay on the surviving parent: nothing needs re-sending
    orphans = {m.thread_ts for m in api._messages[CHANNEL].values() if m.thread_ts}
    assert orphans == {parent.ts}  # no reply sits under a deleted parent
    saved = store_for(variables).load(KEY)
    assert saved is not None and saved.parent_ref == parent.ts
    assert saved.watermark == final.watermark


def test_replies_under_a_parent_that_lost_and_was_deleted_are_sent_again() -> None:
    """A duplicate parent posted by a parallel event carries a newer view, so it wins and the
    first parent is deleted. Its replies went with it; the next event sends them again under
    the surviving parent."""
    api, variables = FakeSlackApi(), FakeVariables()
    theme = StepEntriesTheme()
    writer(api, variables, theme=theme).handle(stage(S, R, P))  # parent P1 with Extract's reply
    (first,) = api.top_level(CHANNEL)
    assert len(api.replies(CHANNEL, first.ts)) == 1

    newer = stage(S, S, R)  # a parallel first event that never saw P1 posts P2
    second = api.post(CHANNEL, render(newer).parent.text)
    wm = newer.watermark
    store_for(variables).save(
        KEY,
        SentState(
            KEY,
            parent_ref=second,
            parent_text=render(newer).parent.text,
            watermark=wm,
            parent_written=wm,
            parent_wms={second: wm},
        ),
    )

    assert writer(api, variables, theme=theme).handle(stage(S, S, S)) is HandleResult.SENT
    (parent,) = api.top_level(CHANNEL)
    assert parent.ts == second
    texts = [m.text for m in api.replies(CHANNEL, second)]
    for title in ("Extract", "Transform", "Load"):
        assert sum(f" {title}" in text for text in texts) == 1, texts


class DuplicateDuringEdit(SlackTransport):
    """Runs `meanwhile` once, just before its first parent edit."""

    def __init__(self, api: FakeSlackApi, meanwhile: Callable[[], None]) -> None:
        super().__init__(api, clock=lambda: 0.0)
        self._meanwhile: Callable[[], None] | None = meanwhile

    def update_parent(
        self,
        channel: str,
        ts: str,
        text: str,
        metadata: Mapping[str, Any] | None,
        *,
        deadline: float,
        still_current: Callable[[], bool] | None = None,
    ) -> bool:
        if self._meanwhile is not None:
            meanwhile, self._meanwhile = self._meanwhile, None
            meanwhile()
        return super().update_parent(
            channel, ts, text, metadata, deadline=deadline, still_current=still_current
        )


def test_the_final_write_follows_a_winner_that_changed_meanwhile() -> None:
    """Re-review (flip.py): E posted P1. The final event B loaded P1, but before B's edit a
    parallel first event D claimed P2 with a newer view, so P2 won and D set out to delete P1.
    B must write to the stored winner P2, never to the stale P1: otherwise P1 wins again, B's
    result goes under it, D's late delete removes it and P2 keeps "In progress" forever."""
    api, variables = FakeSlackApi(), FakeVariables()
    shared = store_for(variables)
    writer(api, variables).handle(stage(R, P, P))
    (first,) = api.top_level(CHANNEL)
    d_view = stage(S, R, P)
    stale_for_d: list[str] = []

    def parallel_claim() -> None:
        text = render(d_view).parent.text
        second = api.post(CHANNEL, text)
        wm = d_view.watermark
        claim = SentState(
            KEY,
            parent_ref=second,
            parent_text=text,
            watermark=wm,
            parent_written=wm,
            parent_wms={second: wm},
        )
        stale_for_d.extend(shared.save(KEY, claim).stale_parents)

    final = stage(S, S, F, finished=ProcessState.FAILED)
    b = writer(api, variables, transport=DuplicateDuringEdit(api, parallel_claim))
    assert b.handle(final, EventKind.RUN_FINISHED) is HandleResult.SENT
    assert stale_for_d == [first.ts]
    for ts in stale_for_d:  # D's delete, decided on its claim's state, lands late
        api.delete(CHANNEL, ts)
        shared.save(KEY, SentState(KEY, cleared_parents=frozenset({ts})))
    writer(api, variables).handle(stage(S, S, F))  # a late non-final event changes nothing

    expected = render(final)
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == expected.parent.text
    assert [m.text for m in api.replies(CHANNEL, parent.ts)] == [
        *(e.text for e in expected.thread),
        *(a.text for a in expected.alerts),
    ]
    saved = shared.load(KEY)
    assert saved is not None and saved.parent_ref == parent.ts and saved.watermark is not None
    assert saved.watermark.finished


def test_a_stale_parent_that_won_again_meanwhile_is_not_deleted() -> None:
    """The delete of a losing parent is decided on a loaded state; right before it, the store
    is read again, and a parent that is now the winner is kept."""
    api, variables = FakeSlackApi(), FakeVariables()
    shared = store_for(variables)
    writer(api, variables).handle(stage(R, P, P))
    (first,) = api.top_level(CHANNEL)
    second = api.post(CHANNEL, "duplicate parent")
    shared.save(
        KEY, SentState(KEY, parent_ref=second, parent_wms={second: stage(S, R, P).watermark})
    )
    reads: list[int] = []

    def first_wins_again() -> None:
        reads.append(1)
        if len(reads) == 2:  # after the event's load, before its delete of the first parent
            newer = stage(S, S, R).watermark
            shared.save(KEY, SentState(KEY, parent_ref=first.ts, parent_wms={first.ts: newer}))

    w = writer(api, variables, before_read=first_wins_again)
    w.handle(stage(S, S, S))
    assert api.message(CHANNEL, first.ts) is not None
    saved = shared.load(KEY)
    assert saved is not None and first.ts not in saved.cleared_parents


class ClaimAfterEdit(SlackTransport):
    """Runs `meanwhile` once, right after its first parent edit reached Slack."""

    def __init__(self, api: FakeSlackApi, meanwhile: Callable[[], None]) -> None:
        super().__init__(api, clock=lambda: 0.0)
        self._meanwhile: Callable[[], None] | None = meanwhile

    def update_parent(
        self,
        channel: str,
        ts: str,
        text: str,
        metadata: Mapping[str, Any] | None,
        *,
        deadline: float,
        still_current: Callable[[], bool] | None = None,
    ) -> bool:
        written = super().update_parent(
            channel, ts, text, metadata, deadline=deadline, still_current=still_current
        )
        if self._meanwhile is not None:
            meanwhile, self._meanwhile = self._meanwhile, None
            meanwhile()
        return written


def test_a_delayed_duplicate_delete_never_removes_the_winner_that_got_the_final_status(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Owner's item 1 (window.py): the final event B edited P1 while P1 was the winner. Before B
    records it, a parallel first event D claims P2 with a newer view (P2 wins) and event X sets out
    to delete P1, but Slack answers X with a 429. B records P1, which wins again, and finishes. A
    retry of X's delete would land after B's check and remove P1 with the final status, leaving
    P2 "In progress" forever. X makes one attempt only and leaves the duplicate."""
    api, variables = FakeSlackApi(), FakeVariables()
    shared = store_for(variables)
    writer(api, variables).handle(stage(R, P, P))
    (first,) = api.top_level(CHANNEL)
    d_view = stage(S, R, P)
    x_paused, b_done = threading.Event(), threading.Event()
    results: dict[str, HandleResult] = {}
    second: list[str] = []

    def x_waits_out_the_retry(seconds: float) -> None:
        x_paused.set()
        assert b_done.wait(WAIT_S)

    held = SlackTransport(api, clock=lambda: 0.0, sleep=x_waits_out_the_retry)
    x = writer(api, variables, transport=held)

    def run_x() -> None:
        try:
            results["x"] = x.handle(d_view)
        finally:
            x_paused.set()

    def parallel_claim_and_delete() -> None:
        text = render(d_view).parent.text
        second.append(api.post(CHANNEL, text))
        wm = d_view.watermark
        claim = SentState(
            KEY,
            parent_ref=second[0],
            parent_text=text,
            watermark=wm,
            parent_written=wm,
            parent_wms={second[0]: wm},
        )
        shared.save(KEY, claim)
        api.fail_next(TransportError("ratelimited", retryable=True, retry_after=1.0))
        thread_x = threading.Thread(target=run_x)
        thread_x.start()
        threads.append(thread_x)
        assert x_paused.wait(WAIT_S)

    threads: list[threading.Thread] = []
    final = stage(S, S, F, finished=ProcessState.FAILED)
    b = writer(api, variables, transport=ClaimAfterEdit(api, parallel_claim_and_delete))
    with caplog.at_level(logging.WARNING, logger="chappe"):
        try:
            results["b"] = b.handle(final, EventKind.RUN_FINISHED)
        finally:
            b_done.set()
        for thread in threads:
            thread.join(WAIT_S)
    assert results["b"] is HandleResult.SENT

    expected = render(final)
    winner = api.message(CHANNEL, first.ts)
    assert winner is not None and winner.text == expected.parent.text
    assert [m.text for m in api.replies(CHANNEL, first.ts)] == [
        *(e.text for e in expected.thread),
        *(a.text for a in expected.alerts),
    ]
    saved = shared.load(KEY)
    assert saved is not None and saved.parent_ref == first.ts and saved.watermark is not None
    assert saved.watermark.finished
    warning = next(
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "duplicate parent" in r.getMessage()
    )
    assert KEY in warning and first.ts in warning and second[0] in warning

    writer(api, variables).handle(stage(S, S, F))  # a later event deletes the duplicate P2
    assert [m.ts for m in api.top_level(CHANNEL)] == [first.ts]


def test_the_winner_is_read_inside_the_delete_attempt() -> None:
    """The store is read again inside the single delete attempt, after its deadline check and
    with nothing in between: a parent that won again up to that moment is kept."""
    api, variables = FakeSlackApi(), FakeVariables()
    shared = store_for(variables)
    writer(api, variables).handle(stage(R, P, P))
    (first,) = api.top_level(CHANNEL)
    second = api.post(CHANNEL, "duplicate parent")
    shared.save(
        KEY, SentState(KEY, parent_ref=second, parent_wms={second: stage(S, R, P).watermark})
    )
    won_again: list[int] = []

    def first_wins_again_at_the_attempt() -> float:
        if not won_again:  # the transport's first look at the clock: the attempt has begun
            won_again.append(1)
            newer = stage(S, S, R).watermark
            shared.save(KEY, SentState(KEY, parent_ref=first.ts, parent_wms={first.ts: newer}))
        return 0.0

    transport = SlackTransport(api, clock=first_wins_again_at_the_attempt)
    writer(api, variables, transport=transport).handle(stage(S, S, S))
    assert won_again
    assert api.message(CHANNEL, first.ts) is not None
    assert (CHANNEL, first.ts) not in api.deleted
    saved = shared.load(KEY)
    assert saved is not None and first.ts not in saved.cleared_parents
