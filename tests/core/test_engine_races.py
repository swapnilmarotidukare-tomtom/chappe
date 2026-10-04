# tests/core/test_engine_races.py
"""Interleavings found in the final review, replayed with the in-memory fakes (no network)."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from chappe.core.engine import HandleResult
from chappe.core.events import EventKind
from chappe.core.model import ProcessState, StepState
from chappe.core.view import ProcessView
from chappe.themes.builtin.thread import ThreadTheme
from chappe.transports.slack.transport import SlackTransport
from tests.support.engine import CHANNEL, KEY, Writer, stage, store_for, writer
from tests.support.fakes import FakeSlackApi, FakeVariables
from tests.support.samples import ProcessViewBuilder, default_context

S, P, R = StepState.SUCCEEDED, StepState.PENDING, StepState.RUNNING
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
        if ran or stored is None or not any("extract" in k for k in stored.sent_keys):
            return
        ran.append(c.handle(stage(S, S, S)))  # sends Transform and Load

    a = thread_writer(api, variables, before_read=parallel_event_after_our_first_reply)
    assert a.handle(stage(S, S, R)) is HandleResult.SENT  # sends Extract, then Transform
    assert ran == [HandleResult.SENT]

    (parent,) = api.top_level(CHANNEL)
    texts = [m.text for m in api.replies(CHANNEL, parent.ts)]
    for title in ("Extract", "Transform", "Load"):
        assert sum(f"*{title}*" in text for text in texts) == 1, texts
