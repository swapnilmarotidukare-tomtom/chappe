# tests/core/test_engine_properties.py
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from chappe.core.engine import HandleResult
from chappe.core.events import EventKind
from chappe.core.model import ProcessState, StepState
from chappe.core.reconcile import slack_ts_key
from chappe.core.view import ProcessView
from chappe.themes.builtin.thread import ThreadTheme
from tests.support.engine import CHANNEL, KEY, render, stage, store_for, writer
from tests.support.fakes import FakeSlackApi, FakeVariables
from tests.support.samples import ProcessViewBuilder, default_context

S, P, R = StepState.SUCCEEDED, StepState.PENDING, StepState.RUNNING
FIRST_VIEWS = [stage(R, P, P), stage(S, R, P), stage(R, R, P)]
VIEWS = [
    stage(R, P, P),
    stage(S, R, P),
    stage(S, S, R),
    stage(S, S, S),
    stage(S, S, S, finished=ProcessState.SUCCEEDED),
]
FINAL = len(VIEWS) - 1


class Interleaver:
    """Runs writers nested inside each other, as parallel Airflow workers would interleave.

    `triggers[guest] = (host, point)` starts writer `guest` at the `point`-th hook of writer `host`
    (host < guest). Hooks are Slack posts and Variable writes. Guests whose point never comes run
    afterwards, in order.
    """

    def __init__(self, triggers: Mapping[int, tuple[int, int]]) -> None:
        self._pending = dict(triggers)
        self._running: list[int] = []
        self._points: dict[int, int] = {}
        self._jobs: list[Callable[[], None]] = []

    def point(self) -> None:
        if not self._running:
            return
        host = self._running[-1]
        point = self._points.get(host, 0)
        self._points[host] = point + 1
        for guest in sorted(g for g, at in self._pending.items() if at == (host, point)):
            del self._pending[guest]
            self._run(guest)

    def _run(self, index: int) -> None:
        self._running.append(index)
        try:
            self._jobs[index]()
        finally:
            self._running.pop()

    def run(self, jobs: list[Callable[[], None]]) -> None:
        self._jobs = jobs
        self._run(0)
        while self._pending:
            guest = min(self._pending)
            del self._pending[guest]
            self._run(guest)


@st.composite
def first_events(draw: st.DrawFn) -> tuple[list[ProcessView], dict[int, tuple[int, int]]]:
    n = draw(st.integers(2, 4))
    views = [draw(st.sampled_from(FIRST_VIEWS)) for _ in range(n)]
    triggers = {
        guest: (draw(st.integers(0, guest - 1)), draw(st.integers(0, 6))) for guest in range(1, n)
    }
    return views, triggers


@settings(max_examples=300, deadline=None)
@given(first_events())
def test_concurrent_first_events_converge_on_the_lowest_parent(
    case: tuple[list[ProcessView], dict[int, tuple[int, int]]],
) -> None:
    """Parallel writers start inside each other's load→set window, at Slack posts and Variable
    writes.

    Not inside the write that stores the first parent: its `get`→`set` gap is the window that
    Variables without compare-and-set leave open (a writer landing there is overwritten).
    """
    views, triggers = case
    api, variables = FakeSlackApi(), FakeVariables()
    shared = store_for(variables)
    interleaver = Interleaver(triggers)

    def before_set(key: str, value: str) -> None:
        stored = shared.load(KEY)  # what the writer just re-read; nothing ran in between
        if stored is not None and stored.parent_ref is not None:
            interleaver.point()

    api.before_post = lambda channel, thread_ts: interleaver.point()
    variables.before_set = before_set
    writers = [writer(api, variables) for _ in views]
    results: dict[int, HandleResult] = {}

    def job(index: int) -> Callable[[], None]:
        def run() -> None:
            results[index] = writers[index].handle(views[index])

        return run

    interleaver.run([job(i) for i in range(len(views))])

    assert set(results.values()) <= {HandleResult.SENT, HandleResult.SKIPPED, HandleResult.YIELDED}
    posted = sum(1 for name, call in api.calls if name == "post" and call["thread_ts"] is None)
    deleted = [ts for _, ts in api.deleted]
    (live,) = api.top_level(CHANNEL)  # exactly one live parent
    assert len(deleted) == posted - 1  # every other parent was deleted
    assert live.ts == min([live.ts, *deleted], key=slack_ts_key)  # the lowest ts won
    saved = shared.load(KEY)
    assert saved is not None
    assert saved.parent_ref == live.ts
    assert saved.stale_parents <= set(deleted)  # anything still listed is already gone


@settings(max_examples=200, deadline=None)
@given(
    st.lists(st.integers(0, FINAL), max_size=6).flatmap(
        lambda extra: st.permutations(list(range(len(VIEWS))) + extra)
    )
)
def test_any_order_or_duplication_ends_in_the_final_render(order: list[int]) -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    final = render(VIEWS[FINAL])
    finished = False
    for index in order:
        kind = EventKind.RUN_FINISHED if index == FINAL else EventKind.STEP_FINISHED
        assert w.handle(VIEWS[index], kind) in (HandleResult.SENT, HandleResult.SKIPPED)
        finished = finished or index == FINAL
        if finished:
            (parent,) = api.top_level(CHANNEL)
            assert parent.text == final.parent.text  # the finished status is never overwritten

    (parent,) = api.top_level(CHANNEL)
    assert parent.text == final.parent.text
    assert [m.text for m in api.replies(CHANNEL, parent.ts)] == [e.text for e in final.thread]


THREAD_CTX = default_context("thread")


def _thread_view(
    states: tuple[StepState, StepState, StepState],
    timed: str | None,
    *,
    later: int = 0,
    finished: bool = False,
) -> ProcessView:
    """Extract, then Transform and Load in parallel; only the step named `timed` has times."""
    builder = ProcessViewBuilder(key=KEY).section("Main")
    for title, state in zip(("Extract", "Transform", "Load"), states, strict=True):
        builder.step(title, state, 60, timed=title == timed)
    if finished:
        builder.finished(ProcessState.SUCCEEDED)
    view = builder.build()
    return replace(view, now=view.now + timedelta(minutes=later))


THREAD_VIEWS = [
    _thread_view((R, P, P), "Extract"),
    _thread_view((S, P, P), "Extract"),
    _thread_view((S, R, R), "Transform"),
    _thread_view((S, S, R), "Transform"),
    _thread_view((S, S, S), "Load", later=1),  # Load's event shows Transform finished, untimed
    _thread_view((S, S, S), "Transform", later=-1),  # Transform's own, older callback
    _thread_view((S, S, S), None, finished=True),
]
THREAD_FINAL = len(THREAD_VIEWS) - 1
# the events that carry each step's own finish times
OWN_EVENTS = {"Extract": {1}, "Transform": {3, 5}, "Load": {4}}


@settings(max_examples=200, deadline=None)
@given(
    st.lists(st.integers(0, THREAD_FINAL), max_size=4).flatmap(
        lambda extra: st.permutations(list(range(len(THREAD_VIEWS))) + extra)
    )
)
def test_thread_parent_shows_the_newest_view_and_every_reply_goes_out_once(
    order: list[int],
) -> None:
    """An older view never changes the parent text; replies are sent once, whatever the order."""
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    w = writer(api, variables, theme=theme, context=THREAD_CTX)
    newest: ProcessView | None = None
    for index in order:
        view = THREAD_VIEWS[index]
        kind = EventKind.RUN_FINISHED if index == THREAD_FINAL else EventKind.STEP_FINISHED
        assert w.handle(view, kind) in (HandleResult.SENT, HandleResult.SKIPPED)
        if newest is None or view.watermark.newer_than(newest.watermark):
            newest = view
        (parent,) = api.top_level(CHANNEL)
        assert parent.text == theme.render(newest, THREAD_CTX).parent.text

    (parent,) = api.top_level(CHANNEL)
    replies = [m.text for m in api.replies(CHANNEL, parent.ts)]
    assert len(replies) == 4  # Extract, Transform, Load, and the result: once each
    before_final = set(order[: order.index(THREAD_FINAL)])
    for title, own_events in OWN_EVENTS.items():
        (reply,) = [text for text in replies if f"*{title}*" in text]
        if own_events & before_final:
            assert "1h 00m" in reply  # its own callback came in time: the reply has its duration
