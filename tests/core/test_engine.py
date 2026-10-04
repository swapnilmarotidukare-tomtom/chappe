# tests/core/test_engine.py
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import timedelta
from typing import Any, ClassVar

import pytest

from chappe.core.engine import HandleResult, parent_payload
from chappe.core.errors import TransportError
from chappe.core.events import ChappeEvent, EventKind
from chappe.core.messages import MessageSet
from chappe.core.model import ProcessState, StepState
from chappe.core.reconcile import SentState
from chappe.core.render import RenderContext
from chappe.core.view import ProcessView
from chappe.stores.airflow_variable import encode, key_for
from chappe.themes.builtin.thread import ThreadTheme
from chappe.transports.slack.transport import EVENT_TYPE, SlackTransport
from tests.support.engine import (
    CHANNEL,
    CTX,
    KEY,
    PreparedSource,
    StepEntriesTheme,
    render,
    stage,
    store_for,
    writer,
)
from tests.support.fakes import FakeSlackApi, FakeVariables
from tests.support.samples import ProcessViewBuilder, default_context

S, P, R, F = StepState.SUCCEEDED, StepState.PENDING, StepState.RUNNING, StepState.FAILED


class BrokenTheme:
    name: ClassVar[str] = "broken"

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        raise RuntimeError("theme bug")


class ExplodingSource(PreparedSource):
    def snapshot(self, event: ChappeEvent) -> ProcessView | None:
        raise RuntimeError("source exploded")


def test_happy_path_posts_once_edits_and_finishes() -> None:
    api, variables, sleeps = FakeSlackApi(), FakeVariables(), []
    w = writer(api, variables, sleep=sleeps.append)
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)

    assert w.handle(stage(R, P, P)) is HandleResult.SENT
    assert w.handle(stage(S, R, P)) is HandleResult.SENT
    assert w.handle(final, EventKind.RUN_FINISHED) is HandleResult.SENT

    expected = render(final)
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == expected.parent.text
    assert parent.metadata == {
        "event_type": EVENT_TYPE,
        "event_payload": parent_payload(KEY, final.watermark),
    }
    replies = api.replies(CHANNEL, parent.ts)
    assert [(m.text, m.broadcast) for m in replies] == [
        (e.text, e.broadcast) for e in expected.thread
    ]
    assert sleeps == [2.0]  # the final read-back waited once

    saved = store_for(variables).load(KEY)
    assert saved is not None
    assert saved.parent_ref == parent.ts
    assert saved.parent_text == expected.parent.text
    assert saved.parent_written == final.watermark
    assert saved.watermark == final.watermark
    assert saved.live_keys == {e.key for e in expected.thread}


def test_a_view_that_is_not_newer_is_skipped() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    w.handle(stage(S, R, P))
    calls = len(api.calls)
    assert w.handle(stage(S, R, P)) is HandleResult.SKIPPED  # same watermark
    assert w.handle(stage(R, P, P)) is HandleResult.SKIPPED  # older
    assert len(api.calls) == calls


def test_theme_crash_falls_back_to_plain(caplog: pytest.LogCaptureFixture) -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    view = stage(S, R, P)
    with caplog.at_level(logging.ERROR, logger="chappe"):
        assert writer(api, variables, theme=BrokenTheme()).handle(view) is HandleResult.SENT
    assert api.top_level(CHANNEL)[0].text == render(view).parent.text
    assert "theme 'broken' failed" in caplog.text


def test_permanent_slack_error_marks_the_process_degraded() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    w.handle(stage(R, P, P))
    api.fail_next(TransportError("not_in_channel", retryable=False))
    assert w.handle(stage(S, R, P)) is HandleResult.DEGRADED
    saved = store_for(variables).load(KEY)
    assert saved is not None and saved.degraded

    calls = len(api.calls)
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)
    assert w.handle(final, EventKind.RUN_FINISHED) is HandleResult.DEGRADED
    assert len(api.calls) == calls  # stopped for this process


def test_no_view_and_kill_switch_send_nothing() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    assert writer(api, variables).handle(None) is HandleResult.NO_PROCESS
    assert writer(api, variables, enabled=False).handle(stage(R, P, P)) is HandleResult.DISABLED
    assert api.calls == []


def test_never_raises(caplog: pytest.LogCaptureFixture) -> None:
    def broken_read() -> None:
        raise RuntimeError("metadata database down")

    api, variables = FakeSlackApi(), FakeVariables()
    exploding = writer(api, variables, source=ExplodingSource())
    unreadable = writer(api, variables, before_read=broken_read)
    with caplog.at_level(logging.ERROR, logger="chappe"):
        assert exploding.handle(stage(R, P, P)) is HandleResult.ERROR
        assert unreadable.handle(stage(R, P, P)) is HandleResult.ERROR
    assert caplog.text.count("chappe: event failed") == 2
    assert api.calls == []  # an unreadable store means no post: silence over a duplicate


def test_parallel_writers_both_keep_their_thread_entries() -> None:
    """Merge-on-save: a writer that saves after a parallel one keeps the other's keys."""
    api, variables = FakeSlackApi(), FakeVariables()
    theme = StepEntriesTheme()
    # Transform and Load run in parallel
    writer(api, variables, theme=theme).handle(stage(S, R, R, P))

    transform_done = stage(S, S, R, P)
    load_done = stage(S, R, S, R)  # Publish started too, so this view is newer
    other = writer(api, variables, theme=theme)
    results: list[HandleResult] = []

    def other_runs_once(channel: str, thread_ts: str | None) -> None:
        api.before_post = None  # once: `other`'s own posts must not start it again
        results.append(other.handle(load_done))

    api.before_post = other_runs_once  # `other` runs inside `racing`'s first post
    racing = writer(api, variables, theme=theme)
    assert racing.handle(transform_done) is HandleResult.SENT
    assert results == [HandleResult.SENT]

    expected = {e.key for v in (transform_done, load_done) for e in theme.render(v, CTX).thread}
    saved = store_for(variables).load(KEY)
    assert saved is not None and expected <= saved.live_keys

    writer(api, variables, theme=theme).handle(stage(S, S, S, S))
    (parent,) = api.top_level(CHANNEL)
    replies = [m.text for m in api.replies(CHANNEL, parent.ts)]
    assert len(replies) == len(set(replies)) == 4  # no key lost, so nothing is sent twice


def test_any_event_deletes_leftover_duplicate_parents() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    w.handle(stage(S, R, P))
    loser = api.post(CHANNEL, "duplicate parent")
    store_for(variables).save(KEY, SentState(KEY, stale_parents=frozenset({loser})))

    assert w.handle(stage(S, R, P)) is HandleResult.SKIPPED
    assert api.message(CHANNEL, loser) is None
    saved = store_for(variables).load(KEY)
    assert saved is not None
    assert loser in saved.cleared_parents and not saved.stale_parents


def test_a_parent_dropped_by_a_stale_write_is_saved_again() -> None:
    """A parallel writer that read before our save overwrites it right after.

    The verify read catches the missing parent and saves it again; the lower ts still wins and ours
    is deleted.
    """
    api, variables = FakeSlackApi(), FakeVariables()
    parallel = api.post(CHANNEL, "parallel parent")  # posted first, so its ts is lower
    view = stage(R, P, P)
    # a parallel first event of the same progress: equal watermarks, so the lower ts wins
    stale_write = encode(
        SentState(
            KEY,
            parent_ref=parallel,
            parent_text="parallel parent",
            parent_wms={parallel: view.watermark},
        )
    )
    writes: list[str] = []

    def count_write(key: str, value: str) -> None:
        writes.append(key)

    def overwrite_after_our_first_save() -> None:
        if len(writes) == 1:
            writes.append("stale")
            variables.set(key_for(KEY), stale_write)

    variables.before_set = count_write
    assert (
        writer(api, variables, before_read=overwrite_after_our_first_save).handle(view)
        is HandleResult.SENT
    )

    (parent,) = api.top_level(CHANNEL)
    assert parent.ts == parallel
    assert parent.text == render(view).parent.text  # adopted and updated with this render
    assert len(api.deleted) == 1  # our own parent
    saved = store_for(variables).load(KEY)
    assert saved is not None
    assert saved.parent_ref == parallel and not saved.stale_parents


def test_final_read_back_repairs_a_late_non_final_writer() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)
    late = stage(S, S, R)
    shared = store_for(variables)

    def late_writer_lands(seconds: float) -> None:
        """During the wait, a non-final event that loaded before the final write edits it."""
        current = shared.load(KEY)
        assert current is not None and current.parent_ref is not None
        stale_text = render(late).parent.text
        api.update(CHANNEL, current.parent_ref, stale_text)
        shared.save(
            KEY,
            SentState(
                KEY,
                parent_ref=current.parent_ref,
                parent_text=stale_text,
                watermark=late.watermark,
                parent_written=late.watermark,
            ),
        )

    w = writer(api, variables, sleep=late_writer_lands)
    w.handle(stage(S, R, P))
    assert w.handle(final, EventKind.RUN_FINISHED) is HandleResult.SENT

    (parent,) = api.top_level(CHANNEL)
    assert parent.text == render(final).parent.text
    saved = shared.load(KEY)
    assert saved is not None
    assert saved.parent_written == final.watermark
    assert saved.watermark == final.watermark


def test_known_limit_until_0_0_4_a_cleared_task_leaves_the_final_status() -> None:
    """Pinned known limit (README, Task 19): "Clearing tasks after a run finished leaves the final
    status until the run finishes again." Finished is sticky, so a non-finished render after a
    finished one is skipped. 0.0.4 changes this on purpose; change this test with it, never by
    accident.
    """
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)
    w.handle(final, EventKind.RUN_FINISHED)

    rerun = stage(S, R, P)  # Transform and Load cleared and running again
    assert w.handle(rerun) is HandleResult.SKIPPED
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == render(final).parent.text


def test_an_ambiguous_post_failure_is_left_to_the_next_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Slack may have posted despite the error, so the engine never re-posts within the event."""
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    api.fail_next(TransportError("internal_error", retryable=True))
    with caplog.at_level(logging.ERROR, logger="chappe"):
        assert w.handle(stage(R, P, P)) is HandleResult.ERROR
    assert [name for name, _ in api.calls] == ["post"]  # tried once, not retried
    assert store_for(variables).load(KEY) is None

    assert w.handle(stage(S, R, P)) is HandleResult.SENT  # the next event posts
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == render(stage(S, R, P)).parent.text


def raising_metrics(name: str) -> None:
    raise RuntimeError("statsd down")


class DeadlineTransport(SlackTransport):
    """Records the deadline of every parent post and update."""

    def __init__(self, api: FakeSlackApi, clock: Callable[[], float]) -> None:
        super().__init__(api, clock=clock)
        self.deadlines: list[float] = []

    def post_parent(
        self, channel: str, text: str, metadata: Mapping[str, Any] | None, *, deadline: float
    ) -> str:
        self.deadlines.append(deadline)
        return super().post_parent(channel, text, metadata, deadline=deadline)

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
        self.deadlines.append(deadline)
        return super().update_parent(
            channel, ts, text, metadata, deadline=deadline, still_current=still_current
        )


def test_a_raising_metrics_hook_still_falls_back_to_plain(caplog: pytest.LogCaptureFixture) -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    view = stage(S, R, P)
    w = writer(api, variables, theme=BrokenTheme(), metrics=raising_metrics)
    with caplog.at_level(logging.ERROR, logger="chappe"):
        assert w.handle(view) is HandleResult.SENT
    assert api.top_level(CHANNEL)[0].text == render(view).parent.text
    assert "chappe: metrics hook failed" in caplog.text


def test_a_raising_metrics_hook_still_records_degraded() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables, metrics=raising_metrics)
    api.fail_next(TransportError("not_in_channel", retryable=False))
    assert w.handle(stage(R, P, P)) is HandleResult.DEGRADED
    saved = store_for(variables).load(KEY)
    assert saved is not None and saved.degraded


def test_final_read_back_waits_only_for_the_time_left() -> None:
    now = [100.0]
    sleeps: list[float] = []
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables, clock=lambda: now[0], sleep=sleeps.append)
    w.handle(stage(S, R, P))
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)

    def time_passes(key: str, value: str) -> None:
        now[0] = 129.5  # the parent write left 0.5s of the final budget (30s from 100.0)

    variables.before_set = time_passes
    assert w.handle(final, EventKind.RUN_FINISHED) is HandleResult.SENT
    assert sleeps == [0.5]


def test_final_read_back_is_skipped_when_the_budget_is_spent() -> None:
    now = [100.0]
    sleeps: list[float] = []
    reads: list[int] = []
    reads_at_last_write: list[int] = []
    api, variables = FakeSlackApi(), FakeVariables()

    def count_read() -> None:
        reads.append(1)

    w = writer(api, variables, clock=lambda: now[0], sleep=sleeps.append, before_read=count_read)
    w.handle(stage(S, R, P))

    def time_runs_out_at_the_parent_write(key: str, value: str) -> None:
        now[0] = 131.0  # past the final budget (30s from 100.0)
        reads_at_last_write[:] = [len(reads)]

    variables.before_set = time_runs_out_at_the_parent_write
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)
    assert w.handle(final, EventKind.RUN_FINISHED) is HandleResult.SENT
    assert sleeps == []
    assert reads_at_last_write == [len(reads)]  # no read-back after the last write


def test_run_finished_gets_the_final_budget_and_other_events_the_event_budget() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    transport = DeadlineTransport(api, clock=lambda: 1000.0)
    w = writer(api, variables, clock=lambda: 1000.0, transport=transport)
    assert w.handle(stage(R, P, P), EventKind.STEP_STARTED) is HandleResult.SENT
    assert w.handle(stage(S, R, P), EventKind.STEP_FINISHED) is HandleResult.SENT
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)
    assert w.handle(final, EventKind.RUN_FINISHED) is HandleResult.SENT
    assert transport.deadlines == [1010.0, 1010.0, 1030.0]


def test_a_hand_deleted_parent_is_replaced_by_a_new_one() -> None:
    """Spec 9.2 "Parent message deleted -> post a new parent and update the store"."""
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    w.handle(stage(R, P, P))
    (old,) = api.top_level(CHANNEL)
    api.delete(CHANNEL, old.ts)  # someone deletes the parent by hand

    view = stage(S, R, P)
    assert w.handle(view) is HandleResult.SENT
    (new,) = api.top_level(CHANNEL)
    assert new.ts != old.ts
    assert new.text == render(view).parent.text
    saved = store_for(variables).load(KEY)
    assert saved is not None
    assert saved.parent_ref == new.ts
    assert old.ts in saved.cleared_parents and not saved.degraded

    # Later events keep using the new parent; the deleted lower ts never wins again.
    assert w.handle(stage(S, S, R)) is HandleResult.SENT
    assert [m.ts for m in api.top_level(CHANNEL)] == [new.ts]


def test_a_failed_final_event_after_a_hand_delete_still_posts_the_alert() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    w.handle(stage(S, R, P))
    (old,) = api.top_level(CHANNEL)
    api.delete(CHANNEL, old.ts)

    failed = stage(S, F, P, finished=ProcessState.FAILED)
    assert w.handle(failed, EventKind.RUN_FINISHED) is HandleResult.SENT
    (parent,) = api.top_level(CHANNEL)
    assert parent.ts != old.ts
    expected = render(failed)
    assert expected.alerts
    replies = [m.text for m in api.replies(CHANNEL, parent.ts)]
    assert all(alert.text in replies for alert in expected.alerts)


def test_an_unreadable_store_sends_nothing_and_keeps_the_variable() -> None:
    """Spec 9.2 "Store unreadable -> skip sending"."""
    api, variables = FakeSlackApi(), FakeVariables()
    variables.data[key_for(KEY)] = "{not json"
    assert writer(api, variables).handle(stage(R, P, P)) is HandleResult.ERROR
    assert api.calls == []
    assert variables.data == {key_for(KEY): "{not json"}


def test_replies_stop_at_the_deadline_and_the_next_event_sends_the_rest() -> None:
    """Never blocks (spec 9.3): once the budget is spent, the rest is left to the next event."""
    now = [0.0]
    api, variables = FakeSlackApi(), FakeVariables()
    theme = StepEntriesTheme()
    posts: list[str | None] = []

    def slow_reply(channel: str, thread_ts: str | None) -> None:
        posts.append(thread_ts)
        if thread_ts is not None:
            now[0] = 11.0  # the first reply used up the 10s event budget

    api.before_post = slow_reply
    view = stage(S, S, R)  # two finished steps: two thread entries
    w = writer(api, variables, theme=theme, clock=lambda: now[0])
    assert w.handle(view) is HandleResult.SENT
    (parent,) = api.top_level(CHANNEL)
    assert len(api.replies(CHANNEL, parent.ts)) == 1
    saved = store_for(variables).load(KEY)
    assert saved is not None and len(saved.live_keys) == 1  # what was sent is saved

    api.before_post = None
    now[0] = 100.0
    assert w.handle(stage(S, S, S)) is HandleResult.SENT
    replies = [m.text for m in api.replies(CHANNEL, parent.ts)]
    assert len(replies) == len(set(replies)) == 3


def test_logs_carry_the_process_key(caplog: pytest.LogCaptureFixture) -> None:
    """Spec 11: every line carries the process key."""
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables)
    w.handle(stage(S, R, P))
    loser = api.post(CHANNEL, "duplicate parent")
    store_for(variables).save(KEY, SentState(KEY, stale_parents=frozenset({loser})))
    api.fail_next(TransportError("cant_delete_message", retryable=False))
    broken = writer(api, variables, theme=BrokenTheme())
    with caplog.at_level(logging.WARNING, logger="chappe"):
        broken.handle(stage(S, S, R))
    delete_line = next(r for r in caplog.records if "duplicate parent" in r.getMessage())
    theme_line = next(r for r in caplog.records if "theme 'broken' failed" in r.getMessage())
    assert KEY in delete_line.getMessage()
    assert KEY in theme_line.getMessage()


def test_a_final_event_out_of_budget_logs_an_error_with_the_process_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Spec 9.3: no later event can correct the final one, so running out of budget is an error."""
    now = [0.0]
    api, variables = FakeSlackApi(), FakeVariables()

    def slow_reply(channel: str, thread_ts: str | None) -> None:
        if thread_ts is not None:
            now[0] = 31.0  # the first reply used up the 30s final budget

    api.before_post = slow_reply
    w = writer(api, variables, theme=StepEntriesTheme(), clock=lambda: now[0])
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)
    with caplog.at_level(logging.WARNING, logger="chappe"):
        assert w.handle(final, EventKind.RUN_FINISHED) is HandleResult.SENT
    (record,) = [r for r in caplog.records if "final event" in r.getMessage()]
    assert record.levelno == logging.ERROR and KEY in record.getMessage()
    assert "were not sent" in record.getMessage()
    assert "next event" not in record.getMessage()

    caplog.clear()
    now[0] = 100.0
    other_api, other_vars = FakeSlackApi(), FakeVariables()
    other_api.before_post = lambda channel, ts: now.__setitem__(0, 111.0) if ts else None
    w = writer(
        other_api, other_vars, theme=StepEntriesTheme(), clock=lambda: now[0], event_budget_s=10.0
    )
    with caplog.at_level(logging.WARNING, logger="chappe"):
        w.handle(stage(S, S, R), EventKind.STEP_FINISHED)
    (record,) = [r for r in caplog.records if "budget" in r.getMessage()]
    assert record.levelno == logging.WARNING  # a later event sends the rest
    assert "left to the next event" in record.getMessage()


THREAD_CTX = default_context("thread")


def parallel_run(
    transform_timed: bool, load_timed: bool, *, finished: bool = False, later: int = 0
) -> ProcessView:
    """Transform and Load run in parallel; only the step whose callback fired carries times."""
    builder = (
        ProcessViewBuilder(key=KEY)
        .section("Main")
        .step("Extract", S, 10, timed=False)
        .step("Transform", S, 261, timed=transform_timed)
        .step("Load", S, 78, timed=load_timed)
    )
    if finished:
        builder.step("Publish", S, 5, timed=False).finished(ProcessState.SUCCEEDED)
    else:
        builder.step("Publish", P)
    view = builder.build()
    return replace(view, now=view.now + timedelta(minutes=later))


def thread_replies(api: FakeSlackApi, title: str) -> list[str]:
    (parent,) = api.top_level(CHANNEL)
    return [m.text for m in api.replies(CHANNEL, parent.ts) if f"*{title}*" in m.text]


def test_thread_race_a_step_shown_finished_by_a_parallel_callback_waits_for_its_own() -> None:
    """Owner's race: Load's callback arrives first and shows Transform succeeded, untimed."""
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    transform_key = "step:main.transform:succeeded"

    load_first = parallel_run(transform_timed=False, load_timed=True)
    assert writer(api, variables, theme=theme, context=THREAD_CTX).handle(load_first) is (
        HandleResult.SENT
    )
    assert thread_replies(api, "Transform") == []
    assert len(thread_replies(api, "Load")) == 1
    saved = store_for(variables).load(KEY)
    assert saved is not None and transform_key not in saved.live_keys

    own = parallel_run(transform_timed=True, load_timed=False, later=1)
    assert writer(api, variables, theme=theme, context=THREAD_CTX).handle(own) is (
        HandleResult.SENT
    )
    (reply,) = thread_replies(api, "Transform")
    assert "4h 21m" in reply
    saved = store_for(variables).load(KEY)
    assert saved is not None and transform_key in saved.live_keys

    final = parallel_run(transform_timed=False, load_timed=False, finished=True)
    writer(api, variables, theme=theme, context=THREAD_CTX).handle(final, EventKind.RUN_FINISHED)
    assert thread_replies(api, "Transform") == [reply]  # never sent twice
    assert len(thread_replies(api, "Load")) == 1


def test_thread_race_fallback_a_lost_own_callback_is_covered_by_the_final_event() -> None:
    """Transform's own callback never arrives: the final view sends its reply once, untimed."""
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    load_first = parallel_run(transform_timed=False, load_timed=True)
    writer(api, variables, theme=theme, context=THREAD_CTX).handle(load_first)
    assert thread_replies(api, "Transform") == []

    final = parallel_run(transform_timed=False, load_timed=False, finished=True)
    assert writer(api, variables, theme=theme, context=THREAD_CTX).handle(
        final, EventKind.RUN_FINISHED
    ) is (HandleResult.SENT)
    (reply,) = thread_replies(api, "Transform")
    assert reply.endswith(
        "*Transform* · Passed · <https://airflow.invalid/dags/orders/runs/run_1/tasks/transform|Log>"
    )
    assert "4h 21m" not in reply
    assert len(thread_replies(api, "Load")) == 1
    saved = store_for(variables).load(KEY)
    assert saved is not None and "step:main.transform:succeeded" in saved.live_keys


def test_thread_race_an_own_callback_older_than_the_parallel_event_still_sends_its_reply() -> None:
    """Owner's race, other ordering: Transform's own callback is older than Load's event.

    The view is not newer, so the parent keeps the newer text; Transform's reply still goes out
    once, with its duration.
    """
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    load_first = parallel_run(transform_timed=False, load_timed=True)
    writer(api, variables, theme=theme, context=THREAD_CTX).handle(load_first)
    before = store_for(variables).load(KEY)
    assert before is not None

    own = parallel_run(transform_timed=True, load_timed=False, later=-1)
    assert not own.watermark.newer_than(load_first.watermark)
    assert writer(api, variables, theme=theme, context=THREAD_CTX).handle(own) is (
        HandleResult.SENT
    )
    (reply,) = thread_replies(api, "Transform")
    assert "4h 21m" in reply
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == theme.render(load_first, THREAD_CTX).parent.text
    assert not [c for c in api.calls if c[0] == "update"]  # the older view never edits the parent
    after = store_for(variables).load(KEY)
    assert after is not None and "step:main.transform:succeeded" in after.live_keys
    assert (after.parent_text, after.parent_written, after.watermark) == (
        before.parent_text,
        before.parent_written,
        before.watermark,
    )

    # handled again (a duplicate delivery): nothing left to send
    calls = len(api.calls)
    assert writer(api, variables, theme=theme, context=THREAD_CTX).handle(own) is (
        HandleResult.SKIPPED
    )
    assert len(api.calls) == calls


def test_an_older_view_without_a_stored_parent_sends_nothing() -> None:
    """No parent yet: a newer event posts it, so an older view leaves the replies to that one."""
    api, variables = FakeSlackApi(), FakeVariables()
    newer = parallel_run(transform_timed=False, load_timed=True)
    store_for(variables).save(KEY, SentState(KEY, watermark=newer.watermark))
    own = parallel_run(transform_timed=True, load_timed=False, later=-1)
    w = writer(api, variables, theme=ThreadTheme(), context=THREAD_CTX)
    assert w.handle(own) is HandleResult.SKIPPED
    assert api.calls == []


def finished_run(load: StepState, state: ProcessState) -> ProcessView:
    """The DAG callback: no step carries times."""
    builder = ProcessViewBuilder(key=KEY).section("Main")
    builder.step("Extract", S, 10, timed=False).step("Transform", S, 261, timed=False)
    return builder.step("Load", load, 78, timed=False).finished(state).build()


def rerun(load: StepState) -> ProcessView:
    """Load was cleared after the run finished; its own callback reports the rerun."""
    builder = ProcessViewBuilder(key=KEY).section("Main")
    builder.step("Extract", S, 10, timed=False).step("Transform", S, 261, timed=False)
    return builder.step("Load", load, 78).build()


def test_known_limit_until_0_0_4_thread_a_rerun_step_after_a_passed_run_posts_nothing() -> None:
    """The thread-theme variant of the known limit: a cleared Load that reruns and fails adds
    no ":x: Load · Failed" reply under the "Passed" message, and the parent keeps the final status.
    """
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    final = finished_run(S, ProcessState.SUCCEEDED)
    writer(api, variables, theme=theme, context=THREAD_CTX).handle(final, EventKind.RUN_FINISHED)
    calls = len(api.calls)

    w = writer(api, variables, theme=theme, context=THREAD_CTX)
    assert w.handle(rerun(F)) is HandleResult.SKIPPED
    assert len(api.calls) == calls
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == theme.render(final, THREAD_CTX).parent.text


def test_a_step_cleared_after_a_failed_run_that_then_succeeds_posts_nothing() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    final = finished_run(F, ProcessState.FAILED)
    writer(api, variables, theme=theme, context=THREAD_CTX).handle(final, EventKind.RUN_FINISHED)
    calls = len(api.calls)

    w = writer(api, variables, theme=theme, context=THREAD_CTX)
    assert w.handle(rerun(S)) is HandleResult.SKIPPED
    assert len(api.calls) == calls
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == theme.render(final, THREAD_CTX).parent.text


def test_a_late_older_final_event_with_another_state_posts_nothing() -> None:
    """No process:<state> broadcast and no alert from an older duplicate RUN_FINISHED."""
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    passed = finished_run(S, ProcessState.SUCCEEDED)
    writer(api, variables, theme=theme, context=THREAD_CTX).handle(passed, EventKind.RUN_FINISHED)
    calls = len(api.calls)

    failed = finished_run(F, ProcessState.FAILED)
    failed = replace(failed, now=passed.now - timedelta(minutes=1))
    assert not failed.watermark.newer_than(passed.watermark)
    w = writer(api, variables, theme=theme, context=THREAD_CTX)
    assert w.handle(failed, EventKind.RUN_FINISHED) is HandleResult.SKIPPED
    assert len(api.calls) == calls


def test_an_older_view_out_of_time_before_its_first_reply_is_skipped() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    load_first = parallel_run(transform_timed=False, load_timed=True)
    writer(api, variables, theme=ThreadTheme(), context=THREAD_CTX).handle(load_first)
    calls = len(api.calls)

    # the deadline is computed at 0s and the snapshot and the store load start in time;
    # every later reading is past it
    ticks = iter([0.0, 0.0, 0.0])
    w = writer(
        api,
        variables,
        theme=ThreadTheme(),
        context=THREAD_CTX,
        clock=lambda: next(ticks, 100.0),
    )
    own = parallel_run(transform_timed=True, load_timed=False, later=-1)
    assert w.handle(own) is HandleResult.SKIPPED
    assert len(api.calls) == calls


class SlowSource(PreparedSource):
    """The task-state read takes the whole budget."""

    def __init__(self, now: list[float], to: float) -> None:
        super().__init__()
        self._now, self._to = now, to
        self.calls = 0

    def snapshot(self, event: ChappeEvent) -> ProcessView | None:
        self.calls += 1
        self._now[0] = self._to
        return super().snapshot(event)


def test_no_task_state_read_when_the_budget_is_already_spent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Spec 9.3: the deadline is checked before every call into Airflow, the first one too."""
    api, variables = FakeSlackApi(), FakeVariables()
    ticks = iter([0.0])  # the deadline is computed at 0s; every later reading is past it
    source = SlowSource([0.0], 0.0)
    w = writer(api, variables, source=source, clock=lambda: next(ticks, 100.0))
    with caplog.at_level(logging.WARNING, logger="chappe"):
        assert w.handle(stage(R, P, P)) is HandleResult.OUT_OF_TIME
    assert source.calls == 0 and api.calls == [] and variables.data == {}
    assert "left to the next event" in caplog.text and "orders/run_1" in caplog.text


def test_a_slow_task_state_read_stops_the_event_before_the_store(
    caplog: pytest.LogCaptureFixture,
) -> None:
    now, reads = [0.0], []
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(
        api,
        variables,
        source=SlowSource(now, 11.0),  # past the 10s event budget
        clock=lambda: now[0],
        before_read=lambda: reads.append(1),
    )
    with caplog.at_level(logging.WARNING, logger="chappe"):
        assert w.handle(stage(R, P, P)) is HandleResult.OUT_OF_TIME
    assert reads == [] and api.calls == []
    (record,) = [r for r in caplog.records if "time budget" in r.getMessage()]
    assert record.levelno == logging.WARNING and KEY in record.getMessage()


def test_a_final_event_out_of_time_before_the_store_logs_an_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    now = [0.0]
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables, source=SlowSource(now, 31.0), clock=lambda: now[0])
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)
    with caplog.at_level(logging.WARNING, logger="chappe"):
        assert w.handle(final, EventKind.RUN_FINISHED) is HandleResult.OUT_OF_TIME
    assert api.calls == []
    (record,) = [r for r in caplog.records if "final event" in r.getMessage()]
    assert record.levelno == logging.ERROR and KEY in record.getMessage()


def test_a_slow_store_read_skips_a_save_that_records_no_slack_call() -> None:
    """The watermark-only save is skipped once the budget is spent; Slack is untouched."""
    now = [0.0]
    api, variables = FakeSlackApi(), FakeVariables()
    w = writer(api, variables, clock=lambda: now[0])
    first = stage(S, R, P)
    w.handle(first)
    before = variables.data[key_for(KEY)]
    calls = len(api.calls)

    def slow_read() -> None:
        now[0] = 11.0  # the load took the whole 10s budget

    later = replace(first, now=first.now + timedelta(minutes=1))  # same text, newer watermark
    slow = writer(api, variables, clock=lambda: now[0], before_read=slow_read)
    assert slow.handle(later) is HandleResult.OUT_OF_TIME
    assert variables.data[key_for(KEY)] == before
    assert len(api.calls) == calls


def test_a_save_that_records_a_slack_post_is_made_even_past_the_deadline() -> None:
    """Skipping it would leave the post untracked: the next event would post it again."""
    now = [0.0]
    api, variables = FakeSlackApi(), FakeVariables()

    def slow_post(channel: str, thread_ts: str | None) -> None:
        now[0] = 11.0  # the parent post used up the budget

    api.before_post = slow_post
    w = writer(api, variables, clock=lambda: now[0])
    w.handle(stage(R, P, P))
    (parent,) = api.top_level(CHANNEL)
    saved = store_for(variables).load(KEY)
    assert saved is not None and saved.parent_ref == parent.ts


def test_a_retrying_older_update_does_not_overwrite_the_final_status() -> None:
    """A's first edit fails with a 5xx; while it waits to retry, the final event B writes and
    reads back. A's retry checks the store first and gives up."""
    api, variables = FakeSlackApi(), FakeVariables()
    writer(api, variables).handle(stage(S, R, P))
    final = stage(S, S, S, finished=ProcessState.SUCCEEDED)
    b = writer(api, variables)
    results: list[HandleResult] = []

    def final_event_runs_meanwhile(seconds: float) -> None:
        results.append(b.handle(final, EventKind.RUN_FINISHED))

    transport = SlackTransport(api, clock=lambda: 0.0, sleep=final_event_runs_meanwhile)
    a = writer(api, variables, transport=transport)
    api.fail_next(TransportError("internal_error", retryable=True))
    assert a.handle(stage(S, S, R)) is HandleResult.YIELDED
    assert results == [HandleResult.SENT]
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == render(final).parent.text
    saved = store_for(variables).load(KEY)
    assert saved is not None and saved.parent_written == final.watermark


def test_an_event_that_loses_the_parent_still_sends_its_own_replies() -> None:
    """Review m-1: Transform's own callback (A) yields the parent to a newer view (B) written
    while A waited to retry; A's timed Transform reply must still go out while the run is
    unfinished, and B's Load reply is not repeated."""
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    setup = ProcessViewBuilder(key=KEY).section("Main").step("Extract", S, 10)
    setup.step("Transform", R).step("Load", R).step("Publish", P)
    writer(api, variables, theme=theme, context=THREAD_CTX).handle(setup.build())

    b = writer(api, variables, theme=theme, context=THREAD_CTX)
    newer = parallel_run(transform_timed=False, load_timed=True, later=1)

    def newer_event_meanwhile(seconds: float) -> None:
        b.handle(newer)

    transport = SlackTransport(api, clock=lambda: 0.0, sleep=newer_event_meanwhile)
    a = writer(api, variables, theme=theme, context=THREAD_CTX, transport=transport)
    api.fail_next(TransportError("internal_error", retryable=True))
    assert a.handle(parallel_run(transform_timed=True, load_timed=False)) is HandleResult.SENT

    (reply,) = thread_replies(api, "Transform")
    assert "4h 21m" in reply
    assert len(thread_replies(api, "Load")) == 1
    (parent,) = api.top_level(CHANNEL)
    assert parent.text == theme.render(newer, THREAD_CTX).parent.text  # B's newer parent stays


def test_an_event_that_loses_the_parent_after_the_run_finished_sends_nothing() -> None:
    api, variables = FakeSlackApi(), FakeVariables()
    theme = ThreadTheme()
    writer(api, variables, theme=theme, context=THREAD_CTX).handle(
        parallel_run(transform_timed=False, load_timed=False)
    )
    final = parallel_run(transform_timed=False, load_timed=False, finished=True)
    b = writer(api, variables, theme=theme, context=THREAD_CTX)

    def final_event_meanwhile(seconds: float) -> None:
        b.handle(final, EventKind.RUN_FINISHED)

    transport = SlackTransport(api, clock=lambda: 0.0, sleep=final_event_meanwhile)
    a = writer(api, variables, theme=theme, context=THREAD_CTX, transport=transport)
    api.fail_next(TransportError("internal_error", retryable=True))
    own = parallel_run(transform_timed=True, load_timed=False, later=1)
    assert a.handle(own) is HandleResult.YIELDED
    (reply,) = thread_replies(api, "Transform")
    assert "4h 21m" not in reply  # the final event's fallback; nothing older follows it
