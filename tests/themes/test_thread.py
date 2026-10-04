from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta, timezone

import pytest

from chappe.core.model import ProcessState, StepState
from chappe.core.view import ProcessView
from chappe.themes import THEMES
from chappe.themes.builtin.plain import PlainTheme
from chappe.themes.builtin.thread import ThreadTheme
from tests.support.samples import SAMPLES, ProcessViewBuilder, default_context, dump_message_set


def render_parent(view: ProcessView) -> str:
    return ThreadTheme().render(view, default_context("thread")).parent.text


def without_times(view: ProcessView, *titles: str) -> ProcessView:
    """Option A: only the triggering step carries times. Drop them from the named steps."""
    sections = tuple(
        replace(
            section,
            steps=tuple(
                replace(step, started_at=None, ended_at=None) if step.title in titles else step
                for step in section.steps
            ),
        )
        for section in view.sections
    )
    return replace(view, sections=sections)


def entry_key(view: ProcessView, title: str) -> str:
    step = next(s for s in view.steps if s.title == title)
    return f"step:{step.key}:{step.state.value}"


def three_steps(last: StepState) -> ProcessViewBuilder:
    return (
        ProcessViewBuilder()
        .section("Main")
        .step("Parquet → Delta", StepState.SUCCEEDED, 78)
        .step("Geometry", StepState.SUCCEEDED, 261)
        .step("Aggregates", last, 10)
    )


def test_registered_in_themes() -> None:
    assert {"thread": ThreadTheme, "plain": PlainTheme} == THEMES
    assert ThreadTheme.name == "thread"


def test_running_step_is_named_in_the_now_line() -> None:
    assert "Now: Geometry" in render_parent(SAMPLES["single_running"])


def test_running_step_without_times_has_no_empty_duration() -> None:
    text = render_parent(without_times(three_steps(StepState.RUNNING).build(), "Aggregates"))
    assert "Now: Aggregates" in text
    assert "Aggregates (" not in text


def test_multi_section_rows_name_each_section() -> None:
    text = render_parent(SAMPLES["multi_running"])
    assert "*Prepare*" in text and "*Regression vs baseline 2026.10.0*" in text


def test_finished_steps_become_thread_entries_and_the_result_is_broadcast() -> None:
    messages = ThreadTheme().render(SAMPLES["multi_passed"], default_context("thread"))
    assert len([e for e in messages.thread if e.key.startswith("step:")]) == 5
    final = messages.thread[-1]
    assert final.key == "process:succeeded" and final.broadcast


def test_finished_step_without_its_times_waits_while_the_process_runs() -> None:
    view = without_times(three_steps(StepState.RUNNING).build(), "Parquet → Delta")
    messages = ThreadTheme().render(view, default_context("thread"))
    assert [e.key for e in messages.thread] == [entry_key(view, "Geometry")]
    assert "4h 21m" in messages.thread[0].text
    assert entry_key(view, "Parquet → Delta") not in {e.key for e in messages.thread}


def test_finished_step_without_its_times_gets_an_entry_once_the_process_finished() -> None:
    running = without_times(three_steps(StepState.RUNNING).build(), "Parquet → Delta")
    finished = three_steps(StepState.SUCCEEDED).finished(ProcessState.SUCCEEDED).build()
    done = without_times(finished, "Parquet → Delta")
    key = entry_key(done, "Parquet → Delta")
    assert key == entry_key(running, "Parquet → Delta")
    entries = {e.key: e for e in ThreadTheme().render(done, default_context("thread")).thread}
    assert key in entries
    assert "*Parquet → Delta* · Passed" in entries[key].text
    assert "1h 18m" not in entries[key].text
    assert "process:succeeded" in entries


# Owner's additions: per-state counts on failed runs, and the run's start time in the header.


def first_row(view: ProcessView) -> str:
    return render_parent(view).split("\n")[1]


def test_failed_run_counts_steps_per_state_instead_of_done_over_total() -> None:
    assert first_row(SAMPLES["single_failed"]).endswith("  1 passed · 1 failed · 1 not run")
    view = (
        ProcessViewBuilder()
        .section("Main")
        .step("Extract", StepState.SUCCEEDED, timed=False)
        .step("Backfill", StepState.SKIPPED, timed=False)
        .step("Transform", StepState.SUCCEEDED, timed=False)
        .step("Load", StepState.FAILED, timed=False)
        .finished(ProcessState.FAILED)
        .build()
    )
    row = first_row(view)
    assert row.endswith("  2 passed · 1 failed · 1 skipped")
    assert "steps" not in row


@pytest.mark.parametrize("name", ["single_running", "single_passed", "with_skipped"])
def test_runs_that_did_not_fail_keep_done_over_total(name: str) -> None:
    done, total = SAMPLES[name].progress
    assert first_row(SAMPLES[name]).endswith(f"  {done}/{total} steps")


def test_failed_multi_section_rows_keep_their_section_detail() -> None:
    text = render_parent(SAMPLES["multi_failed"])
    assert "*Regression vs baseline 2026.10.0* · Failed: Regression" in text
    assert "passed" not in text


def test_header_shows_the_start_time_in_the_configured_timezone() -> None:
    view = SAMPLES["single_running"]
    assert render_parent(view).split("\n")[0].endswith(" · started 08:05")
    ctx = replace(default_context("thread"), tz=timezone(timedelta(hours=2)))
    head = ThreadTheme().render(view, ctx).parent.text.split("\n")[0]
    assert head.endswith(" · started 10:05")


def test_header_omits_the_start_time_when_it_is_unknown() -> None:
    view = minimal_dag_callback()
    assert view.started_at is None
    assert "started" not in render_parent(view)


def minimal_dag_callback() -> ProcessView:
    """The minimal DAG-callback context: no run start date, so no start time and no duration."""
    return (
        ProcessViewBuilder(known_start=False)
        .section("Main")
        .step("Parquet → Delta", StepState.SUCCEEDED, 78, timed=False)
        .step("Geometry", StepState.SUCCEEDED, 261, timed=False)
        .finished(ProcessState.SUCCEEDED)
        .build()
    )


SNAPSHOTS: dict[str, Callable[[], ProcessView]] = {
    "minimal_dag_callback": minimal_dag_callback,
}


@pytest.mark.parametrize("name", sorted(SNAPSHOTS))
def test_snapshot(name: str, chappe_snapshot: Callable[[str, str], None]) -> None:
    messages = ThreadTheme().render(SNAPSHOTS[name](), default_context("thread"))
    chappe_snapshot(f"thread__{name}", dump_message_set(messages))
