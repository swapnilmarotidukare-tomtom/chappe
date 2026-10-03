from datetime import timedelta
from pathlib import Path

import pytest

from chappe.core.messages import Alert, MessageSet, ParentMessage, ThreadEntry
from chappe.core.model import ProcessState, StepState
from chappe.core.view import Watermark
from tests.support.samples import (
    BASE,
    FAILED_SAMPLES,
    SAMPLES,
    SINGLE_SECTION_SAMPLES,
    TEST_MENTION,
    ProcessViewBuilder,
    default_context,
    dump_message_set,
)
from tests.support.snapshots import check_snapshot


def test_timed_steps_follow_each_other() -> None:
    view = (
        ProcessViewBuilder()
        .section("Main")
        .step("A", StepState.SUCCEEDED, minutes=10)
        .step("B", StepState.RUNNING)
        .step("C", StepState.PENDING)
        .build()
    )
    a, b, c = view.steps
    assert a.started_at == BASE == view.started_at
    assert a.ended_at == b.started_at == BASE + timedelta(minutes=10)
    assert b.ended_at is None and c.started_at is None
    assert view.state is ProcessState.RUNNING
    assert view.now == BASE + timedelta(minutes=15)


def test_untimed_steps_carry_no_times_but_still_move_the_clock() -> None:
    view = (
        ProcessViewBuilder()
        .section("Main")
        .step("A", StepState.SUCCEEDED, minutes=10, timed=False)
        .step("B", StepState.RUNNING)
        .build()
    )
    a, b = view.steps
    assert a.started_at is None and a.ended_at is None
    assert b.started_at == BASE + timedelta(minutes=10)


def test_step_needs_a_section() -> None:
    with pytest.raises(ValueError, match="section"):
        ProcessViewBuilder().step("A", StepState.PENDING)


def test_finished_builder_sets_the_run_end() -> None:
    builder = ProcessViewBuilder().section("Main").step("A", StepState.FAILED, 10)
    view = builder.finished(ProcessState.FAILED).build()
    assert view.state is ProcessState.FAILED
    assert view.ended_at == view.now == BASE + timedelta(minutes=15)


def test_samples_cover_every_state() -> None:
    assert {step.state for view in SAMPLES.values() for step in view.steps} == set(StepState)
    assert {view.state for view in SAMPLES.values()} == set(ProcessState)


def test_samples_mix_timed_and_untimed_steps() -> None:
    finished = [step for view in SAMPLES.values() for step in view.steps if step.state.finished]
    assert any(step.ended_at is not None for step in finished)
    assert any(step.ended_at is None for step in finished)
    assert all(view.started_at is not None for view in SAMPLES.values())


def test_finished_samples_carry_no_step_times() -> None:
    for view in SAMPLES.values():
        if view.state.finished:
            assert all(step.started_at is None and step.ended_at is None for step in view.steps)


def test_sample_groups() -> None:
    assert set(FAILED_SAMPLES) == {"single_failed", "multi_failed"}
    assert "single_running" in SINGLE_SECTION_SAMPLES
    assert "multi_running" not in SINGLE_SECTION_SAMPLES
    assert all(SAMPLES[name].sections[0].title == "Main" for name in SINGLE_SECTION_SAMPLES)


def test_sample_watermark_uses_progress_and_now() -> None:
    view = SAMPLES["single_step_finished"]
    assert view.watermark == Watermark(
        finished=False, settled_steps=2, started_steps=2, occurred_at=view.now
    )
    assert SAMPLES["single_passed"].watermark.newer_than(view.watermark)


def test_default_context() -> None:
    ctx = default_context("plain")
    assert ctx.alert_mention == TEST_MENTION
    assert ctx.label(ProcessState.RUNNING) == "In progress"
    assert ctx.text("<x>") == "&lt;x&gt;"


def test_dump_message_set_lists_every_part() -> None:
    messages = MessageSet(
        ParentMessage("p", "f"),
        (ThreadEntry("process:failed", "t", broadcast=True),),
        (Alert("alert:x", "a"),),
    )
    assert dump_message_set(messages) == (
        "=== parent ===\np\n=== fallback ===\nf\n"
        "=== thread ===\n[process:failed] (broadcast) t\n"
        "=== alerts ===\n[alert:x] a\n"
    )


def test_snapshot_is_written_on_update_then_compared(tmp_path: Path) -> None:
    folder = tmp_path / "__snapshots__"
    check_snapshot(folder, "a", "one\n", update=True)
    assert (folder / "a.txt").read_text(encoding="utf-8") == "one\n"
    check_snapshot(folder, "a", "one\n", update=False)
    with pytest.raises(AssertionError, match="snapshot a changed"):
        check_snapshot(folder, "a", "two\n", update=False)


def test_missing_snapshot_fails_without_update(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="--chappe-update-snapshots"):
        check_snapshot(tmp_path, "b", "x\n", update=False)
    assert not (tmp_path / "b.txt").exists()


def test_update_option_is_registered(pytestconfig: pytest.Config) -> None:
    assert isinstance(pytestconfig.getoption("--chappe-update-snapshots"), bool)
