from dataclasses import replace
from datetime import timedelta

import pytest

from chappe.core.messages import MessageSet
from chappe.core.model import ProcessState, StepState
from chappe.themes import THEMES
from chappe.themes.builtin.plain import PlainTheme
from tests.support.samples import (
    FAILED_SAMPLES,
    SAMPLES,
    SINGLE_SECTION_SAMPLES,
    TEST_MENTION,
    ProcessViewBuilder,
    default_context,
)

ALL = sorted(SAMPLES)


def render(name: str, later: timedelta = timedelta(0)) -> MessageSet:
    view = SAMPLES[name]
    return PlainTheme().render(replace(view, now=view.now + later), default_context("plain"))


def keys(messages: MessageSet) -> list[str]:
    return [e.key for e in messages.thread] + [a.key for a in messages.alerts]


def test_registered_as_plain() -> None:
    assert THEMES["plain"] is PlainTheme
    assert PlainTheme.name == "plain"


@pytest.mark.parametrize("name", ALL)
def test_every_sample_renders_and_states_the_process_state_in_words(name: str) -> None:
    messages = render(name)
    assert default_context("plain").label(SAMPLES[name].state) in messages.parent.text


@pytest.mark.parametrize("name", ALL)
def test_render_is_deterministic_and_keys_ignore_the_clock(name: str) -> None:
    assert render(name) == render(name)
    assert keys(render(name)) == keys(render(name, later=timedelta(hours=1)))
    assert len(keys(render(name))) == len(set(keys(render(name))))


@pytest.mark.parametrize("name", FAILED_SAMPLES)
def test_failed_process_alerts_the_configured_mention(name: str) -> None:
    alerts = render(name).alerts
    assert [a.key for a in alerts] == ["alert:process:failed"]
    assert alerts[0].text.startswith(TEST_MENTION)


@pytest.mark.parametrize("name", SINGLE_SECTION_SAMPLES)
def test_single_section_has_no_section_header(name: str) -> None:
    assert SAMPLES[name].sections[0].title not in render(name).parent.text


def test_only_a_finished_run_gets_a_thread_entry() -> None:
    assert render("single_step_finished").thread == ()
    (entry,) = render("single_passed").thread
    assert entry.key == "process:succeeded" and entry.broadcast


def test_failed_alert_names_the_failed_step() -> None:
    assert "Geometry" in render("single_failed").alerts[0].text


def test_dag_supplied_text_is_escaped() -> None:
    text = render("unicode_long").parent.text
    assert "&lt;orders&gt;" in text and "<orders>" not in text


# Owner's Phase 2 decision: failed runs count steps per state.


def status_line(name: str) -> str:
    return render(name).parent.text


def test_failed_run_counts_steps_per_state_instead_of_done_over_total() -> None:
    assert " · :x: Failed · 1 passed · 1 failed · 1 not run · " in status_line("single_failed")
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
    text = PlainTheme().render(view, default_context("plain")).parent.text
    assert " · :x: Failed · 2 passed · 1 failed · 1 skipped · " in text
    assert "steps" not in text


def test_failed_multi_section_run_counts_the_whole_run() -> None:
    text = status_line("multi_failed")
    assert " · :x: Failed · 4 passed · 1 failed · " in text
    assert "steps" not in text


@pytest.mark.parametrize("name", ["single_running", "single_passed", "with_skipped", "pending"])
def test_runs_that_did_not_fail_keep_done_over_total(name: str) -> None:
    done, total = SAMPLES[name].progress
    assert f" · {done}/{total} steps" in status_line(name)
