"""Theme rules (spec 6.4 and the thread-entry rule). Subclass in a test module and set `theme`."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, ClassVar

import pytest

from chappe.core.messages import MessageSet
from chappe.core.model import ProcessState, StepState
from chappe.core.render import RenderContext
from chappe.core.view import ProcessView, StepView
from chappe.ports.theme import Theme
from tests.support.samples import FAILED_SAMPLES, SAMPLES, SINGLE_SECTION_SAMPLES, default_context

ALL_SAMPLES: tuple[str, ...] = tuple(sorted(SAMPLES))
UNFINISHED_SAMPLES: tuple[str, ...] = tuple(
    name for name in ALL_SAMPLES if not SAMPLES[name].state.finished
)
PENDING_SAMPLES: tuple[str, ...] = tuple(
    name for name in ALL_SAMPLES if SAMPLES[name].state is ProcessState.PENDING
)


def _strip(step: StepView, *, pending: bool) -> StepView:
    if pending:
        return dataclasses.replace(
            step, state=StepState.PENDING, started_at=None, ended_at=None, error=None
        )
    return dataclasses.replace(step, started_at=None, ended_at=None)


def without_step_times(view: ProcessView, *, pending: bool = False) -> ProcessView:
    """The view with no step times; with `pending`, every step is also back to pending."""
    sections = tuple(
        dataclasses.replace(
            section, steps=tuple(_strip(step, pending=pending) for step in section.steps)
        )
        for section in view.sections
    )
    return dataclasses.replace(view, sections=sections)


def _keys(messages: MessageSet) -> list[str]:
    return [e.key for e in messages.thread] + [a.key for a in messages.alerts]


class ThemeConformance:
    theme: ClassVar[type[Theme]]
    tokens: ClassVar[Mapping[str, Any] | None] = None

    def context(self) -> RenderContext:
        return default_context(self.theme.name, self.tokens)

    def render_view(self, view: ProcessView) -> MessageSet:
        result = self.theme().render(view, self.context())
        assert isinstance(result, MessageSet), "render() must return a MessageSet"
        return result

    def render(self, sample: str, *, later: timedelta = timedelta(0)) -> MessageSet:
        view = SAMPLES[sample]
        return self.render_view(dataclasses.replace(view, now=view.now + later))

    @pytest.mark.parametrize("sample", ALL_SAMPLES)
    def test_render_is_deterministic(self, sample: str) -> None:
        assert self.render(sample) == self.render(sample)

    @pytest.mark.parametrize("sample", ALL_SAMPLES)
    def test_process_state_is_stated_in_words(self, sample: str) -> None:
        label = self.context().label(SAMPLES[sample].state)
        assert label in self.render(sample).parent.text

    @pytest.mark.parametrize("sample", ALL_SAMPLES)
    def test_messages_stay_within_limits(self, sample: str) -> None:
        limits = self.context().limits
        messages = self.render(sample)
        assert len(messages.parent.text) <= limits.parent_chars
        for item in (*messages.thread, *messages.alerts):
            assert len(item.text) <= limits.entry_chars

    @pytest.mark.parametrize("sample", ALL_SAMPLES)
    def test_keys_are_unique(self, sample: str) -> None:
        keys = _keys(self.render(sample))
        assert len(keys) == len(set(keys))

    @pytest.mark.parametrize("sample", ALL_SAMPLES)
    def test_keys_do_not_depend_on_the_clock(self, sample: str) -> None:
        assert _keys(self.render(sample)) == _keys(self.render(sample, later=timedelta(hours=1)))

    @pytest.mark.parametrize("sample", FAILED_SAMPLES)
    def test_failed_process_alerts_the_configured_target(self, sample: str) -> None:
        alerts = self.render(sample).alerts
        assert alerts, "a failed process must produce at least one alert"
        mention = self.context().alert_mention
        assert mention is not None and any(mention in a.text for a in alerts)

    @pytest.mark.parametrize("sample", SINGLE_SECTION_SAMPLES)
    def test_single_section_has_no_section_header(self, sample: str) -> None:
        title = SAMPLES[sample].sections[0].title
        assert title not in self.render(sample).parent.text

    @pytest.mark.parametrize("sample", PENDING_SAMPLES)
    def test_pending_process_shows_no_duration(self, sample: str) -> None:
        view = SAMPLES[sample]
        assert view.duration is not None
        for later in (timedelta(0), timedelta(hours=3)):
            messages = self.render(sample, later=later)
            took = self.context().duration(view.duration + later)
            assert took not in messages.parent.text, "a run with no step started has no duration"
        assert self.render(sample).parent == self.render(sample, later=timedelta(hours=3)).parent

    @pytest.mark.parametrize("sample", UNFINISHED_SAMPLES)
    def test_untimed_steps_get_no_thread_entries(self, sample: str) -> None:
        view = SAMPLES[sample]
        untimed = self.render_view(without_step_times(view))
        blank = self.render_view(without_step_times(view, pending=True))
        assert [e.key for e in untimed.thread] == [e.key for e in blank.thread], (
            "before the run finishes, a step without its own times must not get a thread entry"
        )

    def test_dag_supplied_text_is_escaped(self) -> None:
        messages = self.render("unicode_long")
        everything = "\n".join([messages.parent.text, *(e.text for e in messages.thread)])
        assert "<orders>" not in everything
        assert "<aggregates>" not in everything
