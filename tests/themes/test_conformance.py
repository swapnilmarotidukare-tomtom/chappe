import random
from collections.abc import Callable
from typing import ClassVar

import pytest

from chappe.core.messages import MessageSet, ParentMessage, ThreadEntry
from chappe.core.render import RenderContext
from chappe.core.view import ProcessView
from chappe.themes import THEMES
from tests.support.conformance import ThemeConformance
from tests.support.samples import SAMPLES, default_context, dump_message_set


class TestPlain(ThemeConformance):
    theme = THEMES["plain"]


class TestLedger(ThemeConformance):
    theme = THEMES["ledger"]


class TestMetro(ThemeConformance):
    theme = THEMES["metro"]


@pytest.mark.parametrize("sample", sorted(SAMPLES))
@pytest.mark.parametrize("theme_name", sorted(THEMES))
def test_snapshot(
    theme_name: str, sample: str, chappe_snapshot: Callable[[str, str], None]
) -> None:
    messages = THEMES[theme_name]().render(SAMPLES[sample], default_context(theme_name))
    chappe_snapshot(f"{theme_name}__{sample}", dump_message_set(messages))


class BrokenTheme:
    """Breaks four rules on purpose: random text, section headers, no alert, untimed entries."""

    name: ClassVar[str] = "plain"  # reuses the plain tokens

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        headers = " ".join(section.title for section in view.sections)
        text = f"{ctx.label(view.state)} {headers} {random.random()}"
        finished = [step for step in view.steps if step.state.finished]
        entries = tuple(ThreadEntry(f"step:{step.key}", step.title) for step in finished)
        return MessageSet(ParentMessage(text), entries)


class BrokenConformance(ThemeConformance):  # no "Test" prefix: pytest does not collect it
    theme = BrokenTheme


@pytest.mark.parametrize(
    ("rule", "sample"),
    [
        ("test_render_is_deterministic", "single_running"),
        ("test_single_section_has_no_section_header", "single_running"),
        ("test_failed_process_alerts_the_configured_target", "single_failed"),
        ("test_untimed_steps_get_no_thread_entries", "single_step_finished"),
    ],
)
def test_the_suite_catches_a_broken_theme(rule: str, sample: str) -> None:
    with pytest.raises(AssertionError):
        getattr(BrokenConformance(), rule)(sample)
