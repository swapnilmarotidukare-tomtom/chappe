# tests/core/test_render.py
from datetime import datetime, timedelta, timezone

from chappe.core.model import ProcessState, StepState
from chappe.core.render import Limits, RenderContext, Tokens, clip, format_duration


class EchoFormatter:
    def escape(self, text: str) -> str:
        return text.replace("<", "&lt;")

    def bold(self, text: str) -> str:
        return f"*{self.escape(text)}*"

    def italic(self, text: str) -> str:
        return f"_{self.escape(text)}_"

    def link(self, url: str, label: str) -> str:
        return f"<{url}|{self.escape(label)}>"

    def mention(self, target: str) -> str:
        return target

    def icon(self, token: str) -> str:
        return token


TOKENS = Tokens(
    icons={s: f":{s}:" for s in ("pending", "running", "succeeded", "failed", "skipped")},
    labels={s: s.title() for s in ("pending", "running", "succeeded", "failed", "skipped")},
    extra={},
)


def test_format_duration() -> None:
    assert format_duration(timedelta(seconds=45)) == "45s"
    assert format_duration(timedelta(minutes=10)) == "10m"
    assert format_duration(timedelta(minutes=65)) == "1h 05m"
    assert format_duration(timedelta(minutes=261)) == "4h 21m"
    assert format_duration(timedelta(seconds=-5)) == "0s"


def test_clip_keeps_short_text_and_marks_cut_text() -> None:
    assert clip("abc", 5) == "abc"
    assert clip("abcdef", 4) == "abc…"


def test_render_context_helpers() -> None:
    ctx = RenderContext(tokens=TOKENS, fmt=EchoFormatter(), tz=timezone.utc, limits=Limits())
    assert ctx.icon(StepState.RUNNING) == ":running:"
    assert ctx.label(ProcessState.FAILED) == "Failed"
    assert ctx.duration(None) == ""
    assert ctx.clock(datetime(2026, 10, 2, 13, 35, tzinfo=timezone.utc)) == "13:35"
    assert ctx.text("<x>") == "&lt;x>"
