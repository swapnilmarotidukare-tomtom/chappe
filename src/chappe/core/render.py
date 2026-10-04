# src/chappe/core/render.py
"""Everything a theme may use besides the run view."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, tzinfo
from typing import Protocol

from chappe.core.model import ProcessState, StepState
from chappe.core.view import StepView


class Formatter(Protocol):
    """Supplied by the transport. Every method takes raw text and escapes it."""

    def escape(self, text: str) -> str: ...
    def bold(self, text: str) -> str: ...
    def italic(self, text: str) -> str: ...
    def link(self, url: str, label: str) -> str: ...
    def mention(self, target: str) -> str: ...
    def icon(self, token: str) -> str: ...


@dataclass(frozen=True, slots=True)
class Tokens:
    icons: Mapping[str, str]
    labels: Mapping[str, str]
    extra: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Limits:
    parent_chars: int = 3000
    entry_chars: int = 1500


def format_duration(value: timedelta) -> str:
    seconds = max(int(value.total_seconds()), 0)
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


@dataclass(frozen=True, slots=True)
class RenderContext:
    tokens: Tokens
    fmt: Formatter
    tz: tzinfo
    alert_mention: str | None = None
    limits: Limits = Limits()
    collapse_done_sections: bool = False  # theme option (ledger)

    def icon(self, state: StepState | ProcessState) -> str:
        return self.fmt.icon(self.tokens.icons[state.value])

    def label(self, state: StepState | ProcessState) -> str:
        return self.tokens.labels[state.value]

    def duration(self, value: timedelta | None) -> str:
        return "" if value is None else format_duration(value)

    def clock(self, value: datetime | None) -> str:
        return "" if value is None else value.astimezone(self.tz).strftime("%H:%M")

    def text(self, value: str) -> str:
        return self.fmt.escape(value)


def step_counts(steps: Sequence[StepView], ctx: RenderContext) -> str:
    """A failed run's count line: "3 passed · 1 failed", then skipped and not run when non-zero."""
    states = [step.state for step in steps]
    passed = states.count(StepState.SUCCEEDED)
    failed = states.count(StepState.FAILED)
    skipped = states.count(StepState.SKIPPED)
    not_run = len(states) - passed - failed - skipped
    words = ctx.tokens.extra
    parts = [
        f"{passed} {words.get('passed', 'passed')}",
        f"{failed} {words.get('failed', 'failed')}",
    ]
    if skipped:
        parts.append(f"{skipped} {words.get('skipped', 'skipped')}")
    if not_run:
        parts.append(f"{not_run} {words.get('not_run', 'not run')}")
    return " · ".join(parts)
