"""Minimal theme: one status line, the result in the thread, an alert on failure.

Also the engine's fallback. It emits no step entries, so the thread-entry rule always holds.
"""

from __future__ import annotations

from typing import ClassVar

from chappe.core.messages import Alert, MessageSet, ParentMessage, ThreadEntry
from chappe.core.model import ProcessState
from chappe.core.render import RenderContext, clip
from chappe.core.view import ProcessView


class PlainTheme:
    name: ClassVar[str] = "plain"

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        done, total = view.progress
        status = f"{ctx.icon(view.state)} {ctx.label(view.state)}"
        steps_word = ctx.tokens.extra.get("steps", "steps")
        parent = f"{ctx.fmt.bold(view.title)} · {status} · {done}/{total} {steps_word}"
        if view.links:
            parent += " · " + " · ".join(ctx.fmt.link(link.url, link.label) for link in view.links)
        fallback = f"{view.title}: {ctx.label(view.state)} ({done}/{total})"
        return MessageSet(
            ParentMessage(clip(parent, ctx.limits.parent_chars), fallback),
            self._result(view, ctx),
            self._alerts(view, ctx),
        )

    def _result(self, view: ProcessView, ctx: RenderContext) -> tuple[ThreadEntry, ...]:
        if not view.state.finished:
            return ()
        text = f"{ctx.icon(view.state)} {ctx.label(view.state)} · {ctx.fmt.bold(view.title)}"
        if view.duration is not None:
            text += f" · {ctx.duration(view.duration)}"
        key = f"process:{view.state.value}"
        return (ThreadEntry(key=key, text=clip(text, ctx.limits.entry_chars), broadcast=True),)

    def _alerts(self, view: ProcessView, ctx: RenderContext) -> tuple[Alert, ...]:
        if view.state is not ProcessState.FAILED:
            return ()
        names = ", ".join(ctx.text(step.title) for step in view.failed_steps)
        mention = f"{ctx.fmt.mention(ctx.alert_mention)} " if ctx.alert_mention else ""
        text = f"{mention}{ctx.icon(view.state)} {ctx.fmt.bold(view.title)}: "
        text += names or ctx.label(view.state)
        return (Alert(key="alert:process:failed", text=clip(text, ctx.limits.entry_chars)),)
