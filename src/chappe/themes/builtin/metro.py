"""Default theme: a status line and the run drawn as a line of stations, in one message.

The thread holds only the failure alert. The stations sit in a code block so durations line up;
Slack shows no `:emoji:` inside code blocks, so the glyphs are plain Unicode, distinct by shape.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

from chappe.core.messages import Alert, MessageSet, ParentMessage
from chappe.core.model import ProcessState, StepState
from chappe.core.render import RenderContext, clip
from chappe.core.view import ProcessView, SectionView, StepView

LINE_WIDTH = 36  # fits a phone screen without wrapping
FENCE = "```"
TITLE_CHARS = 200  # a title alone never eats the parent limit
BRANCH, BRANCH_LAST = "┣━ ", "┗━ "
INDENT, INDENT_LAST = "┃   ", "    "
_GLYPHS = {
    StepState.SUCCEEDED: "●",
    StepState.RUNNING: "◉",
    StepState.FAILED: "✖",
    StepState.SKIPPED: "◌",
    StepState.PENDING: "○",
}


def _inline(text: str) -> str:
    """One line, no backticks: a name in the code block can't break a line or close the fence."""
    return " ".join(text.split()).replace("`", "'")


def _fit(text: str, room: int) -> str:
    room = max(room, 1)
    return text if len(text) <= room else text[: room - 1] + "…"


def _shown(steps: Sequence[StepView], cap: int | None) -> list[StepView | int]:
    """All steps, or the first cap-1 and the last with the count of hidden ones between."""
    if cap is None or len(steps) <= cap:
        return list(steps)
    return [*steps[: cap - 1], len(steps) - cap, steps[-1]]


class MetroTheme:
    name: ClassVar[str] = "metro"

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        return MessageSet(ParentMessage(self._parent(view, ctx)), (), self._alerts(view, ctx))

    # ---- parent ----

    def _parent(self, view: ProcessView, ctx: RenderContext) -> str:
        head = [ctx.fmt.bold(clip(view.title, TITLE_CHARS)), self._status(view, ctx)]
        links = " · ".join(ctx.fmt.link(link.url, link.label) for link in view.links)
        tail = [links] if links else []
        limit = ctx.limits.parent_chars
        longest = max((len(section.steps) for section in view.sections), default=0)
        for cap in [None, *range(longest - 1, 1, -1)]:
            text = "\n".join([*head, FENCE, *self._stations(view, ctx, cap), FENCE, *tail])
            if len(text) <= limit:
                return text
        # no block fits: never an unclosed fence, never a cut link
        links_line = "\n".join(tail)
        if len(links_line) >= limit:
            return clip("\n".join(head), limit)
        room = limit - len(links_line) - (1 if links_line else 0)
        return "\n".join([clip("\n".join(head), room), *tail])

    def _status(self, view: ProcessView, ctx: RenderContext) -> str:
        line = f"{ctx.icon(view.state)} {ctx.label(view.state)}"
        if view.started_at is not None:
            line += f" · {ctx.tokens.extra.get('started', 'started')} {ctx.clock(view.started_at)}"
        if view.duration is not None and view.state is not ProcessState.PENDING:
            line += f" · {ctx.duration(view.duration)}"
        return line

    def _stations(self, view: ProcessView, ctx: RenderContext, cap: int | None) -> list[str]:
        if not view.sections:
            return []
        trunk, *branches = view.sections
        lines = self._trunk(_shown(trunk.steps, cap), view, ctx)
        for index, section in enumerate(branches):
            last = index == len(branches) - 1
            lines.append(self._branch_head(section, ctx, last=last))
            indent = INDENT_LAST if last else INDENT
            for item in _shown(section.steps, cap):
                lines.append(indent + self._item(item, view, ctx, LINE_WIDTH - len(indent)))
        return lines

    def _trunk(
        self, items: list[StepView | int], view: ProcessView, ctx: RenderContext
    ) -> list[str]:
        extra = ctx.tokens.extra
        lines: list[str] = []
        for index, item in enumerate(items):
            previous = items[index - 1] if index else None
            if isinstance(previous, StepView) and isinstance(item, StepView):
                done = previous.state.finished
                lines.append(extra.get("line_done", "┃") if done else extra.get("line_todo", "┆"))
            lines.append(self._item(item, view, ctx, LINE_WIDTH))
        return lines

    def _branch_head(self, section: SectionView, ctx: RenderContext, *, last: bool) -> str:
        mark = BRANCH_LAST if last else BRANCH
        return mark + ctx.text(_fit(_inline(section.title), LINE_WIDTH - len(mark)))

    def _item(self, item: StepView | int, view: ProcessView, ctx: RenderContext, width: int) -> str:
        if isinstance(item, int):
            line_todo = ctx.tokens.extra.get("line_todo", "┆")
            return f"{line_todo}  … {item} {ctx.tokens.extra.get('more', 'more')}"
        return self._station(item, view, ctx, width)

    def _station(self, step: StepView, view: ProcessView, ctx: RenderContext, width: int) -> str:
        """`<glyph>  <name>` with the right text right-aligned to `width` visible characters.

        Widths are measured on the raw text, then escaped: Slack shows `&amp;` as one character.
        """
        glyph = ctx.tokens.extra.get(f"glyph_{step.state.value}", _GLYPHS[step.state])
        right = self._right(step, view, ctx)
        prefix = f"{glyph}  "
        room = width - len(prefix) - (len(right) + 1 if right else 0)
        name = _fit(_inline(step.title), room)
        if not right:
            return prefix + ctx.text(name)
        padding = " " * (width - len(prefix) - len(name) - len(right))
        return prefix + ctx.text(name) + padding + ctx.text(right)

    def _right(self, step: StepView, view: ProcessView, ctx: RenderContext) -> str:
        words = ctx.tokens.extra
        took = step.duration(view.now)
        timed = step.started_at is not None and step.ended_at is not None
        if step.state is StepState.SUCCEEDED:
            return ctx.duration(took) if timed else ""
        if step.state is StepState.RUNNING:
            running = words.get("running", "running")
            return f"{running} {ctx.duration(took)}" if took is not None else running
        if step.state is StepState.FAILED:
            if timed:
                return f"{words.get('failed_after', 'failed after')} {ctx.duration(took)}"
            return words.get("failed", "failed")
        if step.state is StepState.SKIPPED:
            return words.get("skipped", "skipped")
        return ""

    # ---- alerts ----

    def _alerts(self, view: ProcessView, ctx: RenderContext) -> tuple[Alert, ...]:
        if view.state is not ProcessState.FAILED:
            return ()
        mention = f"{ctx.fmt.mention(ctx.alert_mention)} " if ctx.alert_mention else ""
        icon = ctx.fmt.icon(ctx.tokens.extra.get("alert", ":rotating_light:"))
        failed = view.failed_steps
        details = (
            "; ".join(self._failure(step, view, ctx) for step in failed)
            if failed
            else ctx.label(ProcessState.FAILED)
        )
        text = f"{mention}{icon} {ctx.fmt.bold(view.title)} · {details}"
        logs = tuple(link for step in failed for link in step.links)
        if logs:
            text += " · " + " · ".join(ctx.fmt.link(link.url, link.label) for link in logs)
        return (Alert(key="alert:process:failed", text=clip(text, ctx.limits.entry_chars)),)

    def _failure(self, step: StepView, view: ProcessView, ctx: RenderContext) -> str:
        text = f"{ctx.text(step.title)} {ctx.text(self._right(step, view, ctx))}"
        if step.error:
            text += f": {ctx.text(step.error)}"
        return text
