"""Default theme: a status header that links to the run, one line per step, and each step's start
and end in the thread.

A failed run lists only the sections that went wrong. Thread replies carry no clock times and no
icons: Slack shows each reply's own time, and the reply says the step's status in words.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import ClassVar

from chappe.core.messages import Alert, MessageSet, ParentMessage, ThreadEntry
from chappe.core.model import ProcessState, StepState
from chappe.core.render import RenderContext, clip, step_counts
from chappe.core.view import ProcessView, SectionView, StepView

TITLE_CHARS = 200  # a title alone never eats the parent limit or cuts its link


def _timed(step: StepView) -> bool:
    return step.started_at is not None and step.ended_at is not None


@dataclass(frozen=True, slots=True)
class _Block:
    """A section as rendered: its heading line (or None) and its step lines."""

    heading: str | None
    steps: tuple[tuple[str, bool], ...]  # (line, must be kept: failed or running)


class LedgerTheme:
    name: ClassVar[str] = "ledger"

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        parent = ParentMessage(self._parent(view, ctx))
        return MessageSet(parent, self._thread(view, ctx), self._alerts(view, ctx))

    # ---- parent ----

    def _parent(self, view: ProcessView, ctx: RenderContext) -> str:
        header = self._header(view, ctx)
        blocks = self._blocks(view, ctx)
        footer = self._footer(view, ctx)
        limit = ctx.limits.parent_chars
        longest = max((len(block.steps) for block in blocks), default=0)
        # Too long for Slack: keep the steps that matter (failed, running), then as many others
        # as fit, and say how many were left out. Never cut a line or a link in half.
        for cap in [None, *range(longest - 1, -1, -1)]:
            text = "\n".join([header, *self._lines(blocks, cap, ctx), *footer])
            if len(text) <= limit:
                return text
        return clip(header, limit)

    @staticmethod
    def _footer(view: ProcessView, ctx: RenderContext) -> list[str]:
        """A blank line, then when Chappe last edited the message, always in UTC.

        Left out while nothing has started: a waiting run's message must not change over time.
        """
        if view.state is ProcessState.PENDING:
            return []
        words = ctx.tokens.extra
        icon = ctx.fmt.icon(words.get("updated_icon", ""))
        at = view.now.astimezone(timezone.utc).strftime("%H:%M")
        line = f"{words.get('last_updated', 'Last updated')} {at} UTC"
        return ["", f"{icon} {line}" if icon else line]

    def _lines(self, blocks: list[_Block], cap: int | None, ctx: RenderContext) -> list[str]:
        more = ctx.tokens.extra.get("more", "more")
        lines: list[str] = []
        for block in blocks:
            if block.heading is not None:
                lines.append(block.heading)
            kept, hidden = 0, 0
            for line, important in block.steps:
                if important or cap is None or kept < cap:
                    if hidden:
                        lines.append(f"… {hidden} {more}")
                        hidden = 0
                    lines.append(line)
                    kept += 0 if important else 1
                else:
                    hidden += 1
            if hidden:
                lines.append(f"… {hidden} {more}")
        return lines

    def _blocks(self, view: ProcessView, ctx: RenderContext) -> list[_Block]:
        multi = len(view.sections) > 1
        blocks: list[_Block] = []
        for section in view.sections:
            steps = tuple(
                (
                    self._step(step, view, ctx),
                    step.state in (StepState.FAILED, StepState.RUNNING),
                )
                for step in section.steps
            )
            if view.state is ProcessState.FAILED:
                if any(s.state is StepState.FAILED for s in section.steps):
                    counts = step_counts(section.steps, ctx)
                    heading = f"{ctx.fmt.bold(section.title)} · {counts}" if multi else counts
                    blocks.append(_Block(heading, steps))
                continue
            if not multi:
                blocks.append(_Block(None, steps))
            elif ctx.collapse_done_sections and self._done(section):
                blocks.append(_Block(self._section_line(section, ctx), ()))
            else:
                blocks.append(_Block(self._section_line(section, ctx), steps))
        return blocks

    def _header(self, view: ProcessView, ctx: RenderContext) -> str:
        words = ctx.tokens.extra
        mark = ctx.fmt.icon(words.get(f"header_{view.state.value}", ""))
        title_text = clip(view.title, TITLE_CHARS)
        link = view.links[0] if view.links else None
        title = ctx.fmt.link(link.url, title_text) if link else ctx.fmt.escape(title_text)
        took = ctx.duration(view.duration) if view.duration is not None else ""
        label = ctx.label(view.state)
        if view.state is ProcessState.SUCCEEDED and took:
            phrase = f"{label} {words.get('in', 'in')} {took}"
        elif view.state is ProcessState.FAILED and took:
            phrase = f"{label} {words.get('after', 'after')} {took}"
        elif view.state is ProcessState.RUNNING and took:
            phrase = f"{label} · {took}"
        else:
            phrase = label
        line = f"*{title}* · {phrase}"
        if view.started_at is not None:
            line += f" · {words.get('started', 'started')} {ctx.clock(view.started_at)}"
        return f"{mark} {line}" if mark else line

    def _section_line(self, section: SectionView, ctx: RenderContext) -> str:
        words = ctx.tokens.extra
        done = sum(1 for s in section.steps if s.state.finished)
        line = (
            f"{ctx.fmt.bold(section.title)} · {done} {words.get('of', 'of')} "
            f"{len(section.steps)} {words.get('done', 'done')}"
        )
        timed = bool(section.steps) and all(_timed(s) for s in section.steps)
        if ctx.collapse_done_sections and self._done(section) and timed:
            starts = [s.started_at for s in section.steps if s.started_at is not None]
            ends = [s.ended_at for s in section.steps if s.ended_at is not None]
            line += f" · {ctx.duration(max(ends) - min(starts))}"
        return line

    @staticmethod
    def _done(section: SectionView) -> bool:
        return bool(section.steps) and all(
            s.state.finished and s.state is not StepState.FAILED for s in section.steps
        )

    def _step(self, step: StepView, view: ProcessView, ctx: RenderContext) -> str:
        line = f"{ctx.icon(step.state)} {ctx.text(step.title)}"
        words = ctx.tokens.extra
        if step.state is StepState.SUCCEEDED and _timed(step):
            line += f" · {ctx.duration(step.duration(view.now))}"
        elif step.state is StepState.RUNNING:
            if step.started_at is not None:
                since = words.get("running_since", "running since")
                line += f" · {since} {ctx.clock(step.started_at)}"
            else:
                line += f" · {words.get('running', 'running')}"
        elif step.state in (StepState.FAILED, StepState.SKIPPED):
            line += f" · {ctx.label(step.state)}"
        return line

    # ---- thread ----

    def _thread(self, view: ProcessView, ctx: RenderContext) -> tuple[ThreadEntry, ...]:
        """Spec 6.4 rule 7: a reply only from a view with the step's own times, or, for end
        replies without times, once the run has finished. Skipped steps wait for the run's end."""
        entries: list[tuple[datetime, int, int, ThreadEntry]] = []
        started = ctx.tokens.extra.get("reply_started", "STARTED")
        for index, step in enumerate(view.steps):
            if step.started_at is not None:
                text = f"{started}: {ctx.fmt.bold(step.title)}"
                entry = ThreadEntry(f"step:{step.key}:started", clip(text, ctx.limits.entry_chars))
                entries.append((step.started_at, index, 0, entry))
            own_end = step.ended_at is not None and step.state is not StepState.SKIPPED
            if step.state.finished and (own_end or view.state.finished):
                entry = ThreadEntry(
                    f"step:{step.key}:{step.state.value}",
                    clip(self._end(step, view, ctx), ctx.limits.entry_chars),
                )
                entries.append((step.ended_at or view.now, index, 1, entry))
        # equal times: step order, and a step's start before its end
        entries.sort(key=lambda item: (item[0], item[1], item[2]))
        return tuple(entry for _, _, _, entry in entries)

    def _end(self, step: StepView, view: ProcessView, ctx: RenderContext) -> str:
        # the status leads, in capitals, so it reads at a glance whatever the step name
        text = f"{ctx.label(step.state).upper()}: {ctx.fmt.bold(step.title)}"
        if step.state is not StepState.SKIPPED and _timed(step):
            text += f" · {ctx.duration(step.duration(view.now))}"
        if step.error:
            text += f"\n{ctx.text(step.error)}"
        return text

    # ---- alerts ----

    def _alerts(self, view: ProcessView, ctx: RenderContext) -> tuple[Alert, ...]:
        if view.state is not ProcessState.FAILED:
            return ()
        mention = ctx.fmt.mention(ctx.alert_mention) if ctx.alert_mention else ""
        mark = ctx.fmt.icon(ctx.tokens.extra.get("header_failed", ""))
        head = " ".join(part for part in (mention, mark, ctx.fmt.bold(view.title)) if part)
        failed = view.failed_steps
        if not failed:
            text = f"{head} · {ctx.label(ProcessState.FAILED)}"
            return (Alert(key="alert:process:failed", text=clip(text, ctx.limits.entry_chars)),)
        # Name as many failed steps as fit, each with its error and log link, whole.
        limit = ctx.limits.entry_chars
        named: list[StepView] = []
        for step in failed:
            candidate = self._alert_text(head, [*named, step], len(failed), ctx)
            if named and len(candidate) > limit:
                break
            named.append(step)
        return (
            Alert(
                key="alert:process:failed",
                text=clip(self._alert_text(head, named, len(failed), ctx), limit),
            ),
        )

    def _alert_text(self, head: str, steps: list[StepView], total: int, ctx: RenderContext) -> str:
        details = "; ".join(
            ctx.text(s.title) + (f": {ctx.text(s.error)}" if s.error else "") for s in steps
        )
        if total > len(steps):
            details += f"; … {total - len(steps)} {ctx.tokens.extra.get('more', 'more')}"
        text = f"{head} · {details}"
        logs = [link for step in steps for link in step.links]
        if logs:
            text += " · " + " · ".join(ctx.fmt.link(link.url, link.label) for link in logs)
        return text
