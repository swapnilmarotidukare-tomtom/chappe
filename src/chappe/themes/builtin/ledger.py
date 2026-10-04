"""Default theme: a status header that links to the run, one line per step, and each step's start
and end in the thread.

A passing run collapses to its header; a failed run lists only the sections that went wrong.
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from chappe.core.messages import Alert, MessageSet, ParentMessage, ThreadEntry
from chappe.core.model import ProcessState, StepState
from chappe.core.render import RenderContext, clip, step_counts
from chappe.core.view import ProcessView, SectionView, StepView


def _timed(step: StepView) -> bool:
    return step.started_at is not None and step.ended_at is not None


class LedgerTheme:
    name: ClassVar[str] = "ledger"

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        parent = ParentMessage(clip(self._parent(view, ctx), ctx.limits.parent_chars))
        return MessageSet(parent, self._thread(view, ctx), self._alerts(view, ctx))

    # ---- parent ----

    def _parent(self, view: ProcessView, ctx: RenderContext) -> str:
        lines = [self._header(view, ctx)]
        if view.state is ProcessState.SUCCEEDED:
            return lines[0]
        multi = len(view.sections) > 1
        for section in view.sections:
            if view.state is ProcessState.FAILED:
                if any(s.state is StepState.FAILED for s in section.steps):
                    counts = step_counts(section.steps, ctx)
                    lines.append(f"{ctx.fmt.bold(section.title)} · {counts}" if multi else counts)
                    lines += [self._step(step, view, ctx) for step in section.steps]
                continue
            if multi:
                lines.append(self._section_line(section, ctx))
                if ctx.collapse_done_sections and self._done(section):
                    continue
            lines += [self._step(step, view, ctx) for step in section.steps]
        return "\n".join(lines)

    def _header(self, view: ProcessView, ctx: RenderContext) -> str:
        words = ctx.tokens.extra
        mark = ctx.fmt.icon(words.get(f"header_{view.state.value}", ""))
        link = view.links[0] if view.links else None
        title = ctx.fmt.link(link.url, view.title) if link else ctx.fmt.escape(view.title)
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
        timed = section.steps and all(_timed(s) for s in section.steps)
        if ctx.collapse_done_sections and self._done(section) and timed:
            starts = [s.started_at for s in section.steps if s.started_at is not None]
            ends = [s.ended_at for s in section.steps if s.ended_at is not None]
            line += f" · {ctx.duration(max(ends) - min(starts))}"
        return line

    @staticmethod
    def _done(section: SectionView) -> bool:
        return all(s.state.finished and s.state is not StepState.FAILED for s in section.steps)

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
        """Spec 6.4 rule 7: an entry only from a view with the step's own times, or, for end
        replies without times, once the run has finished."""
        entries: list[tuple[datetime, int, ThreadEntry]] = []
        for step in view.steps:
            if step.started_at is not None:
                text = (
                    f"{ctx.icon(StepState.RUNNING)} {ctx.fmt.bold(step.title)} · "
                    f"{ctx.tokens.extra.get('started', 'started')} {ctx.clock(step.started_at)}"
                )
                entry = ThreadEntry(f"step:{step.key}:started", clip(text, ctx.limits.entry_chars))
                entries.append((step.started_at, 0, entry))
            if step.state.finished and (step.ended_at is not None or view.state.finished):
                entry = ThreadEntry(
                    f"step:{step.key}:{step.state.value}",
                    clip(self._end(step, view, ctx), ctx.limits.entry_chars),
                )
                entries.append((step.ended_at or view.now, 1, entry))
        entries.sort(key=lambda item: (item[0], item[1]))  # stable: step order breaks ties
        return tuple(entry for _, _, entry in entries)

    def _end(self, step: StepView, view: ProcessView, ctx: RenderContext) -> str:
        text = f"{ctx.icon(step.state)} {ctx.fmt.bold(step.title)} · {ctx.label(step.state)}"
        if step.ended_at is not None:
            text += f" {ctx.clock(step.ended_at)}"
        if step.state is not StepState.SKIPPED and _timed(step):
            text += f" · {ctx.duration(step.duration(view.now))}"
        if step.error:
            text += f"\n{ctx.text(step.error)}"
        return text

    # ---- alerts ----

    def _alerts(self, view: ProcessView, ctx: RenderContext) -> tuple[Alert, ...]:
        if view.state is not ProcessState.FAILED:
            return ()
        mention = f"{ctx.fmt.mention(ctx.alert_mention)} " if ctx.alert_mention else ""
        mark = ctx.fmt.icon(ctx.tokens.extra.get("header_failed", ":red_circle:"))
        failed = view.failed_steps
        details = (
            "; ".join(
                ctx.text(s.title) + (f": {ctx.text(s.error)}" if s.error else "") for s in failed
            )
            if failed
            else ctx.label(ProcessState.FAILED)
        )
        text = f"{mention}{mark} {ctx.fmt.bold(view.title)} · {details}"
        logs = tuple(link for step in failed for link in step.links)
        if logs:
            text += " · " + " · ".join(ctx.fmt.link(link.url, link.label) for link in logs)
        return (Alert(key="alert:process:failed", text=clip(text, ctx.limits.entry_chars)),)
