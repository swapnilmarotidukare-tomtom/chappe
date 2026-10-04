"""Default theme: status header, one icon row per section, history in the thread."""

from __future__ import annotations

from typing import ClassVar

from chappe.core.messages import Alert, MessageSet, ParentMessage, ThreadEntry
from chappe.core.model import ProcessState, StepState
from chappe.core.render import RenderContext, clip
from chappe.core.view import Link, ProcessView, SectionView, StepView


def _entry_due(step: StepView, view: ProcessView) -> bool:
    """CONTRACT B: only from a view with the step's own times, or once the process finished."""
    return step.state.finished and (step.ended_at is not None or view.state.finished)


def _links(links: tuple[Link, ...], ctx: RenderContext) -> str:
    return " · ".join(ctx.fmt.link(link.url, link.label) for link in links)


def _counts(section: SectionView, ctx: RenderContext) -> str:
    """A failed run's row: "3 passed · 1 failed", then skipped and not run when non-zero."""
    states = [step.state for step in section.steps]
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


class ThreadTheme:
    name: ClassVar[str] = "thread"

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        done, total = view.progress
        fallback = f"{view.title}: {ctx.label(view.state)} ({done}/{total})"
        parent = ParentMessage(clip(self._parent(view, ctx), ctx.limits.parent_chars), fallback)
        return MessageSet(parent, self._thread(view, ctx), self._alerts(view, ctx))

    def _parent(self, view: ProcessView, ctx: RenderContext) -> str:
        head = f"{ctx.icon(view.state)} {ctx.fmt.bold(view.title)} · {ctx.label(view.state)}"
        if view.duration is not None:
            head += f" · {ctx.duration(view.duration)}"
        if view.started_at is not None:
            head += f" · {ctx.tokens.extra.get('started', 'started')} {ctx.clock(view.started_at)}"
        lines = [head]
        multi = len(view.sections) > 1
        steps_word = ctx.tokens.extra.get("steps", "steps")
        for section in view.sections:
            icons = "".join(ctx.icon(step.state) for step in section.steps)
            if multi:
                detail = self._detail(section, ctx)
                lines.append(f"{icons}  {ctx.fmt.bold(section.title)} · {detail}")
            elif view.state is ProcessState.FAILED:
                lines.append(f"{icons}  {_counts(section, ctx)}")
            else:
                finished = sum(1 for step in section.steps if step.state.finished)
                lines.append(f"{icons}  {finished}/{len(section.steps)} {steps_word}")
        if view.current_steps:
            running: list[str] = []
            for step in view.current_steps:
                took = step.duration(view.now)
                suffix = f" ({ctx.duration(took)})" if took is not None else ""
                running.append(ctx.text(step.title) + suffix)
            lines.append(f"{ctx.tokens.extra.get('now', 'Now')}: {', '.join(running)}")
        if view.links:
            lines.append(_links(view.links, ctx))
        return "\n".join(lines)

    def _detail(self, section: SectionView, ctx: RenderContext) -> str:
        failed = [s for s in section.steps if s.state is StepState.FAILED]
        if failed:
            return f"{ctx.label(StepState.FAILED)}: {ctx.text(failed[0].title)}"
        running = [s for s in section.steps if s.state is StepState.RUNNING]
        if running:
            return ctx.text(running[0].title)
        return ctx.label(section.state)

    def _thread(self, view: ProcessView, ctx: RenderContext) -> tuple[ThreadEntry, ...]:
        multi = len(view.sections) > 1
        due: list[tuple[SectionView, StepView]] = [
            (section, step)
            for section in view.sections
            for step in section.steps
            if _entry_due(step, view)
        ]
        due.sort(key=lambda pair: pair[1].ended_at or view.now)
        entries: list[ThreadEntry] = []
        for section, step in due:
            where = f" ({ctx.text(section.title)})" if multi else ""
            label = ctx.label(step.state)
            text = f"{ctx.icon(step.state)} {ctx.fmt.bold(step.title)}{where} · {label}"
            took = step.duration(view.now) if step.ended_at is not None else None
            if step.state is not StepState.SKIPPED and took is not None:
                text += f" · {ctx.duration(took)}"
            if step.links:
                text += " · " + _links(step.links, ctx)
            if step.error:
                text += f"\n{ctx.text(step.error)}"
            key = f"step:{step.key}:{step.state.value}"
            entries.append(ThreadEntry(key=key, text=clip(text, ctx.limits.entry_chars)))
        if view.state.finished:
            text = f"{ctx.icon(view.state)} {ctx.fmt.bold(view.title)} · {ctx.label(view.state)}"
            if view.duration is not None:
                text += f" · {ctx.duration(view.duration)}"
            if view.links:
                text += " · " + _links(view.links, ctx)
            key = f"process:{view.state.value}"
            entries.append(
                ThreadEntry(key=key, text=clip(text, ctx.limits.entry_chars), broadcast=True)
            )
        return tuple(entries)

    def _alerts(self, view: ProcessView, ctx: RenderContext) -> tuple[Alert, ...]:
        if view.state is not ProcessState.FAILED:
            return ()
        mention = f"{ctx.fmt.mention(ctx.alert_mention)} " if ctx.alert_mention else ""
        failed = view.failed_steps
        if failed:
            details = "; ".join(
                ctx.text(s.title) + (f": {ctx.text(s.error)}" if s.error else "") for s in failed
            )
        else:
            details = ctx.label(ProcessState.FAILED)
        text = f"{mention}{ctx.icon(ProcessState.FAILED)} {ctx.fmt.bold(view.title)} · {details}"
        logs = tuple(link for step in failed for link in step.links)
        if logs:
            text += " · " + _links(logs, ctx)
        return (Alert(key="alert:process:failed", text=clip(text, ctx.limits.entry_chars)),)
