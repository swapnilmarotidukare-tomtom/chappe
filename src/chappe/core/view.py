"""The read-only run view handed to themes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from chappe.core.model import ProcessState, StepState

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class Link:
    label: str
    url: str


@dataclass(frozen=True, slots=True)
class AttemptView:
    number: int
    state: StepState
    started_at: datetime | None = None
    ended_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class StepView:
    key: str
    title: str
    state: StepState
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str | None = None
    links: tuple[Link, ...] = ()
    attempts: tuple[AttemptView, ...] = ()

    def duration(self, now: datetime) -> timedelta | None:
        if self.started_at is None:
            return None
        end = self.ended_at if self.ended_at is not None else now
        return max(end - self.started_at, timedelta(0))


@dataclass(frozen=True, slots=True)
class SectionView:
    key: str
    title: str
    steps: tuple[StepView, ...]

    @property
    def state(self) -> ProcessState:
        states = [s.state for s in self.steps]
        if any(s is StepState.FAILED for s in states):
            return ProcessState.FAILED
        if states and all(s.finished for s in states):
            return ProcessState.SUCCEEDED
        if any(s is not StepState.PENDING for s in states):
            return ProcessState.RUNNING
        return ProcessState.PENDING


@dataclass(frozen=True, slots=True)
class Watermark:
    """How far along a view is. Larger is newer.

    Progress decides: a finished view beats any unfinished one, then more settled steps, then more
    started steps. The event time only breaks ties, so worker clock skew cannot reorder writes.
    """

    finished: bool
    settled_steps: int
    started_steps: int
    occurred_at: datetime | None

    def _order(self) -> tuple[bool, int, int, datetime]:
        return (self.finished, self.settled_steps, self.started_steps, self.occurred_at or _EPOCH)

    def newer_than(self, other: Watermark | None) -> bool:
        return other is None or self._order() > other._order()


@dataclass(frozen=True, slots=True)
class ProcessView:
    key: str
    title: str
    state: ProcessState
    started_at: datetime | None
    ended_at: datetime | None
    now: datetime
    sections: tuple[SectionView, ...]
    links: tuple[Link, ...] = ()

    @property
    def steps(self) -> tuple[StepView, ...]:
        return tuple(step for section in self.sections for step in section.steps)

    @property
    def duration(self) -> timedelta | None:
        if self.started_at is None:
            return None
        end = self.ended_at if self.ended_at is not None else self.now
        return max(end - self.started_at, timedelta(0))

    @property
    def progress(self) -> tuple[int, int]:
        steps = self.steps
        return sum(1 for s in steps if s.state.finished), len(steps)

    @property
    def current_steps(self) -> tuple[StepView, ...]:
        return tuple(s for s in self.steps if s.state is StepState.RUNNING)

    @property
    def failed_steps(self) -> tuple[StepView, ...]:
        return tuple(s for s in self.steps if s.state is StepState.FAILED)

    @property
    def next_steps(self) -> tuple[StepView, ...]:
        found: list[StepView] = []
        for section in self.sections:
            for step in section.steps:
                if step.state is StepState.PENDING:
                    found.append(step)
                    break
        return tuple(found)

    @property
    def watermark(self) -> Watermark:
        steps = self.steps
        return Watermark(
            finished=self.state.finished,
            settled_steps=sum(1 for s in steps if s.state.finished),
            started_steps=sum(1 for s in steps if s.state is not StepState.PENDING),
            occurred_at=self.now,
        )
