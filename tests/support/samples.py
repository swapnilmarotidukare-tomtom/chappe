"""Sample runs, a view builder and render helpers for theme tests.

Option A (spike): Airflow gives step states only. Times come from the run and from the step whose
event is handled, so in the samples only that step is timed. Finished runs have no step times.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from chappe.core.messages import MessageSet
from chappe.core.model import ProcessState, StepState
from chappe.core.render import RenderContext
from chappe.core.view import Link, ProcessView, SectionView, StepView
from chappe.themes.tokens import resolve_tokens
from chappe.transports.slack.formatter import SlackFormatter

BASE = datetime(2026, 10, 2, 8, 5, tzinfo=timezone.utc)
TEST_MENTION = "<!subteam^S0TEST>"
RUN_URL = "https://airflow.invalid/dags/orders/runs/run_1"


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_") or "step"


class ProcessViewBuilder:
    """Builds a ProcessView. Steps run one after another from BASE."""

    def __init__(
        self,
        title: str = "orders 2026.10.1",
        key: str = "sample/run_1",
        *,
        known_start: bool = True,
    ) -> None:
        """`known_start=False` builds the minimal DAG-callback view: the run's start is unknown."""
        self._title = title
        self._known_start = known_start
        self._key = key
        self._sections: list[tuple[str, str, list[StepView]]] = []
        self._cursor = BASE
        self._state: ProcessState | None = None

    def section(self, title: str, key: str | None = None) -> ProcessViewBuilder:
        self._sections.append((key or _slug(title), title, []))
        return self

    def step(
        self,
        title: str,
        state: StepState,
        minutes: int = 30,
        error: str | None = None,
        *,
        timed: bool = True,
    ) -> ProcessViewBuilder:
        """Add a step. Every finished step moves the clock; only a timed step carries its times."""
        if not self._sections:
            raise ValueError("call section() before step()")
        section_key, _, steps = self._sections[-1]
        started: datetime | None = None
        ended: datetime | None = None
        if state is not StepState.PENDING:
            started = self._cursor
        if state.finished:
            ended = self._cursor + timedelta(minutes=minutes)
            self._cursor = ended
        slug = _slug(title)
        steps.append(
            StepView(
                key=f"{section_key}.{slug}",
                title=title,
                state=state,
                started_at=started if timed else None,
                ended_at=ended if timed else None,
                error=error,
                links=(Link("Log", f"{RUN_URL}/tasks/{slug}"),),
            )
        )
        return self

    def finished(self, state: ProcessState) -> ProcessViewBuilder:
        self._state = state
        return self

    def build(self, now: datetime | None = None) -> ProcessView:
        sections = tuple(
            SectionView(key, title, tuple(steps)) for key, title, steps in self._sections
        )
        steps = [step for section in sections for step in section.steps]
        state = self._state
        if state is None:
            started = any(step.state is not StepState.PENDING for step in steps)
            state = ProcessState.RUNNING if started else ProcessState.PENDING
        moment = now if now is not None else self._cursor + timedelta(minutes=5)
        return ProcessView(
            key=self._key,
            title=self._title,
            state=state,
            started_at=BASE if self._known_start else None,
            ended_at=moment if state.finished else None,
            now=moment,
            sections=sections,
            links=(Link("Airflow run", RUN_URL),),
        )


S, P, R = StepState.SUCCEEDED, StepState.PENDING, StepState.RUNNING
F, K = StepState.FAILED, StepState.SKIPPED
OOM = "Executor ran out of memory after 3 retries"
SECOND = "Regression vs baseline 2026.10.0"
LONG_STEP = "Generate & consolidate <aggregates> für Ünïcødé " + "x" * 150


def _main() -> ProcessViewBuilder:
    return ProcessViewBuilder().section("Main")


def _prepare() -> ProcessViewBuilder:
    return (
        ProcessViewBuilder()
        .section("Prepare")
        .step("Parquet → Delta", S, 78, timed=False)
        .step("Geometry", S, 261, timed=False)
        .step("Aggregates", S, 10, timed=False)
        .section(SECOND)
    )


def _fifty_steps() -> ProcessView:
    builder = ProcessViewBuilder(title="fifty steps")
    for section in range(1, 6):
        builder.section(f"Section {section}")
        for index in range(1, 11):
            title = f"Step {section}.{index}"
            if section < 3:
                builder.step(title, S, 5, timed=False)
            elif section == 3 and index == 1:
                builder.step(title, R)
            else:
                builder.step(title, P)
    return builder.build()


def _build_samples() -> dict[str, ProcessView]:
    return {
        "pending": _main().step("Parquet → Delta", P).step("Geometry", P).build(),
        # Event: Geometry started.
        "single_running": (
            _main()
            .step("Parquet → Delta", S, 78, timed=False)
            .step("Geometry", R)
            .step("Aggregates", P)
            .build()
        ),
        # Event: Geometry finished; Parquet → Delta finished earlier and carries no times here.
        "single_step_finished": (
            _main()
            .step("Parquet → Delta", S, 78, timed=False)
            .step("Geometry", S, 261)
            .step("Aggregates", P)
            .build()
        ),
        # Event: the DAG callback. No step carries times.
        "single_passed": (
            _main()
            .step("Parquet → Delta", S, 78, timed=False)
            .step("Geometry", S, 261, timed=False)
            .step("Aggregates", S, 10, timed=False)
            .finished(ProcessState.SUCCEEDED)
            .build()
        ),
        "single_failed": (
            _main()
            .step("Parquet → Delta", S, 78, timed=False)
            .step("Geometry", F, 131, error=OOM, timed=False)
            .step("Aggregates", P)
            .finished(ProcessState.FAILED)
            .build()
        ),
        "with_skipped": (
            _main()
            .step("Parquet → Delta", S, 78, timed=False)
            .step("Backfill", K, 0, timed=False)
            .step("Aggregates", S, 10, timed=False)
            .finished(ProcessState.SUCCEEDED)
            .build()
        ),
        "unicode_long": (
            ProcessViewBuilder(title="Ünïcødé <orders> & co")
            .section("Main")
            .step(LONG_STEP, R)
            .build()
        ),
        "multi_running": _prepare().step("ID stability", R).step("Regression", P).build(),
        "multi_passed": (
            _prepare()
            .step("ID stability", S, 224, timed=False)
            .step("Regression", S, 33, timed=False)
            .finished(ProcessState.SUCCEEDED)
            .build()
        ),
        "multi_failed": (
            _prepare()
            .step("ID stability", S, 224, timed=False)
            .step("Regression", F, 46, error=OOM, timed=False)
            .finished(ProcessState.FAILED)
            .build()
        ),
        "fifty_steps": _fifty_steps(),
    }


SAMPLES: dict[str, ProcessView] = _build_samples()
SINGLE_SECTION_SAMPLES: tuple[str, ...] = tuple(
    name for name, view in SAMPLES.items() if len(view.sections) == 1
)
FAILED_SAMPLES: tuple[str, ...] = tuple(
    name for name, view in SAMPLES.items() if view.state is ProcessState.FAILED
)


def default_context(
    theme_name: str,
    tokens: Mapping[str, Any] | None = None,
    alert_mention: str | None = TEST_MENTION,
) -> RenderContext:
    return RenderContext(
        tokens=resolve_tokens(theme_name, tokens),
        fmt=SlackFormatter(),
        tz=timezone.utc,
        alert_mention=alert_mention,
    )


def dump_message_set(messages: MessageSet) -> str:
    lines = ["=== parent ===", messages.parent.text]
    lines += ["=== fallback ===", messages.parent.fallback_text]
    lines.append("=== thread ===")
    lines += [f"[{e.key}]{' (broadcast)' if e.broadcast else ''} {e.text}" for e in messages.thread]
    lines.append("=== alerts ===")
    lines += [f"[{a.key}] {a.text}" for a in messages.alerts]
    return "\n".join(lines) + "\n"
