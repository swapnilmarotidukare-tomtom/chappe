# src/chappe/integrations/airflow/source.py
"""Builds the run view for an Airflow event (spec 7, step 5)."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment

from chappe.config.models import ProcessConfig
from chappe.core.events import ChappeEvent, EventKind
from chappe.core.model import ProcessState, StepState
from chappe.core.view import Link, ProcessView, SectionView, StepView
from chappe.integrations.airflow.reader import TaskStateReader
from chappe.integrations.airflow.structure import build_structure

STATE_MAP: dict[str, StepState] = {
    "none": StepState.PENDING,
    "scheduled": StepState.PENDING,
    "queued": StepState.PENDING,
    "restarting": StepState.PENDING,
    "running": StepState.RUNNING,
    "up_for_retry": StepState.RUNNING,
    "up_for_reschedule": StepState.RUNNING,
    "deferred": StepState.RUNNING,
    "success": StepState.SUCCEEDED,
    "failed": StepState.FAILED,
    "upstream_failed": StepState.FAILED,
    "skipped": StepState.SKIPPED,
    "removed": StepState.SKIPPED,
}
_TEMPLATES = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False)
_warned_templates: set[str] = set()  # one warning per template per process

log = logging.getLogger("chappe")


def process_key(dag_id: str, run_id: str) -> str:
    return f"{dag_id}/{run_id}"


def map_state(airflow_state: str | None) -> StepState:
    return STATE_MAP.get(airflow_state, StepState.PENDING) if airflow_state else StepState.PENDING


def render_title(template: str, *, params: Mapping[str, Any], dag_id: str, run_id: str) -> str:
    try:
        text = (
            _TEMPLATES.from_string(template)
            .render(params=params, dag_id=dag_id, run_id=run_id)
            .strip()
        )
    except Exception as exc:
        if template not in _warned_templates:
            _warned_templates.add(template)
            log.warning(
                "chappe: title template %r failed (%s: %s); titles fall back to %r",
                template,
                type(exc).__name__,
                exc,
                "<dag_id> · <run_id>",
            )
        text = ""
    return text or f"{dag_id} · {run_id}"


def _time(payload: Mapping[str, Any], key: str) -> datetime | None:
    value = payload.get(key)
    if not isinstance(value, datetime):
        return None
    # Event and view times are UTC-aware; a naive payload time is taken as UTC.
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


class AirflowSource:
    """States come from the reader; times only from the event's own task and the run (Option A)."""

    def __init__(
        self, process: ProcessConfig, reader: TaskStateReader, *, ui_base_url: str | None = None
    ) -> None:
        self._process = process
        self._reader = reader
        self._ui = ui_base_url.rstrip("/") if ui_base_url else None

    def _run_url(self, event: ChappeEvent) -> str | None:
        if self._ui is None:
            return None
        return f"{self._ui}/dags/{quote(event.dag_id, safe='')}/runs/{quote(event.run_id, safe='')}"

    def _step(
        self,
        task_id: str,
        title: str,
        state_read: str | None,
        event: ChappeEvent,
        run_url: str | None,
    ) -> StepView:
        links = (Link("Log", f"{run_url}/tasks/{quote(task_id, safe='')}"),) if run_url else ()
        if task_id == event.step_key and event.step_state is not None:
            # The callback's own outcome wins: a failing task still reads "running"
            # in its own on_failure_callback.
            state = event.step_state
            started = _time(event.payload, "step_started_at")
            if started is None and event.kind is EventKind.STEP_STARTED:
                started = event.occurred_at
            ended = (
                (_time(event.payload, "step_ended_at") or event.occurred_at)
                if state.finished
                else None
            )
            return StepView(task_id, title, state, started, ended, event.error, links)
        error = "An upstream task failed" if state_read == "upstream_failed" else None
        return StepView(task_id, title, map_state(state_read), error=error, links=links)

    def snapshot(self, event: ChappeEvent) -> ProcessView | None:
        structure = build_structure(event.payload["dag"], self._process)
        if not structure.steps:
            return None
        states = self._reader.read(event.dag_id, event.run_id)
        run_url = self._run_url(event)
        by_section: dict[str, list[StepView]] = {}
        for step in structure.steps:
            view = self._step(step.task_id, step.title, states.get(step.task_id), event, run_url)
            by_section.setdefault(step.section_key, []).append(view)
        sections = tuple(
            SectionView(s.key, s.title, tuple(by_section.get(s.key, ())))
            for s in structure.sections
        )

        if event.kind is EventKind.RUN_FINISHED and event.process_state is not None:
            state = event.process_state
        elif any(s.state is not StepState.PENDING for section in sections for s in section.steps):
            state = ProcessState.RUNNING
        else:
            state = ProcessState.PENDING

        ended = (
            (_time(event.payload, "run_ended_at") or event.occurred_at) if state.finished else None
        )
        params = event.payload.get("params") or {}
        return ProcessView(
            key=process_key(event.dag_id, event.run_id),
            title=render_title(
                self._process.title, params=params, dag_id=event.dag_id, run_id=event.run_id
            ),
            state=state,
            started_at=_time(event.payload, "run_started_at"),
            ended_at=ended,
            now=event.occurred_at,
            sections=sections,
            links=(Link("Airflow run", run_url),) if run_url else (),
        )
