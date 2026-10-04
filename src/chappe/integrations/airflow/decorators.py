from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from airflow.sdk import BaseOperator

from chappe.integrations.airflow import callbacks
from chappe.integrations.airflow.marker import MARKER_ATTR, MilestoneSpec

log = logging.getLogger("chappe")

_CALLBACKS: tuple[tuple[str, Callable[..., None]], ...] = (
    ("on_execute_callback", callbacks.on_step_started),
    ("on_success_callback", callbacks.on_step_succeeded),
    ("on_failure_callback", callbacks.on_step_failed),
    ("on_skipped_callback", callbacks.on_step_skipped),
)


def _append_callback(op: BaseOperator, attr: str, callback: Callable[..., None]) -> None:
    if not hasattr(op, attr):
        return
    current = getattr(op, attr)
    if current is None:
        updated: list[Any] = [callback]
    elif isinstance(current, (list, tuple)):
        updated = list(current) if callback in current else [*current, callback]
    else:
        updated = [current] if current is callback else [current, callback]
    setattr(op, attr, updated)


def mark_operator(op: BaseOperator, spec: MilestoneSpec) -> BaseOperator:
    try:
        setattr(op, MARKER_ATTR, spec)
        for attr, callback in _CALLBACKS:
            _append_callback(op, attr, callback)
    except Exception:
        log.warning("chappe: could not mark %r as a milestone; it will not be reported", op)
    return op


class _MilestoneTask:
    """Wraps a TaskFlow task so each call marks the operator it creates."""

    def __init__(self, inner: Any, spec: MilestoneSpec) -> None:
        self._inner = inner
        self._spec = spec

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        result = self._inner(*args, **kwargs)
        op = getattr(result, "operator", None)
        if isinstance(op, BaseOperator):
            mark_operator(op, self._spec)
        else:
            log.warning(
                "chappe: @milestone could not find the operator for %r; it will not be reported",
                self._inner,
            )
        return result

    def expand(self, *args: Any, **kwargs: Any) -> Any:
        log.warning(
            "chappe: mapped tasks are not supported in 0.0.1; %r will not be reported",
            getattr(self._inner, "function", self._inner),
        )
        return self._inner.expand(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def milestone(target: Any = None, title: str | None = None, *, section: str | None = None) -> Any:
    """Mark a task as a milestone.

    @milestone("Title") above @task, bare @milestone, or milestone(operator, "Title").
    """
    if isinstance(target, BaseOperator):
        return mark_operator(target, MilestoneSpec(title, section))
    if target is not None and not isinstance(target, str):
        if callable(target) and hasattr(target, "expand"):
            return _MilestoneTask(target, MilestoneSpec(None, section))
        log.warning("chappe: @milestone cannot mark %r; it will not be reported", target)
        return target
    spec = MilestoneSpec(target, section)

    def decorate(task_decorator: Any) -> _MilestoneTask:
        return _MilestoneTask(task_decorator, spec)

    return decorate
