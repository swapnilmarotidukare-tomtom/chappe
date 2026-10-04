# src/chappe/integrations/airflow/reader.py
"""Reads the task states of one run through the Airflow Task SDK runtime (ADR-0004)."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol


class TaskStateReader(Protocol):
    def read(self, dag_id: str, run_id: str) -> Mapping[str, str | None]:
        """Airflow state per task id, for example {"prepare.convert": "success"}."""
        ...


def runtime_task_states(dag_id: str, run_id: str) -> Mapping[str, Any]:
    """The only use of the internal `RuntimeTaskInstance` API; keep it in this function.

    Works in task callbacks and in DAG callbacks. Chappe's other calls through the Task SDK
    runtime are the public `airflow.sdk.Variable` (the store) and `airflow.sdk.Connection` (the
    Slack token); the DAG processor answers those too (spike S7). Never call other runtime
    methods from a DAG callback: the DAG processor does not answer them and the callback hangs.
    """
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    states: dict[str, Any] = RuntimeTaskInstance.get_task_states(dag_id=dag_id, run_ids=[run_id])
    return states


def _state_name(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


class RuntimeTaskStateReader:
    def __init__(
        self, fetch: Callable[[str, str], Mapping[str, Any]] = runtime_task_states
    ) -> None:
        self._fetch = fetch

    def read(self, dag_id: str, run_id: str) -> Mapping[str, str | None]:
        by_run = self._fetch(dag_id, run_id)
        states = by_run.get(run_id) or {}
        return {str(task_id): _state_name(state) for task_id, state in states.items()}
