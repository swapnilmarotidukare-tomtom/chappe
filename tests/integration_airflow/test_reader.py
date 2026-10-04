# tests/integration_airflow/test_reader.py
from collections.abc import Mapping
from typing import Any

import pytest
from airflow.sdk import TaskInstanceState
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

from chappe.integrations.airflow.reader import RuntimeTaskStateReader, runtime_task_states

RUN = "manual__2026-10-02T08:05:00+00:00"


def test_reads_the_states_of_one_run() -> None:
    def fetch(dag_id: str, run_id: str) -> Mapping[str, Any]:
        assert (dag_id, run_id) == ("orders", RUN)
        return {
            RUN: {
                "prepare.convert": TaskInstanceState.SUCCESS,
                "compare_0": "running",
                "later": None,
            }
        }

    states = RuntimeTaskStateReader(fetch).read("orders", RUN)
    assert states == {"prepare.convert": "success", "compare_0": "running", "later": None}


def test_an_unknown_run_reads_as_no_states() -> None:
    assert RuntimeTaskStateReader(lambda dag_id, run_id: {}).read("orders", RUN) == {}


def test_asks_the_runtime_for_exactly_one_run(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def get_task_states(**kwargs: Any) -> dict[str, dict[str, str]]:
        calls.append(kwargs)
        return {RUN: {"prepare.convert": "running"}}

    monkeypatch.setattr(RuntimeTaskInstance, "get_task_states", staticmethod(get_task_states))
    assert runtime_task_states("orders", RUN) == {RUN: {"prepare.convert": "running"}}
    assert calls == [{"dag_id": "orders", "run_ids": [RUN]}]
