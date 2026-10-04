import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, TaskGroup
from airflow.sdk.definitions.param import ParamsDict

from chappe.config.models import ChappeSettings
from chappe.core.engine import HandleResult
from chappe.core.events import ChappeEvent, EventKind
from chappe.core.model import ProcessState, StepState
from chappe.integrations.airflow import callbacks
from chappe.integrations.airflow.notifier import ChappeNotifier, run_state
from chappe.integrations.airflow.runtime import Runtime, set_runtime

T0 = datetime(2026, 10, 2, 8, 0, tzinfo=timezone.utc)
SETTINGS = {"processes": {"orders": {"dags": [{"dag_id": "orders"}], "channel": "C0123456789"}}}


class RecordingEngine:
    def __init__(self, fail: bool = False) -> None:
        self.events: list[ChappeEvent] = []
        self.fail = fail

    def handle(self, event: ChappeEvent) -> HandleResult:
        if self.fail:
            raise RuntimeError("boom")
        self.events.append(event)
        return HandleResult.SENT


class StubRuntime(Runtime):
    def __init__(self, engine: RecordingEngine) -> None:
        super().__init__(ChappeSettings.model_validate(SETTINGS))
        self._stub = engine

    def engine(self, name: str) -> Any:
        return self._stub


class NoAccess:
    """Fails on any attribute read: DAG callbacks must not touch the task instance."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"read ti.{name} in a DAG callback")


@pytest.fixture
def engine() -> Iterator[RecordingEngine]:
    recording = RecordingEngine()
    set_runtime(StubRuntime(recording))
    yield recording
    set_runtime(None)


def orders_dag(dag_id: str = "orders") -> DAG:
    with DAG(dag_id, schedule=None) as dag:
        EmptyOperator(task_id="convert")
    return dag


def task_context(exception: Exception | None = None, ended: bool = False) -> dict[str, Any]:
    return {
        "dag": orders_dag(),
        "run_id": "run_1",
        "dag_run": SimpleNamespace(run_id="run_1", state="running", start_date=T0, end_date=None),
        "ti": SimpleNamespace(
            task_id="convert",
            start_date=T0 + timedelta(minutes=1),
            end_date=T0 + timedelta(minutes=9) if ended else None,
        ),
        "params": {"version": "2026.10.1"},
        "exception": exception,
    }


def dag_context(state: str, reason: str) -> dict[str, Any]:
    """The full DAG-callback context: a template context plus `reason`."""
    return {
        "dag": orders_dag(),
        "run_id": "run_1",
        "dag_run": SimpleNamespace(
            run_id="run_1", state=state, start_date=T0, end_date=T0 + timedelta(hours=1)
        ),
        "ti": NoAccess(),
        "params": {"version": "2026.10.1"},
        "reason": reason,
    }


def test_step_callbacks_dispatch_events(engine: RecordingEngine) -> None:
    callbacks.on_step_started(task_context())
    callbacks.on_step_failed(task_context(exception=ValueError("bad row"), ended=True))
    started, failed = engine.events
    assert (started.kind, started.process, started.run_id) == (
        EventKind.STEP_STARTED,
        "orders",
        "run_1",
    )
    assert (started.step_key, started.step_state) == ("convert", StepState.RUNNING)
    assert started.payload["step_started_at"] == T0 + timedelta(minutes=1)
    assert started.payload["run_started_at"] == T0
    assert started.payload["params"] == {"version": "2026.10.1"}
    assert failed.step_state is StepState.FAILED and failed.error == "ValueError: bad row"
    assert failed.payload["step_ended_at"] == T0 + timedelta(minutes=9)


def test_skipped_and_succeeded_steps(engine: RecordingEngine) -> None:
    callbacks.on_step_succeeded(task_context(ended=True))
    callbacks.on_step_skipped(task_context(ended=True))
    assert [e.step_state for e in engine.events] == [StepState.SUCCEEDED, StepState.SKIPPED]


def test_step_key_is_the_full_task_id_inside_a_task_group(engine: RecordingEngine) -> None:
    with DAG("orders", schedule=None) as dag, TaskGroup("prepare"):
        task = EmptyOperator(task_id="convert")
    assert task.task_id == "prepare.convert"
    ctx = task_context()
    ctx["dag"] = dag
    ctx["ti"] = SimpleNamespace(task_id=task.task_id, start_date=T0, end_date=None)
    callbacks.on_step_started(ctx)
    (event,) = engine.events
    assert event.step_key == "prepare.convert"
    assert event.payload["dag"] is dag


def test_params_are_resolved_to_plain_values(engine: RecordingEngine) -> None:
    ctx = task_context()
    ctx["params"] = ParamsDict({"version": Param("2026.10.1", type="string"), "n": 3})
    callbacks.on_step_started(ctx)
    ctx["params"] = {"version": Param("2026.10.2", type="string")}  # a raw dict of Params
    callbacks.on_step_started(ctx)
    first, second = engine.events
    assert first.payload["params"] == {"version": "2026.10.1", "n": 3}
    assert second.payload["params"] == {"version": "2026.10.2"}
    assert not any(
        isinstance(v, Param) for e in engine.events for v in e.payload["params"].values()
    )


def test_a_failure_that_will_be_retried_keeps_the_step_running(engine: RecordingEngine) -> None:
    ctx = task_context(exception=ValueError("flaky"), ended=True)
    ctx["ti"].state = SimpleNamespace(value="up_for_retry")  # TaskInstanceState is a str enum
    callbacks.on_step_failed(ctx)
    ctx["ti"].state = "up_for_retry"
    callbacks.on_step_failed(ctx)
    ctx["ti"].state = "failed"
    callbacks.on_step_failed(ctx)
    assert [e.step_state for e in engine.events] == [
        StepState.RUNNING,
        StepState.RUNNING,
        StepState.FAILED,
    ]
    assert engine.events[0].error is None and engine.events[2].error == "ValueError: flaky"


def test_notifier_takes_the_run_state_from_dag_run(engine: RecordingEngine) -> None:
    ChappeNotifier().notify(dag_context("failed", "task_failure"))
    ChappeNotifier(process="orders")(
        dag_context("success", "success")
    )  # Airflow calls the notifier
    assert [(e.kind, e.process_state, e.step_key) for e in engine.events] == [
        (EventKind.RUN_FINISHED, ProcessState.FAILED, None),
        (EventKind.RUN_FINISHED, ProcessState.SUCCEEDED, None),
    ]
    assert engine.events[0].payload["run_ended_at"] == T0 + timedelta(hours=1)
    assert engine.events[0].payload["dag"].dag_id == "orders"


def test_notifier_survives_the_minimal_dag_callback_context(engine: RecordingEngine) -> None:
    # Without last_ti the DAG processor sends only {"dag", "run_id", "reason"}.
    ChappeNotifier().notify({"dag": orders_dag(), "run_id": "run_2", "reason": "task_failure"})
    ChappeNotifier().notify({"dag": orders_dag(), "run_id": "run_3", "reason": "success"})
    failed, passed = engine.events
    assert (failed.run_id, failed.process_state) == ("run_2", ProcessState.FAILED)
    assert (passed.run_id, passed.process_state) == ("run_3", ProcessState.SUCCEEDED)
    assert failed.payload["run_started_at"] is None and failed.payload["params"] == {}
    assert failed.payload["dag"].dag_id == "orders"


def test_run_state() -> None:
    enum_like = SimpleNamespace(state=SimpleNamespace(value="failed"))
    assert run_state({"dag_run": enum_like, "reason": "success"}) is ProcessState.FAILED
    assert run_state({"reason": "timed_out"}) is ProcessState.FAILED
    assert run_state({"reason": "all_tasks_deadlocked"}) is ProcessState.FAILED
    assert run_state({"reason": "success"}) is ProcessState.SUCCEEDED


def test_callbacks_never_raise() -> None:
    set_runtime(StubRuntime(RecordingEngine(fail=True)))
    try:
        callbacks.on_step_succeeded(task_context())
        callbacks.on_step_started({})  # no dag at all
        ChappeNotifier().notify(dag_context("success", "success"))
        ChappeNotifier().notify({})  # nothing at all
    finally:
        set_runtime(None)


def test_a_failed_run_finished_event_logs_an_error_with_the_process_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Spec 9.3: a final event that cannot be sent is logged at ERROR with the process key."""
    set_runtime(StubRuntime(RecordingEngine(fail=True)))
    try:
        with caplog.at_level(logging.WARNING, logger="chappe"):
            ChappeNotifier().notify(dag_context("success", "success"))
    finally:
        set_runtime(None)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors and "orders/run_1" in errors[0].getMessage()


def test_unconfigured_dag_is_ignored(engine: RecordingEngine) -> None:
    ctx = task_context()
    ctx["dag"] = orders_dag("other")
    callbacks.on_step_started(ctx)
    ChappeNotifier(process="unknown").notify(dag_context("success", "success"))
    assert engine.events == []
