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
from chappe.integrations.airflow import notifier as notifier_module
from chappe.integrations.airflow import runtime as runtime_module
from chappe.integrations.airflow.connections import ConnectionInfo
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
    final = dag_context("success", "success")
    final["dag"] = orders_dag("other")
    ChappeNotifier().notify(final)
    ChappeNotifier(process="unknown").notify(final)  # warns, then no process for "other"
    assert engine.events == []


TWO_PROCESSES = {
    "processes": {
        "orders": {"dags": [{"dag_id": "orders"}], "channel": "C0123456789"},
        "billing": {"dags": [{"dag_id": "billing"}], "channel": "C0123456789"},
    }
}


@pytest.mark.parametrize("process", ["typo", "billing"])
def test_a_wrong_notifier_process_warns_and_uses_the_dags_own_process(
    process: str, caplog: pytest.LogCaptureFixture
) -> None:
    """An unknown process, or one that does not list this DAG: late over wrong (spec 4.2)."""
    recording = RecordingEngine()
    stub = StubRuntime(recording)
    stub.settings = ChappeSettings.model_validate(TWO_PROCESSES)
    set_runtime(stub)
    try:
        with caplog.at_level(logging.WARNING, logger="chappe"):
            ChappeNotifier(process=process).notify(dag_context("success", "success"))
    finally:
        set_runtime(None)
    (event,) = recording.events
    assert (event.kind, event.process, event.process_state) == (
        EventKind.RUN_FINISHED,
        "orders",
        ProcessState.SUCCEEDED,
    )
    # the parse-time warning (unknown names only) is separate: pick the run-time one
    (warning,) = [r for r in caplog.records if "using the process of DAG" in r.getMessage()]
    assert repr(process) in warning.getMessage() and "'orders'" in warning.getMessage()


@pytest.mark.parametrize(
    ("settings", "env"),
    [
        ({**SETTINGS, "enabled": False}, None),
        (SETTINGS, "0"),
        (
            {
                "processes": {
                    "orders": {
                        "dags": [{"dag_id": "orders"}],
                        "channel": "C0123456789",
                        "enabled": False,
                    }
                }
            },
            None,
        ),
    ],
)
def test_the_kill_switch_returns_before_building_the_engine(
    settings: dict[str, Any],
    env: str | None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    if env is not None:
        monkeypatch.setenv("CHAPPE_ENABLED", env)
    looked_up: list[str] = []

    def no_connection(conn_id: str) -> ConnectionInfo:
        looked_up.append(conn_id)
        raise RuntimeError("connection chappe_slack not found")

    set_runtime(Runtime(ChappeSettings.model_validate(settings), connections=no_connection))
    try:
        with caplog.at_level(logging.DEBUG, logger="chappe"):
            callbacks.on_step_started(task_context())
            ChappeNotifier().notify(dag_context("success", "success"))
    finally:
        set_runtime(None)
    assert looked_up == []
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_the_notifier_renders_no_templates(
    engine: RecordingEngine, caplog: pytest.LogCaptureFixture
) -> None:
    """Under dag.test() the DAG is a SerializedDAG without get_template_env (Airflow 3.2.2)."""
    ctx = dag_context("success", "success")
    ctx["dag"] = SimpleNamespace(dag_id="orders", task_dict={}, task_group=None)
    with caplog.at_level(logging.DEBUG):
        ChappeNotifier()(ctx)  # how Airflow calls it
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    (event,) = engine.events
    assert (event.kind, event.process_state) == (EventKind.RUN_FINISHED, ProcessState.SUCCEEDED)


def test_unknown_process_name_warns_once_at_parse_time(
    engine: RecordingEngine, caplog: pytest.LogCaptureFixture
) -> None:
    notifier_module._warned_unknown.clear()
    with caplog.at_level(logging.WARNING, logger="chappe"):
        ChappeNotifier(process="ordres")
        ChappeNotifier(process="ordres")
        ChappeNotifier(process="orders")
        ChappeNotifier()
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "'ordres'" in warnings[0] and "configured: orders" in warnings[0]


def test_parse_time_check_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom() -> None:
        raise RuntimeError("boom")

    notifier_module._warned_unknown.clear()
    monkeypatch.setattr(runtime_module, "get_runtime", boom)
    assert ChappeNotifier(process="x").process == "x"
