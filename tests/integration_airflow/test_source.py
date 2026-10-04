# tests/integration_airflow/test_source.py
import logging
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup

from chappe.config.models import ProcessConfig
from chappe.core.events import ChappeEvent, EventKind
from chappe.core.model import ProcessState, StepState
from chappe.integrations.airflow import source as source_module
from chappe.integrations.airflow.decorators import milestone
from chappe.integrations.airflow.source import AirflowSource, map_state, process_key, render_title

T0 = datetime(2026, 10, 2, 8, 5, tzinfo=timezone.utc)
NOW = T0 + timedelta(hours=2)
RUN = "manual__2026-10-02T08:05:00+00:00"
UI = "http://airflow:8080"


class FakeReader:
    def __init__(self, states: Mapping[str, str | None]) -> None:
        self.states = states

    def read(self, dag_id: str, run_id: str) -> Mapping[str, str | None]:
        return self.states


def dag() -> DAG:
    with DAG("orders", schedule=None) as d, TaskGroup("prepare"):
        a = milestone(EmptyOperator(task_id="convert"), "Convert")
        b = milestone(EmptyOperator(task_id="geometry"), "Geometry")
        a >> b
    return d


def process(title: str = "{{ params.product }} {{ params.version }}") -> ProcessConfig:
    return ProcessConfig.model_validate(
        {"dags": [{"dag_id": "orders"}], "channel": "C0123456789", "title": title}
    )


def event(
    kind: EventKind,
    *,
    step: str | None = None,
    step_state: StepState | None = None,
    process_state: ProcessState | None = None,
    error: str | None = None,
    the_dag: DAG | None = None,
    **payload: Any,
) -> ChappeEvent:
    data: dict[str, Any] = {
        "dag": the_dag or dag(),
        "params": {"product": "orders", "version": "2026.10.1"},
        "run_started_at": T0,
    }
    data.update(payload)
    return ChappeEvent(
        kind=kind,
        process="orders",
        dag_id="orders",
        run_id=RUN,
        occurred_at=NOW,
        step_key=step,
        step_state=step_state,
        process_state=process_state,
        error=error,
        payload=data,
    )


def source(states: Mapping[str, str | None], ui_base_url: str | None = UI) -> AirflowSource:
    return AirflowSource(process(), FakeReader(states), ui_base_url=ui_base_url)


def test_maps_airflow_states() -> None:
    assert map_state("up_for_retry") is StepState.RUNNING
    assert map_state("upstream_failed") is StepState.FAILED
    assert map_state("removed") is StepState.SKIPPED
    assert map_state(None) is StepState.PENDING
    assert map_state("something_new") is StepState.PENDING


def test_process_key() -> None:
    assert process_key("orders", RUN) == f"orders/{RUN}"


def test_the_event_outcome_overrides_the_state_read() -> None:
    # In its own on_failure_callback a failing task still reads "running".
    view = source({"prepare.convert": "running"}).snapshot(
        event(
            EventKind.STEP_FINISHED,
            step="prepare.convert",
            step_state=StepState.FAILED,
            error="ValueError: bad row",
            step_started_at=T0,
            step_ended_at=T0 + timedelta(minutes=30),
        )
    )
    assert view is not None
    assert view.key == f"orders/{RUN}"
    assert view.title == "orders 2026.10.1"
    assert view.state is ProcessState.RUNNING
    assert view.now == NOW and view.started_at == T0 and view.ended_at is None
    convert, geometry = view.steps
    assert convert.state is StepState.FAILED and convert.error == "ValueError: bad row"
    assert (convert.started_at, convert.ended_at) == (T0, T0 + timedelta(minutes=30))
    assert geometry.state is StepState.PENDING


def test_other_steps_carry_their_state_but_no_times() -> None:
    view = source({"prepare.convert": "success", "prepare.geometry": "running"}).snapshot(
        event(EventKind.STEP_STARTED, step="prepare.geometry", step_state=StepState.RUNNING)
    )
    assert view is not None
    convert, geometry = view.steps
    assert convert.state is StepState.SUCCEEDED
    assert (convert.started_at, convert.ended_at) == (None, None)
    assert geometry.state is StepState.RUNNING and geometry.started_at == NOW


def test_a_finished_step_without_an_end_date_ends_at_the_event_time() -> None:
    view = source({}).snapshot(
        event(EventKind.STEP_FINISHED, step="prepare.convert", step_state=StepState.SUCCEEDED)
    )
    assert view is not None and view.steps[0].ended_at == NOW


def test_upstream_failed_steps_explain_themselves() -> None:
    view = source({"prepare.convert": "failed", "prepare.geometry": "upstream_failed"}).snapshot(
        event(EventKind.RUN_FINISHED, process_state=ProcessState.FAILED)
    )
    assert view is not None
    assert view.steps[1].state is StepState.FAILED
    assert view.steps[1].error == "An upstream task failed"


def test_run_finished_uses_the_event_state_and_the_run_times() -> None:
    end = T0 + timedelta(hours=1)
    view = source({"prepare.convert": "success", "prepare.geometry": "success"}).snapshot(
        event(EventKind.RUN_FINISHED, process_state=ProcessState.SUCCEEDED, run_ended_at=end)
    )
    assert view is not None
    assert view.state is ProcessState.SUCCEEDED
    assert (view.started_at, view.ended_at) == (T0, end)
    assert all(s.state is StepState.SUCCEEDED for s in view.steps)


def test_a_minimal_dag_callback_payload_still_gives_a_final_view() -> None:
    # A DAG callback without dag_run: no params, no run times.
    view = source({}).snapshot(
        event(
            EventKind.RUN_FINISHED,
            process_state=ProcessState.FAILED,
            params=None,
            run_started_at=None,
        )
    )
    assert view is not None
    assert view.title == f"orders · {RUN}"
    assert view.started_at is None and view.ended_at == NOW


def test_mapped_task_keys_do_not_match_milestones() -> None:
    view = source({"prepare.convert_0": "failed"}).snapshot(
        event(EventKind.STEP_STARTED, step="prepare.geometry", step_state=StepState.RUNNING)
    )
    assert view is not None and view.steps[0].state is StepState.PENDING


def test_links_are_url_encoded() -> None:
    view = source({}).snapshot(
        event(EventKind.STEP_STARTED, step="prepare.convert", step_state=StepState.RUNNING)
    )
    assert view is not None
    run_url = f"{UI}/dags/orders/runs/manual__2026-10-02T08%3A05%3A00%2B00%3A00"
    assert [link.url for link in view.links] == [run_url]
    assert view.steps[0].links[0].url == f"{run_url}/tasks/prepare.convert"


def test_no_ui_base_url_means_no_links() -> None:
    view = source({}, ui_base_url=None).snapshot(
        event(EventKind.STEP_STARTED, step="prepare.convert", step_state=StepState.RUNNING)
    )
    assert view is not None
    assert view.links == () and all(s.links == () for s in view.steps)


def test_missing_params_fall_back_to_dag_and_run() -> None:
    assert (
        render_title("{{ params.version }}", params={}, dag_id="orders", run_id=RUN)
        == f"orders · {RUN}"
    )
    assert (
        render_title("{{ dag_id }} {{ params.v }}", params={"v": "1"}, dag_id="orders", run_id=RUN)
        == "orders 1"
    )


def test_no_milestones_means_no_view() -> None:
    with DAG("orders", schedule=None) as plain_dag:
        EmptyOperator(task_id="x")
    assert source({}).snapshot(event(EventKind.STEP_STARTED, the_dag=plain_dag)) is None


def test_naive_payload_times_are_read_as_utc() -> None:
    view = source({}).snapshot(
        event(
            EventKind.STEP_FINISHED,
            step="prepare.convert",
            step_state=StepState.SUCCEEDED,
            run_started_at=datetime(2026, 10, 2, 8, 5),
            step_ended_at=datetime(2026, 10, 2, 8, 6),
        )
    )
    assert view is not None
    assert view.started_at == T0 and view.started_at.tzinfo is not None
    assert view.steps[0].ended_at == datetime(2026, 10, 2, 8, 6, tzinfo=timezone.utc)


def test_a_failing_title_template_warns_once_per_template(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(source_module, "_warned_templates", set())
    with caplog.at_level(logging.WARNING, logger="chappe"):
        for _ in range(3):
            assert (
                render_title("{{ params.version }}", params={}, dag_id="orders", run_id=RUN)
                == f"orders · {RUN}"
            )
        assert render_title("{{ broken", params={}, dag_id="orders", run_id=RUN) == (
            f"orders · {RUN}"
        )
        assert render_title("{{ dag_id }}", params={}, dag_id="orders", run_id=RUN) == "orders"
    first, second = [r.getMessage() for r in caplog.records]
    assert "'{{ params.version }}'" in first and "has no attribute 'version'" in first
    assert "'{{ broken'" in second
