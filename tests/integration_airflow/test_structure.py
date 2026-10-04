import logging

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup, task

from chappe.config.models import MilestoneOverride, ProcessConfig, SectionOverride
from chappe.integrations.airflow import callbacks
from chappe.integrations.airflow.decorators import milestone
from chappe.integrations.airflow.marker import milestone_spec
from chappe.integrations.airflow.structure import build_structure, readable


def process(**overrides: object) -> ProcessConfig:
    data: dict[str, object] = {"dags": [{"dag_id": "orders_pipeline"}], "channel": "C0123456789"}
    data.update(overrides)
    return ProcessConfig.model_validate(data)


def grouped_dag() -> DAG:
    with DAG("orders_pipeline", schedule=None) as dag:
        with TaskGroup("prepare", group_display_name="Prepare data"):
            convert = milestone(
                EmptyOperator(task_id="convert", task_display_name="Parquet → Delta")
            )
            cleanup = EmptyOperator(task_id="cleanup")
            geometry = milestone(EmptyOperator(task_id="geometry"), "Geometry")
            convert >> cleanup >> geometry
        with TaskGroup("compare"), TaskGroup("inner"):
            stability = milestone(EmptyOperator(task_id="id_stability"), "ID stability")
        geometry >> stability
    return dag


def test_sections_come_from_outermost_task_groups() -> None:
    structure = build_structure(grouped_dag(), process())
    assert [(s.key, s.title) for s in structure.sections] == [
        ("prepare", "Prepare data"),
        ("compare", "Compare"),
    ]
    assert [(s.task_id, s.title, s.section_key) for s in structure.steps] == [
        ("prepare.convert", "Parquet → Delta", "prepare"),
        ("prepare.geometry", "Geometry", "prepare"),
        ("compare.inner.id_stability", "ID stability", "compare"),
    ]


def test_no_task_groups_gives_one_default_section() -> None:
    with DAG("orders_pipeline", schedule=None) as dag:
        a = milestone(EmptyOperator(task_id="a"))
        b = milestone(EmptyOperator(task_id="b_step"))
        b >> a
    structure = build_structure(
        dag, process(dags=[{"dag_id": "orders_pipeline", "section": "Orders"}])
    )
    assert [(s.key, s.title) for s in structure.sections] == [("orders_pipeline", "Orders")]
    assert [s.task_id for s in structure.steps] == ["b_step", "a"]  # dependency order
    assert [s.title for s in structure.steps] == ["B step", "A"]


def test_config_overrides_titles_sections_and_hidden() -> None:
    structure = build_structure(
        grouped_dag(),
        process(
            sections={"prepare": SectionOverride(title="Prep")},
            milestones={
                "prepare.geometry": MilestoneOverride(title="Generate geometry", section="Geo"),
                "prepare.convert": MilestoneOverride(hidden=True),
            },
        ),
    )
    assert [(s.key, s.title) for s in structure.sections] == [
        ("Geo", "Geo"),
        ("compare", "Compare"),
    ]
    assert [s.title for s in structure.steps] == ["Generate geometry", "ID stability"]


def test_decorator_section_argument_wins() -> None:
    with DAG("orders_pipeline", schedule=None) as dag, TaskGroup("prepare"):
        milestone(EmptyOperator(task_id="x"), "X", section="Special")
    structure = build_structure(dag, process())
    assert [(s.key, s.title) for s in structure.sections] == [("Special", "Special")]


def test_taskflow_decorator_marks_the_operator_and_keeps_user_callbacks() -> None:
    def mine(context: object) -> None:
        return None

    with DAG("orders_pipeline", schedule=None) as dag:

        @milestone("Convert")
        @task(on_success_callback=mine)
        def convert() -> None:
            return None

        convert()

    op = dag.get_task("convert")
    spec = milestone_spec(op)
    assert spec is not None and spec.title == "Convert"
    assert op.on_success_callback == [mine, callbacks.on_step_succeeded]
    assert callbacks.on_step_started in op.on_execute_callback


def test_mapped_milestone_is_ignored_with_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    with (
        caplog.at_level(logging.WARNING, logger="chappe"),
        DAG("orders_pipeline", schedule=None) as dag,
    ):

        @milestone("Compare")
        @task
        def compare(version: str) -> None:
            return None

        compare.expand(version=["a", "b"])
    assert "mapped tasks are not supported" in caplog.text
    assert build_structure(dag, process()).steps == ()


def test_readable() -> None:
    assert readable("prepare_data-v2") == "Prepare data v2"


def test_bare_milestone_above_task() -> None:
    with DAG("orders_pipeline", schedule=None) as dag:

        @milestone
        @task
        def convert_data() -> None:
            return None

        convert_data()

    spec = milestone_spec(dag.get_task("convert_data"))
    assert spec is not None and spec.title is None
    assert [s.title for s in build_structure(dag, process()).steps] == ["Convert data"]


def test_default_section_falls_back_to_the_dag_id() -> None:
    with DAG("orders_pipeline", schedule=None) as dag:
        milestone(EmptyOperator(task_id="a"))
    structure = build_structure(dag, process())
    assert [(s.key, s.title) for s in structure.sections] == [
        ("orders_pipeline", "Orders pipeline")
    ]


def test_title_falls_back_to_display_name_then_readable_task_id() -> None:
    with DAG("orders_pipeline", schedule=None) as dag:
        milestone(EmptyOperator(task_id="shown", task_display_name="Shown name"))
        milestone(EmptyOperator(task_id="load_to_delta"))
    assert [s.title for s in build_structure(dag, process()).steps] == [
        "Shown name",
        "Load to delta",
    ]


def test_milestone_on_something_unsupported_warns_and_returns_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = object()
    with caplog.at_level(logging.WARNING, logger="chappe"):
        assert milestone(target) is target
    assert "cannot mark" in caplog.text


def test_tuple_callbacks_are_extended() -> None:
    def mine(context: object) -> None:
        return None

    with DAG("orders_pipeline", schedule=None):
        op = EmptyOperator(task_id="t")
    op.on_success_callback = (mine,)  # type: ignore[assignment]
    milestone(op)
    assert op.on_success_callback == [mine, callbacks.on_step_succeeded]


def test_marking_failure_does_not_raise(caplog: pytest.LogCaptureFixture) -> None:
    class Broken(EmptyOperator):
        def __setattr__(self, name: str, value: object) -> None:
            if name == "chappe_milestone":
                raise RuntimeError("boom")
            super().__setattr__(name, value)

    with caplog.at_level(logging.WARNING, logger="chappe"), DAG("orders_pipeline", schedule=None):
        op = Broken(task_id="t")
        assert milestone(op) is op
    assert "could not mark" in caplog.text
