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
