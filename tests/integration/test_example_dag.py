import importlib.util
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

from chappe.config.loader import load_settings
from chappe.core.engine import EngineSettings
from chappe.integrations.airflow import runtime as runtime_module
from chappe.integrations.airflow.connections import ConnectionInfo
from chappe.integrations.airflow.notifier import ChappeNotifier
from chappe.integrations.airflow.runtime import Runtime
from chappe.integrations.airflow.source import process_key
from chappe.stores.airflow_variable import AirflowVariableStore
from chappe.transports.slack.transport import SlackTransport
from tests.support.fakes import FakeSlackApi, FakeVariables

pytestmark = [
    pytest.mark.integration,
    # raised inside Airflow during dag.test() (its execution API client, its ORM), not by Chappe
    pytest.mark.filterwarnings("ignore:Using `httpx` with `starlette.testclient` is deprecated"),
    pytest.mark.filterwarnings("ignore:The ``noload`` loader strategy is deprecated"),
]
ROOT = Path(__file__).resolve().parents[2]
DAGS = ROOT / "examples/dags"
CHANNEL = "C0123456789"


_db_ready = False


@pytest.fixture(autouse=True)
def airflow_db(network_attempts: list[str]) -> None:
    """A fresh sqlite metadata DB in the throwaway AIRFLOW_HOME set by tests/conftest.py.

    Function-scoped so a network attempt during initdb fails a test; built once per session.
    """
    global _db_ready
    if _db_ready:
        return
    from airflow import settings
    from airflow.utils.db import initdb

    home = os.environ["AIRFLOW_HOME"]
    assert str(settings.engine.url) == f"sqlite:///{home}/airflow.db"  # never ~/airflow
    initdb()
    _db_ready = True


@pytest.fixture(autouse=True)
def airflow_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """dag.test() syncs the DAG from its bundle (the dags folder) into the DB first."""
    from airflow import settings

    # the default resolves the host's FQDN through DNS: a network call
    monkeypatch.setenv("AIRFLOW__CORE__HOSTNAME_CALLABLE", "socket.gethostname")

    monkeypatch.setattr(settings, "DAGS_FOLDER", str(DAGS))
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS))
    monkeypatch.setattr(sys, "dont_write_bytecode", True)  # no __pycache__ in examples/


@pytest.fixture(autouse=True)
def reset_runtime() -> Iterator[None]:
    yield
    runtime_module.set_runtime(None)


def load_example() -> ModuleType:
    spec = importlib.util.spec_from_file_location("chappe_example", DAGS / "chappe_example.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def wire(
    monkeypatch: pytest.MonkeyPatch, states: dict[str, str]
) -> tuple[FakeSlackApi, AirflowVariableStore]:
    api, variables = FakeSlackApi(), FakeVariables()
    store = AirflowVariableStore(get=variables.get, set=variables.set)
    runtime = Runtime(
        load_settings(ROOT / "examples/chappe.yaml"),
        connections=lambda _: ConnectionInfo(password="xoxb-test"),
    )
    engine = runtime.engine("example")
    engine._transport = SlackTransport(api)  # test seam: replace the real Slack client
    engine._store = store  # test seam: Variables in memory
    engine._settings = EngineSettings(channel=CHANNEL, final_check_delay_s=0)
    runtime_module.set_runtime(runtime)

    def get_task_states(dag_id: str, run_ids: list[str], **_: Any) -> dict[str, dict[str, str]]:
        return {run_id: dict(states) for run_id in run_ids}

    monkeypatch.setattr(RuntimeTaskInstance, "get_task_states", staticmethod(get_task_states))
    monkeypatch.setattr("time.sleep", lambda s: None)
    return api, store


STEPS = ["Extract", "Transform", "Load", "Report"]


def replies_by_step(texts: list[str]) -> dict[str, list[str]]:
    """Step replies (`<icon> *<name>* · ...`) grouped by step name, in posting order."""
    found: dict[str, list[str]] = {}
    for text in texts:
        if text.count("*") >= 2 and not text.startswith("<@"):
            found.setdefault(text.split("*")[1], []).append(text.split("* · ", 1)[1])
    return found


def run(module: ModuleType, api: FakeSlackApi, conf: dict[str, Any], reason: str) -> str:
    dag_run = module.dag.test(run_conf=conf)
    assert str(getattr(dag_run.state, "value", dag_run.state)) == (
        "success" if reason == "success" else "failed"
    )
    # What the task callbacks (@milestone) left during dag.test(): a parent, not yet final.
    assert any(name == "post" for name, _ in api.calls)
    (parent,) = api.top_level(CHANNEL)
    header = parent.text.split("\n")[0]
    assert header.startswith(":large_yellow_circle: *<") and " · In progress" in header
    # The DAG callback, as the DAG processor sends it (minimal context, finding 5). On Airflow
    # 3.2.2, dag.test()'s own DAG callback passes a SerializedDAG whose tasks carry no milestone
    # marker, so it sends nothing.
    ChappeNotifier().notify({"dag": module.dag, "run_id": dag_run.run_id, "reason": reason})
    return str(dag_run.run_id)


def assert_kept(store: AirflowVariableStore, run_id: str, parent_ts: str) -> None:
    stored = store.load(process_key("chappe_example", run_id))
    assert stored is not None and stored.parent_ref == parent_ts
    assert stored.watermark is not None and stored.watermark.finished


def test_the_example_is_one_flat_dag_of_four_milestones() -> None:
    module = load_example()
    assert [t.task_id for t in module.dag.topological_sort()] == [
        "extract",
        "transform",
        "load",
        "report",
    ]


def test_passing_run_lists_every_step_with_each_start_and_end_in_the_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_example()
    api, store = wire(monkeypatch, {t.task_id: "success" for t in module.dag.tasks})
    run_id = run(module, api, {}, "success")

    (parent,) = api.top_level(CHANNEL)
    # a known limit: README.md, "Known limits of 0.0.1" (DAG callback without dag_run or params)
    # the notifier's minimal DAG-callback context carries no params and no run times, so the
    # title falls back to "<dag_id> · <run_id>" and the header has no duration or "started" part
    lines = parent.text.split("\n")
    assert lines[0].startswith(":large_green_circle: *<http://localhost:8080/dags/chappe_example/")
    assert "|chappe_example · " in lines[0] and lines[0].endswith(">* · Completed")
    # the example config uses the custom :chappe-*: icons for steps
    assert lines[1:] == [f":chappe-done: {name}" for name in STEPS]
    replies = [m.text for m in api.replies(CHANNEL, parent.ts)]
    assert not any(m.broadcast for m in api.replies(CHANNEL, parent.ts))
    steps = replies_by_step(replies)
    assert sorted(steps) == sorted(STEPS)
    for name in STEPS:
        started, ended = steps[name]
        assert started == "started", (name, started)
        assert ended.startswith("Completed"), (name, ended)
    assert_kept(store, run_id, parent.ts)


def test_failing_run_lists_the_failing_steps_and_alerts_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_example()
    states = {t.task_id: "success" for t in module.dag.tasks}
    states["load"] = "failed"
    states["report"] = "upstream_failed"
    api, store = wire(monkeypatch, states)
    run_id = run(module, api, {"fail": True}, "task_failure")

    (parent,) = api.top_level(CHANNEL)
    lines = parent.text.split("\n")
    assert lines[0].startswith(":red_circle: *<") and lines[0].endswith(">* · Failed")
    # upstream_failed maps to StepState.FAILED (integrations/airflow/source.py)
    assert lines[1:] == [
        "2 completed · 2 failed",
        ":chappe-done: Extract",
        ":chappe-done: Transform",
        ":chappe-failed: Load · Failed",
        ":chappe-failed: Report · Failed",
    ]
    replies = [m.text for m in api.replies(CHANNEL, parent.ts)]
    (alert,) = [text for text in replies if text.startswith("<@U0123456789>")]
    assert alert.startswith("<@U0123456789> :red_circle: *chappe_example · ")
    assert "* · Load" in alert and "Report: An upstream task failed" in alert
    steps = replies_by_step(replies)
    assert steps["Load"][0] == "started"
    assert steps["Load"][-1].startswith("Failed ")
    assert steps["Report"] == ["Failed\nAn upstream task failed"]  # never started: end only
    assert_kept(store, run_id, parent.ts)
