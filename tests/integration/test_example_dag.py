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


GLYPHS = "●◉✖◌○"


def block(text: str) -> list[str]:
    lines = text.split("\n")
    start = lines.index("```")
    end = lines.index("```", start + 1)
    return lines[start + 1 : end]


def station_names(text: str) -> list[str]:
    """`<glyph> <name>` for each station of the parent's code block, in order."""
    return [f"{line[0]} {line[3:].split('  ')[0]}" for line in block(text) if line[:1] in GLYPHS]


def connectors(text: str) -> list[str]:
    return [line for line in block(text) if line in ("┃", "┆")]


def run(module: ModuleType, api: FakeSlackApi, conf: dict[str, Any], reason: str) -> str:
    dag_run = module.dag.test(run_conf=conf)
    assert str(getattr(dag_run.state, "value", dag_run.state)) == (
        "success" if reason == "success" else "failed"
    )
    # What the task callbacks (@milestone) left during dag.test(): a parent, not yet final.
    assert any(name == "post" for name, _ in api.calls)
    (parent,) = api.top_level(CHANNEL)
    status = parent.text.split("\n")[1]
    assert status.startswith(":large_yellow_circle: In progress")
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


def test_passing_run_posts_one_message_and_nothing_in_its_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_example()
    api, store = wire(monkeypatch, {t.task_id: "success" for t in module.dag.tasks})
    run_id = run(module, api, {}, "success")

    (parent,) = api.top_level(CHANNEL)
    lines = parent.text.split("\n")
    # a known limit: README.md, "Known limits of 0.0.1" (DAG callback without dag_run or params)
    # the notifier's minimal DAG-callback context carries no params and no run times, so the
    # title falls back to "<dag_id> · <run_id>" and the status line has no "started" part
    assert lines[0].startswith("*chappe_example · ")
    assert lines[1] == ":large_green_circle: Passed"
    assert station_names(parent.text) == ["● Extract", "● Transform", "● Load", "● Report"]
    assert connectors(parent.text) == ["┃", "┃", "┃"]
    assert api.replies(CHANNEL, parent.ts) == []
    assert_kept(store, run_id, parent.ts)


def test_failing_run_shows_the_failed_station_and_alerts_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_example()
    states = {t.task_id: "success" for t in module.dag.tasks}
    states["load"] = "failed"
    states["report"] = "upstream_failed"
    api, store = wire(monkeypatch, states)
    run_id = run(module, api, {"fail": True}, "task_failure")

    (parent,) = api.top_level(CHANNEL)
    assert parent.text.split("\n")[1] == ":red_circle: Failed"
    # upstream_failed maps to StepState.FAILED (integrations/airflow/source.py)
    assert station_names(parent.text) == ["● Extract", "● Transform", "✖ Load", "✖ Report"]
    thread = api.replies(CHANNEL, parent.ts)
    (alert,) = thread
    assert alert.text.startswith("<@U0123456789> :rotating_light: *chappe_example · ")
    assert "Load" in alert.text
    assert_kept(store, run_id, parent.ts)
