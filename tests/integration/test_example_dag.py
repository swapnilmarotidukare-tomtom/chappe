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


@pytest.fixture(scope="module", autouse=True)
def airflow_db() -> None:
    """A fresh sqlite metadata DB in the throwaway AIRFLOW_HOME set by tests/conftest.py."""
    from airflow import settings
    from airflow.utils.db import initdb

    home = os.environ["AIRFLOW_HOME"]
    assert str(settings.engine.url) == f"sqlite:///{home}/airflow.db"  # never ~/airflow
    initdb()


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
        connections=lambda _: ConnectionInfo(
            host=None, login=None, password="xoxb-test", port=None
        ),
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


def run(module: ModuleType, conf: dict[str, Any], reason: str) -> str:
    dag_run = module.dag.test(run_conf=conf)
    ChappeNotifier().notify({"dag": module.dag, "run_id": dag_run.run_id, "reason": reason})
    return str(dag_run.run_id)


def test_passing_run_posts_one_parent_and_keeps_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_example()
    api, store = wire(monkeypatch, {t.task_id: "success" for t in module.dag.tasks})
    run_id = run(module, {}, "success")

    (parent,) = api.top_level(CHANNEL)
    assert "Passed" in parent.text and "4/4" in parent.text  # cleanup_tmp is not a milestone
    (final,) = api.replies(CHANNEL, parent.ts)
    assert "Passed" in final.text
    stored = store.load(process_key("chappe_example", run_id))
    assert stored is not None and stored.parent_ref == parent.ts
    assert stored.watermark is not None and stored.watermark.finished


def test_failing_run_ends_failed_with_an_alert(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_example()
    states = {t.task_id: "success" for t in module.dag.tasks}
    states["compare.regression"] = "failed"
    api, _ = wire(monkeypatch, states)
    run(module, {"fail": True}, "task_failure")

    (parent,) = api.top_level(CHANNEL)
    assert "Failed" in parent.text
    replies = [m.text for m in api.replies(CHANNEL, parent.ts)]
    assert any(
        text.startswith("<@U0123456789>") and "Regression checks" in text for text in replies
    )
