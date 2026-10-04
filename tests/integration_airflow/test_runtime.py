import logging
from pathlib import Path

import pytest

from chappe.config.loader import load_settings
from chappe.core.engine import Engine
from chappe.core.errors import ChappeConfigError
from chappe.integrations.airflow import runtime as runtime_module
from chappe.integrations.airflow.connections import ConnectionInfo
from chappe.integrations.airflow.runtime import Runtime, pick_theme
from chappe.themes.builtin.plain import PlainTheme

CONFIG = """\
chappe:
  defaults:
    ui_base_url: http://localhost:8080
  processes:
    orders:
      dags: [{dag_id: orders}]
      channel: C0123456789
"""


def write_config(tmp_path: Path, text: str = CONFIG) -> Path:
    path = tmp_path / "chappe.yaml"
    path.write_text(text)
    return path


def test_builds_one_engine_per_process(tmp_path: Path) -> None:
    looked_up: list[str] = []

    def connections(conn_id: str) -> ConnectionInfo:
        looked_up.append(conn_id)
        return ConnectionInfo(host=None, login=None, password="xoxb-test", port=None)

    runtime = Runtime(load_settings(write_config(tmp_path)), connections=connections)
    assert runtime.resolve(None, "orders") is not None
    assert runtime.resolve(None, "unknown") is None
    assert runtime.resolve("unknown", "orders") is None
    engine = runtime.engine("orders")
    assert isinstance(engine, Engine) and runtime.engine("orders") is engine
    assert looked_up == ["chappe_slack"]


def test_a_slack_connection_without_a_token_is_a_config_error(tmp_path: Path) -> None:
    runtime = Runtime(
        load_settings(write_config(tmp_path)),
        connections=lambda _: ConnectionInfo(host=None, login=None, password=None, port=None),
    )
    with pytest.raises(ChappeConfigError, match="chappe_slack"):
        runtime.engine("orders")


def test_unknown_theme_falls_back_to_plain(caplog: pytest.LogCaptureFixture) -> None:
    assert isinstance(pick_theme("plain"), PlainTheme)
    with caplog.at_level(logging.WARNING, logger="chappe"):
        assert isinstance(pick_theme("fancy"), PlainTheme)
    assert "unknown theme 'fancy'" in caplog.text


def test_missing_config_disables_chappe_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("CHAPPE_CONFIG", raising=False)
    runtime_module.set_runtime(None)
    assert runtime_module.get_runtime() is None
    assert runtime_module.get_runtime() is None
    assert caplog.text.count("chappe is disabled") == 1
    runtime_module.set_runtime(None)


def test_config_is_loaded_from_chappe_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CHAPPE_CONFIG", str(write_config(tmp_path)))
    runtime_module.set_runtime(None)
    runtime = runtime_module.get_runtime()
    assert runtime is not None and runtime.resolve(None, "orders") is not None
    runtime_module.set_runtime(None)
