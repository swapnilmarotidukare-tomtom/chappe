import logging
import sys
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Any

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


@pytest.fixture
def fresh_runtime() -> Iterator[None]:
    """The next get_runtime() loads the config again; the global is reset after the test."""
    runtime_module.set_runtime(None)
    yield
    runtime_module.set_runtime(None)


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
    assert runtime.resolve("unknown", "orders") == runtime.resolve(None, "orders")  # warns
    assert runtime.resolve("unknown", "other") is None
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


@pytest.mark.usefixtures("fresh_runtime")
def test_missing_config_disables_chappe_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("CHAPPE_CONFIG", raising=False)
    assert runtime_module.get_runtime() is None
    assert runtime_module.get_runtime() is None
    assert caplog.text.count("chappe is disabled") == 1


@pytest.mark.usefixtures("fresh_runtime")
def test_an_unexpected_load_error_disables_chappe_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(path: Any = None) -> Any:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(runtime_module, "load_settings", broken)
    assert runtime_module.get_runtime() is None
    assert runtime_module.get_runtime() is None
    assert caplog.text.count("chappe is disabled") == 1
    assert "disk on fire" in caplog.text


@pytest.mark.usefixtures("fresh_runtime")
def test_config_is_loaded_from_chappe_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CHAPPE_CONFIG", str(write_config(tmp_path)))
    runtime = runtime_module.get_runtime()
    assert runtime is not None and runtime.resolve(None, "orders") is not None


def test_metrics_use_the_sdk_stats_without_deprecation_warnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # re-import, so an import-time deprecation warning would be seen here
    monkeypatch.delitem(sys.modules, "airflow.stats", raising=False)
    monkeypatch.delitem(sys.modules, "airflow.sdk.observability.stats", raising=False)
    counted: list[str] = []
    from airflow.sdk._shared.observability.metrics import stats

    monkeypatch.setattr(stats.Stats, "incr", lambda name, *a, **k: counted.append(name))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        runtime_module._metrics("chappe.events.sent")
    assert counted == ["chappe.events.sent"]


def test_unavailable_metrics_are_logged_once_at_debug(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setitem(sys.modules, "airflow.sdk.observability.stats", None)  # ImportError
    monkeypatch.setattr(runtime_module, "_metrics_warned", False)
    with caplog.at_level(logging.DEBUG, logger="chappe"):
        runtime_module._metrics("chappe.events.sent")
        runtime_module._metrics("chappe.events.sent")
    lines = [r for r in caplog.records if "metrics" in r.getMessage()]
    assert len(lines) == 1 and lines[0].levelno == logging.DEBUG
