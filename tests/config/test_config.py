# tests/config/test_config.py
from pathlib import Path

import pytest

from chappe.config.loader import chappe_enabled, load_settings
from chappe.config.models import ProcessConfig
from chappe.core.errors import ChappeConfigError

VALID = """
chappe:
  processes:
    orders:
      dags: [{dag_id: orders_pipeline, section: Orders}]
      channel: C0123456789
      title: "{{ params.product }} {{ params.version }}"
      alerts: {mention: "<!subteam^S0123>"}
      milestones:
        cleanup: {hidden: true}
"""


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "chappe.yaml"
    path.write_text(text)
    return path


def with_defaults(defaults: str) -> str:
    return VALID.replace("chappe:\n", f"chappe:\n  defaults:\n{defaults}")


def orders(tmp_path: Path, text: str = VALID) -> ProcessConfig:
    found = load_settings(write(tmp_path, text)).process_for_dag("orders_pipeline")
    assert found is not None
    return found[1]


def test_valid_config_loads_with_defaults(tmp_path: Path) -> None:
    settings = load_settings(write(tmp_path, VALID))
    found = settings.process_for_dag("orders_pipeline")
    assert found is not None
    name, process = found
    assert name == "orders"
    assert process.channel == "C0123456789"
    assert settings.defaults.store.type == "airflow_variable"
    assert settings.defaults.transport.type == "slack"
    assert settings.defaults.transport.connection_id == "chappe_slack"
    assert settings.defaults.ui_base_url is None
    assert settings.theme_for(process).name == "plain"
    assert process.milestones["cleanup"].hidden
    assert settings.process_for_dag("other") is None


def test_ui_base_url_is_read_from_defaults(tmp_path: Path) -> None:
    text = with_defaults("    ui_base_url: https://airflow.example.com\n")
    settings = load_settings(write(tmp_path, text))
    assert settings.defaults.ui_base_url == "https://airflow.example.com"


def test_only_the_airflow_variable_store_is_accepted(tmp_path: Path) -> None:
    text = with_defaults("    store: {type: slack_metadata}\n")
    with pytest.raises(ChappeConfigError, match="airflow_variable"):
        load_settings(write(tmp_path, text))


def test_the_old_airflow_api_block_is_rejected(tmp_path: Path) -> None:
    text = with_defaults("    airflow_api: {connection_id: chappe_airflow_api}\n")
    with pytest.raises(ChappeConfigError, match="airflow_api"):
        load_settings(write(tmp_path, text))


@pytest.mark.parametrize("channel", ["'#pipeline-runs'", "pipeline-runs", "D0123456789"])
def test_channel_must_be_a_channel_id(tmp_path: Path, channel: str) -> None:
    with pytest.raises(ChappeConfigError) as info:
        load_settings(write(tmp_path, VALID.replace("C0123456789", channel)))
    assert "channel ID" in str(info.value)
    assert "About panel" in str(info.value)


def test_dm_id_is_named_as_a_dm(tmp_path: Path) -> None:
    with pytest.raises(ChappeConfigError, match="direct-message"):
        load_settings(write(tmp_path, VALID.replace("C0123456789", "D0123456789")))


def test_private_channel_id_is_accepted(tmp_path: Path) -> None:
    assert orders(tmp_path, VALID.replace("C0123456789", "G0123456789")).channel == "G0123456789"


def test_bare_yaml_on_key_is_read_as_on(tmp_path: Path) -> None:
    text = VALID.replace('"<!subteam^S0123>"}', '"<!subteam^S0123>", on: final_failure}')
    assert orders(tmp_path, text).alerts.on == "final_failure"


def test_more_than_one_dag_is_rejected_until_0_0_3(tmp_path: Path) -> None:
    text = VALID.replace(
        "dags: [{dag_id: orders_pipeline, section: Orders}]", "dags: [{dag_id: a}, {dag_id: b}]"
    )
    with pytest.raises(ChappeConfigError, match=r"0\.0\.3"):
        load_settings(write(tmp_path, text))


def test_unknown_keys_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ChappeConfigError, match="chanel"):
        load_settings(write(tmp_path, VALID.replace("channel:", "chanel: x\n      channel:")))


def test_bad_title_template_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ChappeConfigError, match="title"):
        load_settings(write(tmp_path, VALID.replace("{{ params.version }}", "{{ params.version ")))


def test_unknown_timezone_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ChappeConfigError, match="unknown timezone"):
        load_settings(write(tmp_path, with_defaults("    time: {timezone: Mars/Base}\n")))


def test_a_dag_in_two_processes_is_rejected(tmp_path: Path) -> None:
    text = (
        VALID
        + """
    copy:
      dags: [{dag_id: orders_pipeline}]
      channel: C0123456789
"""
    )
    with pytest.raises(ChappeConfigError, match="orders_pipeline"):
        load_settings(write(tmp_path, text))


def test_missing_config_path_is_explained(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CHAPPE_CONFIG", raising=False)
    with pytest.raises(ChappeConfigError, match="CHAPPE_CONFIG"):
        load_settings()


def test_config_path_comes_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CHAPPE_CONFIG", str(write(tmp_path, VALID)))
    assert "orders" in load_settings().processes


def test_kill_switch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = load_settings(write(tmp_path, VALID))
    monkeypatch.delenv("CHAPPE_ENABLED", raising=False)
    assert chappe_enabled(settings)
    monkeypatch.setenv("CHAPPE_ENABLED", "false")
    assert not chappe_enabled(settings)
