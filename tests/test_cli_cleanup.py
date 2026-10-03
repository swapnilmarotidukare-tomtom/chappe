# tests/test_cli_cleanup.py
from datetime import datetime, timedelta, timezone

import pytest

from chappe.cli import main, parse_duration
from chappe.core.reconcile import SentState
from chappe.stores import airflow_variable
from chappe.stores.airflow_variable import AirflowVariableStore, key_for
from tests.support.fakes import FakeVariables


def test_parse_duration() -> None:
    assert parse_duration("7d") == timedelta(days=7)
    assert parse_duration("12h") == timedelta(hours=12)


@pytest.mark.parametrize("text", ["", "7", "7w", "d", "-1d", "0d", "1.5d"])
def test_parse_duration_rejects_other_forms(text: str) -> None:
    with pytest.raises(ValueError, match="for example 7d"):
        parse_duration(text)


@pytest.fixture
def variables(monkeypatch: pytest.MonkeyPatch) -> FakeVariables:
    fake = FakeVariables()
    monkeypatch.setattr(airflow_variable, "airflow_db_keys", fake.keys)
    monkeypatch.setattr(airflow_variable, "airflow_db_get", fake.get)
    monkeypatch.setattr(airflow_variable, "airflow_db_delete", fake.delete)
    return fake


def test_cleanup_command(variables: FakeVariables, capsys: pytest.CaptureFixture[str]) -> None:
    long_ago = datetime(2020, 1, 1, tzinfo=timezone.utc)
    store = AirflowVariableStore(get=variables.get, set=variables.set, now=lambda: long_ago)
    store.save("d/r", SentState("d/r"))

    assert main(["cleanup", "--older-than", "7d", "--dry-run"]) == 0
    assert f"would delete {key_for('d/r')}" in capsys.readouterr().out
    assert variables.keys() == [key_for("d/r")]

    assert main(["cleanup", "--older-than", "7d"]) == 0
    assert "deleted 1 Variable(s)" in capsys.readouterr().out
    assert variables.keys() == []


def test_cleanup_reports_airflow_errors(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def unreachable() -> list[str]:
        raise RuntimeError("database unreachable")

    monkeypatch.setattr(airflow_variable, "airflow_db_keys", unreachable)
    assert main(["cleanup", "--older-than", "12h"]) == 1
    assert "database unreachable" in capsys.readouterr().err


def test_cleanup_rejects_a_bad_duration(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["cleanup", "--older-than", "7w"])
    assert exc.value.code == 2
    assert "--older-than" in capsys.readouterr().err
