from pathlib import Path

import pytest

from chappe.cli import main

CONFIG = (
    "chappe:\n  processes:\n    orders:\n      dags: [{dag_id: d}]\n      channel: C0123456789\n"
)

DAG = """\
from airflow.sdk import DAG
from chappe.integrations.airflow import ChappeNotifier
import chappe.integrations.airflow as chappe_airflow

name = "dynamic"
with DAG("d", on_success_callback=ChappeNotifier(process="orders")):
    pass
with DAG("e", on_failure_callback=ChappeNotifier("ordres")):
    pass
with DAG("f", on_failure_callback=chappe_airflow.ChappeNotifier(process="nope")):
    pass
with DAG("g", on_failure_callback=ChappeNotifier(process=name)):
    pass
with DAG("h", on_failure_callback=ChappeNotifier()):
    pass
with DAG("i", on_failure_callback=ChappeNotifier(process=None)):
    pass
other = SomethingElse(process="ghost")
"""


def setup(tmp_path: Path, dag: str = DAG) -> tuple[Path, Path]:
    config = tmp_path / "c.yaml"
    config.write_text(CONFIG)
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "orders_dag.py").write_text(dag)
    return config, dags


def test_unknown_literal_names_are_errors_with_file_and_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config, dags = setup(tmp_path)
    assert main(["validate-config", str(config), "--dags", str(dags)]) == 1
    captured = capsys.readouterr()
    file = dags / "orders_dag.py"
    assert f"{file}:8: ChappeNotifier(process='ordres') is not a configured process" in captured.err
    assert f"{file}:10: ChappeNotifier(process='nope')" in captured.err
    assert "configured: orders" in captured.err
    assert "'orders')" not in captured.err and "ghost" not in captured.err
    assert "OK" not in captured.out


def test_non_literal_names_are_info_and_do_not_fail(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dag = "name = 'x'\nChappeNotifier(process=name)\nChappeNotifier(process='orders')\n"
    config, dags = setup(tmp_path, dag)
    file = dags / "orders_dag.py"
    assert main(["validate-config", str(config), "--dags", str(file)]) == 0
    out = capsys.readouterr().out
    assert f"{file}:2: ChappeNotifier process cannot be checked (not a string literal)" in out
    assert "OK (1 processes)" in out


def test_dags_option_is_repeatable_and_a_syntax_error_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config, dags = setup(tmp_path, "ChappeNotifier(process='orders')\n")
    broken = tmp_path / "broken.py"
    broken.write_text("def (:\n")
    assert main(["validate-config", str(config), "--dags", str(dags), "--dags", str(broken)]) == 1
    assert "broken.py: cannot scan" in capsys.readouterr().err


def test_a_missing_dags_path_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config, _ = setup(tmp_path)
    assert main(["validate-config", str(config), "--dags", str(tmp_path / "nope.py")]) == 1
    assert "cannot scan" in capsys.readouterr().err


def test_without_dags_nothing_is_scanned(tmp_path: Path) -> None:
    config, _ = setup(tmp_path)
    assert main(["validate-config", str(config)]) == 0
