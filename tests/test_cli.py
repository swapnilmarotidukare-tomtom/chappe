# tests/test_cli.py
from pathlib import Path

import pytest

from chappe.cli import build_parser, main

OK_CONFIG = "chappe:\n  processes:\n    p:\n      dags: [{dag_id: d}]\n      channel: C0123456789\n"


def test_validate_config_ok(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "c.yaml"
    path.write_text(OK_CONFIG)
    assert main(["validate-config", str(path)]) == 0
    assert "OK (1 processes)" in capsys.readouterr().out


def test_validate_config_reads_chappe_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "c.yaml"
    path.write_text(OK_CONFIG)
    monkeypatch.setenv("CHAPPE_CONFIG", str(path))
    assert main(["validate-config"]) == 0
    assert "OK" in capsys.readouterr().out


def test_validate_config_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "c.yaml"
    path.write_text(OK_CONFIG.replace("C0123456789", "'#x'"))
    assert main(["validate-config", str(path)]) == 1
    assert "channel ID" in capsys.readouterr().err


def test_a_command_is_required() -> None:
    with pytest.raises(SystemExit) as info:
        main([])
    assert info.value.code == 2


def test_every_command_has_a_handler() -> None:
    args = build_parser().parse_args(["validate-config", "x.yaml"])
    assert callable(args.handler)


def test_validate_config_rejects_an_unknown_theme(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "c.yaml"
    bad_default = "chappe:\n  defaults:\n    theme: {name: neon}\n"
    path.write_text(OK_CONFIG.replace("chappe:\n", bad_default))
    assert main(["validate-config", str(path)]) == 1
    err = capsys.readouterr().err
    assert "defaults.theme: unknown theme 'neon'; available themes: ledger, metro, plain" in err


def test_validate_config_rejects_an_unknown_process_theme(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "c.yaml"
    path.write_text(OK_CONFIG + "      theme: {name: neon}\n")
    assert main(["validate-config", str(path)]) == 1
    assert "processes.p.theme: unknown theme 'neon'" in capsys.readouterr().err


def test_validate_config_rejects_bad_token_overrides(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "c.yaml"
    path.write_text(OK_CONFIG + "      theme: {name: plain, tokens: {colours: {}}}\n")
    assert main(["validate-config", str(path)]) == 1
    err = capsys.readouterr().err
    assert "processes.p.theme: unknown token section(s) ['colours']" in err
