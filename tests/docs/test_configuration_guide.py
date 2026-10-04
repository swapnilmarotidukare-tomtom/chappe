"""The configuration guide's reference is generated from the config models (offline)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

from chappe.config.models import ConfigFile

ROOT = Path(__file__).resolve().parents[2]
GUIDE = ROOT / "docs" / "guides" / "configuration.md"
SCRIPT = ROOT / "scripts" / "render_config_reference.py"


def load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("render_config_reference", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_committed_guide_is_up_to_date() -> None:
    script = load_script()
    committed = GUIDE.read_text(encoding="utf-8")
    assert script.splice(committed, script.render()) == committed, (
        "docs/guides/configuration.md is out of date with src/chappe/config/models.py; "
        "regenerate it with: uv run python scripts/render_config_reference.py"
    )


def test_every_field_of_every_model_is_in_the_reference() -> None:
    script = load_script()
    block = script.render()
    for model in script.models(ConfigFile):
        assert f"### `{model.__name__}`" in block
        for name in model.model_fields:
            assert f"| `{name}` |" in block, f"{model.__name__}.{name}"


def test_every_field_has_a_description() -> None:
    script = load_script()
    missing = [
        f"{model.__name__}.{name}"
        for model in script.models(ConfigFile)
        for name, field in model.model_fields.items()
        if not field.description
    ]
    assert missing == []


def test_the_output_is_deterministic() -> None:
    script = load_script()
    assert script.render() == script.render()


def test_splice_replaces_only_the_marked_block() -> None:
    script = load_script()
    text = f"before\n{script.BEGIN}\nold\n{script.END}\nafter\n"
    assert script.splice(text, "new\n") == f"before\n{script.BEGIN}\nnew\n{script.END}\nafter\n"


def test_splice_needs_both_markers() -> None:
    script = load_script()
    with pytest.raises(ValueError, match="markers"):
        script.splice("no markers here", "x")


def test_check_fails_on_a_stale_guide_and_write_fixes_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    script = load_script()
    guide = tmp_path / "guide.md"
    guide.write_text(f"# G\n{script.BEGIN}\nstale\n{script.END}\n", encoding="utf-8")
    assert script.main(["--check", str(guide)]) == 1
    assert "out of date" in capsys.readouterr().err
    assert guide.read_text(encoding="utf-8").count("stale") == 1  # --check never writes
    assert script.main([str(guide)]) == 0
    assert script.main(["--check", str(guide)]) == 0
