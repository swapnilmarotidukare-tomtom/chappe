import pytest

from chappe.core.errors import ChappeConfigError
from chappe.themes.tokens import resolve_tokens


def test_builtin_tokens_load() -> None:
    tokens = resolve_tokens("metro", None)
    assert tokens.icons["running"] == ":large_yellow_circle:"
    assert tokens.labels["succeeded"] == "Passed"
    assert tokens.extra["glyph_running"] == "◉"


def test_plain_tokens_load() -> None:
    tokens = resolve_tokens("plain", None)
    assert tokens.icons["failed"] == ":x:"
    assert tokens.labels["running"] == "In progress"
    assert "now" not in tokens.extra


def test_override_extends_and_replaces_keys() -> None:
    tokens = resolve_tokens("metro", {"extends": "metro", "icons": {"running": ":airflow_spin:"}})
    assert tokens.icons["running"] == ":airflow_spin:"
    assert tokens.icons["failed"] == ":red_circle:"


def test_unknown_token_keys_are_rejected() -> None:
    with pytest.raises(ChappeConfigError, match=r"icons\.runing"):
        resolve_tokens("metro", {"icons": {"runing": ":x:"}})
    with pytest.raises(ChappeConfigError, match="colour"):
        resolve_tokens("metro", {"colour": {}})


def test_unknown_base_theme_is_rejected() -> None:
    with pytest.raises(ChappeConfigError, match="no built-in tokens"):
        resolve_tokens("neon", None)


def test_non_mapping_sections_are_rejected() -> None:
    with pytest.raises(ChappeConfigError, match="icons"):
        resolve_tokens("metro", {"icons": None})
    with pytest.raises(ChappeConfigError, match="labels"):
        resolve_tokens("metro", {"labels": ["x"]})
