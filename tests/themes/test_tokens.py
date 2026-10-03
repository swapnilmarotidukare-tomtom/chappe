import pytest

from chappe.core.errors import ChappeConfigError
from chappe.themes.tokens import resolve_tokens


def test_builtin_tokens_load() -> None:
    tokens = resolve_tokens("thread", None)
    assert tokens.icons["running"] == ":hourglass_flowing_sand:"
    assert tokens.labels["succeeded"] == "Passed"
    assert tokens.extra["now"] == "Now"


def test_plain_tokens_load() -> None:
    tokens = resolve_tokens("plain", None)
    assert tokens.icons["failed"] == ":x:"
    assert tokens.labels["running"] == "In progress"
    assert "now" not in tokens.extra


def test_override_extends_and_replaces_keys() -> None:
    tokens = resolve_tokens("thread", {"extends": "thread", "icons": {"running": ":airflow_spin:"}})
    assert tokens.icons["running"] == ":airflow_spin:"
    assert tokens.icons["failed"] == ":x:"


def test_unknown_token_keys_are_rejected() -> None:
    with pytest.raises(ChappeConfigError, match=r"icons.runing"):
        resolve_tokens("thread", {"icons": {"runing": ":x:"}})
    with pytest.raises(ChappeConfigError, match="colour"):
        resolve_tokens("thread", {"colour": {}})


def test_unknown_base_theme_is_rejected() -> None:
    with pytest.raises(ChappeConfigError, match="no built-in tokens"):
        resolve_tokens("neon", None)
