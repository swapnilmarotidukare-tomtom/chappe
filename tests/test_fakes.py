# tests/test_fakes.py
import pytest

from chappe.core.errors import TransportError
from tests.support.fakes import FakeSlackApi, FakeVariables

CHANNEL = "C0123456789"


def test_before_post_lets_a_concurrent_writer_post_first() -> None:
    api = FakeSlackApi()

    def other_writer(channel: str, thread_ts: str | None) -> None:
        api.before_post = None
        api.post(channel, "other")

    api.before_post = other_writer
    mine = api.post(CHANNEL, "mine")
    assert [m.text for m in api.top_level(CHANNEL)] == ["other", "mine"]
    assert mine == "1790000000.000002"


def test_fake_slack_rejects_unknown_messages_like_slack() -> None:
    api = FakeSlackApi()
    with pytest.raises(TransportError, match="message_not_found"):
        api.update(CHANNEL, "1790000000.000001", "x")
    with pytest.raises(TransportError, match="message_not_found"):
        api.delete(CHANNEL, "1790000000.000001")


def test_fake_variables_hook_runs_before_each_set() -> None:
    variables = FakeVariables()
    seen: list[tuple[str, str, str | None]] = []
    variables.before_set = lambda key, value: seen.append((key, value, variables.get(key)))
    variables.set("k", "1")
    variables.set("k", "2")
    assert seen == [("k", "1", None), ("k", "2", "1")]
    variables.delete("k")
    variables.delete("k")  # deleting a missing key is a no-op
    assert variables.get("k") is None
    assert variables.keys() == []
