from typing import Any

import pytest
from slack_sdk.errors import SlackApiError, SlackClientError
from slack_sdk.web import SlackResponse

from chappe.core.errors import TransportError
from chappe.transports.slack.api import WebClientSlackApi

CHANNEL = "C0123456789"


class StubClient:
    def __init__(self, result: Any = None, error: BaseException | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        def method(**kwargs: Any) -> Any:
            self.calls.append((name, kwargs))
            if self.error is not None:
                raise self.error
            return self.result

        return method


def api_with(stub: StubClient, timeouts: list[float] | None = None) -> WebClientSlackApi:
    api = WebClientSlackApi("xoxb-test-not-a-real-token")

    def client_for(timeout: float) -> Any:
        if timeouts is not None:
            timeouts.append(timeout)
        return stub

    api._client_for = client_for  # type: ignore[method-assign]
    return api


def slack_error(status: int, error: str, headers: dict[str, str] | None = None) -> SlackApiError:
    response = SlackResponse(
        client=None,  # type: ignore[arg-type]
        http_verb="POST",
        api_url="https://slack.com/api/chat.postMessage",
        req_args={},
        data={"ok": False, "error": error},
        headers=headers or {},
        status_code=status,
    )
    return SlackApiError(error, response)


def test_client_has_no_builtin_retry_handlers() -> None:
    assert WebClientSlackApi("xoxb-test-not-a-real-token")._client_for(5.0).retry_handlers == []


def test_each_request_times_out_within_the_time_left() -> None:
    client = WebClientSlackApi("xoxb-test-not-a-real-token", timeout=5.0)._client_for(1.25)
    assert client.timeout == 1.25  # a float: urllib accepts it, int() would round 0.9 to 0

    timeouts: list[float] = []
    api = api_with(StubClient({"ok": True, "ts": "1.1"}), timeouts)
    api.post(CHANNEL, "hi", timeout=0.4)
    api.update(CHANNEL, "1.1", "v2", timeout=30.0)
    api.delete(CHANNEL, "1.1")
    assert timeouts == [0.4, 5.0, 5.0]  # min(client timeout, time left)


def test_post_returns_ts_verbatim_and_passes_metadata() -> None:
    stub = StubClient({"ok": True, "ts": "1790000000.100200"})
    meta = {"event_type": "chappe_process", "event_payload": {"a": 1}}
    assert api_with(stub).post(CHANNEL, "hi", metadata=meta) == "1790000000.100200"
    assert stub.calls == [
        ("chat_postMessage", {"channel": CHANNEL, "text": "hi", "metadata": meta})
    ]


def test_reply_broadcast_is_sent_only_with_thread_ts() -> None:
    stub = StubClient({"ts": "1.2"})
    api = api_with(stub)
    api.post(CHANNEL, "top", broadcast=True)
    api.post(CHANNEL, "reply", thread_ts="1.1", broadcast=True)
    assert "reply_broadcast" not in stub.calls[0][1]
    assert stub.calls[1][1]["thread_ts"] == "1.1"
    assert stub.calls[1][1]["reply_broadcast"] is True


def test_update_and_delete_call_slack() -> None:
    stub = StubClient({"ok": True})
    api = api_with(stub)
    api.update(CHANNEL, "1.1", "v2", metadata={"event_type": "x", "event_payload": {}})
    api.delete(CHANNEL, "1.1")
    assert [name for name, _ in stub.calls] == ["chat_update", "chat_delete"]
    assert stub.calls[0][1]["metadata"] == {"event_type": "x", "event_payload": {}}
    assert stub.calls[1][1] == {"channel": CHANNEL, "ts": "1.1"}


def test_rate_limit_error_is_mapped() -> None:
    stub = StubClient(error=slack_error(429, "ratelimited", {"Retry-After": "2"}))
    with pytest.raises(TransportError) as info:
        api_with(stub).post(CHANNEL, "hi")
    assert (info.value.code, info.value.retryable, info.value.retry_after) == (
        "ratelimited",
        True,
        2.0,
    )


def test_server_error_is_retryable_and_permanent_error_is_not() -> None:
    with pytest.raises(TransportError) as info:
        api_with(StubClient(error=slack_error(500, "internal_error"))).delete(CHANNEL, "1.1")
    assert info.value.code == "internal_error" and info.value.retryable
    with pytest.raises(TransportError) as info:
        api_with(StubClient(error=slack_error(200, "not_in_channel"))).delete(CHANNEL, "1.1")
    assert info.value.code == "not_in_channel" and not info.value.retryable


@pytest.mark.parametrize("error", [TimeoutError(), OSError()])
def test_network_errors_are_retryable(error: BaseException) -> None:
    with pytest.raises(TransportError) as info:
        api_with(StubClient(error=error)).update(CHANNEL, "1.1", "x")
    assert info.value.code == "network_error" and info.value.retryable


def test_other_client_errors_are_permanent() -> None:
    with pytest.raises(TransportError) as info:
        api_with(StubClient(error=SlackClientError("bad"))).update(CHANNEL, "1.1", "x")
    assert info.value.code == "client_error" and not info.value.retryable
