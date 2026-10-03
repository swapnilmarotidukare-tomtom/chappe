# tests/transports/test_slack_transport.py
import pytest

from chappe.core.errors import TransportError
from chappe.transports.slack.api import MESSAGE_NOT_FOUND, call_with_retry, transport_error
from chappe.transports.slack.transport import EVENT_TYPE, SlackTransport
from tests.support.fakes import FakeSlackApi

CHANNEL = "C0123456789"
META = {"process_key": "orders/manual__1"}


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_parent_reply_and_delete() -> None:
    api = FakeSlackApi()
    transport = SlackTransport(api, clock=Clock())
    parent = transport.post_parent(CHANNEL, "hello", META, deadline=10)
    reply = transport.post_reply(CHANNEL, parent, "step done", broadcast=True, deadline=10)
    assert [m.text for m in api.top_level(CHANNEL)] == ["hello"]
    assert [(m.text, m.broadcast) for m in api.replies(CHANNEL, parent)] == [("step done", True)]
    transport.delete(CHANNEL, reply, deadline=10)
    assert api.replies(CHANNEL, parent) == []
    assert api.deleted == [(CHANNEL, reply)]


def test_parent_carries_chappe_metadata_on_post_and_update() -> None:
    api = FakeSlackApi()
    transport = SlackTransport(api, clock=Clock())
    parent = transport.post_parent(CHANNEL, "v1", META, deadline=10)
    message = api.message(CHANNEL, parent)
    assert message is not None
    assert message.metadata == {"event_type": EVENT_TYPE, "event_payload": META}
    transport.update_parent(CHANNEL, parent, "v2", {**META, "n": 2}, deadline=10)
    message = api.message(CHANNEL, parent)
    assert message is not None and message.text == "v2"
    assert message.metadata == {"event_type": "chappe_process", "event_payload": {**META, "n": 2}}


def test_deleting_a_message_that_is_already_gone_succeeds() -> None:
    api = FakeSlackApi()
    transport = SlackTransport(api, clock=Clock())
    parent = transport.post_parent(CHANNEL, "hello", META, deadline=10)
    transport.delete(CHANNEL, parent, deadline=10)
    transport.delete(CHANNEL, parent, deadline=10)  # Slack answers message_not_found
    assert api.deleted == [(CHANNEL, parent)]
    assert api.message(CHANNEL, parent) is None


def test_retryable_errors_are_retried_honouring_retry_after() -> None:
    clock = Clock()
    api = FakeSlackApi()
    api.fail_next(TransportError("ratelimited", retryable=True, retry_after=3))
    transport = SlackTransport(api, clock=clock, sleep=clock.sleep)
    transport.post_parent(CHANNEL, "hello", META, deadline=10)
    assert clock.now == 3
    assert len(api.top_level(CHANNEL)) == 1


def test_without_retry_after_the_backoff_starts_at_half_a_second() -> None:
    clock = Clock()
    api = FakeSlackApi()
    transport = SlackTransport(api, clock=clock, sleep=clock.sleep)
    parent = transport.post_parent(CHANNEL, "hello", META, deadline=10)
    api.fail_next(TransportError("internal_error", retryable=True))
    transport.update_parent(CHANNEL, parent, "again", META, deadline=10)
    assert clock.now == 0.5


def test_retry_gives_up_at_the_deadline() -> None:
    clock = Clock()
    calls: list[int] = []

    def always_limited() -> None:
        calls.append(1)
        raise TransportError("ratelimited", retryable=True, retry_after=4)

    with pytest.raises(TransportError):
        call_with_retry(always_limited, deadline=10, clock=clock, sleep=clock.sleep)
    assert len(calls) == 3  # at t=0, 4, 8; the next wait would pass the deadline


def test_permanent_errors_are_not_retried() -> None:
    api = FakeSlackApi()
    api.fail_next(TransportError("not_in_channel", retryable=False))
    with pytest.raises(TransportError, match="not_in_channel"):
        SlackTransport(api, clock=Clock()).post_parent(CHANNEL, "hello", META, deadline=10)


def test_slack_errors_are_classified() -> None:
    limited = transport_error("ratelimited", status=429, headers={"Retry-After": "3"})
    assert limited.retryable and limited.retry_after == 3.0
    lower = transport_error("ratelimited", status=429, headers={"retry-after": "7"})
    assert lower.retry_after == 7.0
    gone = transport_error("message_not_found", status=200, headers={})
    assert gone.code == MESSAGE_NOT_FOUND and not gone.retryable
    assert transport_error("internal_error", status=200, headers={}).retryable
    assert transport_error("unknown_error", status=503, headers={}).retryable
    assert not transport_error("not_in_channel", status=200, headers={}).retryable


def test_a_post_is_not_retried_after_an_ambiguous_error() -> None:
    for code in ("internal_error", "request_timeout", "network_error"):
        clock = Clock()
        api = FakeSlackApi()
        api.fail_next(TransportError(code, retryable=True))
        transport = SlackTransport(api, clock=clock, sleep=clock.sleep)
        with pytest.raises(TransportError, match=code):
            transport.post_parent(CHANNEL, "hello", META, deadline=10)
        assert clock.now == 0
        assert len([c for c in api.calls if c[0] == "post"]) == 1

    api = FakeSlackApi()
    parent = api.post(CHANNEL, "p")
    api.fail_next(TransportError("internal_error", retryable=True))
    with pytest.raises(TransportError, match="internal_error"):
        SlackTransport(api, clock=Clock()).post_reply(
            CHANNEL, parent, "r", broadcast=False, deadline=10
        )
    assert api.replies(CHANNEL, parent) == []


def test_a_post_is_retried_when_rate_limited() -> None:
    clock = Clock()
    api = FakeSlackApi()
    parent = api.post(CHANNEL, "p")
    api.fail_next(TransportError("ratelimited", retryable=True, retry_after=2))
    transport = SlackTransport(api, clock=clock, sleep=clock.sleep)
    transport.post_reply(CHANNEL, parent, "r", broadcast=False, deadline=10)
    assert clock.now == 2
    assert len(api.replies(CHANNEL, parent)) == 1


def test_update_still_retries_ambiguous_errors() -> None:
    clock = Clock()
    api = FakeSlackApi()
    transport = SlackTransport(api, clock=clock, sleep=clock.sleep)
    parent = transport.post_parent(CHANNEL, "v1", META, deadline=10)
    api.fail_next(TransportError("internal_error", retryable=True))
    transport.update_parent(CHANNEL, parent, "v2", META, deadline=10)
    assert clock.now == 0.5
    message = api.message(CHANNEL, parent)
    assert message is not None and message.text == "v2"


def test_retry_after_is_clamped_and_non_finite_ignored() -> None:
    assert (
        transport_error("ratelimited", status=429, headers={"Retry-After": "-5"}).retry_after == 0
    )
    assert (
        transport_error("ratelimited", status=429, headers={"Retry-After": "nan"}).retry_after
        is None
    )
    assert (
        transport_error("ratelimited", status=429, headers={"Retry-After": "inf"}).retry_after
        is None
    )
    assert transport_error("whatever", status=429, headers={}).code == "ratelimited"


def test_no_call_is_made_once_the_deadline_has_passed() -> None:
    clock = Clock()
    clock.now = 10.0
    calls: list[int] = []

    def call() -> None:
        calls.append(1)

    with pytest.raises(TransportError) as info:
        call_with_retry(call, deadline=10, clock=clock, sleep=clock.sleep)
    assert (info.value.code, info.value.retryable) == ("deadline", True)
    assert calls == []


def test_a_retry_that_would_start_at_the_deadline_is_not_made() -> None:
    clock = Clock()
    calls: list[int] = []

    def limited() -> None:
        calls.append(1)
        raise TransportError("ratelimited", retryable=True, retry_after=10)

    with pytest.raises(TransportError, match="deadline"):
        call_with_retry(limited, deadline=10, clock=clock, sleep=clock.sleep)
    assert calls == [1]  # the wait fits exactly, but no time is left for the call itself
    assert clock.now == 10


def test_every_slack_call_gets_the_time_left_as_its_timeout() -> None:
    clock = Clock()
    clock.now = 2.5
    api = FakeSlackApi()
    transport = SlackTransport(api, clock=clock, sleep=clock.sleep)
    parent = transport.post_parent(CHANNEL, "hello", META, deadline=10)
    api.fail_next(TransportError("internal_error", retryable=True))
    transport.update_parent(CHANNEL, parent, "again", META, deadline=10)  # retried at t=3.0
    reply = transport.post_reply(CHANNEL, parent, "r", broadcast=False, deadline=4)
    transport.delete(CHANNEL, reply, deadline=3.25)
    assert api.timeouts == [7.5, 7.5, 7.0, 1.0, 0.25]
