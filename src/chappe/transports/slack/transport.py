# src/chappe/transports/slack/transport.py
"""The Slack transport: posts, edits and deletes messages within a time budget."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

from chappe.core.errors import TransportError
from chappe.transports.slack.api import (
    MESSAGE_NOT_FOUND,
    SlackApi,
    call_with_retry,
    is_rate_limited,
)
from chappe.transports.slack.formatter import SlackFormatter

T = TypeVar("T")

EVENT_TYPE = "chappe_process"


def _envelope(metadata: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if metadata is None:
        return None
    return {"event_type": EVENT_TYPE, "event_payload": dict(metadata)}


class SlackTransport:
    def __init__(
        self,
        api: SlackApi,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._api = api
        self._clock = clock
        self._sleep = sleep
        self._formatter = SlackFormatter()

    @property
    def formatter(self) -> SlackFormatter:
        return self._formatter

    def _retry(self, fn: Callable[[], T], deadline: float) -> T:
        return call_with_retry(fn, deadline=deadline, clock=self._clock, sleep=self._sleep)

    def _retry_post(self, fn: Callable[[], T], deadline: float) -> T:
        """Posts are not idempotent: retry only when Slack certainly did not post."""
        return call_with_retry(
            fn, deadline=deadline, clock=self._clock, sleep=self._sleep, retry_on=is_rate_limited
        )

    def post_parent(
        self, channel: str, text: str, metadata: Mapping[str, Any] | None, *, deadline: float
    ) -> str:
        envelope = _envelope(metadata)
        return self._retry_post(lambda: self._api.post(channel, text, metadata=envelope), deadline)

    def update_parent(
        self,
        channel: str,
        ts: str,
        text: str,
        metadata: Mapping[str, Any] | None,
        *,
        deadline: float,
    ) -> None:
        envelope = _envelope(metadata)
        self._retry(lambda: self._api.update(channel, ts, text, metadata=envelope), deadline)

    def post_reply(
        self, channel: str, parent_ts: str, text: str, *, broadcast: bool, deadline: float
    ) -> str:
        return self._retry_post(
            lambda: self._api.post(channel, text, thread_ts=parent_ts, broadcast=broadcast),
            deadline,
        )

    def delete(self, channel: str, ts: str, *, deadline: float) -> None:
        """Delete a message. A message that is already gone counts as deleted."""
        try:
            self._retry(lambda: self._api.delete(channel, ts), deadline)
        except TransportError as exc:
            if exc.code != MESSAGE_NOT_FOUND:
                raise
