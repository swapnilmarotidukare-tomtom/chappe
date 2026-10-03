# tests/support/fakes.py
"""In-memory Slack and Airflow Variables for tests. No network, no Airflow."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from chappe.core.errors import TransportError
from chappe.transports.slack.api import MESSAGE_NOT_FOUND


@dataclass(frozen=True, slots=True)
class FakeMessage:
    ts: str
    text: str
    thread_ts: str | None
    metadata: Mapping[str, Any] | None
    broadcast: bool = False


class FakeSlackApi:
    """Behaves like the parts of the Slack Web API Chappe uses."""

    def __init__(self) -> None:
        self._seq = 0
        self._messages: dict[str, dict[str, FakeMessage]] = {}
        self._failures: list[TransportError] = []
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.deleted: list[tuple[str, str]] = []
        self.before_post: Callable[[str, str | None], None] | None = None

    def fail_next(self, error: TransportError) -> None:
        """The next call (of any method) raises `error`; queued errors fire in order."""
        self._failures.append(error)

    def _maybe_fail(self) -> None:
        if self._failures:
            raise self._failures.pop(0)

    def _existing(self, channel: str, ts: str) -> FakeMessage:
        message = self._messages.get(channel, {}).get(ts)
        if message is None:
            raise TransportError(MESSAGE_NOT_FOUND, retryable=False)
        return message

    def post(
        self,
        channel: str,
        text: str,
        *,
        thread_ts: str | None = None,
        broadcast: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        if self.before_post is not None:
            self.before_post(channel, thread_ts)
        self.calls.append(
            (
                "post",
                {
                    "channel": channel,
                    "text": text,
                    "thread_ts": thread_ts,
                    "broadcast": broadcast,
                    "metadata": metadata,
                },
            )
        )
        self._maybe_fail()
        self._seq += 1
        ts = f"1790000000.{self._seq:06d}"
        self._messages.setdefault(channel, {})[ts] = FakeMessage(
            ts, text, thread_ts, copy.deepcopy(metadata), broadcast
        )
        return ts

    def update(
        self, channel: str, ts: str, text: str, *, metadata: Mapping[str, Any] | None = None
    ) -> None:
        call = {"channel": channel, "ts": ts, "text": text, "metadata": metadata}
        self.calls.append(("update", call))
        self._maybe_fail()
        old = self._existing(channel, ts)
        self._messages[channel][ts] = FakeMessage(
            ts,
            text,
            old.thread_ts,
            copy.deepcopy(metadata) if metadata is not None else old.metadata,
            old.broadcast,
        )

    def delete(self, channel: str, ts: str) -> None:
        self.calls.append(("delete", {"channel": channel, "ts": ts}))
        self._maybe_fail()
        self._existing(channel, ts)
        del self._messages[channel][ts]
        self.deleted.append((channel, ts))

    def message(self, channel: str, ts: str) -> FakeMessage | None:
        return self._messages.get(channel, {}).get(ts)

    def top_level(self, channel: str) -> list[FakeMessage]:
        messages = self._messages.get(channel, {}).values()
        return sorted((m for m in messages if m.thread_ts is None), key=lambda m: m.ts)

    def replies(self, channel: str, parent_ts: str) -> list[FakeMessage]:
        messages = self._messages.get(channel, {}).values()
        return sorted((m for m in messages if m.thread_ts == parent_ts), key=lambda m: m.ts)


class FakeVariables:
    """Dict-backed Airflow Variables. `before_set` lets a test run a parallel writer."""

    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.before_set: Callable[[str, str], None] | None = None

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def set(self, key: str, value: str) -> None:
        if self.before_set is not None:
            self.before_set(key, value)
        self.data[key] = value

    def delete(self, key: str) -> None:
        self.data.pop(key, None)

    def keys(self) -> list[str]:
        return sorted(self.data)
