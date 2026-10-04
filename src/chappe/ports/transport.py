# src/chappe/ports/transport.py
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol

from chappe.core.render import Formatter


class Transport(Protocol):
    """Sends messages. `deadline` is a `time.monotonic()` value; calls give up after it."""

    @property
    def formatter(self) -> Formatter: ...

    def post_parent(
        self, channel: str, text: str, metadata: Mapping[str, Any], *, deadline: float
    ) -> str:
        """Post a top-level message and return its ts."""
        ...

    def update_parent(
        self,
        channel: str,
        ts: str,
        text: str,
        metadata: Mapping[str, Any],
        *,
        deadline: float,
        still_current: Callable[[], bool] | None = None,
    ) -> bool:
        """Edit a top-level message. Before every attempt, retries included, `still_current()`
        is asked; when it says no, nothing is written and the result is False."""
        ...

    def post_reply(
        self, channel: str, parent_ts: str, text: str, *, broadcast: bool, deadline: float
    ) -> str:
        """Post into the parent's thread and return the reply's ts."""
        ...

    def delete(self, channel: str, ts: str, *, deadline: float) -> None:
        """Delete a message. A message that is already gone counts as deleted."""
        ...
