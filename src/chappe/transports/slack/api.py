# src/chappe/transports/slack/api.py
"""Thin, typed wrapper over the Slack Web API with error classification and retries."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol, TypeVar

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError, SlackClientError

from chappe.core.errors import TransportError

T = TypeVar("T")

RETRYABLE = frozenset(
    {
        "ratelimited",
        "rate_limited",
        "internal_error",
        "fatal_error",
        "service_unavailable",
        "request_timeout",
        "network_error",
    }
)
MESSAGE_NOT_FOUND = "message_not_found"
RATE_LIMITED = "ratelimited"
_RATE_LIMIT_CODES = frozenset({RATE_LIMITED, "rate_limited"})


def is_rate_limited(error: TransportError) -> bool:
    """True when Slack definitely did not act on the request, so a retry cannot duplicate it."""
    return error.code in _RATE_LIMIT_CODES


class SlackApi(Protocol):
    def post(
        self,
        channel: str,
        text: str,
        *,
        thread_ts: str | None = None,
        broadcast: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> str: ...

    def update(
        self, channel: str, ts: str, text: str, *, metadata: Mapping[str, Any] | None = None
    ) -> None: ...

    def delete(self, channel: str, ts: str) -> None: ...


def _seconds(raw: Any) -> float | None:
    try:
        value = float(raw) if raw else None
    except (TypeError, ValueError):
        return None
    if value is None or not math.isfinite(value):
        return None
    return max(0.0, value)


def transport_error(code: str, *, status: int, headers: Mapping[str, Any]) -> TransportError:
    """Classify a Slack error. `message_not_found` is permanent; a delete treats it as done."""
    retry_after = _seconds(headers.get("Retry-After") or headers.get("retry-after"))
    if status == 429:
        code = RATE_LIMITED
    retryable = code in RETRYABLE or status == 429 or status >= 500
    return TransportError(code, retryable=retryable, retry_after=retry_after)


def call_with_retry(
    fn: Callable[[], T],
    *,
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    retry_on: Callable[[TransportError], bool] | None = None,
) -> T:
    """Call `fn`, retrying until the next wait would pass `deadline`.

    By default every retryable error is retried; `retry_on` narrows that (a post that
    may already have landed must only be retried when Slack certainly did nothing).
    """
    attempt = 0
    while True:
        try:
            return fn()
        except TransportError as exc:
            if not exc.retryable or (retry_on is not None and not retry_on(exc)):
                raise
            wait = exc.retry_after if exc.retry_after is not None else min(0.5 * 2**attempt, 8.0)
            if clock() + wait > deadline:
                raise
            sleep(wait)
            attempt += 1


class WebClientSlackApi:
    """Real Slack client. Needs the `chat:write` scope only."""

    def __init__(self, token: str, *, timeout: float = 5.0) -> None:
        self._client = WebClient(token=token, timeout=int(timeout), retry_handlers=[])

    def _call(self, method: str, **kwargs: Any) -> Any:
        try:
            return getattr(self._client, method)(**kwargs)
        except SlackApiError as exc:
            response = exc.response
            raise transport_error(
                str(response.get("error", "unknown_error")),
                status=response.status_code,
                headers=response.headers or {},
            ) from exc
        except (TimeoutError, OSError) as exc:
            raise TransportError("network_error", retryable=True) from exc
        except SlackClientError as exc:
            raise TransportError("client_error", retryable=False) from exc

    def post(
        self,
        channel: str,
        text: str,
        *,
        thread_ts: str | None = None,
        broadcast: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        kwargs: dict[str, Any] = {"channel": channel, "text": text}
        if thread_ts is not None:
            kwargs.update(thread_ts=thread_ts, reply_broadcast=broadcast)
        if metadata is not None:
            kwargs["metadata"] = dict(metadata)
        return str(self._call("chat_postMessage", **kwargs)["ts"])

    def update(
        self, channel: str, ts: str, text: str, *, metadata: Mapping[str, Any] | None = None
    ) -> None:
        kwargs: dict[str, Any] = {"channel": channel, "ts": ts, "text": text}
        if metadata is not None:
            kwargs["metadata"] = dict(metadata)
        self._call("chat_update", **kwargs)

    def delete(self, channel: str, ts: str) -> None:
        self._call("chat_delete", channel=channel, ts=ts)
