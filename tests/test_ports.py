# tests/test_ports.py
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

from chappe.core.errors import ChappeError, StoreError, TransportError
from chappe.core.events import ChappeEvent, EventKind
from chappe.core.reconcile import SentState, merge_sent
from chappe.core.render import Formatter
from chappe.core.view import ProcessView
from chappe.ports.source import Source
from chappe.ports.store import Store
from chappe.ports.transport import Transport

T = datetime(2026, 10, 2, 13, 35, tzinfo=timezone.utc)
EVENT = ChappeEvent(
    kind=EventKind.RUN_FINISHED, process="orders/r1", dag_id="orders", run_id="r1", occurred_at=T
)


class DictStore:
    def __init__(self) -> None:
        self.rows: dict[str, SentState] = {}

    def load(self, process_key: str) -> SentState | None:
        return self.rows.get(process_key)

    def save(self, process_key: str, state: SentState) -> SentState:
        merged = merge_sent(self.rows.get(process_key), state)
        self.rows[process_key] = merged
        return merged


class PlainFormatter:
    def escape(self, text: str) -> str:
        return text

    def bold(self, text: str) -> str:
        return text

    def italic(self, text: str) -> str:
        return text

    def link(self, url: str, label: str) -> str:
        return label

    def mention(self, target: str) -> str:
        return target

    def icon(self, token: str) -> str:
        return token


class RecordingTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    @property
    def formatter(self) -> Formatter:
        return PlainFormatter()

    def post_parent(
        self, channel: str, text: str, metadata: Mapping[str, Any], *, deadline: float
    ) -> str:
        self.calls.append(("post_parent", text))
        return "1.000001"

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
        self.calls.append(("update_parent", ts))
        return True

    def post_reply(
        self, channel: str, parent_ts: str, text: str, *, broadcast: bool, deadline: float
    ) -> str:
        self.calls.append(("post_reply", parent_ts))
        return "1.000002"

    def delete(self, channel: str, ts: str, *, deadline: float) -> None:
        self.calls.append(("delete", ts))

    def delete_duplicate(
        self, channel: str, ts: str, *, deadline: float, still_stale: Callable[[], bool]
    ) -> bool:
        self.calls.append(("delete_duplicate", ts))
        return still_stale()


class NoMilestones:
    def snapshot(self, event: ChappeEvent) -> ProcessView | None:
        return None


def test_store_save_returns_the_merged_state() -> None:
    store: Store = DictStore()
    assert store.load("orders/r1") is None
    store.save("orders/r1", SentState("orders/r1", sent_keys=frozenset({("a", "1.000001")})))
    merged = store.save(
        "orders/r1", SentState("orders/r1", sent_keys=frozenset({("b", "1.000001")}))
    )
    assert merged.live_keys == frozenset({"a", "b"})
    assert store.load("orders/r1") == merged


def test_transport_shape() -> None:
    transport: Transport = RecordingTransport()
    meta = {"process_key": "orders/r1"}  # the Chappe payload; the Slack transport wraps it
    ts = transport.post_parent("C123", "Orders", meta, deadline=10.0)
    transport.update_parent("C123", ts, "Orders done", meta, deadline=10.0)
    transport.post_reply("C123", ts, "extract done", broadcast=False, deadline=10.0)
    transport.delete("C123", "0.5", deadline=10.0)
    assert transport.delete_duplicate("C123", "0.6", deadline=10.0, still_stale=lambda: True)
    names = [name for name, _ in transport.calls]
    assert names == ["post_parent", "update_parent", "post_reply", "delete", "delete_duplicate"]
    assert transport.formatter.bold("x") == "x"


def test_source_may_return_no_view() -> None:
    source: Source = NoMilestones()
    assert source.snapshot(EVENT) is None


def test_event_defaults() -> None:
    assert EVENT.step_key is None and EVENT.process_state is None
    assert dict(EVENT.payload) == {}


def test_errors() -> None:
    error = TransportError("ratelimited", retryable=True, retry_after=2.0)
    assert isinstance(error, ChappeError)
    assert str(error) == error.code == "ratelimited"
    assert error.retryable is True
    assert error.retry_after == 2.0
    assert issubclass(StoreError, ChappeError)
