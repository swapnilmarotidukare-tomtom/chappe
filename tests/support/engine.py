# tests/support/engine.py
"""Engine test helpers: prepared views, a source returning them, and a wired engine per writer."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from chappe.core.engine import Engine, EngineSettings, HandleResult
from chappe.core.events import ChappeEvent, EventKind
from chappe.core.messages import MessageSet
from chappe.core.model import ProcessState, StepState
from chappe.core.view import ProcessView
from chappe.ports.theme import Theme
from chappe.stores.airflow_variable import AirflowVariableStore
from chappe.themes.builtin.plain import PlainTheme
from chappe.transports.slack.transport import SlackTransport
from tests.support.fakes import FakeSlackApi, FakeVariables
from tests.support.samples import ProcessViewBuilder, default_context

CHANNEL = "C0123456789"
KEY = "orders/run_1"
CTX = default_context("plain")
TITLES = ("Extract", "Transform", "Load", "Publish")


def stage(*states: StepState, finished: ProcessState | None = None) -> ProcessView:
    """One section with a step per state, titled from TITLES."""
    builder = ProcessViewBuilder(key=KEY).section("Main")
    for title, state in zip(TITLES, states, strict=False):
        builder.step(title, state)
    if finished is not None:
        builder.finished(finished)
    return builder.build()


def render(view: ProcessView) -> MessageSet:
    return PlainTheme().render(view, CTX)


class PreparedSource:
    """Returns the view the test prepared; the event itself is ignored."""

    def __init__(self) -> None:
        self.view: ProcessView | None = None

    def snapshot(self, event: ChappeEvent) -> ProcessView | None:
        return self.view


def _nothing() -> None:
    return None


def _no_sleep(seconds: float) -> None:
    return None


def store_for(
    variables: FakeVariables, before_read: Callable[[], None] = _nothing
) -> AirflowVariableStore:
    """A Variable store on the shared fake; `before_read` runs ahead of every Variable read."""

    def get(key: str) -> str | None:
        before_read()
        return variables.get(key)

    return AirflowVariableStore(get=get, set=variables.set)


@dataclass
class Writer:
    """One Airflow worker: its own engine and source, sharing Slack and Variables with others."""

    engine: Engine
    source: PreparedSource

    def handle(
        self, view: ProcessView | None, kind: EventKind = EventKind.STEP_FINISHED
    ) -> HandleResult:
        self.source.view = view
        event = ChappeEvent(
            kind=kind,
            process="orders",
            dag_id="orders",
            run_id="run_1",
            occurred_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
        )
        return self.engine.handle(event)


def writer(
    api: FakeSlackApi,
    variables: FakeVariables,
    *,
    theme: Theme | None = None,
    source: PreparedSource | None = None,
    sleep: Callable[[float], None] = _no_sleep,
    before_read: Callable[[], None] = _nothing,
    enabled: bool = True,
) -> Writer:
    prepared = source if source is not None else PreparedSource()
    engine = Engine(
        source=prepared,
        theme=theme if theme is not None else PlainTheme(),
        fallback_theme=PlainTheme(),
        context=CTX,
        transport=SlackTransport(api),
        store=store_for(variables, before_read),
        settings=EngineSettings(channel=CHANNEL),
        enabled=lambda: enabled,
        sleep=sleep,
    )
    return Writer(engine, prepared)
