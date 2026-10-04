# src/chappe/integrations/airflow/runtime.py
"""Loads config once per process and builds one engine per Chappe process."""

from __future__ import annotations

import logging
from collections.abc import Callable
from zoneinfo import ZoneInfo

from chappe.config.loader import chappe_enabled, load_settings
from chappe.config.models import ChappeSettings, ProcessConfig
from chappe.core.engine import Engine, EngineSettings
from chappe.core.errors import ChappeConfigError
from chappe.core.render import RenderContext
from chappe.integrations.airflow.connections import ConnectionInfo, airflow_connection
from chappe.integrations.airflow.reader import RuntimeTaskStateReader
from chappe.integrations.airflow.source import AirflowSource
from chappe.ports.theme import Theme
from chappe.stores.airflow_variable import airflow_variable_store
from chappe.themes import THEMES
from chappe.themes.builtin.plain import PlainTheme
from chappe.themes.tokens import resolve_tokens
from chappe.transports.slack.api import WebClientSlackApi
from chappe.transports.slack.transport import SlackTransport

log = logging.getLogger("chappe")


_metrics_warned = False


def _metrics(name: str) -> None:
    global _metrics_warned
    try:
        from airflow.sdk.observability.stats import Stats

        Stats.incr(name)
    except Exception as exc:
        if not _metrics_warned:
            _metrics_warned = True
            log.debug("chappe: Airflow metrics are unavailable (%s); not counting events", exc)


def pick_theme(name: str) -> Theme:
    theme = THEMES.get(name)
    if theme is None:
        log.warning("chappe: unknown theme %r; using plain", name)
        return PlainTheme()
    return theme()


class Runtime:
    def __init__(
        self,
        settings: ChappeSettings,
        connections: Callable[[str], ConnectionInfo] = airflow_connection,
    ) -> None:
        self.settings = settings
        self._connections = connections
        self._engines: dict[str, Engine] = {}

    def resolve(self, process: str | None, dag_id: str) -> tuple[str, ProcessConfig] | None:
        if process is not None:
            config = self.settings.processes.get(process)
            if config is not None and any(ref.dag_id == dag_id for ref in config.dags):
                return process, config
            # Late over wrong (spec 4.2): never post one DAG's run to another process's channel.
            problem = "does not list this DAG" if config is not None else "is not configured"
            log.warning(
                "chappe: ChappeNotifier(process=%r) %s; using the process of DAG %r instead",
                process,
                problem,
                dag_id,
            )
        return self.settings.process_for_dag(dag_id)

    def engine(self, name: str) -> Engine:
        if name not in self._engines:
            self._engines[name] = self._build(name)
        return self._engines[name]

    def _slack_token(self) -> str:
        conn_id = self.settings.defaults.transport.connection_id
        token = self._connections(conn_id).password
        if not token:
            raise ChappeConfigError(
                f"connection {conn_id!r} has no password; put the Slack bot token there"
            )
        return token

    def _build(self, name: str) -> Engine:
        settings, process = self.settings, self.settings.processes[name]
        defaults = settings.defaults
        theme_config = settings.theme_for(process)
        theme = pick_theme(theme_config.name)
        tokens = theme_config.tokens if theme.name == theme_config.name else None
        transport = SlackTransport(WebClientSlackApi(self._slack_token()))
        source = AirflowSource(process, RuntimeTaskStateReader(), ui_base_url=defaults.ui_base_url)
        context = RenderContext(
            tokens=resolve_tokens(theme.name, tokens),
            fmt=transport.formatter,
            tz=ZoneInfo(defaults.time.timezone),
            alert_mention=process.alerts.mention,
        )
        return Engine(
            source=source,
            theme=theme,
            fallback_theme=PlainTheme(),
            context=context,
            transport=transport,
            store=airflow_variable_store(),
            settings=EngineSettings(
                channel=process.channel,
                event_budget_s=defaults.budgets.event_seconds,
                final_budget_s=defaults.budgets.final_seconds,
                final_check_delay_s=defaults.budgets.final_check_delay_seconds,
            ),
            enabled=lambda: chappe_enabled(settings) and process.enabled,
            metrics=_metrics,
        )


_runtime: Runtime | None = None
_loaded = False


def set_runtime(runtime: Runtime | None) -> None:
    """Install a runtime (tests). `None` makes the next `get_runtime()` load the config again."""
    global _runtime, _loaded
    _runtime, _loaded = runtime, runtime is not None


def _drop_process(name: str, problem: str) -> None:
    """Spec 9.1: a config error inside one process disables Chappe for that process only."""
    log.error("chappe: process %r is disabled: %s", name, problem)


def get_runtime() -> Runtime | None:
    global _runtime, _loaded
    if _loaded:
        return _runtime
    _loaded = True
    try:
        _runtime = Runtime(load_settings(on_process_error=_drop_process))
    except Exception as exc:  # a config error, or anything else: never raise into Airflow
        log.error("chappe is disabled: %s", exc)
        _runtime = None
    return _runtime
