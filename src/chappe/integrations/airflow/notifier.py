# src/chappe/integrations/airflow/notifier.py
"""DAG-level callback. Runs in the DAG processor."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from airflow.sdk import BaseNotifier

from chappe.core.model import ProcessState
from chappe.integrations.airflow.callbacks import dispatch_run_finished

log = logging.getLogger("chappe")


def run_state(context: Mapping[str, Any]) -> ProcessState:
    """The run state from `dag_run`, or from the callback reason when there is no `dag_run`."""
    state = getattr(context.get("dag_run"), "state", None)
    value = getattr(state, "value", state)
    if value == "success":
        return ProcessState.SUCCEEDED
    if value == "failed":
        return ProcessState.FAILED
    return ProcessState.SUCCEEDED if context.get("reason") == "success" else ProcessState.FAILED


class ChappeNotifier(BaseNotifier):
    """Use as the DAG's on_success_callback and on_failure_callback."""

    template_fields = ()

    def __init__(self, process: str | None = None) -> None:
        super().__init__()
        self.process = process

    def render_template_fields(self, context: Any, jinja_env: Any = None) -> None:
        """No templates (template_fields is empty). The base class asks the DAG for a Jinja env,
        which a SerializedDAG under dag.test() lacks, and would log an error every run."""

    def __call__(self, *args: Any) -> None:
        # BaseNotifier renders templates before notify() and re-raises; nothing may reach Airflow
        try:
            super().__call__(*args)
        except Exception:
            log.exception("chappe: notifier failed; the pipeline is not affected")

    def notify(self, context: Mapping[str, Any]) -> None:
        try:
            dispatch_run_finished(context, self.process, run_state(context))
        except Exception:
            log.exception("chappe: notifier failed; the pipeline is not affected")
