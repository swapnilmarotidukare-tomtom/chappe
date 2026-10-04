# src/chappe/integrations/airflow/callbacks.py
"""Task callbacks attached by @milestone, and the run-finished dispatch used by ChappeNotifier."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from airflow.sdk import Param

from chappe.core.events import ChappeEvent, EventKind
from chappe.core.model import ProcessState, StepState
from chappe.integrations.airflow.source import process_key

log = logging.getLogger("chappe")

RETRY_STATE = "up_for_retry"


def _error_text(context: Mapping[str, Any]) -> str | None:
    exc = context.get("exception")
    return f"{type(exc).__name__}: {exc}"[:300] if exc else None


def _plain(value: Any) -> Any:
    return value.resolve(suppress_exception=True) if isinstance(value, Param) else value


def _params(params: Any) -> dict[str, Any]:
    """Plain values, so titles render: a ParamsDict or a dict of `Param` objects resolve here."""
    if not params:
        return {}
    dump = getattr(params, "dump", None)
    if callable(dump):  # ParamsDict: resolves every Param and suppresses validation errors
        return {str(key): _plain(value) for key, value in dict(dump()).items()}
    return {str(key): _plain(params[key]) for key in params}


def _payload(context: Mapping[str, Any], ti: Any) -> dict[str, Any]:
    """What the source needs, from plain attributes only.

    No runtime calls here: a DAG callback hangs on most of them (findings, change 3).
    """
    dag_run = context.get("dag_run")  # missing in the minimal DAG-callback context
    return {
        "dag": context["dag"],
        "params": _params(context.get("params")),
        "run_started_at": getattr(dag_run, "start_date", None),
        "run_ended_at": getattr(dag_run, "end_date", None),
        "step_started_at": getattr(ti, "start_date", None),
        "step_ended_at": getattr(ti, "end_date", None),
    }


def _run_id(context: Mapping[str, Any]) -> str:
    run_id = context.get("run_id")
    return str(run_id if run_id is not None else context["dag_run"].run_id)


def _key_for_log(context: Mapping[str, Any]) -> str:
    try:
        return process_key(context["dag"].dag_id, _run_id(context))
    except Exception:
        return "?"


def _dispatch(
    context: Mapping[str, Any],
    kind: EventKind,
    *,
    step_state: StepState | None = None,
    process_state: ProcessState | None = None,
    process: str | None = None,
    keep_error: bool = True,
) -> None:
    try:
        from chappe.integrations.airflow.runtime import get_runtime

        runtime = get_runtime()
        if runtime is None:
            return
        dag = context["dag"]
        found = runtime.resolve(process, dag.dag_id)
        if found is None:
            return
        name, _ = found
        # DAG callbacks: the context's "ti" is the last task, not the event's step.
        ti = context.get("ti") if kind is not EventKind.RUN_FINISHED else None
        event = ChappeEvent(
            kind=kind,
            process=name,
            dag_id=dag.dag_id,
            run_id=_run_id(context),
            occurred_at=datetime.now(timezone.utc),
            # the full task id ("prepare.convert"): the source looks steps up by it
            step_key=ti.task_id if ti is not None else None,
            step_state=step_state,
            process_state=process_state,
            error=_error_text(context) if keep_error else None,
            payload=_payload(context, ti),
        )
        runtime.engine(name).handle(event)
    except Exception:
        if kind is EventKind.RUN_FINISHED:
            # spec 9.3: no later event corrects the final message
            log.exception(
                "chappe: the final message for %s was not sent; the pipeline is not affected",
                _key_for_log(context),
            )
        else:
            log.exception(
                "chappe: callback failed for %s; the pipeline is not affected",
                _key_for_log(context),
            )


def _will_retry(context: Mapping[str, Any]) -> bool:
    state = getattr(context.get("ti"), "state", None)
    return str(getattr(state, "value", state)) == RETRY_STATE


def on_step_started(context: Mapping[str, Any]) -> None:
    _dispatch(context, EventKind.STEP_STARTED, step_state=StepState.RUNNING)


def on_step_succeeded(context: Mapping[str, Any]) -> None:
    _dispatch(context, EventKind.STEP_FINISHED, step_state=StepState.SUCCEEDED)


def on_step_failed(context: Mapping[str, Any]) -> None:
    try:
        retrying = _will_retry(context)
    except Exception:
        retrying = False
    if retrying:
        # the task runs again: the step is still running, not failed
        _dispatch(context, EventKind.STEP_FINISHED, step_state=StepState.RUNNING, keep_error=False)
    else:
        _dispatch(context, EventKind.STEP_FINISHED, step_state=StepState.FAILED)


def on_step_skipped(context: Mapping[str, Any]) -> None:
    _dispatch(context, EventKind.STEP_FINISHED, step_state=StepState.SKIPPED)


def dispatch_run_finished(
    context: Mapping[str, Any], process: str | None, state: ProcessState
) -> None:
    _dispatch(context, EventKind.RUN_FINISHED, process_state=state, process=process)
