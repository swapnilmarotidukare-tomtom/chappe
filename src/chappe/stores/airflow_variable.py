# src/chappe/stores/airflow_variable.py
"""Delivery state in one Airflow Variable per run (ADR-0005, contract D).

Airflow is imported only inside the default wiring, so this module loads without it.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from chappe.core.errors import StoreError
from chappe.core.reconcile import SentState, merge_sent
from chappe.core.view import Watermark

log = logging.getLogger(__name__)

KEY_PREFIX = "chappe__"
PAYLOAD_VERSION = 1

Get = Callable[[str], str | None]
Set = Callable[[str, str], None]
Delete = Callable[[str], None]

_DECODE_ERRORS = (ValueError, KeyError, TypeError)


def variable_key(dag_id: str, run_id: str) -> str:
    """Short and safe: Airflow dag ids are `[A-Za-z0-9_.-]`, the run id only enters the hash."""
    digest = hashlib.sha1(f"{dag_id}/{run_id}".encode()).hexdigest()[:16]
    return f"{KEY_PREFIX}{dag_id[:80]}__{digest}"


def key_for(process_key: str) -> str:
    """`process_key` is `"<dag_id>/<run_id>"`; a dag id never contains `/`."""
    dag_id, _, run_id = process_key.partition("/")
    return variable_key(dag_id, run_id)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _time(value: Any) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _watermark_json(wm: Watermark | None) -> dict[str, Any] | None:
    if wm is None:
        return None
    return {
        "finished": wm.finished,
        "settled": wm.settled_steps,
        "started": wm.started_steps,
        "occurred_at": _iso(wm.occurred_at),
    }


def _watermark(data: Any) -> Watermark | None:
    if data is None:
        return None
    return Watermark(
        finished=bool(data["finished"]),
        settled_steps=int(data["settled"]),
        started_steps=int(data["started"]),
        occurred_at=_time(data["occurred_at"]),
    )


def encode(state: SentState) -> str:
    return json.dumps(
        {
            "v": PAYLOAD_VERSION,
            "process_key": state.process_key,
            "parent_ref": state.parent_ref,
            "parent_text": state.parent_text,
            "wm": _watermark_json(state.watermark),
            "parent_written": _watermark_json(state.parent_written),
            "sent_keys": sorted(state.sent_keys),
            "stale_parents": sorted(state.stale_parents),
            "cleared_parents": sorted(state.cleared_parents),
            "degraded": state.degraded,
            "updated_at": _iso(state.updated_at),
        },
        sort_keys=True,
    )


def decode(raw: str) -> SentState:
    """Parse a v1 payload. Raises ValueError, KeyError or TypeError on anything else."""
    data = json.loads(raw)
    if not isinstance(data, dict) or data.get("v") != PAYLOAD_VERSION:
        raise ValueError("not a v1 Chappe payload")
    parent_ref = data["parent_ref"]
    return SentState(
        process_key=str(data["process_key"]),
        parent_ref=None if parent_ref is None else str(parent_ref),
        parent_text=str(data["parent_text"]),
        watermark=_watermark(data["wm"]),
        parent_written=_watermark(data["parent_written"]),
        sent_keys=frozenset(str(k) for k in data["sent_keys"]),
        stale_parents=frozenset(str(ts) for ts in data["stale_parents"]),
        cleared_parents=frozenset(str(ts) for ts in data["cleared_parents"]),
        degraded=bool(data["degraded"]),
        updated_at=_time(data["updated_at"]),
    )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AirflowVariableStore:
    """Implements the Store port. Never deletes its Variable (D5); `chappe cleanup` does."""

    def __init__(self, *, get: Get, set: Set, now: Callable[[], datetime] = _utcnow) -> None:
        self._get = get
        self._set = set
        self._now = now

    def load(self, process_key: str) -> SentState | None:
        """The stored state, or None when there is no Variable yet.

        Raises StoreError when the Variable cannot be read or decoded (another payload version).
        """
        key = key_for(process_key)
        try:
            raw = self._get(key)
        except Exception as exc:
            raise StoreError(f"cannot read Airflow Variable {key}: {exc}") from exc
        if raw is None:
            return None
        try:
            state = decode(raw)
        except _DECODE_ERRORS as exc:
            # Spec 9.2 "Store unreadable -> skip sending": without the state, a post risks a
            # duplicate parent, and a save would overwrite what is there.
            raise StoreError(f"cannot read Airflow Variable {key}: {exc!r}") from exc
        if state.process_key != process_key:
            # another run whose key hashes the same: treat as absent (a 64-bit collision)
            log.warning(
                "chappe: Variable %s holds process %r, not %r; ignoring it",
                key,
                state.process_key,
                process_key,
            )
            return None
        return state

    def save(self, process_key: str, state: SentState) -> SentState:
        """Merge `state` into what is stored now and write the result (contract D1)."""
        if state.process_key != process_key:
            raise StoreError(f"state of {state.process_key!r} saved under {process_key!r}")
        key = key_for(process_key)
        new = replace(state, updated_at=self._now())
        merged = merge_sent(self.load(process_key), new)  # re-read right before the write
        try:
            self._set(key, encode(merged))
        except Exception as exc:
            raise StoreError(f"cannot write Airflow Variable {key}: {exc}") from exc
        return merged


def _sdk_get(key: str) -> str | None:
    from airflow.sdk import Variable

    value = Variable.get(key, default=None)
    return None if value is None else str(value)


def _sdk_set(key: str, value: str) -> None:
    from airflow.sdk import Variable

    Variable.set(key, value)


def airflow_variable_store() -> AirflowVariableStore:
    """The store wired to `airflow.sdk.Variable`; works in task and DAG callbacks (spike S7)."""
    return AirflowVariableStore(get=_sdk_get, set=_sdk_set)


def _written_at(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    try:
        written = decode(raw).updated_at
    except _DECODE_ERRORS:
        return None
    if written is not None and written.tzinfo is None:
        written = written.replace(tzinfo=timezone.utc)
    return written


def cleanup(
    older_than: timedelta,
    *,
    now: datetime,
    list_keys: Callable[[], Iterable[str]],
    get: Get,
    delete: Delete,
    dry_run: bool = False,
) -> list[str]:
    """Delete `chappe__*` Variables last written before `now - older_than`; return their keys."""
    cutoff = now - older_than
    old: list[str] = []
    for key in sorted(list_keys()):
        if not key.startswith(KEY_PREFIX):
            continue
        written = _written_at(get(key))
        if written is None or written >= cutoff:
            continue
        old.append(key)
        if not dry_run:
            delete(key)
    return old


def airflow_db_keys() -> list[str]:
    """Keys of all `chappe__*` Variables, from the Airflow metadata database."""
    from airflow.models.variable import Variable
    from airflow.utils.session import create_session
    from sqlalchemy import select

    query = select(Variable.key).where(Variable.key.startswith(KEY_PREFIX, autoescape=True))
    with create_session() as session:
        keys = [str(key) for key in session.scalars(query).all()]
    return [key for key in keys if key.startswith(KEY_PREFIX)]


def airflow_db_get(key: str) -> str | None:
    from airflow.models.variable import Variable
    from airflow.utils.session import create_session
    from sqlalchemy import select

    with create_session() as session:
        row = session.scalars(select(Variable).where(Variable.key == key)).first()
        return None if row is None else str(row.val)


def airflow_db_delete(key: str) -> None:
    from airflow.models.variable import Variable

    Variable.delete(key)
