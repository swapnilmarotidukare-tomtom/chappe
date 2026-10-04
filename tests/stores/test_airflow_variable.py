# tests/stores/test_airflow_variable.py
from __future__ import annotations

import json
import logging
import re
import string
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from chappe.core.errors import StoreError
from chappe.core.reconcile import SentState
from chappe.core.view import Watermark
from chappe.stores.airflow_variable import (
    AirflowVariableStore,
    cleanup,
    key_for,
    variable_key,
)
from tests.support.fakes import FakeVariables

PK = "orders/manual__2026-10-02T13:35:00+00:00"
NOW = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
T = datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc)
RUNNING = Watermark(False, 1, 2, T)
FINAL = Watermark(True, 3, 3, T + timedelta(minutes=5))
LOW, HIGH = "1790000000.000002", "1790000000.000010"
LOGGER = "chappe.stores.airflow_variable"


def store_on(variables: FakeVariables, now: datetime = NOW) -> AirflowVariableStore:
    return AirflowVariableStore(get=variables.get, set=variables.set, now=lambda: now)


def test_nothing_stored_yet() -> None:
    assert store_on(FakeVariables()).load(PK) is None


def test_save_then_load_round_trips() -> None:
    variables = FakeVariables()
    store = store_on(variables)
    state = SentState(
        PK,
        parent_ref=LOW,
        parent_text="parent",
        watermark=RUNNING,
        parent_written=RUNNING,
        parent_wms={LOW: RUNNING},
        sent_keys=frozenset({("step:b", LOW), ("step:a", LOW)}),
        stale_parents=frozenset({HIGH}),
        cleared_parents=frozenset({"1790000000.000001"}),
        degraded=True,
    )
    expected = replace(state, updated_at=NOW)
    assert store.save(PK, state) == expected
    assert store.load(PK) == expected

    assert key_for(PK) == variable_key("orders", "manual__2026-10-02T13:35:00+00:00")
    payload = json.loads(variables.data[key_for(PK)])
    assert payload["v"] == 2
    assert payload["process_key"] == PK
    assert payload["sent_keys"] == [["step:a", LOW], ["step:b", LOW]]
    assert payload["parent_wms"] == {LOW: payload["wm"]}
    assert payload["wm"] == {
        "finished": False,
        "settled": 1,
        "started": 2,
        "occurred_at": "2026-10-02T09:00:00+00:00",
    }
    assert payload["updated_at"] == "2026-10-03T12:00:00+00:00"


def test_a_variable_of_another_process_is_ignored(caplog: pytest.LogCaptureFixture) -> None:
    variables = FakeVariables()
    store = store_on(variables)
    store.save("orders/other", SentState("orders/other", parent_ref=LOW))
    variables.data[key_for(PK)] = variables.data[key_for("orders/other")]
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert store.load(PK) is None
    assert "orders/other" in caplog.text


@pytest.mark.parametrize("raw", ["{not json", "[]", '{"v": 99}', '{"v": 1, "process_key": "x"}'])
def test_an_unreadable_variable_is_a_store_error(raw: str) -> None:
    """Spec 9.2 "Store unreadable -> skip sending": without the state a post risks a duplicate."""
    variables = FakeVariables()
    variables.data[key_for(PK)] = raw
    store = store_on(variables)
    with pytest.raises(StoreError, match=re.escape(key_for(PK))):
        store.load(PK)
    with pytest.raises(StoreError):
        store.save(PK, SentState(PK, parent_ref=LOW))  # never clobbers what it cannot read
    assert variables.data[key_for(PK)] == raw


def test_variable_errors_become_store_errors() -> None:
    def broken_get(key: str) -> str | None:
        raise RuntimeError("supervisor gone")

    def broken_set(key: str, value: str) -> None:
        raise RuntimeError("supervisor gone")

    variables = FakeVariables()
    with pytest.raises(StoreError, match="supervisor gone"):
        AirflowVariableStore(get=broken_get, set=variables.set).load(PK)
    with pytest.raises(StoreError, match="supervisor gone"):
        AirflowVariableStore(get=variables.get, set=broken_set).save(PK, SentState(PK))
    with pytest.raises(StoreError, match="saved under"):
        store_on(variables).save(PK, SentState("orders/other"))


def test_parallel_writers_keep_each_others_keys() -> None:
    variables = FakeVariables()
    a, b = store_on(variables), store_on(variables)
    seen_by_a: list[SentState | None] = []

    def a_reads_before_b_writes(key: str, value: str) -> None:
        variables.before_set = None
        seen_by_a.append(a.load(PK))

    variables.before_set = a_reads_before_b_writes
    b.save(PK, SentState(PK, parent_ref=LOW, watermark=FINAL, sent_keys=frozenset({("b", LOW)})))
    assert seen_by_a == [None]  # A read between B's re-read and B's write: it saw nothing

    mine = SentState(PK, parent_ref=LOW, watermark=RUNNING, sent_keys=frozenset({("a", LOW)}))
    merged = a.save(PK, mine)
    assert merged.live_keys == {"a", "b"}
    assert merged.watermark == FINAL  # finished stays
    assert store_on(variables).load(PK) == merged


def test_the_lowest_parent_ts_is_adopted_and_the_other_marked_stale() -> None:
    store = store_on(FakeVariables())
    store.save(PK, SentState(PK, parent_ref=HIGH, watermark=RUNNING))
    merged = store.save(PK, SentState(PK, parent_ref=LOW, watermark=RUNNING))
    assert (merged.parent_ref, merged.stale_parents) == (LOW, {HIGH})
    # The writer that posted HIGH saves again and learns that LOW is the parent.
    assert store.save(PK, SentState(PK, parent_ref=HIGH)).parent_ref == LOW
    # Once HIGH is deleted and recorded as cleared, it is no longer stale.
    cleared = store.save(PK, SentState(PK, parent_ref=LOW, cleared_parents=frozenset({HIGH})))
    assert cleared.stale_parents == frozenset()
    assert store.load(PK) == cleared


DAG_IDS = st.text(alphabet=string.ascii_letters + string.digits + "_.-", min_size=1, max_size=250)
RUN_IDS = st.text(max_size=250)
SAFE_KEY = re.compile(r"chappe__[A-Za-z0-9_.-]{1,80}__[0-9a-f]{16}")


@given(DAG_IDS, RUN_IDS)
@example("orders", "manual__2026-10-02T13:35:00+00:00")
@example("x" * 250, "scheduled__2026-10-02T00:00:00+00:00 ✓ ünïcode:+/")
def test_variable_key_is_short_and_safe(dag_id: str, run_id: str) -> None:
    key = variable_key(dag_id, run_id)
    assert len(key) <= 250
    assert SAFE_KEY.fullmatch(key)
    assert key_for(f"{dag_id}/{run_id}") == key


def test_dag_ids_sharing_a_long_prefix_get_different_keys() -> None:
    prefix = "d" * 80
    assert variable_key(prefix + "_a", "r") != variable_key(prefix + "_b", "r")


def test_cleanup_deletes_only_old_chappe_variables() -> None:
    variables = FakeVariables()
    store_on(variables, now=NOW - timedelta(days=10)).save("old/r", SentState("old/r"))
    store_on(variables, now=NOW - timedelta(days=1)).save("new/r", SentState("new/r"))
    variables.set("unrelated", "{}")
    variables.set(variable_key("broken", "r"), "{not json")

    def run(dry_run: bool) -> list[str]:
        return cleanup(
            timedelta(days=7),
            now=NOW,
            list_keys=variables.keys,
            get=variables.get,
            delete=variables.delete,
            dry_run=dry_run,
        )

    assert run(dry_run=True) == [key_for("old/r")]
    assert key_for("old/r") in variables.data
    assert run(dry_run=False) == [key_for("old/r")]
    assert variables.keys() == sorted([key_for("new/r"), "unrelated", variable_key("broken", "r")])
    assert run(dry_run=False) == []


def test_times_without_an_offset_are_read_as_utc() -> None:
    variables = FakeVariables()
    store = store_on(variables)
    store.save(PK, SentState(PK, watermark=RUNNING))
    payload = json.loads(variables.data[key_for(PK)])
    payload["wm"]["occurred_at"] = "2026-10-02T09:00:00"
    payload["updated_at"] = "2026-10-03T12:00:00"
    variables.data[key_for(PK)] = json.dumps(payload)

    loaded = store.load(PK)
    assert loaded is not None
    assert loaded.watermark == RUNNING  # naive would raise TypeError on comparison
    assert loaded.updated_at == NOW
    assert loaded.watermark is not None and loaded.watermark.occurred_at is not None
    assert loaded.watermark.occurred_at.tzinfo is not None


V1_PAYLOAD = {
    "v": 1,
    "process_key": PK,
    "parent_ref": LOW,
    "parent_text": "parent",
    "wm": {
        "finished": False,
        "settled": 1,
        "started": 2,
        "occurred_at": "2026-10-02T09:00:00+00:00",
    },
    "parent_written": None,
    "sent_keys": ["step:a", "step:b"],
    "stale_parents": [HIGH],
    "cleared_parents": [],
    "degraded": False,
    "updated_at": "2026-10-03T12:00:00+00:00",
}


def test_a_v1_variable_is_read_and_written_back_as_v2() -> None:
    """Runs in flight during an upgrade keep their parent and their sent keys."""
    variables = FakeVariables()
    variables.data[key_for(PK)] = json.dumps(V1_PAYLOAD)
    store = store_on(variables)
    state = store.load(PK)
    assert state is not None
    assert state.sent_keys == {("step:a", LOW), ("step:b", LOW)}
    assert state.live_keys == {"step:a", "step:b"}
    assert state.parent_wms == {LOW: RUNNING}
    assert (state.parent_ref, state.stale_parents) == (LOW, {HIGH})

    store.save(PK, SentState(PK, sent_keys=frozenset({("step:c", LOW)})))
    payload = json.loads(variables.data[key_for(PK)])
    assert payload["v"] == 2
    assert payload["sent_keys"] == [["step:a", LOW], ["step:b", LOW], ["step:c", LOW]]


def test_a_v1_variable_without_a_parent_keeps_no_keys() -> None:
    """v1 kept keys whose parent was deleted; v2 sends them again under the new parent."""
    variables = FakeVariables()
    variables.data[key_for(PK)] = json.dumps(
        {**V1_PAYLOAD, "parent_ref": None, "stale_parents": []}
    )
    state = store_on(variables).load(PK)
    assert state is not None
    assert (state.parent_ref, state.sent_keys, state.parent_wms) == (None, frozenset(), {})
