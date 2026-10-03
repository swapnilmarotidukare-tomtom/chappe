# tests/core/test_merge_sent.py
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from chappe.core.reconcile import SentState, merge_sent, slack_ts_key
from chappe.core.view import Watermark

T = datetime(2026, 10, 2, tzinfo=timezone.utc)
RUNNING = Watermark(False, 1, 2, T)
LATER = Watermark(False, 2, 3, T)
FINAL = Watermark(True, 3, 3, T)
LOW = "1791050263.100000"
HIGH = "1791050263.984869"


def test_slack_ts_key_orders_by_seconds_then_microseconds() -> None:
    assert slack_ts_key(HIGH) == (1791050263, 984869)
    assert slack_ts_key("1791050263") == (1791050263, 0)
    assert slack_ts_key("1.5") == slack_ts_key("1.500000") == (1, 500000)
    assert slack_ts_key("9.5") < slack_ts_key("10.000001")  # string order says the opposite
    assert slack_ts_key("1.000009") < slack_ts_key(
        "1.00001"
    )  # 9 µs < 10 µs; string order says the opposite


@pytest.mark.parametrize("ts", ["", "abc", "1.2.3", ".5", "1.", "-1.5", "1.5x"])
def test_slack_ts_key_rejects_malformed_ts(ts: str) -> None:
    with pytest.raises(ValueError):
        slack_ts_key(ts)


SECONDS = st.integers(0, 3_000_000_000)
FRACTIONS = st.text(alphabet="0123456789", min_size=1, max_size=6)


@given(SECONDS, FRACTIONS, SECONDS, FRACTIONS)
def test_slack_ts_key_matches_numeric_order(s1: int, f1: str, s2: int, f2: str) -> None:
    a, b = f"{s1}.{f1}", f"{s2}.{f2}"
    assert (slack_ts_key(a) < slack_ts_key(b)) == (Decimal(a) < Decimal(b))
    assert (slack_ts_key(a) == slack_ts_key(b)) == (Decimal(a) == Decimal(b))


def test_merge_keeps_both_writers_keys_and_the_newer_watermark() -> None:
    stored = SentState("k", parent_ref=LOW, watermark=LATER, sent_keys=frozenset({"a"}))
    new = SentState(
        "k",
        parent_ref=LOW,
        watermark=RUNNING,
        sent_keys=frozenset({"b"}),
        degraded=True,
        updated_at=T,
    )
    merged = merge_sent(stored, new)
    assert merged.sent_keys == {"a", "b"}
    assert merged.watermark == LATER
    assert merged.degraded
    assert merged.updated_at == T


def test_merge_into_nothing_returns_the_new_state() -> None:
    new = SentState(
        "k",
        parent_ref=LOW,
        parent_text="p",
        watermark=RUNNING,
        parent_written=RUNNING,
        updated_at=T,
    )
    assert merge_sent(None, new) == new


def test_lowest_ts_wins_and_the_other_parent_becomes_stale() -> None:
    stored = SentState("k", parent_ref=HIGH)
    merged = merge_sent(stored, SentState("k", parent_ref=LOW))
    assert merged.parent_ref == LOW
    assert merged.stale_parents == {HIGH}
    # The other order converges on the same parent.
    assert (
        merge_sent(SentState("k", parent_ref=LOW), SentState("k", parent_ref=HIGH)).parent_ref
        == LOW
    )


def test_a_cleared_parent_is_no_longer_stale() -> None:
    stored = SentState("k", parent_ref=LOW, stale_parents=frozenset({HIGH}))
    merged = merge_sent(stored, SentState("k", parent_ref=LOW, cleared_parents=frozenset({HIGH})))
    assert merged.stale_parents == frozenset()
    assert merged.cleared_parents == {HIGH}
    # A late writer that still thinks HIGH is its parent does not resurrect it.
    again = merge_sent(merged, SentState("k", parent_ref=HIGH))
    assert again.parent_ref == LOW
    assert again.stale_parents == frozenset()


def test_a_live_duplicate_takes_over_when_the_canonical_parent_is_cleared() -> None:
    stored = merge_sent(SentState("k", parent_ref=LOW), SentState("k", parent_ref=HIGH))
    assert (stored.parent_ref, stored.stale_parents) == (LOW, {HIGH})
    merged = merge_sent(stored, SentState("k", cleared_parents=frozenset({LOW})))
    assert merged.parent_ref == HIGH
    assert merged.stale_parents == frozenset()


def test_a_cleared_canonical_parent_never_wins_again() -> None:
    """A hand-deleted parent is cleared; a new, higher parent takes over (spec 9.2)."""
    stored = SentState("k", parent_ref=LOW)
    cleared = merge_sent(stored, SentState("k", cleared_parents=frozenset({LOW})))
    assert cleared.parent_ref is None
    assert cleared.stale_parents == frozenset()
    replaced = merge_sent(cleared, SentState("k", parent_ref=HIGH))
    assert replaced.parent_ref == HIGH
    # A late writer that still holds the deleted, lower ts does not win it back.
    late = merge_sent(replaced, SentState("k", parent_ref=LOW))
    assert late.parent_ref == HIGH
    assert late.stale_parents == frozenset()
    # Nor does a first save that carries a cleared ref of its own.
    assert (
        merge_sent(
            None, SentState("k", parent_ref=LOW, cleared_parents=frozenset({LOW}))
        ).parent_ref
        is None
    )


def test_the_last_parent_write_wins_the_text() -> None:
    stored = SentState(
        "k", parent_ref=LOW, parent_text="later", watermark=LATER, parent_written=LATER
    )
    keys_only = merge_sent(stored, SentState("k", parent_ref=LOW, sent_keys=frozenset({"a"})))
    assert (keys_only.parent_text, keys_only.parent_written) == ("later", LATER)
    # A late writer edited the parent after us: its write is recorded despite an older watermark.
    late = merge_sent(
        stored, SentState("k", parent_ref=LOW, parent_text="older", parent_written=RUNNING)
    )
    assert (late.parent_text, late.parent_written) == ("older", RUNNING)
    assert late.watermark == LATER


def test_a_finished_watermark_is_kept() -> None:
    stored = SentState("k", parent_ref=LOW, watermark=FINAL)
    merged = merge_sent(
        stored, SentState("k", parent_ref=LOW, watermark=Watermark(False, 9, 9, T.replace(hour=5)))
    )
    assert merged.watermark == FINAL


# Distinct Slack ts values; plain string order disagrees with numeric order for some pairs.
TS = st.sampled_from(
    [
        "999999999.999999",
        "1791050263.000001",
        "1791050263.000010",
        "1791050263.100000",
        "1791050263.984869",
        "1791050264.000000",
    ]
)
WATERMARKS = st.none() | st.builds(
    Watermark,
    st.booleans(),
    st.integers(0, 3),
    st.integers(0, 3),
    st.none() | st.sampled_from([T, T + timedelta(minutes=1)]),
)
STATES = st.builds(
    SentState,
    process_key=st.just("k"),
    parent_ref=st.none() | TS,
    parent_text=st.sampled_from(["", "a", "b"]),
    watermark=WATERMARKS,
    parent_written=WATERMARKS,
    sent_keys=st.frozensets(st.sampled_from(["a", "b", "c", "d"])),
    stale_parents=st.frozensets(TS, max_size=3),
    cleared_parents=st.frozensets(TS, max_size=2),
    degraded=st.booleans(),
    updated_at=st.none() | st.sampled_from([T, T + timedelta(days=1)]),
)


def same_order(a: Watermark | None, b: Watermark | None) -> bool:
    if a is None or b is None:
        return a is b
    return not a.newer_than(b) and not b.newer_than(a)


@given(STATES, STATES)
def test_merge_is_commutative_on_the_merged_fields(a: SentState, b: SentState) -> None:
    ab, ba = merge_sent(a, b), merge_sent(b, a)
    assert ab.sent_keys == ba.sent_keys == a.sent_keys | b.sent_keys
    assert ab.parent_ref == ba.parent_ref
    assert ab.stale_parents == ba.stale_parents
    assert ab.cleared_parents == ba.cleared_parents
    assert ab.degraded == ba.degraded
    assert same_order(ab.watermark, ba.watermark)


@given(STATES, STATES)
def test_merge_is_idempotent(a: SentState, b: SentState) -> None:
    merged = merge_sent(a, b)
    assert merge_sent(merged, merged) == merged
    assert merge_sent(merged, b) == merged


@given(st.lists(STATES, min_size=1, max_size=6))
def test_merging_a_history_keeps_every_invariant(history: list[SentState]) -> None:
    acc: SentState | None = None
    for state in history:
        acc = merge_sent(acc, state)
    assert acc is not None

    cleared = frozenset().union(*(s.cleared_parents for s in history))
    # parent_ref is the lowest ref ever merged that is not cleared (a deleted parent never wins);
    # a ref merged as stale is still a parent in Slack, so it counts
    refs = {s.parent_ref for s in history if s.parent_ref is not None}
    refs |= frozenset().union(*(s.stale_parents for s in history))
    live = refs - cleared
    assert acc.parent_ref == (min(live, key=slack_ts_key) if live else None)

    assert acc.cleared_parents == cleared
    assert not acc.stale_parents & cleared  # a cleared ts is never stale again
    assert acc.parent_ref not in acc.stale_parents
    assert acc.stale_parents == live - {acc.parent_ref}

    assert acc.sent_keys == frozenset().union(*(s.sent_keys for s in history))
    if any(s.watermark is not None and s.watermark.finished for s in history):
        assert acc.watermark is not None and acc.watermark.finished  # finished is never lost
