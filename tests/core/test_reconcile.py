# tests/core/test_reconcile.py
from datetime import datetime, timezone

from hypothesis import given
from hypothesis import strategies as st

from chappe.core.messages import Alert, MessageSet, ParentMessage, ThreadEntry
from chappe.core.reconcile import SentState, plan_sends, write_allowed
from chappe.core.view import Watermark

T = datetime(2026, 10, 2, tzinfo=timezone.utc)
RUNNING = Watermark(False, 1, 2, T)
LATER = Watermark(False, 2, 3, T)
FINAL = Watermark(True, 3, 3, T)
PARENT = "1791050263.984869"


def messages(
    text: str = "p", keys: tuple[str, ...] = ("a",), alerts: tuple[str, ...] = ()
) -> MessageSet:
    return MessageSet(
        ParentMessage(text, text),
        tuple(ThreadEntry(k, k) for k in keys),
        tuple(Alert(k, k) for k in alerts),
    )


def test_first_event_posts_the_parent_and_every_entry() -> None:
    plan = plan_sends(messages(keys=("a", "b"), alerts=("x",)), None)
    assert plan.post_parent and not plan.update_parent
    assert [e.key for e in plan.entries] == ["a", "b"]
    assert [a.key for a in plan.alerts] == ["x"]


def test_known_parent_is_edited_only_when_its_text_changed() -> None:
    sent = SentState(
        "k", parent_ref=PARENT, parent_text="p", watermark=RUNNING, sent_keys=frozenset({"a"})
    )
    assert plan_sends(messages("p", ("a",)), sent).empty
    plan = plan_sends(messages("q", ("a", "b")), sent)
    assert plan.update_parent and not plan.post_parent
    assert [e.key for e in plan.entries] == ["b"]


def test_write_rules() -> None:
    assert write_allowed(RUNNING, None)
    assert write_allowed(RUNNING, SentState("k"))  # no watermark seen yet
    later_sent = SentState("k", parent_ref=PARENT, watermark=LATER)
    assert not write_allowed(RUNNING, later_sent)  # an older render loses
    assert not write_allowed(LATER, later_sent)  # an equal render is not newer
    assert write_allowed(FINAL, later_sent)
    final_sent = SentState("k", parent_ref=PARENT, watermark=FINAL)
    assert not write_allowed(
        Watermark(False, 9, 9, T.replace(hour=5)), final_sent
    )  # finished is sticky


@given(st.sets(st.text(min_size=1, max_size=5)), st.sets(st.text(min_size=1, max_size=5)))
def test_plan_never_resends_known_keys(rendered: set[str], known: set[str]) -> None:
    sent = SentState("k", parent_ref=PARENT, parent_text="p", sent_keys=frozenset(known))
    plan = plan_sends(messages("p", tuple(sorted(rendered))), sent)
    assert {e.key for e in plan.entries} == rendered - known
