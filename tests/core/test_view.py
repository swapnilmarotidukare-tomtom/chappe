from datetime import datetime, timedelta, timezone

from hypothesis import given
from hypothesis import strategies as st

from chappe.core.model import ProcessState, StepState
from chappe.core.view import ProcessView, SectionView, StepView, Watermark

T0 = datetime(2026, 10, 2, 8, 0, tzinfo=timezone.utc)


def step(key: str, state: StepState, start: int | None = None, end: int | None = None) -> StepView:
    return StepView(
        key=key,
        title=key.title(),
        state=state,
        started_at=None if start is None else T0 + timedelta(minutes=start),
        ended_at=None if end is None else T0 + timedelta(minutes=end),
    )


def view(state: ProcessState, *sections: SectionView, now: int = 120) -> ProcessView:
    return ProcessView(
        key="dag/run",
        title="orders 2026.10.1",
        state=state,
        started_at=T0,
        ended_at=None,
        now=T0 + timedelta(minutes=now),
        sections=sections,
    )


def test_section_state_is_derived_from_its_steps() -> None:
    assert SectionView("s", "S", (step("a", StepState.PENDING),)).state is ProcessState.PENDING
    running = (step("a", StepState.SUCCEEDED, 0, 5), step("b", StepState.RUNNING, 5))
    assert SectionView("s", "S", running).state is ProcessState.RUNNING
    done = (step("a", StepState.SUCCEEDED, 0, 5), step("b", StepState.SKIPPED, 5, 5))
    assert SectionView("s", "S", done).state is ProcessState.SUCCEEDED
    failed = (step("a", StepState.FAILED, 0, 5), step("b", StepState.PENDING))
    assert SectionView("s", "S", failed).state is ProcessState.FAILED


def test_progress_current_next_and_failed_steps() -> None:
    v = view(
        ProcessState.RUNNING,
        SectionView(
            "prep",
            "Prep",
            (
                step("a", StepState.SUCCEEDED, 0, 10),
                step("b", StepState.RUNNING, 10),
                step("c", StepState.PENDING),
            ),
        ),
        SectionView(
            "cmp", "Cmp", (step("d", StepState.FAILED, 0, 3), step("e", StepState.PENDING))
        ),
    )
    assert v.progress == (2, 5)
    assert [s.key for s in v.current_steps] == ["b"]
    assert [s.key for s in v.next_steps] == ["c", "e"]
    assert [s.key for s in v.failed_steps] == ["d"]


def test_step_duration_runs_until_now_while_running() -> None:
    assert step("a", StepState.RUNNING, 10).duration(T0 + timedelta(minutes=40)) == timedelta(
        minutes=30
    )
    assert step("b", StepState.PENDING).duration(T0) is None


def test_watermark_counts_steps_and_uses_the_event_time() -> None:
    v = view(
        ProcessState.RUNNING,
        SectionView(
            "s",
            "S",
            (
                step("a", StepState.SUCCEEDED, 0, 10),
                step("b", StepState.RUNNING, 10),
                step("c", StepState.PENDING),
            ),
        ),
    )
    assert v.watermark == Watermark(
        finished=False, settled_steps=1, started_steps=2, occurred_at=T0 + timedelta(minutes=120)
    )


def test_more_settled_steps_win_over_a_later_clock() -> None:
    behind = view(
        ProcessState.RUNNING,
        SectionView(
            "s", "S", (step("a", StepState.SUCCEEDED, 0, 10), step("b", StepState.RUNNING, 10))
        ),
        now=60,
    )
    # This worker's clock is 30 minutes behind, but its view has one more settled step.
    ahead = view(
        ProcessState.RUNNING,
        SectionView(
            "s",
            "S",
            (step("a", StepState.SUCCEEDED, 0, 10), step("b", StepState.SUCCEEDED, 10, 20)),
        ),
        now=30,
    )
    assert ahead.watermark.newer_than(behind.watermark)
    assert not behind.watermark.newer_than(ahead.watermark)


def test_a_finished_watermark_is_newer_than_any_unfinished_one() -> None:
    early_final = Watermark(True, 1, 1, T0)
    late_running = Watermark(False, 5, 5, T0 + timedelta(hours=1))
    assert early_final.newer_than(late_running)
    assert not late_running.newer_than(early_final)
    assert late_running.newer_than(None)
    assert not late_running.newer_than(late_running)


def test_started_steps_break_ties_on_settled_steps() -> None:
    more_started = Watermark(False, 1, 3, T0)
    fewer_started = Watermark(False, 1, 2, T0 + timedelta(hours=1))
    assert more_started.newer_than(fewer_started)
    assert not fewer_started.newer_than(more_started)


def test_occurred_at_only_breaks_ties() -> None:
    first = Watermark(False, 1, 2, T0)
    second = Watermark(False, 1, 2, T0 + timedelta(seconds=1))
    assert second.newer_than(first)
    assert not first.newer_than(second)
    assert first.newer_than(Watermark(False, 1, 2, None))  # no event time counts as the epoch
    assert not Watermark(False, 1, 2, T0).newer_than(first)  # equal is not newer


TIMES = st.none() | st.datetimes(timezones=st.just(timezone.utc))


@given(
    st.booleans(),
    st.integers(0, 50),
    st.integers(1, 50),
    st.integers(0, 50),
    st.integers(0, 50),
    TIMES,
    TIMES,
)
def test_clock_skew_cannot_reorder_progress(
    finished: bool,
    settled: int,
    extra: int,
    started_a: int,
    started_b: int,
    time_a: datetime | None,
    time_b: datetime | None,
) -> None:
    ahead = Watermark(finished, settled + extra, started_a, time_a)
    behind = Watermark(finished, settled, started_b, time_b)
    assert ahead.newer_than(behind)
    assert not behind.newer_than(ahead)
