from dataclasses import replace

from chappe.core.model import ProcessState, StepState
from chappe.core.view import ProcessView
from chappe.themes import THEMES
from chappe.themes.builtin.ledger import LedgerTheme
from tests.support.samples import SAMPLES, TEST_MENTION, ProcessViewBuilder, default_context

S, R, P, F = StepState.SUCCEEDED, StepState.RUNNING, StepState.PENDING, StepState.FAILED
LINK = "<https://airflow.invalid/dags/orders/runs/run_1|orders 2026.10.1>"
SECOND = "Regression vs baseline 2026.10.0"


def render(view: ProcessView, *, collapse: bool = False):  # type: ignore[no-untyped-def]
    ctx = replace(default_context("ledger"), collapse_done_sections=collapse)
    return LedgerTheme().render(view, ctx)


def parent(view: ProcessView, *, collapse: bool = False) -> list[str]:
    return render(view, collapse=collapse).parent.text.split("\n")


def test_registered_as_ledger() -> None:
    assert THEMES["ledger"] is LedgerTheme
    assert LedgerTheme.name == "ledger"


def test_running_single_section_has_no_section_line() -> None:
    # Geometry started at 09:23 (after Parquet's 78 min); now is 09:28
    assert parent(SAMPLES["single_running"]) == [
        f":large_yellow_circle: *{LINK}* · In progress · 1h 23m · started 08:05",
        ":white_check_mark: Parquet → Delta",
        ":hourglass_flowing_sand: Geometry · running since 09:23",
        ":white_circle: Aggregates",
    ]


def test_a_finished_step_with_its_own_times_shows_its_duration() -> None:
    lines = parent(SAMPLES["single_step_finished"])
    assert lines[2] == ":white_check_mark: Geometry · 4h 21m"


def test_running_multi_section_lists_each_section_with_counts() -> None:
    assert parent(SAMPLES["multi_running"]) == [
        f":large_yellow_circle: *{LINK}* · In progress · 5h 54m · started 08:05",
        "*Prepare* · 3 of 3 done",
        ":white_check_mark: Parquet → Delta",
        ":white_check_mark: Geometry",
        ":white_check_mark: Aggregates",
        f"*{SECOND}* · 0 of 2 done",
        ":hourglass_flowing_sand: ID stability · running since 13:54",
        ":white_circle: Regression",
    ]


def test_collapse_folds_a_done_section_into_one_line() -> None:
    lines = parent(SAMPLES["multi_running"], collapse=True)
    assert lines[1:3] == ["*Prepare* · 3 of 3 done", f"*{SECOND}* · 0 of 2 done"]


def test_collapse_shows_the_span_only_when_every_step_is_timed() -> None:
    view = (
        ProcessViewBuilder()
        .section("Prepare")
        .step("Extract", S, 10)
        .step("Transform", S, 20)
        .section("Load")
        .step("Load", R)
        .build()
    )
    assert parent(view, collapse=True)[1] == "*Prepare* · 2 of 2 done · 30m"


def test_passed_run_is_exactly_one_line() -> None:
    # 78 + 261 + 10 min of steps, then 5 min: ended 13:59
    assert parent(SAMPLES["single_passed"]) == [
        f":large_green_circle: *{LINK}* · Passed in 5h 54m · started 08:05"
    ]


def test_failed_single_section_shows_counts_without_a_title() -> None:
    assert parent(SAMPLES["single_failed"]) == [
        f":red_circle: *{LINK}* · Failed after 3h 34m · started 08:05",
        "1 passed · 1 failed · 1 not run",
        ":white_check_mark: Parquet → Delta",
        ":x: Geometry · Failed",
        ":white_circle: Aggregates",
    ]


def test_failed_multi_section_lists_only_the_failing_section() -> None:
    lines = parent(SAMPLES["multi_failed"])
    assert lines[1:] == [
        f"*{SECOND}* · 1 passed · 1 failed",
        ":white_check_mark: ID stability",
        ":x: Regression · Failed",
    ]


def test_pending_says_waiting_without_a_duration() -> None:
    assert parent(SAMPLES["pending"])[0] == f":white_circle: *{LINK}* · Waiting · started 08:05"


def test_title_without_a_link_is_plain_bold() -> None:
    view = replace(SAMPLES["single_passed"], links=())
    assert parent(view)[0].startswith(":large_green_circle: *orders 2026.10.1* · Passed in ")


def test_an_empty_header_token_leaves_no_mark() -> None:
    ctx = default_context("ledger", {"extra": {"header_running": ""}})
    text = LedgerTheme().render(SAMPLES["single_running"], ctx).parent.text
    assert text.startswith(f"*{LINK}* · In progress")


def test_thread_gets_a_start_and_an_end_reply_from_the_steps_own_times() -> None:
    entries = render(SAMPLES["single_step_finished"]).thread
    assert [(e.key, e.text) for e in entries] == [
        ("step:main.geometry:started", ":hourglass_flowing_sand: *Geometry* · started 09:23"),
        ("step:main.geometry:succeeded", ":white_check_mark: *Geometry* · Passed 13:44 · 4h 21m"),
    ]
    assert not any(e.broadcast for e in entries)


def test_a_running_step_gets_only_its_start_reply() -> None:
    entries = render(SAMPLES["single_running"]).thread
    assert [e.key for e in entries] == ["step:main.geometry:started"]


def test_a_finished_run_gives_every_finished_step_an_end_reply_untimed() -> None:
    entries = render(SAMPLES["single_failed"]).thread
    assert [(e.key, e.text) for e in entries] == [
        ("step:main.parquet_delta:succeeded", ":white_check_mark: *Parquet → Delta* · Passed"),
        (
            "step:main.geometry:failed",
            ":x: *Geometry* · Failed\nExecutor ran out of memory after 3 retries",
        ),
    ]


def test_alert_names_the_failed_steps_with_error_and_log() -> None:
    (alert,) = render(SAMPLES["single_failed"]).alerts
    assert alert.key == "alert:process:failed"
    assert alert.text == (
        f"{TEST_MENTION} :red_circle: *orders 2026.10.1* · Geometry: Executor ran out of memory "
        "after 3 retries · <https://airflow.invalid/dags/orders/runs/run_1/tasks/geometry|Log>"
    )


def test_alert_without_a_known_failed_step_says_failed() -> None:
    view = ProcessViewBuilder().section("Main").step("Load", S, 3).finished(ProcessState.FAILED)
    (alert,) = render(view.build()).alerts
    assert alert.text == f"{TEST_MENTION} :red_circle: *orders 2026.10.1* · Failed"


def test_no_alert_unless_failed() -> None:
    assert render(SAMPLES["single_passed"]).alerts == ()


def test_names_are_escaped() -> None:
    view = ProcessViewBuilder().section("Main").step("A & <b>", R).build()
    assert parent(view)[1] == ":hourglass_flowing_sand: A &amp; &lt;b&gt; · running since 08:05"
