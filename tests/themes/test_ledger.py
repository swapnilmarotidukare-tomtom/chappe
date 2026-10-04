from dataclasses import replace

from chappe.core.model import ProcessState, StepState
from chappe.core.render import Limits
from chappe.core.view import ProcessView
from chappe.themes import THEMES
from chappe.themes.builtin.ledger import LedgerTheme
from tests.support.samples import SAMPLES, TEST_MENTION, ProcessViewBuilder, default_context

S, R, P, F = StepState.SUCCEEDED, StepState.RUNNING, StepState.PENDING, StepState.FAILED
LINK = "<https://airflow.invalid/dags/orders/runs/run_1|orders 2026.10.1>"
SECOND = "Regression vs baseline 2026.10.0"


def footer(at: str) -> list[str]:
    return ["", f":stopwatch: Last updated: 2026-10-02 {at}:00 UTC"]


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
        f":large_yellow_circle: *{LINK}* · In progress · started 2026-10-02 08:05:00 UTC",
        ":white_check_mark: Parquet → Delta",
        ":hourglass_flowing_sand: Geometry · running since 2026-10-02 09:23:00 UTC",
        ":white_circle: Aggregates",
        *footer("09:28"),
    ]


def test_a_finished_step_with_its_own_times_shows_its_duration() -> None:
    lines = parent(SAMPLES["single_step_finished"])
    assert lines[2] == ":white_check_mark: Geometry · 4h 21m"


def test_running_multi_section_lists_each_section_with_counts() -> None:
    assert parent(SAMPLES["multi_running"]) == [
        f":large_yellow_circle: *{LINK}* · In progress · started 2026-10-02 08:05:00 UTC",
        "*Prepare* · 3 of 3 done",
        ":white_check_mark: Parquet → Delta",
        ":white_check_mark: Geometry",
        ":white_check_mark: Aggregates",
        f"*{SECOND}* · 0 of 2 done",
        ":hourglass_flowing_sand: ID stability · running since 2026-10-02 13:54:00 UTC",
        ":white_circle: Regression",
        *footer("13:59"),
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


def test_completed_run_lists_every_step() -> None:
    # 78 + 261 + 10 min of steps, then 5 min: ended 13:59
    assert parent(SAMPLES["single_passed"]) == [
        f":large_green_circle: *{LINK}* · Completed in 5h 54m · started 2026-10-02 08:05:00 UTC",
        ":white_check_mark: Parquet → Delta",
        ":white_check_mark: Geometry",
        ":white_check_mark: Aggregates",
        *footer("13:59"),
    ]


def test_failed_single_section_shows_counts_without_a_title() -> None:
    assert parent(SAMPLES["single_failed"]) == [
        f":red_circle: *{LINK}* · Failed after 3h 34m · started 2026-10-02 08:05:00 UTC",
        "1 completed · 1 failed · 1 not run",
        ":white_check_mark: Parquet → Delta",
        ":x: Geometry · Failed",
        ":white_circle: Aggregates",
        *footer("11:39"),
    ]


def test_failed_multi_section_lists_only_the_failing_section() -> None:
    lines = parent(SAMPLES["multi_failed"])
    assert lines[1:] == [
        f"*{SECOND}* · 1 completed · 1 failed",
        ":white_check_mark: ID stability",
        ":x: Regression · Failed",
        *footer("18:29"),
    ]


def test_pending_says_waiting_without_a_duration() -> None:
    waiting = f":white_circle: *{LINK}* · Waiting · started 2026-10-02 08:05:00 UTC"
    assert parent(SAMPLES["pending"])[0] == waiting


def test_title_without_a_link_is_plain_bold() -> None:
    view = replace(SAMPLES["single_passed"], links=())
    assert parent(view)[0].startswith(":large_green_circle: *orders 2026.10.1* · Completed in ")


def test_an_empty_header_token_leaves_no_mark() -> None:
    ctx = default_context("ledger", {"extra": {"header_running": ""}})
    text = LedgerTheme().render(SAMPLES["single_running"], ctx).parent.text
    assert text.startswith(f"*{LINK}* · In progress")


def test_thread_gets_a_start_and_an_end_reply_from_the_steps_own_times() -> None:
    entries = render(SAMPLES["single_step_finished"]).thread
    assert [(e.key, e.text) for e in entries] == [
        ("step:main.geometry:started", "STARTED: *Geometry*"),
        ("step:main.geometry:succeeded", "COMPLETED: *Geometry* · 4h 21m"),
    ]
    assert not any(e.broadcast for e in entries)


def test_a_running_step_gets_only_its_start_reply() -> None:
    entries = render(SAMPLES["single_running"]).thread
    assert [e.key for e in entries] == ["step:main.geometry:started"]


def test_a_finished_run_gives_every_finished_step_an_end_reply_untimed() -> None:
    entries = render(SAMPLES["single_failed"]).thread
    assert [(e.key, e.text) for e in entries] == [
        ("step:main.parquet_delta:succeeded", "COMPLETED: *Parquet → Delta*"),
        (
            "step:main.geometry:failed",
            "FAILED: *Geometry*\nExecutor ran out of memory after 3 retries",
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
    since = "running since 2026-10-02 08:05:00 UTC"
    assert parent(view)[1] == f":hourglass_flowing_sand: A &amp; &lt;b&gt; · {since}"


def test_a_failed_step_with_its_own_times_gets_its_duration_and_error_in_the_thread() -> None:
    view = (
        ProcessViewBuilder().section("Main").step("Load", F, 2, error="ValueError: bad row").build()
    )
    (start, end) = render(view).thread
    assert start.text == "STARTED: *Load*"
    assert end.key == "step:main.load:failed"
    assert end.text == "FAILED: *Load* · 2m\nValueError: bad row"


def test_a_skipped_step_gets_its_end_reply_only_once_the_run_finished() -> None:
    running = ProcessViewBuilder().section("Main").step("Backfill", StepState.SKIPPED, 0).build()
    assert [e.key for e in render(running).thread] == ["step:main.backfill:started"]
    done = (
        ProcessViewBuilder()
        .section("Main")
        .step("Backfill", StepState.SKIPPED, 0)
        .finished(ProcessState.SUCCEEDED)
        .build()
    )
    assert "step:main.backfill:skipped" in [e.key for e in render(done).thread]


def test_replies_follow_the_steps_own_times() -> None:
    view = (
        ProcessViewBuilder()
        .section("Main")
        .step("Extract", S, 2)
        .step("Transform", S, 3)
        .step("Load", R)
        .build()
    )
    assert [e.key for e in render(view).thread] == [
        "step:main.extract:started",
        "step:main.extract:succeeded",
        "step:main.transform:started",
        "step:main.transform:succeeded",
        "step:main.load:started",
    ]


def test_a_single_section_never_collapses() -> None:
    assert parent(SAMPLES["single_passed"], collapse=True)[1:] == [
        ":white_check_mark: Parquet → Delta",
        ":white_check_mark: Geometry",
        ":white_check_mark: Aggregates",
        *footer("13:59"),
    ]


def test_a_failed_section_stays_listed_when_collapsing() -> None:
    assert parent(SAMPLES["multi_failed"], collapse=True)[1:] == parent(SAMPLES["multi_failed"])[1:]


def test_a_long_run_keeps_the_failed_step_and_says_how_many_are_hidden() -> None:
    builder = ProcessViewBuilder().section("Main")
    for index in range(1, 201):
        state = F if index == 150 else (S if index < 150 else P)
        builder.step(f"Step number {index}", state, 1, timed=False)
    text = render(builder.finished(ProcessState.FAILED).build()).parent.text
    assert len(text) <= Limits().parent_chars
    assert ":x: Step number 150 · Failed" in text.split("\n")
    assert any(line.startswith("… ") and line.endswith(" more") for line in text.split("\n"))


def test_a_long_title_keeps_its_link_whole() -> None:
    view = replace(SAMPLES["single_running"], title="x" * 5000)
    header = render(view).parent.text.split("\n")[0]
    assert header.count("<https://") == 1 and ">* · In progress" in header


def test_a_long_alert_names_whole_steps_only() -> None:
    builder = ProcessViewBuilder().section("Main")
    for index in range(1, 31):
        builder.step(f"Step {index}", F, 1, error="e" * 120, timed=False)
    (alert,) = render(builder.finished(ProcessState.FAILED).build()).alerts
    assert len(alert.text) <= Limits().entry_chars
    assert alert.text.count("<https://") == alert.text.count("|Log>")
    assert " more · <" in alert.text


def test_an_empty_failed_mark_leaves_no_double_space_in_the_alert() -> None:
    ctx = default_context("ledger", {"extra": {"header_failed": ""}})
    (alert,) = LedgerTheme().render(SAMPLES["single_failed"], ctx).alerts
    assert alert.text.startswith(f"{TEST_MENTION} *orders 2026.10.1* · Geometry")


def test_last_updated_is_always_utc_whatever_the_configured_timezone() -> None:
    from datetime import timedelta, timezone

    ctx = replace(default_context("ledger"), tz=timezone(timedelta(hours=5, minutes=30)))
    lines = LedgerTheme().render(SAMPLES["single_running"], ctx).parent.text.split("\n")
    # the run's start follows the config timezone
    assert lines[0].endswith("· started 2026-10-02 13:35:00 UTC+05:30")
    assert lines[-2:] == footer("09:28")  # the footer stays in UTC


def test_a_waiting_run_has_no_last_updated_line() -> None:
    assert "Last updated" not in render(SAMPLES["pending"]).parent.text
