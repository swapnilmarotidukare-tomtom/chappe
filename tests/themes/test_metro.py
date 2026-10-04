import re
from dataclasses import replace

from chappe.core.model import ProcessState, StepState
from chappe.core.render import Limits
from chappe.core.view import ProcessView
from chappe.themes import THEMES
from chappe.themes.builtin.metro import FENCE, LINE_WIDTH, MetroTheme
from tests.support.samples import SAMPLES, TEST_MENTION, ProcessViewBuilder, default_context

S, R, P, F, K = (
    StepState.SUCCEEDED,
    StepState.RUNNING,
    StepState.PENDING,
    StepState.FAILED,
    StepState.SKIPPED,
)


def parent(view: ProcessView, limit: int | None = None) -> str:
    ctx = default_context("metro")
    if limit is not None:
        ctx = replace(ctx, limits=Limits(parent_chars=limit, entry_chars=1500))
    return MetroTheme().render(view, ctx).parent.text


def block(text: str) -> list[str]:
    """The lines between the opening and the closing code fence."""
    lines = text.split("\n")
    start = lines.index(FENCE)
    end = lines.index(FENCE, start + 1)
    return lines[start + 1 : end]


def four_steps() -> ProcessViewBuilder:
    # BASE is 08:05 UTC; Extract runs 08:05-08:07 (timed), Transform starts 08:07, now = 08:12
    return (
        ProcessViewBuilder()
        .section("Main")
        .step("Extract", S, 2)
        .step("Transform", R)
        .step("Load", P)
        .step("Report", P)
    )


def test_registered_as_metro() -> None:
    assert THEMES["metro"] is MetroTheme
    assert MetroTheme.name == "metro"


def test_running_run_renders_title_status_stations_and_link() -> None:
    assert parent(four_steps().build()).split("\n") == [
        "*orders 2026.10.1*",
        ":large_yellow_circle: In progress · started 08:05 · 7m",
        FENCE,
        "●  Extract" + " " * 24 + "2m",
        "┃",
        "◉  Transform" + " " * 14 + "running 5m",
        "┆",
        "○  Load",
        "┆",
        "○  Report",
        FENCE,
        "<https://airflow.invalid/dags/orders/runs/run_1|Airflow run>",
    ]


def test_every_station_line_with_right_text_is_exactly_the_line_width() -> None:
    checked = 0
    for name in ("single_running", "single_step_finished", "multi_running", "fifty_steps"):
        for line in block(parent(SAMPLES[name])):
            body = line[4:] if line.startswith(("┃   ", "    ")) else line
            # a station with right text: glyph, two spaces, name, then a gap of 2+ spaces
            if body[:1] in "●◉✖◌○" and re.search(r"\S {2,}\S", body[3:]):
                assert len(line) == LINE_WIDTH, (name, line)
                checked += 1
    assert checked >= 4  # the samples do carry right text


def test_passed_run_has_a_green_status_and_solid_connectors() -> None:
    text = parent(SAMPLES["single_passed"])
    assert text.split("\n")[1].startswith(":large_green_circle: Passed · started 08:05 · ")
    assert block(text) == ["●  Parquet → Delta", "┃", "●  Geometry", "┃", "●  Aggregates"]


def test_failed_and_skipped_stations() -> None:
    failed = block(parent(SAMPLES["single_failed"]))
    assert failed[2] == "✖  Geometry" + " " * (LINE_WIDTH - len("✖  Geometry") - 6) + "failed"
    assert failed[3] == "┃"
    assert failed[4] == "○  Aggregates"
    skipped = block(parent(SAMPLES["with_skipped"]))
    assert skipped[2] == "◌  Backfill" + " " * (LINE_WIDTH - len("◌  Backfill") - 7) + "skipped"


def test_right_text_without_times() -> None:
    view = (
        ProcessViewBuilder()
        .section("Main")
        .step("Extract", S, 2, timed=False)
        .step("Transform", R, timed=False)
        .build()
    )
    lines = block(parent(view))
    assert lines[0] == "●  Extract"
    assert lines[2] == "◉  Transform" + " " * (LINE_WIDTH - len("◉  Transform") - 7) + "running"


def visible(line: str) -> str:
    """What Slack shows: entities count as one character."""
    return line.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")


def test_a_long_name_is_cut_to_the_line_width() -> None:
    (line,) = block(parent(SAMPLES["unicode_long"]))
    # "running 5m" takes 10 characters plus one space; the name gets 22 and ends with "…"
    assert line == "◉  Generate &amp; consolidat… running 5m"
    assert len(visible(line)) == LINE_WIDTH


def test_escaping_does_not_shift_the_alignment() -> None:
    view = ProcessViewBuilder().section("Main").step("A & B <c>", R).build()
    (line,) = block(parent(view))
    assert line == "◉  A &amp; B &lt;c&gt;" + " " * 14 + "running 5m"
    assert len(visible(line)) == LINE_WIDTH


def test_sections_branch_off_the_trunk() -> None:
    lines = block(parent(SAMPLES["multi_running"]))
    assert lines[:5] == ["●  Parquet → Delta", "┃", "●  Geometry", "┃", "●  Aggregates"]
    assert lines[5] == "┗━ Regression vs baseline 2026.10.0"
    assert lines[6].startswith("    ◉  ID stability") and len(lines[6]) == LINE_WIDTH
    assert lines[7] == "    ○  Regression"


def test_middle_branches_use_the_tee_and_keep_the_trunk_bar() -> None:
    lines = block(parent(SAMPLES["fifty_steps"]))
    assert "┣━ Section 2" in lines
    assert "┗━ Section 5" in lines
    assert any(line.startswith("┃   ●  Step 2.1") for line in lines)
    assert any(line.startswith("    ○  Step 5.1") for line in lines)


def test_a_big_run_collapses_stations_and_keeps_the_fence_closed() -> None:
    builder = ProcessViewBuilder(title="big").section("Main")
    for index in range(1, 61):
        builder.step(f"Step {index}", S, 1, timed=False)
    text = parent(builder.build(), limit=600)
    assert len(text) <= 600
    lines = text.split("\n")
    assert lines.count(FENCE) == 2
    stations = block(text)
    assert stations[0] == "●  Step 1"
    assert stations[-1] == "●  Step 60"
    assert any(line.startswith("┆  … ") and line.endswith(" more") for line in stations)


def test_no_thread_entries_in_any_state() -> None:
    for view in SAMPLES.values():
        assert MetroTheme().render(view, default_context("metro")).thread == ()


def test_alert_names_each_failed_step_with_error_and_log() -> None:
    (alert,) = MetroTheme().render(SAMPLES["multi_failed"], default_context("metro")).alerts
    assert alert.key == "alert:process:failed"
    assert alert.text.startswith(
        f"{TEST_MENTION} :rotating_light: *orders 2026.10.1* · Regression "
    )
    assert "failed: Executor ran out of memory after 3 retries" in alert.text
    assert alert.text.endswith(
        "<https://airflow.invalid/dags/orders/runs/run_1/tasks/regression|Log>"
    )


def test_alert_with_a_timed_failure_says_how_long_and_without_failed_steps_says_failed() -> None:
    timed = (
        ProcessViewBuilder()
        .section("Main")
        .step("Load", F, 3, error="boom")
        .finished(ProcessState.FAILED)
        .build()
    )
    (alert,) = MetroTheme().render(timed, default_context("metro")).alerts
    assert "Load failed after 3m: boom" in alert.text
    nothing = (
        ProcessViewBuilder()
        .section("Main")
        .step("Load", S, 3)
        .finished(ProcessState.FAILED)
        .build()
    )
    (alert,) = MetroTheme().render(nothing, default_context("metro")).alerts
    assert alert.text == f"{TEST_MENTION} :rotating_light: *orders 2026.10.1* · Failed"


def test_no_alert_unless_failed() -> None:
    assert MetroTheme().render(SAMPLES["single_passed"], default_context("metro")).alerts == ()


def test_a_title_cannot_close_the_fence_or_break_the_line() -> None:
    view = ProcessViewBuilder().section("Main").step("a```b\nc", R).build()
    text = parent(view)
    assert text.split("\n").count(FENCE) == 2
    (line,) = block(text)
    assert line.startswith("◉  a'''b c")


def test_a_section_title_cannot_close_the_fence_or_break_the_line() -> None:
    view = (
        ProcessViewBuilder()
        .section("Main")
        .step("One", S, 1)
        .section("Br```x\ny")
        .step("Two", R)
        .build()
    )
    text = parent(view)
    assert text.split("\n").count(FENCE) == 2
    assert "┗━ Br'''x y" in block(text)


def test_when_no_block_fits_the_link_is_kept_whole() -> None:
    text = parent(four_steps().build(), limit=120)
    assert FENCE not in text
    assert text.endswith("<https://airflow.invalid/dags/orders/runs/run_1|Airflow run>")
    assert len(text) <= 120


def test_token_overrides_reach_the_stations() -> None:
    ctx = default_context("metro", {"extra": {"glyph_running": "*", "running": "läuft"}})
    lines = block(MetroTheme().render(four_steps().build(), ctx).parent.text)
    assert lines[2] == "*  Transform" + " " * 16 + "läuft 5m"
