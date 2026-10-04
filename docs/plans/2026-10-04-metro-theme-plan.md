# Metro Theme Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the cluttered `thread` theme with a clean `metro` theme as the default (one run = one message: title, status line, the run drawn as a line of stations), and shrink the example to one flat DAG with four steps.

**Architecture:** A new built-in theme `MetroTheme` (`src/chappe/themes/builtin/metro.py` + `metro.yaml`) implementing the existing `Theme` port: it renders the parent message only (title, status line, a fenced code block of stations, links) and one alert on failure; no thread entries. It is registered in `THEMES`, becomes the config default, and the `thread` theme is deleted. The engine is not touched. Engine tests that need a theme with step replies keep a test-only copy of the old theme under `tests/support/`.

**Tech Stack:** Python ≥3.10, pydantic config models, pytest with text snapshots (`--chappe-update-snapshots`), Airflow 3.2.2 `dag.test()` for the example test, uv.

**Spec:** `docs/specs/2026-10-04-metro-theme-design.md` (amends `docs/specs/2026-10-03-chappe-0.0.1-design.md` §6).

## Global Constraints

- Principle: "Chappe never breaks or blocks a pipeline. It prefers being late over being wrong, and silence over a duplicate."
- Messages are plain Slack `mrkdwn` text with emoji. No Block Kit, no attachment colors, no buttons. A fenced code block (```` ``` ````) is plain mrkdwn and allowed.
- `chappe.core` and `chappe.ports` import neither `airflow` nor `slack_sdk` (import-linter); themes live in `chappe.themes` and use only `chappe.core`.
- Theme rules (spec 0.0.1 §6.4, enforced by `tests/support/conformance.py`): deterministic; every state rendered and the process state stated in words in the parent; within `ctx.limits`; a failed process always alerts the configured mention; stable keys; a single section renders without a section header; DAG-supplied text escaped.
- Station line width: `LINE_WIDTH = 36` characters. Glyphs: `●` succeeded, `◉` running, `✖` failed, `◌` skipped, `○` pending; connectors `┃` (after a finished station) and `┆` (otherwise).
- Status circles (tokens `icons`): pending `:white_circle:`, running `:large_yellow_circle:`, succeeded `:large_green_circle:`, failed `:red_circle:`, skipped `:white_circle:`. Labels: `Waiting`, `In progress`, `Passed`, `Failed`, `Skipped`.
- Alert key stays `alert:process:failed`; alert icon token `extra.alert = ":rotating_light:"`.
- Token override rules are unchanged: `icons` and `labels` accept only the five state keys; free-form words and glyphs live in `extra`.
- No network from tests or anything run (the suite blocks sockets session-wide). No secrets. Conventional Commits. `mypy --strict` clean on `src/`.
- Do not touch `releases/`, the `v0.0.1` tag, or the engine (`src/chappe/core/engine.py`).

## Review Focus

1. A step name longer than the line, and DAG-supplied `&`, `<`, `>` in a name: the line stays 36 visible characters, a long name ends with `…`, and escaping does not shift the alignment — pinned in Task 1 (`test_a_long_name_is_cut_to_the_line_width`, `test_escaping_does_not_shift_the_alignment`).
2. A step that is running or failed without its own times (Option A: only the triggering step is timed): the right text says `running` / `failed` with no empty duration — pinned in Task 1 (`test_right_text_without_times`).
3. A huge pipeline near the parent limit: the code fence is always closed and the text stays within the limit, with `… <n> more` replacing hidden stations — pinned in Task 1 (`test_a_big_run_collapses_stations_and_keeps_the_fence_closed`).
4. An existing config that still says `theme: {name: thread}`: `validate-config` names the problem and lists `metro, plain` — pinned in Task 2 (`test_the_removed_thread_theme_is_rejected_with_the_new_list`).
5. A passing example run must post exactly one channel message and nothing in its thread; a failing one exactly one alert — pinned in Task 3 (example DAG tests).

---

### Task 1: The metro theme

**Files:**
- Create: `src/chappe/themes/builtin/metro.py`
- Create: `src/chappe/themes/builtin/metro.yaml`
- Modify: `src/chappe/themes/__init__.py` (register `metro`; keep `thread` for now — Task 2 removes it)
- Modify: `tests/themes/test_conformance.py` (add `TestMetro`)
- Create: `tests/themes/test_metro.py`
- Create (generated, reviewed): `tests/themes/__snapshots__/metro__*.txt`

**Interfaces:**
- Consumes: `chappe.core.render.RenderContext` (`icon`, `label`, `duration`, `clock`, `text`, `fmt.bold`, `fmt.link`, `fmt.mention`, `fmt.icon`, `tokens.extra`, `limits.parent_chars`, `limits.entry_chars`, `alert_mention`), `clip`, `chappe.core.view.ProcessView/SectionView/StepView`, `chappe.core.messages.MessageSet/ParentMessage/Alert`.
- Produces: `chappe.themes.builtin.metro.MetroTheme` (`name = "metro"`, `render(view, ctx) -> MessageSet`), module constants `LINE_WIDTH = 36`, `FENCE = "```"`; `THEMES["metro"]`; built-in tokens file `metro.yaml`.

- [ ] **Step 1: Write the tokens file**

`src/chappe/themes/builtin/metro.yaml`:

```yaml
icons:
  pending: ":white_circle:"
  running: ":large_yellow_circle:"
  succeeded: ":large_green_circle:"
  failed: ":red_circle:"
  skipped: ":white_circle:"
labels:
  pending: "Waiting"
  running: "In progress"
  succeeded: "Passed"
  failed: "Failed"
  skipped: "Skipped"
extra:
  started: "started"
  running: "running"
  failed: "failed"
  failed_after: "failed after"
  skipped: "skipped"
  more: "more"
  alert: ":rotating_light:"
  glyph_succeeded: "●"
  glyph_running: "◉"
  glyph_failed: "✖"
  glyph_skipped: "◌"
  glyph_pending: "○"
  line_done: "┃"
  line_todo: "┆"
```

- [ ] **Step 2: Write the failing tests**

`tests/themes/test_metro.py`:

```python
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
    assert alert.text.startswith(f"{TEST_MENTION} :rotating_light: *orders 2026.10.1* · Regression ")
    assert "failed: Executor ran out of memory after 3 retries" in alert.text
    assert alert.text.endswith("<https://airflow.invalid/dags/orders/runs/run_1/tasks/regression|Log>")


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
```

Add to `tests/themes/test_conformance.py`, after `TestThread`:

```python
class TestMetro(ThemeConformance):
    theme = THEMES["metro"]
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/themes/test_metro.py -q`
Expected: FAIL at collection with `ModuleNotFoundError: No module named 'chappe.themes.builtin.metro'`.

- [ ] **Step 4: Write the theme**

`src/chappe/themes/builtin/metro.py`:

```python
"""Default theme: a status line and the run drawn as a line of stations, in one message.

The thread holds only the failure alert. The stations sit in a code block so durations line up;
Slack shows no `:emoji:` inside code blocks, so the glyphs are plain Unicode, distinct by shape.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

from chappe.core.messages import Alert, MessageSet, ParentMessage
from chappe.core.model import ProcessState, StepState
from chappe.core.render import RenderContext, clip
from chappe.core.view import ProcessView, SectionView, StepView

LINE_WIDTH = 36  # fits a phone screen without wrapping
FENCE = "```"
TITLE_CHARS = 200  # a title alone never eats the parent limit
BRANCH, BRANCH_LAST = "┣━ ", "┗━ "
INDENT, INDENT_LAST = "┃   ", "    "
_GLYPHS = {
    StepState.SUCCEEDED: "●",
    StepState.RUNNING: "◉",
    StepState.FAILED: "✖",
    StepState.SKIPPED: "◌",
    StepState.PENDING: "○",
}

def _fit(text: str, room: int) -> str:
    room = max(room, 1)
    return text if len(text) <= room else text[: room - 1] + "…"


def _shown(steps: Sequence[StepView], cap: int | None) -> list[StepView | int]:
    """All steps, or the first cap-1 and the last with the count of hidden ones between."""
    if cap is None or len(steps) <= cap:
        return list(steps)
    return [*steps[: cap - 1], len(steps) - cap, steps[-1]]


class MetroTheme:
    name: ClassVar[str] = "metro"

    def render(self, view: ProcessView, ctx: RenderContext) -> MessageSet:
        return MessageSet(ParentMessage(self._parent(view, ctx)), (), self._alerts(view, ctx))

    # ---- parent ----

    def _parent(self, view: ProcessView, ctx: RenderContext) -> str:
        head = [ctx.fmt.bold(clip(view.title, TITLE_CHARS)), self._status(view, ctx)]
        links = " · ".join(ctx.fmt.link(link.url, link.label) for link in view.links)
        tail = [links] if links else []
        limit = ctx.limits.parent_chars
        longest = max((len(section.steps) for section in view.sections), default=0)
        for cap in [None, *range(longest - 1, 1, -1)]:
            text = "\n".join([*head, FENCE, *self._stations(view, ctx, cap), FENCE, *tail])
            if len(text) <= limit:
                return text
        return clip("\n".join([*head, *tail]), limit)  # no block fits: never an unclosed fence

    def _status(self, view: ProcessView, ctx: RenderContext) -> str:
        line = f"{ctx.icon(view.state)} {ctx.label(view.state)}"
        if view.started_at is not None:
            line += f" · {ctx.tokens.extra.get('started', 'started')} {ctx.clock(view.started_at)}"
        if view.duration is not None and view.state is not ProcessState.PENDING:
            line += f" · {ctx.duration(view.duration)}"
        return line

    def _stations(self, view: ProcessView, ctx: RenderContext, cap: int | None) -> list[str]:
        if not view.sections:
            return []
        trunk, *branches = view.sections
        lines = self._trunk(_shown(trunk.steps, cap), view, ctx)
        for index, section in enumerate(branches):
            last = index == len(branches) - 1
            lines.append(self._branch_head(section, ctx, last=last))
            indent = INDENT_LAST if last else INDENT
            for item in _shown(section.steps, cap):
                lines.append(indent + self._item(item, view, ctx, LINE_WIDTH - len(indent)))
        return lines

    def _trunk(self, items: list[StepView | int], view: ProcessView, ctx: RenderContext) -> list[str]:
        extra = ctx.tokens.extra
        lines: list[str] = []
        for index, item in enumerate(items):
            previous = items[index - 1] if index else None
            if isinstance(previous, StepView) and isinstance(item, StepView):
                done = previous.state.finished
                lines.append(extra.get("line_done", "┃") if done else extra.get("line_todo", "┆"))
            lines.append(self._item(item, view, ctx, LINE_WIDTH))
        return lines

    def _branch_head(self, section: SectionView, ctx: RenderContext, *, last: bool) -> str:
        mark = BRANCH_LAST if last else BRANCH
        return mark + ctx.text(_fit(section.title, LINE_WIDTH - len(mark)))

    def _item(self, item: StepView | int, view: ProcessView, ctx: RenderContext, width: int) -> str:
        if isinstance(item, int):
            line_todo = ctx.tokens.extra.get("line_todo", "┆")
            return f"{line_todo}  … {item} {ctx.tokens.extra.get('more', 'more')}"
        return self._station(item, view, ctx, width)

    def _station(self, step: StepView, view: ProcessView, ctx: RenderContext, width: int) -> str:
        """`<glyph>  <name>` with the right text right-aligned to `width` visible characters.

        Widths are measured on the raw text, then escaped: Slack shows `&amp;` as one character.
        """
        glyph = ctx.tokens.extra.get(f"glyph_{step.state.value}", _GLYPHS[step.state])
        right = self._right(step, view, ctx)
        prefix = f"{glyph}  "
        room = width - len(prefix) - (len(right) + 1 if right else 0)
        name = _fit(step.title, room)
        if not right:
            return prefix + ctx.text(name)
        padding = " " * (width - len(prefix) - len(name) - len(right))
        return prefix + ctx.text(name) + padding + ctx.text(right)

    def _right(self, step: StepView, view: ProcessView, ctx: RenderContext) -> str:
        words = ctx.tokens.extra
        took = step.duration(view.now)
        timed = step.started_at is not None and step.ended_at is not None
        if step.state is StepState.SUCCEEDED:
            return ctx.duration(took) if timed else ""
        if step.state is StepState.RUNNING:
            running = words.get("running", "running")
            return f"{running} {ctx.duration(took)}" if took is not None else running
        if step.state is StepState.FAILED:
            if timed:
                return f"{words.get('failed_after', 'failed after')} {ctx.duration(took)}"
            return words.get("failed", "failed")
        if step.state is StepState.SKIPPED:
            return words.get("skipped", "skipped")
        return ""

    # ---- alerts ----

    def _alerts(self, view: ProcessView, ctx: RenderContext) -> tuple[Alert, ...]:
        if view.state is not ProcessState.FAILED:
            return ()
        mention = f"{ctx.fmt.mention(ctx.alert_mention)} " if ctx.alert_mention else ""
        icon = ctx.fmt.icon(ctx.tokens.extra.get("alert", ":rotating_light:"))
        failed = view.failed_steps
        details = (
            "; ".join(self._failure(step, view, ctx) for step in failed)
            if failed
            else ctx.label(ProcessState.FAILED)
        )
        text = f"{mention}{icon} {ctx.fmt.bold(view.title)} · {details}"
        logs = tuple(link for step in failed for link in step.links)
        if logs:
            text += " · " + " · ".join(ctx.fmt.link(link.url, link.label) for link in logs)
        return (Alert(key="alert:process:failed", text=clip(text, ctx.limits.entry_chars)),)

    def _failure(self, step: StepView, view: ProcessView, ctx: RenderContext) -> str:
        text = f"{ctx.text(step.title)} {ctx.text(self._right(step, view, ctx))}"
        if step.error:
            text += f": {ctx.text(step.error)}"
        return text
```

Notes for the implementer:
- Keep the code as written; if a line exceeds the line length, run `uv run ruff format` and say so.
- The trunk draws a connector only between two shown steps; the `… <n> more` line replaces stations and their connectors.

Register it in `src/chappe/themes/__init__.py`:

```python
from chappe.themes.builtin.metro import MetroTheme
from chappe.themes.builtin.plain import PlainTheme
from chappe.themes.builtin.thread import ThreadTheme

THEMES: dict[str, type[Theme]] = {"metro": MetroTheme, "thread": ThreadTheme, "plain": PlainTheme}
```

- [ ] **Step 5: Run the theme tests**

Run: `uv run pytest tests/themes/test_metro.py -q`
Expected: all pass. If an exact-text assertion fails, compare against the spec (`docs/specs/2026-10-04-metro-theme-design.md` §1) — the spec wins; fix the code, not the expectation, unless the expectation contradicts the spec (then report it).

- [ ] **Step 6: Generate and review the metro snapshots, run conformance**

Run: `uv run pytest tests/themes/test_conformance.py -q --chappe-update-snapshots -k metro`
Then: `uv run pytest tests/themes -q`
Expected: all pass. Read every new `tests/themes/__snapshots__/metro__*.txt`: each parent has a title line, a status line, one opening and one closing fence, station lines of 36 characters where they carry right text, the `Airflow run` link; `=== thread ===` is empty in every file; only `metro__single_failed.txt` and `metro__multi_failed.txt` have an alert. Paste `metro__single_running.txt` and `metro__multi_failed.txt` into the report.

- [ ] **Step 7: Full checks and commit**

Run: `uv run ruff check && uv run ruff format --check && uv run mypy && uv run lint-imports && uv run pytest -q`
Expected: all clean, all pass.

```bash
git add src/chappe/themes/builtin/metro.py src/chappe/themes/builtin/metro.yaml src/chappe/themes/__init__.py tests/themes/test_metro.py tests/themes/test_conformance.py tests/themes/__snapshots__/metro__*.txt
git commit -m "feat(themes): add the metro theme"
```

---

### Task 2: Metro by default; remove the thread theme

**Files:**
- Modify: `src/chappe/config/models.py:47` (default and description)
- Modify: `src/chappe/themes/__init__.py` (drop `thread`)
- Delete: `src/chappe/themes/builtin/thread.py`, `src/chappe/themes/builtin/thread.yaml`, `tests/themes/test_thread.py`, `tests/themes/__snapshots__/thread__*.txt`
- Create: `tests/support/thread_theme.py` (test-only copy of the old theme for engine tests)
- Modify: `tests/core/test_engine.py`, `tests/core/test_engine_races.py`, `tests/core/test_engine_properties.py` (import the test copy; `default_context("plain")`)
- Modify: `tests/themes/test_conformance.py` (drop `TestThread`), `tests/themes/test_tokens.py`, `tests/config/test_config.py`, `tests/test_cli.py`
- Modify: `docs/guides/configuration.md` (regenerated reference + the hand-written example blocks)

**Interfaces:**
- Consumes: `MetroTheme`, `THEMES["metro"]` from Task 1.
- Produces: `THEMES == {"metro": MetroTheme, "plain": PlainTheme}`; `ThemeConfig().name == "metro"`; `tests.support.thread_theme.ThreadTheme` (test-only; same behaviour as the deleted theme, renders with any tokens that have the five icons/labels).

- [ ] **Step 1: Write the failing tests**

In `tests/config/test_config.py`, change line 51 to:

```python
    assert settings.theme_for(process).name == "metro"
```

and add after `test_an_unknown_theme_is_rejected_at_load`:

```python
def test_the_removed_thread_theme_is_rejected_with_the_new_list(tmp_path: Path) -> None:
    text = with_defaults("    theme: {name: thread}\n")
    with pytest.raises(
        ChappeConfigError,
        match=r"defaults\.theme: unknown theme 'thread'; available themes: metro, plain",
    ):
        load_settings(write(tmp_path, text))
```

In `tests/test_cli.py:54` change the expected list:

```python
    assert "defaults.theme: unknown theme 'neon'; available themes: metro, plain" in err
```

In `tests/themes/test_tokens.py` replace `test_builtin_tokens_load` and switch the remaining `"thread"` names to `"metro"`:

```python
def test_builtin_tokens_load() -> None:
    tokens = resolve_tokens("metro", None)
    assert tokens.icons["running"] == ":large_yellow_circle:"
    assert tokens.labels["succeeded"] == "Passed"
    assert tokens.extra["glyph_running"] == "◉"


def test_override_extends_and_replaces_keys() -> None:
    tokens = resolve_tokens("metro", {"extends": "metro", "icons": {"running": ":airflow_spin:"}})
    assert tokens.icons["running"] == ":airflow_spin:"
    assert tokens.icons["failed"] == ":red_circle:"
```

(Every other `resolve_tokens("thread", …)` in that file becomes `resolve_tokens("metro", …)`; the assertions there are about errors and do not depend on the theme.)

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/config/test_config.py tests/test_cli.py tests/themes/test_tokens.py -q`
Expected: FAIL — default is still `thread`, the available list still includes `thread`.

- [ ] **Step 3: Move the old theme into test support**

`git mv src/chappe/themes/builtin/thread.py tests/support/thread_theme.py`, then edit its docstring's first line to:

```python
"""Test-only copy of the removed `thread` theme: one reply per finished step and a broadcast final
result. Engine tests use it to exercise thread entries, broadcasts and the thread-entry rule; it
renders with the plain tokens (`default_context("plain")`)."""
```

Leave the class name `ThreadTheme` and its code unchanged. In `tests/core/test_engine.py`, `tests/core/test_engine_races.py` and `tests/core/test_engine_properties.py`:
- replace `from chappe.themes.builtin.thread import ThreadTheme` with `from tests.support.thread_theme import ThreadTheme`;
- replace `default_context("thread")` with `default_context("plain")`.

The plain tokens have the same icons and labels as the old thread tokens, and every `extra` word the old theme reads has a default in its code, so rendered text is unchanged. If any engine test asserts text that differs (it should not), report it before changing the assertion.

- [ ] **Step 4: Remove the theme and switch the default**

```bash
git rm src/chappe/themes/builtin/thread.yaml tests/themes/test_thread.py tests/themes/__snapshots__/thread__*.txt
```

`src/chappe/themes/__init__.py`:

```python
"""Themes and their tokens."""

from __future__ import annotations

from chappe.ports.theme import Theme
from chappe.themes.builtin.metro import MetroTheme
from chappe.themes.builtin.plain import PlainTheme

THEMES: dict[str, type[Theme]] = {"metro": MetroTheme, "plain": PlainTheme}
```

`src/chappe/config/models.py` line 47:

```python
    name: str = Field(default="metro", description="`metro` (default) or `plain` (the flat fallback).")
```

`tests/themes/test_conformance.py`: delete the `TestThread` class.

- [ ] **Step 5: Regenerate the configuration guide and fix its hand-written examples**

Run: `uv run python scripts/render_config_reference.py` (rewrites the generated block between the markers in `docs/guides/configuration.md`), then `uv run python scripts/render_config_reference.py --check` → exit 0.

In the hand-written example block of `docs/guides/configuration.md` (around lines 58 and 70), replace `theme: {name: thread}` with `theme: {name: metro}`, and in the per-process example replace

```yaml
      theme:
        name: thread
        tokens:
          extends: thread
          icons: {running: ":loading:"}
```

with

```yaml
      theme:
        name: metro
        tokens:
          extends: metro
          icons: {running: ":loading:"}
```

Search the guide for any other mention of the thread theme (`grep -n -i "thread" docs/guides/configuration.md`) and keep only mentions of Slack threads (the failure alert is a thread reply).

- [ ] **Step 6: Run the tests, then the full checks**

Run: `uv run pytest tests/config tests/test_cli.py tests/themes tests/core -q`
Expected: all pass.
Then: `grep -rn "builtin.thread\|\"thread\"\|name: thread" src tests docs/guides examples` → only `examples/chappe.yaml` may still match (Task 3 changes it). If `tests/integration/test_example_dag.py` fails now because the example config names `thread`, that is expected; Task 3 rewrites it — do NOT change the example in this task, but say so in the report. Run the full checks excluding nothing; report the example test failure if it occurs.

Run: `uv run ruff check && uv run ruff format --check && uv run mypy && uv run lint-imports && uv run pytest -q`

- [ ] **Step 7: Commit**

```bash
git add -A src/chappe/themes src/chappe/config/models.py tests/support/thread_theme.py tests/core tests/themes tests/config/test_config.py tests/test_cli.py docs/guides/configuration.md
git commit -m "feat(themes)!: make metro the default and remove the thread theme"
```

(`git add -A` limited to these paths records the deletions; check `git status` shows nothing else staged.)

---

### Task 3: A flat four-step example

**Files:**
- Modify: `examples/dags/chappe_example.py` (rewrite)
- Modify: `examples/chappe.yaml` (drop the `theme:` line)
- Modify: `tests/integration/test_example_dag.py` (new assertions)
- Modify: `examples/README.md` (scenario table and any thread-theme wording)

**Interfaces:**
- Consumes: metro as the default theme (Task 2).
- Produces: DAG `chappe_example` with tasks `extract`, `transform`, `load`, `report`, all `@milestone`, linear; params `product`, `version`, `fail` (when `fail` is true, `load` raises).

- [ ] **Step 1: Write the failing tests**

Replace the test functions and helpers after `load_example()` in `tests/integration/test_example_dag.py` (keep the fixtures, `wire()` and `assert_kept()` as they are) with:

```python
GLYPHS = "●◉✖◌○"


def block(text: str) -> list[str]:
    lines = text.split("\n")
    start = lines.index("```")
    end = lines.index("```", start + 1)
    return lines[start + 1 : end]


def station_names(text: str) -> list[str]:
    """`<glyph> <name>` for each station of the parent's code block, in order."""
    return [f"{line[0]} {line[3:].split('  ')[0]}" for line in block(text) if line[:1] in GLYPHS]


def connectors(text: str) -> list[str]:
    return [line for line in block(text) if line in ("┃", "┆")]


def run(module: ModuleType, api: FakeSlackApi, conf: dict[str, Any], reason: str) -> str:
    dag_run = module.dag.test(run_conf=conf)
    assert str(getattr(dag_run.state, "value", dag_run.state)) == (
        "success" if reason == "success" else "failed"
    )
    # What the task callbacks (@milestone) left during dag.test(): a parent, not yet final.
    assert any(name == "post" for name, _ in api.calls)
    (parent,) = api.top_level(CHANNEL)
    status = parent.text.split("\n")[1]
    assert status.startswith(":large_yellow_circle: In progress")
    # The DAG callback, as the DAG processor sends it (minimal context, finding 5). On Airflow
    # 3.2.2, dag.test()'s own DAG callback passes a SerializedDAG whose tasks carry no milestone
    # marker, so it sends nothing.
    ChappeNotifier().notify({"dag": module.dag, "run_id": dag_run.run_id, "reason": reason})
    return str(dag_run.run_id)


def test_the_example_is_one_flat_dag_of_four_milestones() -> None:
    module = load_example()
    assert [t.task_id for t in module.dag.topological_sort()] == [
        "extract",
        "transform",
        "load",
        "report",
    ]


def test_passing_run_posts_one_message_and_nothing_in_its_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_example()
    api, store = wire(monkeypatch, {t.task_id: "success" for t in module.dag.tasks})
    run_id = run(module, api, {}, "success")

    (parent,) = api.top_level(CHANNEL)
    lines = parent.text.split("\n")
    assert lines[0] == "*orders 2026.10.1*"
    assert lines[1].startswith(":large_green_circle: Passed · started ")
    assert station_names(parent.text) == ["● Extract", "● Transform", "● Load", "● Report"]
    assert connectors(parent.text) == ["┃", "┃", "┃"]
    assert api.replies(CHANNEL, parent.ts) == []
    assert_kept(store, run_id, parent.ts)


def test_failing_run_shows_the_failed_station_and_alerts_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_example()
    states = {t.task_id: "success" for t in module.dag.tasks}
    states["load"] = "failed"
    states["report"] = "upstream_failed"
    api, store = wire(monkeypatch, states)
    run_id = run(module, api, {"fail": True}, "task_failure")

    (parent,) = api.top_level(CHANNEL)
    assert parent.text.split("\n")[1].startswith(":red_circle: Failed · started ")
    assert station_names(parent.text) == ["● Extract", "● Transform", "✖ Load", "○ Report"]
    thread = api.replies(CHANNEL, parent.ts)
    (alert,) = thread
    assert alert.text.startswith("<@U0123456789> :rotating_light: *orders 2026.10.1* · Load ")
    assert_kept(store, run_id, parent.ts)
```

Check how `upstream_failed` maps in `src/chappe/integrations/airflow/source.py` (Airflow task state → `StepState`); if it maps to something other than `PENDING`, use the glyph that mapping produces and say so in the report.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/integration/test_example_dag.py -q`
Expected: FAIL (the example still has task groups and names `thread`).

- [ ] **Step 3: Rewrite the example**

`examples/dags/chappe_example.py`:

```python
import time

from airflow.sdk import DAG, task

from chappe import milestone
from chappe.integrations.airflow import ChappeNotifier

with DAG(
    "chappe_example",
    schedule=None,
    params={"product": "orders", "version": "2026.10.1", "fail": False},
    on_success_callback=ChappeNotifier(),
    on_failure_callback=ChappeNotifier(),
) as dag:

    @milestone("Extract")
    @task
    def extract() -> None:
        time.sleep(5)

    @milestone("Transform")
    @task
    def transform() -> None:
        time.sleep(5)

    @milestone("Load")
    @task
    def load(params: dict | None = None) -> None:
        time.sleep(5)
        if params and params.get("fail"):
            raise RuntimeError("load failed (example)")

    @milestone("Report")
    @task
    def report() -> None:
        time.sleep(5)

    extract() >> transform() >> load() >> report()
```

`examples/chappe.yaml`: delete the line `    theme: {name: thread}   # the default; "plain" is the flat fallback`.

- [ ] **Step 4: Run the example tests**

Run: `uv run pytest tests/integration/test_example_dag.py -q`
Expected: 3 passed. Then check the assertion strength: temporarily remove `@milestone("Report")` from the example, run the passing test, confirm it fails, restore the decorator (do not commit the mutation).

- [ ] **Step 5: Update `examples/README.md`**

In the scenario table (section 4), the "Passing run" row's Expected column becomes: "One message: a title, a status line that moves from 🟡 In progress to 🟢 Passed, and four stations (Extract, Transform, Load, Report) that turn from ○ to ● with their durations. Nothing is posted in its thread. Admin → Variables shows one `chappe__chappe_example__…` Variable with a masked value." Update the failing-run row the same way: "🔴 Failed, the Load station shows ✖, Report stays ○, and one alert in the thread mentions you." Read the rest of the file and remove any other wording about the thread theme, sections or per-step replies; keep everything else unchanged.

- [ ] **Step 6: Full checks and commit**

Run: `uv run ruff check && uv run ruff format --check && uv run mypy && uv run lint-imports && uv run pytest -q`
Expected: all clean, all pass (no failures left from Task 2).

```bash
git add examples/dags/chappe_example.py examples/chappe.yaml examples/README.md tests/integration/test_example_dag.py
git commit -m "docs(example): a flat four-step example on the metro theme"
```

---

### Task 4: Docs and changelog

**Files:**
- Modify: `README.md` (the "What it does" sample and its paragraph)
- Modify: `docs/specs/2026-10-03-chappe-0.0.1-design.md` (one amendment note; the two config examples; the `THEMES` sentence)
- Modify: `CHANGELOG.md` (`## Unreleased`)

**Interfaces:**
- Consumes: the rendered text of `tests/themes/__snapshots__/metro__single_running.txt` (Task 1) and the example (Task 3).
- Produces: documentation only.

- [ ] **Step 1: README**

In `README.md`, section "What it does":
- Replace the sentence "The step history and the final result go in that message's thread." with "The message shows every step as a station on a line, with its duration."
- Keep "When a run fails, an alert mentions your on-call." and add " in the message's thread" before the full stop.
- Replace the fenced `text` sample with the example run (four steps, in progress), taken from what `MetroTheme` renders for it: render it once in a Python shell (`uv run python -c "…"` using `tests.support.samples.ProcessViewBuilder` with title `orders 2026.10.1`, section `Main`, steps `Extract` S 2, `Transform` R, `Load` P, `Report` P) and paste the output with the link reduced to `Airflow run`. It must match `test_running_run_renders_title_status_stations_and_link` in Task 1.

Search README for other mentions of the thread theme or per-step replies (`grep -n -i "thread" README.md`) and keep only those about the failure alert or about Slack threads in general; the Known limits entries about thread replies (untimed replies' order, duplicate step reply, replies re-sent under a new parent) no longer apply to the default theme: rewrite each to say it applies to themes that post step replies (none of the built-in themes in this release posts them) or remove it if it only concerned the thread theme. List what you changed in the report.

- [ ] **Step 2: Spec 0.0.1 amendment**

At the top of `docs/specs/2026-10-03-chappe-0.0.1-design.md`, under the title, add:

```markdown
> **Amended 2026-10-04:** the default theme is `metro` and the `thread` theme was removed — see `docs/specs/2026-10-04-metro-theme-design.md`.
```

In its two config examples (lines ~152 and ~165) replace `thread` with `metro` (`theme: { name: metro }`, `name: metro`, `extends: metro`). In §5.1, change `THEMES = {"thread": ..., "plain": ...}` to `THEMES = {"metro": ..., "plain": ...}`. Do not rewrite other sections.

- [ ] **Step 3: CHANGELOG**

Add at the top of `CHANGELOG.md`, above `## 0.0.2 (planned)`:

```markdown
## Unreleased

- New default theme `metro`: one message per run with a status line and the run drawn as a line of stations, each with its duration. Only a failure alert goes into the thread; a passing run is exactly one channel message.
- **Breaking:** the `thread` theme is removed. A config that names it (`theme: {name: thread}`) no longer loads: remove the line (metro is the default) or name `metro`.
- The example is now one flat DAG with four steps: Extract → Transform → Load → Report.
```

If `## 0.0.2 (planned)` lists the `metro` theme as deferred, remove that bullet.

- [ ] **Step 4: Checks and commit**

Run: `uv run pytest tests/docs -q && uv run ruff check && uv run pytest -q`
Expected: all pass.

```bash
git add README.md docs/specs/2026-10-03-chappe-0.0.1-design.md CHANGELOG.md
git commit -m "docs: describe the metro theme as the default"
```
