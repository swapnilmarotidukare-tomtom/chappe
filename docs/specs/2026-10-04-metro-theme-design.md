# Metro theme as the default — design

Date: 2026-10-04. Status: approved in conversation, pending review of this document.
Amends: `docs/specs/2026-10-03-chappe-0.0.1-design.md` (§6 themes: `metro` moves from the 0.0.2 roadmap into the
next release and becomes the default; `thread` is removed). Source of the layout: the "Metro Line" option of the
owner's notification-redesign artifact, adapted to Chappe's rule of plain Slack `mrkdwn` text (no Block Kit, no
buttons, no attachment colors).

## Goal

The thread theme's run message is cluttered: a state word next to an icon that already says it, a row of
check marks per section, and a reply in the thread for every step plus a final broadcast. The metro theme shows a
run as one clean message: a title, one status line, and the run drawn as a line of stations with each step's
duration. A passing run is exactly one channel message. The example DAG becomes the simplest possible showcase.

## Decisions (owner, 2026-10-04)

1. Metro is the default theme. `plain` stays as the fallback.
2. The `thread` theme is deleted (code, tokens, snapshots, tests, docs).
3. The thread holds only the failure alert: no per-step replies, no final broadcast.
4. The status line uses a colored circle plus a word (🟡 In progress, 🟢 Passed, 🔴 Failed, ⚪ Waiting).
5. The example is one flat DAG with four linear `@milestone` tasks; no task groups, no non-milestone task.

## 1. The run message

One parent message per run, edited in place. Three parts, in this order:

~~~~
*orders 2026.10.1*
🟡 In progress · started 10:46 · 6m 15s
```text
●  Extract              1m 04s
┃
◉  Transform    running 2m 01s
┆
○  Load
┆
○  Report
```
<https://airflow…/runs/…|Airflow run>
~~~~

**Title line:** the process title in bold (`ctx.fmt.bold(view.title)`), alone on its line.

**Status line:** `<circle> <state word> · started HH:MM · <duration>`.
- Circle and word come from tokens per process state: pending ⚪ Waiting, running 🟡 In progress, succeeded 🟢
  Passed, failed 🔴 Failed. Circles are `:emoji:` codes so workspaces can override them.
- `started HH:MM` uses `ctx.clock(view.started_at)` (config timezone); omitted when `started_at` is None.
- The duration is the run's duration (`view.duration`), omitted when None or when the run is pending.
- This is the only place the run's state appears in words.

**Station block:** a fenced code block (```` ``` ````). Slack does not render `:emoji:` inside code blocks, so the
glyphs are plain Unicode, distinct by shape (spec §6.4 rule 2: no state by color alone):

| Step state | Glyph | Right-hand text |
|---|---|---|
| succeeded | `●` | its duration, if known |
| running | `◉` | `running <duration so far>`, or `running` if the start is unknown |
| failed | `✖` | `failed after <duration>`, or `failed` if the duration is unknown |
| skipped | `◌` | `skipped` |
| pending | `○` | nothing |

- A step's duration is shown only when Chappe has the step's own times (unchanged known limit).
- Between two stations a connector line: `┃` when the station above has finished (succeeded, failed, skipped),
  `┆` otherwise.
- Line format: `<glyph>  <name><padding><right text>`, the name left-aligned and the right text right-aligned so
  the line is `LINE_WIDTH = 36` characters (fits a phone without wrapping). A name that does not fit is cut and
  ends with `…`. Width counts characters (`len`), which is adequate for the Latin and arrow characters in step
  names; East Asian wide characters may misalign (accepted).
- Names are escaped with `ctx.text()` (Slack needs `&`, `<`, `>` escaped inside code blocks too).

**Links line:** the view's links (`Airflow run`), as today, after the code block. Omitted when there are none.

### Sections

- One section: stations only, no section header (spec §6.4 rule 6).
- More than one: the first section is the trunk, drawn as above. Each later section branches off below it:
  `┣━ <section title>` (or `┗━ ` for the last), then its stations indented by four characters (`┃   ` under a
  `┣━` branch, `    ` under the last), with the same glyphs and right text, line width still 36 including the
  indent. Connector lines are not drawn inside branches (the artifact's layout).

### Size limit

The parent must stay within `ctx.limits.parent_chars` and the code block must always be closed, so the theme
budgets lines instead of using `clip()` on the whole text. If the full block would exceed the limit, the theme keeps
the first and last stations of the trunk and of each section and replaces the stations in between with one line
`┆  … <n> more`, reducing until it fits. The title and status lines are clipped only if they alone exceed the limit
(an absurdly long title).

## 2. Thread and alerts

- Thread entries: none. `render()` returns an empty thread tuple.
- Alerts (spec §6.4 rule 4: a failed process always produces at least one alert): one alert per failed run,
  posted as a thread reply, key `alert:process:failed` (unchanged, stable):
  `<mention> 🚨 *<title>* · <failed step> failed after <duration>: <error> · <Log link>`. Several failed steps are
  joined with `; `; no failed step known → `<mention> 🚨 *<title>* · Failed`. The alert icon is a token
  (`:rotating_light:`). Mention, error escaping, log links and clipping to `entry_chars` as today.

## 3. Tokens, config, registry

- New `src/chappe/themes/builtin/metro.py` (`MetroTheme`, `name = "metro"`) and `metro.yaml`:
  - `icons`: the status circles per process state, plus `alert: ":rotating_light:"`.
  - `labels`: `Waiting`, `In progress`, `Passed`, `Failed`, `Skipped`.
  - `extra`: `started`, `running`, `failed_after` ("failed after"), `skipped`, `more` ("more"), and the glyphs
    (`glyph_succeeded`, `glyph_running`, `glyph_failed`, `glyph_skipped`, `glyph_pending`, `line_done`, `line_todo`),
    so a workspace can change the words and glyphs through YAML token overrides like any other token.
    Check against `src/chappe/themes/tokens.py` which token groups exist and how per-state icons are looked up; fit
    the circles and glyphs into the existing structure rather than inventing a new one.
- `THEMES = {"metro": MetroTheme, "plain": PlainTheme}`.
- `ThemeConfig.name` default `"metro"`, description "`metro` (default) or `plain` (the flat fallback)."; the
  configuration guide is regenerated.
- A config naming `thread` (or any unknown theme) is rejected when the config loads (spec 9.1, existing
  behaviour): `chappe validate-config` exits 1 with "unknown theme 'thread'; available themes: metro, plain". At
  run time a bad `defaults.theme` makes the whole config invalid, so Chappe sends nothing and logs the error; a
  bad `processes.<name>.theme` drops only that process. This is a breaking change for configs that name `thread`;
  the CHANGELOG says so and tells users to remove the line or name `metro`.
- Removed: `thread.py`, `thread.yaml`, `tests/themes/test_thread.py`, `thread__*.txt` snapshots, and every
  reference to the thread theme in docs, examples and tests.

## 4. Example and docs

- `examples/dags/chappe_example.py`: one DAG, four linear tasks, each `@milestone`: `Extract`, `Transform`,
  `Load`, `Report` (`extract() >> transform() >> load() >> report()`). No task groups, no helper task. The title
  keeps coming from params (product, version), as `examples/chappe.yaml` already does; the config file drops the
  `theme:` line, since metro is the default.
- `tests/integration/test_example_dag.py` rewritten for the new DAG and theme: exact status line, exact station
  lines for a passing run and a failing run, no thread entries on a passing run, one alert on a failing run.
- `examples/README.md` scenario rows updated.
- README: the "What it does" sample becomes a metro message; any thread-theme wording elsewhere goes.
- Spec 0.0.1 §6 and the roadmap table: note that `metro` is the default from the next release and `thread` was
  removed (one short amendment pointing to this document). ADRs untouched unless one names the thread theme as the
  default.
- CHANGELOG: an `## Unreleased` section: metro theme (default), thread theme removed, simpler example.

## 5. Tests

- Snapshots for metro for every existing sample (`tests/support/samples.py`): pending, single running, single
  passed, single failed, single step finished, multi running/passed/failed, with skipped, fifty steps, unicode
  long, minimal DAG callback.
- The conformance suite (`tests/themes/test_conformance.py`) runs for metro and plain and passes unchanged.
- Unit tests: line width is 36 for every station line; a long name is cut with `…`; connector choice (`┃`/`┆`);
  right text per state including unknown durations; branches for multi-section; size budgeting keeps the code
  fence closed and inserts `… <n> more` (assert the text ends with the closing fence and stays within the limit);
  no thread entries for any state; alert text for one, several and no known failed steps.
- Full checks: ruff, format, mypy --strict, lint-imports, pytest. No network.

## Out of scope

- The engine bug seen on 2026-10-04 in Slack: the final result was broadcast twice for one run. Metro no longer
  broadcasts, but the failure alert uses the same engine path, so the bug remains open and is tracked separately.
- A release: this ships after 0.0.1 (wheel and tag v0.0.1 unchanged).
