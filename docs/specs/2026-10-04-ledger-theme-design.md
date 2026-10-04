# Ledger theme, blocked state, icon tokens and viewer-local times — design

Date: 2026-10-04. Status: draft for the owner's review.
Builds on: `docs/specs/2026-10-04-metro-theme-design.md` (branch `feat/metro-theme`). Source: the owner's
"Release 0.0.1.1" notes and the Block Kit mock-up of the run message (used only as a picture of the text: the
messages stay plain `mrkdwn`, no Block Kit).

## Owner decisions (2026-10-04)

1. Built as a new theme, `ledger`, on the metro branch, next to `metro` and `plain`.
2. `ledger` becomes the default theme. `metro` and `plain` stay selectable.
3. A running step reads `running since <time>`, the time shown in each viewer's own timezone.
4. The thread carries each step's start and end (one reply when a step starts, one when it ends), and the top of
   the message always shows the run's status.
5. This round builds and reviews; the release (version, wheel, tag) is a separate step later.

Items marked **(decided here)** are the controller's reading of an ambiguous note; the owner confirms or corrects
them in this review.

## 1. Blocked state

- New step state `StepState.BLOCKED = "blocked"`, `finished == True`.
- The Airflow source maps `upstream_failed` to `BLOCKED` (today `FAILED`). A blocked step carries no error text
  (today it gets "An upstream task failed"; that text goes).
- **Section state (decided here):** the note reads "a section with BLOCKED but no FAILED steps derives as FAILED only
  if the run failed; otherwise treat BLOCKED like FAILED", which gives FAILED either way. So: a section with any
  FAILED or BLOCKED step is FAILED. (A section cannot know the run's state; this keeps `SectionView.state` local.)
- Counts (failed runs, plain and ledger): `<n> passed · <n> failed`, then `· <n> blocked`, `· <n> skipped`,
  `· <n> not run` when non-zero, in that order. Example: `2 passed · 1 failed · 1 blocked · 1 not run`.
- `ProcessView.failed_steps` stays FAILED only; a new `ProcessView.blocked_steps`. Alerts name FAILED steps only,
  never blocked ones (every theme).
- Watermark: a blocked step is settled (finished), like skipped.
- Tokens: `blocked` joins `STATE_KEYS` (`icons` and `labels`, label `Blocked`). Every built-in token file gets it.
  The conformance samples gain a run with a blocked step so every state is rendered by every theme.

## 2. Icon tokens

- Built-in defaults stay standard emoji that work in any workspace.
- New token section `header_icons`, keyed by process state (`pending`, `running`, `succeeded`, `failed`). Validated
  like `icons` (unknown keys rejected). It is the status mark at the start of a message's first line.
  - Owner's note: running empty = no header icon. Owner's later answer: "the top shall indicate status".
    **(decided here)** Defaults show a mark for every state: pending `:white_circle:`, running
    `:large_yellow_circle:`, succeeded `:large_green_circle:`, failed `:red_circle:`. An empty string in an override
    means no mark for that state.
- Built-in token files may name a base with a top-level `extends:` (one level), so a variant file lists only what it
  changes.
- New built-in token file `ledger-chappe.yaml` (`extends: ledger`) with the custom emoji: succeeded
  `:chappe-done:`, running `:chappe-running:`, pending `:chappe-pending:`, skipped `:chappe-skipped:`, failed
  `:chappe-failed:`, blocked `:chappe-blocked:`. (The note says `thread-chappe`; `thread` no longer exists, so the
  file is named after the theme it extends.)
- `examples/chappe.yaml` uses `theme: {tokens: {extends: ledger-chappe}}` with a comment: the six `:chappe-*:` emoji
  must exist in the workspace first, otherwise remove the line to get the default emoji.

## 3. The `ledger` theme

Built-in theme `ledger` (`src/chappe/themes/builtin/ledger.py`, tokens `ledger.yaml`). Default icons: succeeded
`:white_check_mark:`, running `:hourglass_flowing_sand:`, pending `:white_circle:`, skipped `:fast_forward:`, failed
`:x:`, blocked `:no_entry:`. Labels: Waiting, In progress, Passed, Failed, Skipped, Blocked.

### 3.1 Parent message

**Header line** (always the first line):
`<header icon> *<title link>* · <state phrase> · started <clock>`
- `<title link>`: the run's Airflow link with the title as its label (`*<url|title>*`); just `*title*` when the
  view has no link. There is no separate "Airflow run" line.
- State phrase: pending `Waiting` (no duration); running `In progress · <duration>`; passed
  `Passed in <duration>`; failed `Failed after <duration>`. The duration part is left out when unknown.
- `started <clock>` is left out when the run's start is unknown.
- The header icon is left out (with its space) when the token is empty.

**Pending and running:** the header, then the sections.
- More than one section: each section starts with `*<section title>* · <done> of <total> done`.
- One section: no section line (spec 0.0.1 §6.4 rule 6); the step lines follow the header directly.
- Step line: `<icon> <name>` plus, when known:
  - succeeded: ` · <duration>` (only from the step's own times);
  - running: ` · running since <clock>`, or ` · running` when the start is unknown;
  - failed ` · Failed`, blocked ` · Blocked`, skipped ` · Skipped`; pending: nothing.
- Config `theme.collapse_done_sections` (default `false`): when true, a section whose steps all finished and none
  failed or is blocked renders as one line `*<title>* · <n> of <n> done`, followed by ` · <span>` only when every
  step in it carries its own times (span = first start to last end) **(decided here)**. Single-section runs never
  collapse (there is no section line to collapse into).

**Passed:** exactly one line, the header.

**Failed:** the header, then only the sections that contain a FAILED or BLOCKED step **(decided here: the note says
"failed"; a section of only blocked steps also went wrong and is listed)**, each fully listed:
- more than one section: `*<section title>* · <counts>`; one section: the counts line without a title;
- then every step of that section as a step line.

### 3.2 Thread

Per owner decision 4, one reply when a step starts and one when it ends. Spec 0.0.1 §6.4 rule 7 applies:
- Start reply, key `step:<step key>:started`: `<running icon> *<name>* · started <clock>`. Emitted only from a view
  that carries the step's start time. Never emitted for a step whose start Chappe did not see (no back-filling).
- End reply, key `step:<step key>:<final state>`: `<icon> *<name>* · <label> <clock of end> · <duration>`; duration
  only when the step has its own start and end. A failed step adds its error on the next line. Emitted from a view
  carrying the step's end time, or, without times, once the run has finished (so every finished step gets one end
  reply). Blocked and skipped steps get an end reply only once the run has finished.
- No final broadcast **(decided here)**: the owner asked for the status at the top; a passing run keeps one channel
  message plus its step replies in the thread.

### 3.3 Alert

One alert per failed run, key `alert:process:failed`, posted as a thread reply:
`<mention> <failed header icon> *<title>* · <failed step>: <error>; <failed step>: <error> · <Log links>`. Blocked
steps are never named. No FAILED step known → `<mention> <icon> *<title>* · Failed`. The error text reaches Slack
through the failed step's end reply in the thread, which carries its own callback's error.

## 4. Viewer-local times

- `Formatter.clock(value: datetime, tz: tzinfo) -> str` joins the formatter protocol. The Slack formatter emits
  `<!date^<unix seconds>^{time}|<HH:MM> <zone>>`, fallback text in the configured timezone (default
  `08:05 UTC`). Themes never format times themselves.
- `RenderContext.clock(value)` delegates to `fmt.clock(value, tz)`. `RenderContext.plain_clock(value)` returns
  plain `HH:MM` in the configured timezone, for places where Slack does not render date tokens: inside code blocks
  (metro's station block) **(decided here)**.
- Tests snapshot the themes with the Slack formatter and with a plain test formatter (`tests/support`) whose
  `clock` returns `HH:MM`.

## 5. Mrkdwn safety

- `SlackFormatter.bold(text)` must produce one bold token or none: it replaces each literal `*` in the text with
  `∗` (U+2217) so only its own markers remain, and trims spaces inside the markers (Slack ignores `* x*`).
  Test: titles like `*OID*s`, `OIDs*` and ` OIDs ` render as a single bold token.

## 6. Other themes

- `plain`: renders `blocked` (counts include it), uses `header_icons` for its status mark, no other change.
- `metro`: renders `blocked` as glyph `⊘` with right text `blocked` (token `extra.glyph_blocked`, word
  `extra.blocked`); its status line uses `ctx.clock`, its station block uses `ctx.plain_clock`; alerts skip blocked
  steps. Layout otherwise unchanged.

## 7. Config and example

- `ThemeConfig.collapse_done_sections: bool = False`; passed to themes through `RenderContext`; the configuration
  guide is regenerated.
- `ThemeConfig.name` default `ledger`, description "`ledger` (default), `metro`, or `plain` (the flat fallback)."
- The example DAG stays as it is (Extract → Transform → Load → Report); a failing run already gives a failed step
  (Load) and a blocked downstream step (Report).

## 8. Real-Slack check (owner)

The owner runs the example scenarios with `scripts/dev-airflow.sh` (token from `~/.config/chappe/dev.env`, passed as
`AIRFLOW_CONN_CHAPPE_SLACK` only) with `extends: ledger-chappe`: passing run, failing run with a blocked downstream
step, two parallel runs. The controller records the owner's results as descriptions in
`docs/spike/0.0.1-findings.md` under "0.0.1.1".

## 9. Docs

README "What it does" sample becomes a ledger message; Known limits updated (step replies are back: untimed reply
order, same-moment duplicate reply, start replies only for starts Chappe saw); CHANGELOG `## Unreleased` gains
ledger (default), the blocked state, `header_icons`, `ledger-chappe` tokens, viewer-local times; the 0.0.1 spec
amendment note points here too.

## 10. Tests

- Model: BLOCKED is finished; section state rule; counts order; `failed_steps` excludes blocked; source maps
  `upstream_failed` → BLOCKED.
- Tokens: `blocked` and `header_icons` resolve and validate; `extends` inside built-in files; `ledger-chappe`
  resolves with ledger's labels.
- Formatter: `clock` output; `bold` safety cases.
- Ledger: exact text for pending, running (single and multi section), collapsed sections, passed (one line),
  failed (only failing sections, counts, blocked listed), header icon empty case, title link with and without a
  link; thread start/end replies obey rule 7 (no start reply without a start time; end replies untimed only once
  finished); alert names failed steps only.
- Snapshots for every sample with the Slack formatter and the plain test formatter; conformance suite for ledger,
  metro and plain, with the new blocked sample.
- Example DAG test: passing run = one channel message, a start and an end reply per step; failing run = Load
  failed, Report blocked, one alert naming Load only.

## Out of scope

- The engine's double final broadcast seen on 2026-10-04 (ledger has no broadcast; alerts and replies use keyed
  dedup).
- The release (version 0.0.1.1, wheel, tag) — a separate step after the owner's real-Slack check.
