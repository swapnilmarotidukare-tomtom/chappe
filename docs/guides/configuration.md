# Configuration

Chappe reads one YAML file, found through the `CHAPPE_CONFIG` environment variable (the Airflow option `[chappe] config_file` is not read yet). The file never holds secrets. Check it with `chappe validate-config PATH` (exit code 0 or 1); without a path it reads `CHAPPE_CONFIG`.

## Required setup

All four are needed before the first run.

1. **Slack app** with the bot scope `chat:write` and nothing else, invited to the channel (see [Slack setup](#slack-setup)).
2. **The bot token in the connection `chappe_slack`**, supplied as the environment variable `AIRFLOW_CONN_CHAPPE_SLACK` or by a secrets backend (see [Airflow setup](#airflow-setup)). Never in the YAML file or the repository.
3. **`[secrets] use_cache` off** (the default). With it on, Chappe disables itself.
4. **macOS only, for local runs**: `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` and `no_proxy='*'` in the environment of every Airflow process.

## Slack setup

1. Create your own internal Slack app per workspace. Never distribute a shared Chappe app.
2. Add the bot token scope `chat:write`. It is the only scope Chappe needs and the only one it should have: Chappe never reads channel history.
3. Install the app to the workspace and copy the bot token (it starts with `xoxb-`).
4. Invite the bot to every channel Chappe posts to (`/invite @<your app>`). Without this, Slack answers `not_in_channel` and Chappe marks the process degraded.
5. Use the channel **ID** in the config (channel details → "About"; it starts with `C` or `G`). `#name` and direct-message IDs (`D…`) are rejected by validation.

## Airflow setup

- Airflow 3.2.x. Other minors are not tested.
- Connection `chappe_slack`: the bot token in the `password` field. It is the only connection Chappe uses. The connection id is the config key `transport.connection_id` (default `chappe_slack`); the environment variable is named after it, upper case: `AIRFLOW_CONN_CHAPPE_SLACK`. Set it in the environment of the scheduler, the DAG processor and the workers, or serve it from an Airflow secrets backend:

  ```bash
  export AIRFLOW_CONN_CHAPPE_SLACK='{"conn_type": "generic", "password": "<bot token>"}'
  ```

  Take the value from your secret store; do not type the token into a shell history or commit it. For a local run, `airflow connections add chappe_slack --conn-type generic --conn-password ...` or the UI (Admin → Connections) also work, and store the token in the Airflow metadata database.

- Keep `[secrets] use_cache` at `False` (the default; `AIRFLOW__SECRETS__USE_CACHE=False`). With the cache on, `Variable.get` can return a stale run state, so Chappe disables itself and logs one ERROR: "chappe is disabled: [secrets] use_cache must be False, because Chappe's run state must be read fresh".
- Add `chappe` to `[core] sensitive_var_conn_names` (or `AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES=chappe`) so the values of Chappe's Variables are masked in the UI. They hold delivery state only, no secrets.
- Chappe keeps one Variable per run, `chappe__<dag_id[:80]>__<hash>`, and never deletes it at the end of the run. Remove old ones where the Airflow CLI runs (it needs the metadata database), for example from a scheduled job:

  ```bash
  chappe cleanup --older-than 7d --dry-run   # list what would be deleted
  chappe cleanup --older-than 7d             # delete; durations accept Nd and Nh, N at least 1
  ```

  A Variable Chappe cannot read is left alone; delete it by hand.
- Kill switch: `CHAPPE_ENABLED=false` in the environment, `enabled: false` at the top level, or `enabled: false` on a process.
- macOS local development only: export `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` and `no_proxy='*'` before `airflow standalone`, or Slack calls from DAG callbacks crash in the forked DAG-processor child. Also put the virtual environment's `bin` on `PATH` so `airflow standalone` finds the `airflow` command. Linux is not affected.

## Checking the config

`chappe validate-config PATH` checks structure, channel IDs, title templates, themes and tokens. Add `--dags PATH` (a file or directory, repeatable) to also scan DAG files for `ChappeNotifier(process=...)` names that are not configured. The scan sees only literal process names: a name computed at run time is reported as "cannot be checked", and the scan does not check that the process lists the DAG. Those are caught by a WARNING when the DAG file is parsed and at run time, when Chappe warns and uses the process of the DAG itself.

## Example

```yaml
chappe:
  enabled: true
  defaults:
    transport: {type: slack, connection_id: chappe_slack}
    store: {type: airflow_variable}
    theme: {name: metro}
    time: {timezone: UTC}
    budgets: {event_seconds: 10, final_seconds: 30, final_check_delay_seconds: 2}
    ui_base_url: https://airflow.example.com
  processes:
    regression:
      dags:
        - dag_id: regression
          section: Regression
      channel: C0123456789
      title: "{{ params.product }} {{ params.version }}"
      theme:
        name: metro
        tokens:
          extends: metro
          icons: {running: ":loading:"}
      alerts: {mention: "<!subteam^S00000000>", on: final_failure}
      sections:
        prepare: {title: Prepare data}
      milestones:
        prepare.geometry: {title: Generate geometry}
        prepare.cleanup_tmp: {hidden: true}
```

## Reference

Generated from `src/chappe/config/models.py` by `scripts/render_config_reference.py`; do not edit by hand. After changing a model, run `uv run python scripts/render_config_reference.py`. A test fails while the block is out of date.

<!-- BEGIN GENERATED REFERENCE -->
### `ConfigFile`

The YAML file.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `chappe` | `ChappeSettings` | none | yes | All Chappe settings. |

### `ChappeSettings`

Everything under the top-level `chappe:` key.

Found at `chappe`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `enabled` | boolean | `true` | no | Global switch. `CHAPPE_ENABLED=false` also disables Chappe. |
| `defaults` | `Defaults` | defaults of `Defaults` | no | Settings shared by all processes. |
| `processes` | mapping of string → `ProcessConfig` | `{}` | no | One entry per process; its name is what `ChappeNotifier(process=...)` uses. |

### `Defaults`

Settings shared by all processes.

Found at `chappe.defaults`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `transport` | `TransportConfig` | defaults of `TransportConfig` | no | Message delivery. |
| `store` | `StoreConfig` | defaults of `StoreConfig` | no | Delivery state. |
| `theme` | `ThemeConfig` | defaults of `ThemeConfig` | no | Theme for processes that set none. |
| `time` | `TimeConfig` | defaults of `TimeConfig` | no | Time display. |
| `budgets` | `BudgetConfig` | defaults of `BudgetConfig` | no | Time limits. |
| `ui_base_url` | string or null | `null` | no | Airflow UI base URL for links to runs and tasks; no links when unset. |

### `TransportConfig`

How messages are delivered.

Found at `chappe.defaults.transport`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `type` | `"slack"` | `"slack"` | no | Only `slack` in 0.0.1. |
| `connection_id` | string | `"chappe_slack"` | no | Airflow connection that holds the Slack bot token in its `password` field. |

### `StoreConfig`

Where Chappe keeps what it has already sent for each run.

Found at `chappe.defaults.store`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `type` | `"airflow_variable"` | `"airflow_variable"` | no | Only `airflow_variable` in 0.0.1. |

### `ThemeConfig`

How messages look.

Found at `chappe.defaults.theme`, `chappe.processes.<name>.theme`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `name` | string | `"ledger"` | no | `ledger` (default), `metro`, or `plain` (the flat fallback). |
| `collapse_done_sections` | boolean | `false` | no | ledger: show a fully done section as one line instead of listing its steps. |
| `tokens` | mapping or null | `null` | no | Icon and label overrides; `extends` names the theme the tokens start from. |

### `TimeConfig`

Time display.

Found at `chappe.defaults.time`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `timezone` | string | `"UTC"` | no | IANA zone name for clock times in messages, e.g. `Europe/Amsterdam`. |

### `BudgetConfig`

Time limits for Chappe's own work in one callback. The deadline is checked before each Airflow call, but a call into Airflow, once started, is not bounded by it.

Found at `chappe.defaults.budgets`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `event_seconds` | number (> 0) | `10.0` | no | Time budget for one step or run-started event. |
| `final_seconds` | number (> 0) | `30.0` | no | Time budget for the run-finished event. |
| `final_check_delay_seconds` | number (≥ 0) | `2.0` | no | Wait before the final event reads the store back and posts its replies. |

### `ProcessConfig`

What one channel message represents: the DAG's run and where it is posted.

Found at `chappe.processes.<name>`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `enabled` | boolean | `true` | no | Per-process switch. |
| `dags` | list of `DagRef` (at least 1) | none | yes | Exactly one DAG in 0.0.1 (multi-DAG processes arrive in 0.0.3). |
| `link` | `LinkConfig` or null | `null` | no | Accepted and ignored in 0.0.1. |
| `channel` | string | none | yes | Slack channel ID (`C…` or `G…`), not `#name`. |
| `title` | string | `"{{ dag_id }} · {{ run_id }}"` | no | Message title, a template over `params`, `dag_id` and `run_id`. Falls back to `<dag_id> · <run_id>` when a value is missing. |
| `theme` | `ThemeConfig` or null | `null` | no | Theme for this process; `defaults.theme` when unset. |
| `alerts` | `AlertConfig` | defaults of `AlertConfig` | no | Failure alerts. |
| `sections` | mapping of string → `SectionOverride` | `{}` | no | Section overrides, keyed by section key. |
| `milestones` | mapping of string → `MilestoneOverride` | `{}` | no | Milestone overrides, keyed by the full task id (e.g. `prepare.geometry`). |

### `DagRef`

A DAG that belongs to a process.

Found at `chappe.processes.<name>.dags`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `dag_id` | string | none | yes | The DAG's id. |
| `section` | string or null | `null` | no | Title of the DAG's default section (milestones outside any task group). |

### `LinkConfig`

How the runs of a process's DAGs find each other (multi-DAG processes, 0.0.3).

Found at `chappe.processes.<name>.link`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `key` | string | none | yes | Template for the process key. Accepted and ignored in 0.0.1. |

### `AlertConfig`

Alerts for a failed run.

Found at `chappe.processes.<name>.alerts`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `mention` | string or null | `null` | no | Slack mention for failure alerts, in Slack syntax (`<@U…>`, `<!subteam^S…>`). |
| `on` | `"final_failure"` | `"final_failure"` | no | When to alert. Only `final_failure` in 0.0.1. |

### `SectionOverride`

Override for one section.

Found at `chappe.processes.<name>.sections.<name>`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `title` | string | none | yes | Section title. |

### `MilestoneOverride`

Override for one milestone, without changing the DAG.

Found at `chappe.processes.<name>.milestones.<name>`.

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `title` | string or null | `null` | no | Step title. |
| `section` | string or null | `null` | no | Section for the step; `section=` on `@milestone` wins. |
| `hidden` | boolean | `false` | no | Leave the step out of the message. |
<!-- END GENERATED REFERENCE -->

## How sections and titles resolve

A milestone's section:

1. `section=` on `@milestone`.
2. `section` in the config's `milestones:` override.
3. The milestone's outermost task group.
4. The DAG's default section: the DAG's `section:` entry in config, or the DAG id made readable.

A section's title:

1. `title` in the config's `sections:` override (keyed by the section key).
2. The text given to `section=` on `@milestone` or in the `milestones:` override, when the section came from there.
3. The task group's `group_display_name`.
4. The group id made readable (`prepare_data` → "Prepare data"); for the DAG's default section, its `section:` from config, else the DAG id made readable.

A process with a single section shows no section header. A task group without milestones never becomes a section. A step's title is the config's `milestones:` title, then the `@milestone` title, then the task's `task_display_name`, then its task id made readable (the part after the last `.`).
