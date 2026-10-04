# Running the example against real Slack

## 1. Slack app

Create your own internal Slack app per workspace. Never distribute a shared Chappe app.

1. In your workspace, create a Slack app ("From scratch"), add the bot token scope `chat:write` (the only scope Chappe needs) and install it to the workspace.
2. Invite the bot to a test channel (`/invite @<your app>` in the channel). Without this, Slack answers `not_in_channel`.
3. Copy the channel ID (channel details → "About", starts with `C` or `G`). Chappe needs the ID, not `#name`.
4. Put the channel ID and your user or group mention into `examples/chappe.yaml`.

The bot token goes into the `chappe_slack` Airflow connection (step 3 below). Never put it in a file in this repository.

## 2. Start Airflow (local dev)

From the repository root:

```bash
uv sync --all-groups
export PATH="$PWD/.venv/bin:$PATH"   # airflow standalone starts its components as `airflow …` from PATH
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/examples/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES=chappe   # Chappe's Variables show masked in the UI
export AIRFLOW__SECRETS__USE_CACHE=False   # the default; with the cache on, Chappe disables itself
export CHAPPE_CONFIG="$PWD/examples/chappe.yaml"
# macOS only: without these, Slack calls in the forked DAG-processor child crash (objc fork safety)
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES no_proxy='*'
airflow standalone
```

Chappe needs Airflow's `[secrets] use_cache` to be `False` (the default): with the cache on, `Variable.get` can return a stale run state and Chappe would post duplicates. When it is on, Chappe logs "chappe is disabled: [secrets] use_cache must be False, because Chappe's run state must be read fresh" once per process and sends nothing.

`airflow standalone` prints the admin password. The UI is at http://localhost:8080. Use the same exports in every terminal you use below.

`.airflow/` holds `airflow.db`, and after step 3 that database contains the Slack bot token. Do not share or copy it, and delete `.airflow/` when you are done. It is in `.gitignore`.

## 3. Add the Slack connection

```bash
read -rs CHAPPE_SLACK_TOKEN   # paste the xoxb- bot token; it is not echoed or kept in shell history
airflow connections add chappe_slack --conn-type generic --conn-password "$CHAPPE_SLACK_TOKEN"
unset CHAPPE_SLACK_TOKEN
```

`--conn-password` puts the token in the command's arguments for a moment, where other processes on the machine can see it. To avoid that, add the connection in the UI instead (Admin → Connections → add `chappe_slack`, type Generic, token in Password).

## 4. Scenarios

Use `airflow dags trigger`, not `dag.test()`: under `dag.test()` the task updates post, but no final message is sent.

Check each scenario in the test channel. After every run, also check:

- The DAG-processor log (the `airflow standalone` console, or `$AIRFLOW_HOME/logs/dag_processor/…`) shows "Executing on_success dag callback" or "Executing on_failure dag callback", and none of "chappe: notifier failed", `get_template_env`, "Unable to decode message".
- The run's Variable shows `"finished": true` under `"wm"`: `airflow variables get <key>`, with the `chappe__chappe_example__…` key from `airflow variables list` (the UI masks the value).
- The final message lands within a few seconds of the run finishing.

| Scenario | Command | Expected |
|---|---|---|
| Passing run | `airflow dags trigger chappe_example` | One message that moves from "In progress" to "Passed · 4/4 steps"; one final reply, also shown in the channel. Admin → Variables shows one `chappe__chappe_example__…` Variable with a masked value |
| Failing run | `airflow dags trigger chappe_example --conf '{"fail": true}'` | Message ends "Failed"; exactly one final reply; an alert reply mentions you and names "Regression checks" |
| Two parallel runs | run `airflow dags trigger chappe_example` twice within a second | Two separate messages, one per run, no interleaving. If a duplicate parent appears for a moment, it is deleted and one message per run remains |
| Cleared task after finish | after a passing run, clear `compare.regression` in that run (UI: task → Clear, without downstream) | Known limit: the message keeps showing "Passed" while the task runs again. It shows the new final status only when the run finishes again |
| Cleanup (dry run) | `airflow variables list \| grep chappe__`, then `chappe cleanup --older-than 1h --dry-run` | The list shows one `chappe__chappe_example__…` Variable per run above. The dry run lists none (they are new) and deletes nothing |
| Kill switch | restart with `CHAPPE_ENABLED=false`, trigger | No message; the run is unaffected |
| Bad config | set `channel: "#test"`, restart, trigger | No message for this process; the log says "chappe: process '<name>' is disabled" with the reason (an error in `defaults` says "chappe is disabled"); the run is unaffected |

## 5. Catch process-name typos before deploy

```bash
chappe validate-config chappe.yaml --dags dags/
```

`--dags PATH` (repeatable; a file or a folder) scans the DAG files with Python's `ast`, without importing them or Airflow, and fails (exit 1, with `file:line`) when `ChappeNotifier(process="...")` names a process that is not in the config. Limits: the scan sees only string literals. A name built at run time (a variable, an f-string) is listed as "cannot be checked" and does not fail. Whether the named process lists this DAG is not checked. Dynamic names are caught only by the DAG-parse-time WARNING (`ChappeNotifier(process=...)` logs once per unknown name in the DAG-processor log) and by the run-time fallback (warn, then use the DAG's own process).
