# Chappe

<p align="center"><img src="docs/assets/chappe-telegraph.jpg" alt="A Chappe telegraph on top of a church tower, in a 19th-century painting" width="560"></p>

<p align="center"><em>A Chappe telegraph on a church tower, 19th-century painting</em></p>

## What it does

Chappe reports the progress of Apache Airflow 3 runs to Slack. Each DAG run gets one channel message that is edited in place as milestones start and finish. The message shows every step as a station on a line; a step's duration appears in the update its own callback makes, and the status line carries the run's total time. When a run fails, an alert mentions your on-call in the message's thread. DAG authors mark the tasks that matter with `@milestone`; channel, theme and icons are configuration.

````text
*orders 2026.10.1*
:large_yellow_circle: In progress · started 08:05 · 7m
```
●  Extract
┃
◉  Transform             since 08:07
┆
○  Load
┆
○  Report
```
Airflow run
````

## Why the name

Claude Chappe (1763–1805), a French inventor, built the optical telegraph with his brothers in the 1790s. Towers on hilltops carried a crossbar with two movable arms, and each position of the arms was a signal. An operator read the next tower through a telescope, copied the position, and the message moved from tower to tower. The first line, from Paris to Lille, opened in 1794: news that took days by courier arrived in hours. The network grew to hundreds of stations across France and was used until the electric telegraph replaced it in the mid-19th century. The word "telegraph", from the Greek for "far writer", was first used for Chappe's system. Like a Chappe tower, this library does one job. It relays the state of a run, as one signal that changes in place, to people who are far from the machine room and not watching Airflow.

## Principle

> **Chappe never breaks or blocks a pipeline. It prefers being late over being wrong, and silence over a duplicate.**

- A Slack outage never fails a task: Chappe's errors are logged, not raised into Airflow.
- A delayed update is fine. A wrong status is not.
- When Chappe cannot tell whether a post landed, it does not retry it, because a retry would post twice whenever the first try did land. A later event may still post again in that case; see [Known limits](#known-limits-of-001).

## When it helps

- Release or regression pipelines whose title comes from params, such as product and version.
- Long multi-stage pipelines where people ask "where is it now?".
- Nightly data loads followed by stakeholders who have no Airflow access.
- On-call who should be pinged only when a run fails.
- A team channel that should show one tidy message per run, not a stream of task notifications.

**Status:** Chappe 0.0.1 is an internal release: the wheel is in `releases/` and the tag is `v0.0.1`. It is not published on PyPI, has no public documentation site, and is not licensed for use outside the owning organization.

## Quick start

1. Install into your Airflow image (Airflow 3.2.x): install the wheel from this repository's `releases/` directory — see [Install](#install).
2. Create your own internal Slack app in your workspace with the bot scope `chat:write` (and no other), invite the bot to the channel, and give Airflow the bot token as the connection `chappe_slack`: the environment variable `AIRFLOW_CONN_CHAPPE_SLACK` or a secrets backend, never the YAML file or this repository. See [configuration](docs/guides/configuration.md#required-setup).
3. Set these Airflow options (or their `AIRFLOW__…` environment variables):
   - `[secrets] use_cache = False`. This is the default, and it must stay off: with the cache on, Chappe disables itself.
   - `[core] sensitive_var_conn_names` includes `chappe`, so Chappe's Variables show masked in the UI.
   - Local runs on macOS: `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` and `no_proxy='*'` in the environment, or Slack calls from DAG callbacks crash.
4. Write `chappe.yaml` and point `CHAPPE_CONFIG` at it:

   ```yaml
   chappe:
     defaults:
       ui_base_url: https://airflow.example.com
     processes:
       regression:
         dags: [{dag_id: regression}]
         channel: C0123456789
         title: "{{ params.product }} {{ params.version }}"
         alerts: {mention: "<!subteam^S00000000>"}
   ```

5. Mark milestones and attach the notifier:

   ```python
   from airflow.sdk import dag, task
   from chappe import milestone
   from chappe.integrations.airflow.notifier import ChappeNotifier


   @dag(schedule=None, on_success_callback=ChappeNotifier(), on_failure_callback=ChappeNotifier())
   def regression():
       @milestone("Parquet → Delta")
       @task
       def convert(): ...

       convert()


   regression()
   ```

6. Check the config in CI: `chappe validate-config chappe.yaml`.
7. Schedule `chappe cleanup --older-than 7d`: Chappe keeps one Airflow Variable per run and never deletes it itself.

The full reference is in [docs/guides/configuration.md](docs/guides/configuration.md). A runnable example is in [examples/](examples/README.md). Design decisions are in [docs/adr/](docs/adr/).

## Install

Chappe 0.0.1 is distributed as a wheel committed to this private repository under `releases/`, at the tag `v0.0.1`. There is no package index and nothing is published to PyPI. Download the wheel and its `.sha256` file from the tag through the GitHub API, check the hash, then install the wheel with the Airflow constraints for your Airflow version and Python. Replace `<org>/<repo>` with this repository.

The token is a fine-grained GitHub token with read-only Contents access to this repository. Keep it in your CI's secret store. Never put it in a URL, a Dockerfile `ARG` or `ENV`, or a file that ends up in an image layer.

### In a Docker image

curl reads the token from a config file mounted as a BuildKit secret (`curl -K`), so the token never appears in a process's arguments:

```dockerfile
# syntax=docker/dockerfile:1
FROM apache/airflow:3.2.2-python3.12
RUN --mount=type=secret,id=gh_curl,uid=50000 \
    set -eu; cd /tmp; \
    for f in chappe-0.0.1-py3-none-any.whl chappe-0.0.1-py3-none-any.whl.sha256; do \
      curl -fsSL -K /run/secrets/gh_curl -H "Accept: application/vnd.github.raw" -o "$f" "https://api.github.com/repos/<org>/<repo>/contents/releases/$f?ref=v0.0.1"; \
    done; \
    sha256sum -c chappe-0.0.1-py3-none-any.whl.sha256; \
    pip install --no-cache-dir chappe-0.0.1-py3-none-any.whl \
      -c https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt; \
    rm -f chappe-0.0.1-py3-none-any.whl*
```

The secret file holds one line, a curl config option with the token in place of `<token>`:

```text
header = "Authorization: Bearer <token>"
```

Create it from your CI's secret store with permissions only its owner can read, outside the build context, pass it to the build, and delete it afterwards. It must never be in the build context or committed. The snippet below is meant for one CI step, with the token in `GH_TOKEN`; it also cleans up after itself when pasted into an interactive shell:

```bash
gh_curl="$(mktemp)"                 # outside the build context, readable only by its owner
(umask 077; printf 'header = "Authorization: Bearer %s"\n' "${GH_TOKEN:?set GH_TOKEN}" > "$gh_curl")
built=0
docker build --secret id=gh_curl,src="$gh_curl" . || built=$?
rm -f "$gh_curl"                    # deleted right after the build, also when it fails
test "$built" -eq 0
```

That is `docker build --secret id=gh_curl,src=<path outside the build context> .` with a temporary file. `printf` is a builtin in common shells (bash, zsh, dash), so the token is not a process argument there either.

- The token never appears in a process's arguments (argv), in an image layer, or in a URL. The secret is mounted only for that `RUN` step. `uid=50000` is the user of the official Airflow image; change it if your image runs as another user.
- The build needs BuildKit, the default since Docker Engine 23. On older engines set `DOCKER_BUILDKIT=1`.
- The wheel's SHA256 is checked before it is installed. A mismatch stops the build.
- Match the image tag, the constraints version and the Python version to your deployment. Instead of the upstream constraints URL you can use the file vendored in this repository, `constraints/airflow-3.2.2-py3.12.txt`.

### Without Docker

Use the same download, check and install steps. The shell writes the same curl config file with its `printf` builtin, from a token in an environment variable that is never echoed or logged, and curl reads it with `-K`; the token is never a curl argument:

```bash
gh_curl="$(mktemp)"
(umask 077; printf 'header = "Authorization: Bearer %s"\n' "${GH_TOKEN:?set GH_TOKEN}" > "$gh_curl")
fetched=0
for f in chappe-0.0.1-py3-none-any.whl chappe-0.0.1-py3-none-any.whl.sha256; do
  curl -fsSL -K "$gh_curl" -H "Accept: application/vnd.github.raw" \
       -o "$f" "https://api.github.com/repos/<org>/<repo>/contents/releases/$f?ref=v0.0.1" \
    || { fetched=$?; break; }
done
rm -f "$gh_curl"                    # deleted right after the downloads, also when they fail
test "$fetched" -eq 0
sha256sum -c chappe-0.0.1-py3-none-any.whl.sha256   # on macOS: shasum -a 256 -c
pip install chappe-0.0.1-py3-none-any.whl -c constraints/airflow-3.2.2-py3.12.txt
```

The SHA256 of the release is in the committed `.sha256` file and in the [changelog](CHANGELOG.md).

## Known limits of 0.0.1

Where a limit names a Slack message, it is the run's message or a duplicate of it. "By hand" means in Slack or with the Airflow CLI.

### Scope

- Airflow 3.2.x only. Newer minors need a test run first, so the `airflow` extra is capped below 3.3.
- Notifier mode only (`ChappeNotifier` plus `@milestone`). There is no Airflow plugin and there are no listeners.
- One DAG per process. The config already takes `dags: [...]` and `link:`; multi-DAG processes arrive in 0.0.3.
- Mapped tasks (`.expand()`) are not modelled. A `@milestone` on a mapped task is ignored with a warning.
- The config file is found only through `CHAPPE_CONFIG`; the Airflow option `[chappe] config_file` is not read yet. `chappe preview` and `chappe list-components` arrive in 0.0.2.
- `chappe validate-config --dags` sees only literal process names in `ChappeNotifier(process=...)`. A computed name is reported as not checkable, and the scan does not check that the process lists the DAG. A wrong name is caught by a warning at DAG parse time and at run time, when Chappe uses the DAG's own process.
- Changing a process's theme or tokens while a run is in flight is the developer's responsibility; Chappe does not pin a theme per run.
- Local development on macOS: Slack calls from DAG callbacks crash in the forked DAG-processor child unless `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` and `no_proxy='*'` are set. Linux is not affected.

### Reruns and cleanup

- Clearing tasks after a run finished leaves the final status until the run finishes again. Retry and repair history arrives in 0.0.4.
- After a cleared task re-finishes a finished run, the header shows the rerun's start time and duration, because Airflow resets the run's start date on clear. Fixed with repairs in 0.0.4.
- Chappe keeps one Airflow Variable per run (`chappe__<dag_id>__<hash>`). They accumulate; run `chappe cleanup --older-than 7d` regularly. Chappe never deletes the Variable of a run at its end, because a late callback would then post a new message.
- If a run's Variable becomes unreadable (hand edit, or a newer Chappe version's format), Chappe stops sending for that run rather than risk a duplicate, and `chappe cleanup` leaves it alone. Delete the Variable by hand to reset the run.
- With `[secrets] use_cache` on, Chappe disables itself and logs one ERROR, because a cached read could miss a parallel writer's message and post a duplicate. Turn the option off.

### Duplicates and lost writes

- When the first events of a run arrive in parallel, two parent messages can rarely be posted. Chappe heals this itself: the message carrying the newest status wins (equal ones: the lowest Slack timestamp) and the other is deleted. Anything that was posted under the deleted message (an alert, or the replies of a theme that posts step replies; no built-in theme does) is sent again under the winner by the next event that renders it, but only while the run is unfinished; after the final event it is not repeated.
- A duplicate run message is deleted with one Slack request, never retried, after a fresh store read. If it becomes the winner while that request is in flight (for example because it just got the final status), it is deleted anyway and the run shows the other message's older status, such as "In progress"; after the run has finished nothing repairs it. Very rare. A failed delete is left to the next event; after the final event there is none, so a duplicate left then stays for good: delete it by hand.
- Chappe never retries a post that failed in an ambiguous way (timeout, Slack 5xx), because it may have landed. If it did land, the next event posts again and the first message stays as an orphan: without history scope Chappe cannot find it. Delete it by hand.
- If someone deletes the run's message by hand, the next event posts a new one. Alerts already sent under the deleted message are sent again in the new thread only by events that still render them, and only while the run is unfinished; nothing is repeated after the final event. The same holds for the step replies of a theme that posts them (none of the built-in themes does), which also come back without their duration once the step's own event has passed.
- The final check reads the store back once, a few seconds after the final message. Every parent edit, retries included, first checks the store and gives up when a newer render is stored, so a retrying non-final writer cannot overwrite the final status. One edit already in flight when the final event writes can still land after the check and leave a stale status until the next event of the run (normally none). Rare; it needs a single Slack request slower than the check delay.
- If the final alert's post fails in an ambiguous way (timeout, Slack 5xx), it is not retried, because it may have landed; no later event exists, so on-call may not be mentioned. The error is logged with the process key.
- Applies only to themes that post step replies (none of the built-in themes does): a step's reply can still be posted twice (one with a duration, one without) in a window of milliseconds: every event re-reads the store right before each reply, and the final event posts its replies only after its check delay, so a duplicate needs one event's reply to be in flight exactly while the other re-reads.

### What the message shows

- Step durations are shown only for a step whose own event Chappe processed. Airflow's runtime read returns states only, so other steps show their state without a duration. With the default `metro` theme, the final message shows no step durations, only the run's total time, because it is rendered from the DAG callback, which has no step times.
- Applies only to themes that post step replies (none of the built-in themes does): replies whose step had no own callback appear in the order they were sent, not in step order.
- With the default `metro` theme, the failure alert is built from the run-end DAG callback. It names the failed step or steps, without their duration and, for a step that failed on its own, without its error text. On-call learns which step failed; the error is in the task log, which the alert links when `ui_base_url` is set.
- If Airflow runs a DAG callback without the run's context (no `dag_run`, no params), a title template that uses params falls back to `<dag_id> · <run_id>` in the final message.

### Time

- Time budgets bound Chappe's own work and its Slack calls, not the calls into Airflow: Chappe checks the deadline before each task-state read and Variable read or write (saving a Slack call it has already made is never skipped), but a hung Airflow call holds the callback until Airflow's own execution-API timeout or the DAG processor's callback timeout. DNS resolution inside a Slack call is not bounded by the per-request timeout.
