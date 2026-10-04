# ADR-0003: Notifier mode first

Status: Accepted (2026-10-04)

## Context

Chappe can attach to Airflow in two ways: a `BaseNotifier` on the DAG plus task callbacks set by `@milestone` (notifier mode), or a listener plugin installed with the package (plugin mode). Listeners for run events execute inside the scheduler. Rendering or Slack calls there would let one slow component slow scheduling for every DAG.

## Decision

0.0.1 attaches through notifier mode only: `ChappeNotifier` as the DAG's success and failure callback, and the task callbacks attached by `@milestone`. All Chappe work runs in the task process or the DAG-processor child, bounded by time budgets. Plugin mode comes in 0.0.2, and only with listeners that record an event and return, after performance testing.

## Consequences

- One line per DAG plus the markers; nothing runs in the scheduler.
- A slow Slack adds at most the event budget to a callback. In the DAG processor a hanging callback delays the next DAG callback for that file, so Chappe bounds its own calls and uses only `get_task_states` and the connection lookup there ([ADR-0004](0004-airflow-callback-context.md)).
- Both modes will share the same core, configuration and themes.
- Process names in `ChappeNotifier(process=...)` live in DAG code, so `chappe validate-config` can check only literal ones (`--dags`); the rest is caught by a warning at parse time and at run time.

## Rejected alternatives

- Plugin mode with rendering in the scheduler.
- A one-line import that monkeypatches DAGs.
- A polling service outside Airflow.
