# ADR-0005: Keep sent state in an Airflow Variable for 0.0.1

Status: Accepted (2026-10-04)

## Context

The store remembers, for each run, which Slack message is the parent (`ts`), the watermark of the last write, and the output keys already sent. The spec placed this state in Slack message metadata: the bot finds its own parent by scanning `conversations.history`. That needs the `channels:history` and `groups:history` scopes.

The spike's Slack app has only `chat:write`. Adding history scopes means reinstalling the app, and in the `tomtom` workspace that may need admin approval. The owner chose the simplest workaround for 0.0.1.

The spike (`docs/spike/0.0.1-findings.md`, S7) showed that `airflow.sdk.Variable` get and set work from task callbacks and from DAG callbacks (the DAG processor proxies both), with JSON round-trips in 11–25 ms.

## Decision

- Task 13 implements `AirflowVariableStore` behind the existing `Store` port. It keeps one Variable per run, keyed `chappe__{dag_id[:80]}__{sha1("dag_id/run_id")[:16]}` (under Airflow's 250-character limit and free of the `:` and `+` in run ids; the value repeats the process key, and a mismatch is treated as absent). The value is JSON, payload version 2: `{"v", "process_key", "parent_ref", "parent_text", "wm", "parent_written", "parent_wms", "sent_keys", "stale_parents", "cleared_parents", "degraded", "updated_at"}`. `parent_wms` maps each parent ts to the newest watermark written to it; `sent_keys` are pairs of key and the parent ts the reply went under. Version 1 payloads (plain keys, no `parent_wms`) are still read and are written back as version 2. Every save re-reads and merges, never overwrites (spec 8.1).
- Chappe attaches a smaller payload, `{"v", "process_key", "wm"}`, as Slack message metadata on every parent post and update. It costs nothing, and it keeps a later Slack metadata store possible.
- The Slack app needs `chat:write` only.
- Parallel first events: after posting a new parent, Chappe merges it into the Variable with the watermark it wrote and reads it back. The parent carrying the newest written view wins; ties go to the lowest Slack ts (spec 7.1 rule 4). Every losing parent is deleted, by this event or a later one; the spike confirmed a bot can delete its own message. Replies are recorded with the parent they went under, so replies under a deleted parent are sent again under the winner. This heals duplicates but does not prevent them, because Variables have no compare-and-set.

## Consequences

- No extra Slack scopes and no history scans, so no paging cost and no history rate limits.
- State lives in the Airflow metadata database. Every Airflow deployment that runs the DAG shares it; a second Airflow posting to the same channel does not.
- Variables accumulate, one per run. The final event never deletes its Variable (a late callback would post a new parent); `chappe cleanup --older-than 7d` (with `--dry-run`) deletes old ones where the Airflow CLI runs. Schedule it.
- Variables show in the Airflow UI and can be edited or deleted by hand. A deleted Variable makes the next event post a new parent, which counts as a duplicate.
- Two parallel first events can still post two parents for a moment. The healing rule above deletes the loser, so one parent remains; the parent carrying the newest written view wins, so a late, older claim never deletes the parent that already shows the final status. A writer that lands entirely inside another's re-read→write gap can still be overwritten (no compare-and-set); the next event repairs what it can.
- If history scopes are granted later, a `SlackMetadataStore` can replace this adapter. The questions S3b, S3c and S3e in the findings must be answered first.
