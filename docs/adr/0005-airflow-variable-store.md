# ADR-0005: Keep sent state in an Airflow Variable for 0.0.1

Status: Proposed (pending the review gate after the 0.0.1 spike)

## Context

The store remembers, for each run, which Slack message is the parent (`ts`), the watermark of the last write, and the output keys already sent. The spec placed this state in Slack message metadata: the bot finds its own parent by scanning `conversations.history`. That needs the `channels:history` and `groups:history` scopes.

The spike's Slack app has only `chat:write`. Adding history scopes means reinstalling the app, and in the `tomtom` workspace that may need admin approval. The owner chose the simplest workaround for 0.0.1.

The spike (`docs/spike/0.0.1-findings.md`, S7) showed that `airflow.sdk.Variable` get and set work from task callbacks and from DAG callbacks (the DAG processor proxies both), with JSON round-trips in 11–25 ms.

## Decision

- Task 13 implements `AirflowVariableStore` behind the existing `Store` port. It keeps one Variable per run, keyed `chappe/<dag_id>/<run_id>`, holding the existing flat payload plus the parent `ts`: `{"v", "process_key", "parent_ts", "wm_finished", "wm_last_change", "wm_settled", "sent_keys", "degraded"}`.
- Chappe still attaches the same payload as Slack message metadata on every post and update. It costs nothing, and it keeps a later Slack metadata store possible without migrating anything.
- The Slack app needs `chat:write` only.
- Parallel first events: after posting a new parent, Chappe merges it into the Variable with the watermark it wrote and reads it back. The parent carrying the newest written view wins; ties go to the lowest Slack ts (spec 7.1 rule 4). Every losing parent is deleted, by this event or a later one; the spike confirmed a bot can delete its own message. Replies are recorded with the parent they went under, so replies under a deleted parent are sent again under the winner. This heals duplicates but does not prevent them, because Variables have no compare-and-set.

## Consequences

- No extra Slack scopes and no history scans, so no paging cost and no history rate limits.
- State lives in the Airflow metadata database. Every Airflow deployment that runs the DAG shares it; a second Airflow posting to the same channel does not.
- Variables accumulate, one per run. 0.0.1 documents this as a known limit (manual cleanup, for example with `airflow variables delete`). Automatic cleanup comes later.
- Variables show in the Airflow UI and can be edited or deleted by hand. A deleted Variable makes the next event post a new parent, which counts as a duplicate.
- Two parallel first events can still post two parents if both read back their own write. Accepted for 0.0.1 under "silence over a duplicate" being best effort here; the read-back makes it rare.
- If history scopes are granted later, a `SlackMetadataStore` can replace this adapter. The questions S3b, S3c and S3e in the findings must be answered first.
