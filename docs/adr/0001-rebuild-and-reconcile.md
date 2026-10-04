# ADR-0001: Rebuild the full message set on every event, then key-diff

Status: Accepted (2026-10-04)

## Context

Callbacks can arrive late, twice, out of order or not at all. Chappe must still end with a message that matches the run. Incremental updates ("step X finished, edit line X") drift as soon as one event is missed, and they cannot repair anything.

## Decision

On every event the source reads the whole run from Airflow (`get_task_states`, [ADR-0004](0004-airflow-callback-context.md)) and builds a `ProcessView`. The theme renders the complete `MessageSet`: parent text, keyed thread entries, keyed alerts. The engine then decides what to send. The watermark and the other write rules ([ADR-0002](0002-write-rules.md)) decide whether the parent may be written; the keys decide which thread entries and alerts are new, against what the store says was already sent ([ADR-0005](0005-airflow-variable-store.md)).

## Consequences

- Any later event repairs a missed or failed one.
- Themes are pure functions of the view and are tested with snapshots.
- Each event costs one state read and one render, which is cheap next to a Slack call.
- Thread entries are append-only by key: an entry whose text would change later is not re-sent.
- A step's duration is known only from its own callback; the runtime read returns states only.

## Rejected alternatives

- Incremental updates from event deltas: they drift, and they are unsafe with repairs.
- A poller outside Airflow that renders on a timer: extra infrastructure to operate.
