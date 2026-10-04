# ADR-0002: Write rules for the parent message

Status: Accepted (2026-10-04)

## Context

Every render is correct when it is computed ([ADR-0001](0001-rebuild-and-reconcile.md)), but Slack keeps whichever edit arrives last. Task callbacks and the DAG callback run in different processes ([ADR-0004](0004-airflow-callback-context.md)) and can overlap. The store is an Airflow Variable without compare-and-set ([ADR-0005](0005-airflow-variable-store.md)), so two writers can read the same state. The bot has only `chat:write` and cannot read the message back from Slack.

## Decision

Spec section 7.1 has the full text. In short:

1. **Watermark.** A view's watermark orders by (finished, settled steps, started steps, event time). Progress decides; the event time only breaks ties, so worker clock skew cannot reorder writes. A parent write needs a strictly newer watermark than the stored one.
2. **Finished is sticky.** A render that has not seen the run finish never replaces one that has. While the stored run is unfinished, an older view still sends thread entries and alerts whose keys were never sent, but never touches the parent.
3. **Check before each edit.** Before every attempt to edit the parent, retries included, the writer reads the store (`still_current`). If a newer render is stored it gives up; if the stored winner is another parent it moves the write there, at most twice, and then yields. A write that waited cannot land over a newer one.
4. **Merge on save.** `Store.save` re-reads the Variable right before writing and merges: sent keys are unioned, the newer watermark is kept, `degraded` is OR-ed, and the newest watermark written to each parent is kept per parent. The race window shrinks from the whole event to milliseconds.
5. **Final read-back.** After the final event writes the parent it waits `final_check_delay_seconds`, reads the store once and writes the final text to the stored parent once more, unless a newer render is stored. The final event posts its thread entries and alerts after this wait.
6. **Newest written view wins; ties go to the lowest Slack timestamp.** When parallel first events each post a parent, the parent that carries the newest written view wins and only it is edited. Between equal views the lowest `ts` wins. Lowest `ts` alone is not enough: an older event that posted first but saved late would delete the parent that already shows the final status. The merged state alone decides, so all writers converge on the same message however they interleave.
7. **One guarded delete attempt.** Each event tries once to delete each losing parent (`Transport.delete_duplicate`): no retry, and the store is read again right before the single request, which is skipped when that parent has won again. A failed or out-of-time attempt leaves the duplicate to the next event. A reply recorded under a deleted parent counts as not sent, so the next event that renders it sends it again under the winner.

## Consequences

- No Slack history reads, so the bot needs only `chat:write`.
- Two writers that read and write inside the same few milliseconds can still lose an update; the next event or the read-back repairs it.
- A delete leaves a window of one Slack request: if the deleted parent becomes the winner while that request is in flight, the run shows the other parent's older status, and after the final event nothing repairs it.
- A duplicate left after the final event (a failed delete) stays for good and has to be deleted by hand.
- A cleared task after a run finished leaves the final status until the run finishes again; this is lifted with repairs in 0.0.4.
- A store with compare-and-set (Postgres, later) can enforce rules 1, 2 and 6 atomically behind the same `Store` port.

## Rejected alternatives

- Relying on fresh reads alone: a slow writer still overwrites a newer edit.
- Ordering by wall-clock time: worker clocks differ.
- Lowest Slack timestamp always wins: it can delete the parent that carries the final status.
- Retrying duplicate deletes: a retry that waited (a 429's `Retry-After`) can land after the parent won again.
- Reading the Slack message back before writing: needs history scopes ([ADR-0005](0005-airflow-variable-store.md)).
