# Chappe

> **Chappe never breaks or blocks a pipeline. It prefers being late over being wrong, and silence over a duplicate.**

Chappe relays Apache Airflow run progress to Slack: one message per run, edited in place, with step-by-step history in its thread.

**Status:** internal pre-release (0.0.1 in development). Not licensed for use outside the owning organization.

## Known limits

- A duplicate run message is deleted with one Slack request, never retried, after a fresh store read. If it becomes the winner while that request is in flight (for example because it just got the final status), it is deleted anyway and the run shows the other message's older status, such as "In progress"; after the run has finished nothing repairs it. Very rare. A failed delete is left to the next event; after the final event there is none, so delete that duplicate by hand.
- After a cleared task re-finishes a finished run, the header shows the rerun's start time and duration, because Airflow resets the run's start date on clear. Fixed with repairs in 0.0.4.
