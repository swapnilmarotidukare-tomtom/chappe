# Chappe

> **Chappe never breaks or blocks a pipeline. It prefers being late over being wrong, and silence over a duplicate.**

Chappe relays Apache Airflow run progress to Slack: one message per run, edited in place, with step-by-step history in its thread.

**Status:** internal pre-release (0.0.1 in development). Not licensed for use outside the owning organization.

## Known limits

- After a cleared task re-finishes a finished run, the header shows the rerun's start time and duration, because Airflow resets the run's start date on clear. Fixed with repairs in 0.0.4.
