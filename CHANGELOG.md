# Changelog

## 0.0.2 (planned)

- TODO: CI matrix for Airflow 3.2 + 3.3, then lift the cap.
- Deferred from 0.0.1:
  - Internal release workflow (`release-internal.yml`).
  - Theme-authoring guide.
  - Deprecation policy.
  - `chappe preview` and `chappe list-components`.
  - The Airflow option `[chappe] config_file` (0.0.1 reads `CHAPPE_CONFIG` only).
  - Plugin mode with record-and-return listeners, after performance testing.
  - The `metro`, `now_next` and `tree` themes.
  - The entry-point registry, once the first external theme or store exists, and the remaining conformance suites.
- Later: processes spanning several DAGs in 0.0.3; retries and repairs in 0.0.4, which also lifts the sticky-finish limit and the cleared-task header.

## 0.0.1 (unreleased)

- One live Slack message per Airflow DAG run, edited in place, with step history in its thread.
- `@milestone` decorator and `ChappeNotifier` (notifier mode). Requires Airflow 3.2.x.
- Task states are read through the Airflow Task SDK runtime; no REST API and no extra connection. The only connection is `chappe_slack`, with the bot token in `password`; the bot needs only the `chat:write` scope.
- Airflow Variable store, one Variable per run, with write rules: watermark ordering, sticky finished state, merge on save, healing of duplicate parent messages (newest written view wins, ties to the lowest timestamp) with one guarded delete attempt, and a final read-back. See ADR-0002.
- `thread` (default) and `plain` themes; icon and label tokens.
- `chappe validate-config` (with `--dags` to scan for unknown process names) and `chappe cleanup --older-than 7d [--dry-run]`.
- Time budgets for every callback; Chappe disables itself when `[secrets] use_cache` is on.
- Configuration guide generated from the config models, ADRs 0001-0005 and a runnable example.
- Known limits are listed in the README.
