# Contributing

## Setup

```bash
uv sync --all-groups
uv run pre-commit install
```

## Checks (CI runs the same)

```bash
uv run ruff check
uv run ruff format --check
uv run mypy
uv run lint-imports
uv run pytest
```

Tests never use the network. The suite blocks sockets, and a test fails if anything tried to connect, even when the code under test swallowed the error. Use the fakes in `tests/support/` (a fake Slack API, fake Variables) and never a real token. `tests/integration` runs the example DAG in-process with `dag.test()`.

`scripts/update-constraints.sh` downloads Airflow's constraints. It is run by hand when Airflow is bumped, never from a test.

## Layering

`chappe.core` and `chappe.ports` import neither `airflow` nor `slack_sdk`; `import-linter` enforces this. Airflow code stays in `chappe.integrations.airflow`, Slack code in `chappe.transports.slack`.

## Snapshots

Theme snapshots live in `tests/themes/__snapshots__/`. After an intended change in a theme, regenerate them and review the text diff:

```bash
uv run pytest tests/themes --chappe-update-snapshots
git diff tests/themes/__snapshots__
```

## Generated documentation

The reference tables in `docs/guides/configuration.md` come from `src/chappe/config/models.py`. After changing a model, run:

```bash
uv run python scripts/render_config_reference.py
```

A test fails while the committed guide is out of date.

## Local Airflow

See `examples/README.md`. On macOS, export `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` and `no_proxy='*'` before `airflow standalone`, and put `.venv/bin` on `PATH` so `airflow standalone` finds the `airflow` command.

## Commits

Conventional Commits: `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`.
