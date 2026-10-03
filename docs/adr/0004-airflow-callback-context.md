# ADR-0004: Read Airflow state through the Task SDK runtime

Status: Proposed (pending the review gate after the 0.0.1 spike)

## Context

Chappe rebuilds the whole message from Airflow's current state on every event, so every callback must read the states of all tasks in the run. The spec assumed callbacks could not do this and planned a fallback: the Airflow REST API through a `chappe_airflow_api` connection with a username and password. That fallback depends on the auth manager. `/auth/token` exists for the simple and FAB auth managers, but may not exist under SSO.

The spike on Airflow 3.2.2 (`docs/spike/0.0.1-findings.md`) found:

- Task callbacks run in the task process, under the task supervisor. DAG callbacks run in a child of the DAG processor.
- Both processes proxy `RuntimeTaskInstance.get_task_states(dag_id, run_ids=[run_id])` to the execution API, with no HTTP and no credentials in user code.
- The DAG processor accepts only a subset of runtime calls. `get_dagrun_state` and `get_task_breadcrumbs` hang there until the processor times out.
- `Connection.get()` and outbound HTTP work in both processes.
- `BaseNotifier`, `Connection`, `BaseOperator` and `TaskGroup` import from `airflow.sdk`.

## Decision

- The Airflow source reads task states with `RuntimeTaskInstance.get_task_states` only. It does not call any other runtime method from a DAG callback.
- The run state comes from `context["dag_run"].state`. When `dag_run` is missing, it comes from the callback kind (success or failure).
- The source overlays the callback's own outcome on the state it reads, because a failing task still reads `running` in its own `on_failure_callback`.
- Remove `RestTaskStateReader`, the `chappe_airflow_api` connection and the `httpx` dependency. The only secret is `chappe_slack`.
- Import Airflow classes from `airflow.sdk`. The minimum supported Airflow is 3.2.
- Step times: pending the open decision in the findings (option A recommended).

## Consequences

- No auth-manager dependency, and one connection to configure instead of two.
- The runtime path uses `airflow.sdk.execution_time.task_runner.RuntimeTaskInstance`, an internal module. Pin and test each supported Airflow minor, and keep the import behind one adapter function.
- Step durations are partial unless REST comes back (option C).
- Every DAG-callback path needs a time bound. A blocked callback delays other DAGs' callbacks in the same DAG processor.
