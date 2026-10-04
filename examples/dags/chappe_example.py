import time

from airflow.sdk import DAG, TaskGroup, task

from chappe import milestone
from chappe.integrations.airflow.notifier import ChappeNotifier

with DAG(
    "chappe_example",
    schedule=None,
    params={"product": "orders", "version": "2026.10.1", "fail": False},
    on_success_callback=ChappeNotifier(),
    on_failure_callback=ChappeNotifier(),
) as dag:
    with TaskGroup("prepare", group_display_name="Prepare"):

        @milestone("Parquet → Delta")
        @task
        def convert() -> None:
            time.sleep(5)

        @task
        def cleanup_tmp() -> None:
            time.sleep(1)

        @milestone("Geometry")
        @task
        def geometry() -> None:
            time.sleep(5)

        convert() >> cleanup_tmp() >> geometry()

    with TaskGroup("compare", group_display_name="vs orders 2026.09.1"):

        @milestone("ID stability")
        @task
        def id_stability() -> None:
            time.sleep(5)

        @milestone("Regression checks")
        @task
        def regression(params: dict | None = None) -> None:
            time.sleep(5)
            if params and params.get("fail"):
                raise RuntimeError("regression check failed (example)")

        id_stability() >> regression()

    dag.get_task("prepare.geometry") >> dag.get_task("compare.id_stability")
