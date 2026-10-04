import time

from airflow.sdk import DAG, task

from chappe import milestone
from chappe.integrations.airflow import ChappeNotifier

with DAG(
    "chappe_example",
    schedule=None,
    params={"product": "orders", "version": "2026.10.1", "fail": False},
    on_success_callback=ChappeNotifier(),
    on_failure_callback=ChappeNotifier(),
) as dag:

    @milestone("Extract")
    @task
    def extract() -> None:
        time.sleep(5)

    @milestone("Transform")
    @task
    def transform() -> None:
        time.sleep(5)

    @milestone("Load")
    @task
    def load(params: dict | None = None) -> None:
        time.sleep(5)
        if params and params.get("fail"):
            raise RuntimeError("load failed (example)")

    @milestone("Report")
    @task
    def report() -> None:
        time.sleep(5)

    extract() >> transform() >> load() >> report()
