"""Chappe relays Apache Airflow run progress to chat."""

from typing import Any

__version__ = "0.0.1.dev0"


def __getattr__(name: str) -> Any:
    if name == "milestone":
        from chappe.integrations.airflow.decorators import milestone

        return milestone
    raise AttributeError(name)
