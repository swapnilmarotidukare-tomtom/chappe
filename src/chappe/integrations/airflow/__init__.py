"""Apache Airflow 3 integration: callbacks, notifier, source and runtime wiring.

Airflow is imported here and, lazily, by the Variable store (`chappe.stores.airflow_variable`);
core, ports, themes and the Slack transport never import it.

`ChappeNotifier` and `milestone` are re-exported lazily (spec 4.2), so importing this package
alone does not import Airflow.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from chappe.integrations.airflow.decorators import milestone
    from chappe.integrations.airflow.notifier import ChappeNotifier

__all__ = ["ChappeNotifier", "milestone"]


def __getattr__(name: str) -> Any:
    if name == "ChappeNotifier":
        from chappe.integrations.airflow.notifier import ChappeNotifier

        return ChappeNotifier
    if name == "milestone":
        from chappe.integrations.airflow.decorators import milestone

        return milestone
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
