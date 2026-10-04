import subprocess
import sys

import pytest

import chappe
import chappe.integrations.airflow


def test_version_is_the_development_version() -> None:
    assert chappe.__version__ == "0.0.1.dev0"


def test_the_airflow_package_exports_the_spec_names_lazily() -> None:
    """Spec 4.2: `from chappe.integrations.airflow import ChappeNotifier`."""
    from chappe.integrations.airflow import ChappeNotifier, milestone
    from chappe.integrations.airflow.decorators import milestone as decorators_milestone
    from chappe.integrations.airflow.notifier import ChappeNotifier as notifier_class

    assert ChappeNotifier is notifier_class
    assert milestone is decorators_milestone
    with pytest.raises(AttributeError):
        _ = chappe.integrations.airflow.nothing_here  # type: ignore[attr-defined]


def test_importing_the_airflow_package_does_not_import_airflow() -> None:
    code = (
        "import sys, chappe.integrations.airflow; "
        "assert 'airflow' not in sys.modules, 'airflow was imported'"
    )
    subprocess.run([sys.executable, "-c", code], check=True, timeout=60)
