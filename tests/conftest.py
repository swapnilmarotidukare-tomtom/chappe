"""Shared pytest option and fixtures."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterator

import pytest
from hypothesis import settings

# CI runs the same examples every time: no randomness, no example database, no deadline.
settings.register_profile("ci", derandomize=True, database=None, deadline=None)
if os.environ.get("HYPOTHESIS_PROFILE"):
    settings.load_profile(os.environ["HYPOTHESIS_PROFILE"])
elif os.environ.get("CI"):
    settings.load_profile("ci")

pytest_plugins = ("pytester",)

_AIRFLOW_HOME = pytest.StashKey[str]()
_SAVED_ENV = pytest.StashKey[dict[str, str | None]]()

# Rich assertion diffs inside the helpers in tests/support.
pytest.register_assert_rewrite("tests.support")


def pytest_configure(config: pytest.Config) -> None:
    """Block the network and point Airflow at a throwaway home, before collection.

    The block covers collection and module/session fixtures too; an attempt there fails the
    session. Airflow reads AIRFLOW_HOME (and builds its DB engine) at import time, so a fixture
    would be too late: without this, tests would use ~/airflow.
    """
    from tests.support.network import GUARD

    GUARD.install()
    config.pluginmanager.register(GUARD, "chappe-network-guard")
    if "airflow" in sys.modules:
        raise pytest.UsageError("airflow was imported before tests/conftest.py could isolate it")
    home = tempfile.mkdtemp(prefix="chappe-airflow-home-")
    config.stash[_AIRFLOW_HOME] = home
    env = {
        "AIRFLOW_HOME": home,
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{home}/airflow.db",
        "AIRFLOW__CORE__DAGS_FOLDER": os.path.join(home, "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    }
    config.stash[_SAVED_ENV] = {name: os.environ.get(name) for name in env}
    os.environ.update(env)


def pytest_unconfigure(config: pytest.Config) -> None:
    from tests.support.network import GUARD

    GUARD.uninstall()
    for name, value in config.stash.get(_SAVED_ENV, {}).items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
    home = config.stash.get(_AIRFLOW_HOME, None)
    if home is not None:
        shutil.rmtree(home, ignore_errors=True)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--chappe-update-snapshots",
        action="store_true",
        default=False,
        help="rewrite snapshot files instead of comparing them",
    )


@pytest.fixture
def chappe_snapshot(request: pytest.FixtureRequest) -> Callable[[str, str], None]:
    """Compare text with __snapshots__/<name>.txt next to the test file."""
    from tests.support.snapshots import check_snapshot  # after register_assert_rewrite

    update = bool(request.config.getoption("--chappe-update-snapshots"))
    folder = request.path.parent / "__snapshots__"

    def check(name: str, text: str) -> None:
        check_snapshot(folder, name, text, update=update)

    return check


@pytest.fixture(autouse=True)
def network_attempts() -> Iterator[list[str]]:
    """Collect the network attempts blocked while this test runs; fail at teardown if any.

    The block itself is installed for the whole session in pytest_configure. A test that
    deliberately provokes a block clears the yielded list after asserting on it.
    """
    from tests.support.network import GUARD, fail_if_blocked

    attempts: list[str] = []
    GUARD.test_attempts = attempts
    try:
        yield attempts
    finally:
        GUARD.test_attempts = None
    fail_if_blocked(attempts)
