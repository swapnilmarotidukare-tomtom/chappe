"""Shared pytest option and fixtures."""

from __future__ import annotations

import os
import shutil
import socket
import sys
import tempfile
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from hypothesis import settings

# CI runs the same examples every time: no randomness, no example database, no deadline.
settings.register_profile("ci", derandomize=True, database=None, deadline=None)
if os.environ.get("HYPOTHESIS_PROFILE"):
    settings.load_profile(os.environ["HYPOTHESIS_PROFILE"])
elif os.environ.get("CI"):
    settings.load_profile("ci")

_AIRFLOW_HOME = pytest.StashKey[str]()

# Rich assertion diffs inside the helpers in tests/support.
pytest.register_assert_rewrite("tests.support")


def pytest_configure(config: pytest.Config) -> None:
    """Point Airflow at a throwaway home before anything imports it.

    Airflow reads AIRFLOW_HOME (and builds its DB engine) at import time, so a fixture would be
    too late: without this, tests would use ~/airflow.
    """
    if "airflow" in sys.modules:
        raise pytest.UsageError("airflow was imported before tests/conftest.py could isolate it")
    home = tempfile.mkdtemp(prefix="chappe-airflow-home-")
    config.stash[_AIRFLOW_HOME] = home
    os.environ["AIRFLOW_HOME"] = home
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{home}/airflow.db"
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.path.join(home, "dags")
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"


def pytest_unconfigure(config: pytest.Config) -> None:
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


_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


def fail_if_blocked(attempts: list[str]) -> None:
    """Fail the running test if any network attempt was blocked (a caught raise is not enough)."""
    if attempts:
        pytest.fail(f"network access was attempted in tests: {', '.join(attempts)}", pytrace=False)


def _check_target(target: Any, attempts: list[str] | None = None) -> None:
    """Raise unless target is loopback or a unix socket path."""
    if isinstance(target, (str, bytes)):  # AF_UNIX path
        return
    host = target[0] if isinstance(target, tuple) and target else target
    if isinstance(host, bytes):
        host = host.decode(errors="replace")
    if host in _LOOPBACK_HOSTS:
        return
    if attempts is not None:
        attempts.append(str(host))
    raise RuntimeError(f"network access is blocked in tests: {host}")


@pytest.fixture(autouse=True)
def network_attempts(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Block non-loopback network use and record each attempt; fail at teardown if any.

    A test that deliberately provokes a block clears the yielded list after asserting on it.
    """
    attempts: list[str] = []
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_create_connection = socket.create_connection
    real_getaddrinfo = socket.getaddrinfo

    def connect(self: socket.socket, address: Any) -> None:
        if self.family != socket.AF_UNIX:
            _check_target(address, attempts)
        return real_connect(self, address)

    def connect_ex(self: socket.socket, address: Any) -> int:
        if self.family != socket.AF_UNIX:
            _check_target(address, attempts)
        return real_connect_ex(self, address)

    def create_connection(address: Any, *args: Any, **kwargs: Any) -> socket.socket:
        _check_target(address, attempts)
        return real_create_connection(address, *args, **kwargs)

    def getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        if host not in (None, "", "0.0.0.0", "::"):
            _check_target((host,), attempts)
        return real_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "create_connection", create_connection)
    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    yield attempts
    fail_if_blocked(attempts)
