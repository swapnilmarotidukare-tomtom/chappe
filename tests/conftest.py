"""Shared pytest option and fixtures."""

from __future__ import annotations

import os
import socket
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

# Rich assertion diffs inside the helpers in tests/support.
pytest.register_assert_rewrite("tests.support")


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


def _check_target(target: Any) -> None:
    """Raise unless target is loopback or a unix socket path."""
    if isinstance(target, (str, bytes)):  # AF_UNIX path
        return
    host = target[0] if isinstance(target, tuple) and target else target
    if isinstance(host, bytes):
        host = host.decode(errors="replace")
    if host in _LOOPBACK_HOSTS:
        return
    raise RuntimeError(f"network access is blocked in tests: {host}")


@pytest.fixture(autouse=True)
def _block_network(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Fail any test that tries to reach a non-loopback host."""
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_create_connection = socket.create_connection
    real_getaddrinfo = socket.getaddrinfo

    def connect(self: socket.socket, address: Any) -> None:
        if self.family != socket.AF_UNIX:
            _check_target(address)
        return real_connect(self, address)

    def connect_ex(self: socket.socket, address: Any) -> int:
        if self.family != socket.AF_UNIX:
            _check_target(address)
        return real_connect_ex(self, address)

    def create_connection(address: Any, *args: Any, **kwargs: Any) -> socket.socket:
        _check_target(address)
        return real_create_connection(address, *args, **kwargs)

    def getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        _check_target((host,))
        return real_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "create_connection", create_connection)
    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    yield
