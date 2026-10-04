"""The test suite's network block: non-loopback connects, name lookups and datagrams raise.

Every blocked attempt is also recorded, so a test whose code swallows the error still fails, and
an attempt outside any test (collection, a module- or session-scoped fixture) fails the session.
"""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any

import pytest

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
_WILDCARD_HOSTS = (None, "", "0.0.0.0", "::")


def fail_if_blocked(attempts: list[str]) -> None:
    """Fail the running test if any network attempt was blocked (a caught raise is not enough)."""
    if attempts:
        pytest.fail(f"network access was attempted in tests: {', '.join(attempts)}", pytrace=False)


def describe_outside_attempts(attempts: list[str]) -> str:
    return (
        "network access was attempted outside any test (collection or a module/session fixture): "
        + ", ".join(attempts)
    )


class NetworkGuard:
    """Patches the socket module once; records attempts for the running test or, between tests,
    for the session. Also a pytest plugin: an attempt outside a test fails the session."""

    def __init__(self) -> None:
        self.installed = False
        self.test_attempts: list[str] | None = None  # set while a test runs
        self.outside_attempts: list[str] = []
        self._saved: list[tuple[Any, str, Any]] = []

    def _record(self, host: str) -> None:
        target = self.test_attempts if self.test_attempts is not None else self.outside_attempts
        target.append(host)

    def check(self, target: Any) -> None:
        """Raise (and record) unless target is loopback or a unix socket path."""
        if isinstance(target, (str, bytes)):  # AF_UNIX path
            return
        host = target[0] if isinstance(target, tuple) and target else target
        if isinstance(host, bytes):
            host = host.decode(errors="replace")
        if host in _LOOPBACK_HOSTS:
            return
        self._record(str(host))
        raise RuntimeError(f"network access is blocked in tests: {host}")

    def install(self) -> None:
        if self.installed:
            return
        check = self.check
        real_connect = socket.socket.connect
        real_connect_ex = socket.socket.connect_ex
        real_sendto = socket.socket.sendto
        real_sendmsg = socket.socket.sendmsg
        real_create_connection = socket.create_connection
        real_getaddrinfo = socket.getaddrinfo

        def connect(self: socket.socket, address: Any) -> None:
            if self.family != socket.AF_UNIX:
                check(address)
            return real_connect(self, address)

        def connect_ex(self: socket.socket, address: Any) -> int:
            if self.family != socket.AF_UNIX:
                check(address)
            return real_connect_ex(self, address)

        def sendto(self: socket.socket, data: Any, *args: Any) -> int:
            # sendto(data, address) or sendto(data, flags, address)
            if self.family != socket.AF_UNIX and args:
                check(args[-1])
            return real_sendto(self, data, *args)

        def sendmsg(self: socket.socket, buffers: Any, *args: Any, **kwargs: Any) -> int:
            # sendmsg(buffers[, ancdata[, flags[, address]]])
            address = args[2] if len(args) > 2 else kwargs.get("address")
            if self.family != socket.AF_UNIX and address is not None:
                check(address)
            return real_sendmsg(self, buffers, *args, **kwargs)

        def create_connection(address: Any, *args: Any, **kwargs: Any) -> socket.socket:
            check(address)
            return real_create_connection(address, *args, **kwargs)

        def getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
            if host not in _WILDCARD_HOSTS:
                check((host,))
            return real_getaddrinfo(host, *args, **kwargs)

        def lookup(real: Callable[..., Any]) -> Callable[..., Any]:
            def blocked(host: Any, *args: Any, **kwargs: Any) -> Any:
                check((host,))
                return real(host, *args, **kwargs)

            return blocked

        patches: list[tuple[Any, str, Any]] = [
            (socket.socket, "connect", connect),
            (socket.socket, "connect_ex", connect_ex),
            (socket.socket, "sendto", sendto),
            (socket.socket, "sendmsg", sendmsg),
            (socket, "create_connection", create_connection),
            (socket, "getaddrinfo", getaddrinfo),
        ]
        for name in ("gethostbyname", "gethostbyname_ex", "gethostbyaddr"):
            patches.append((socket, name, lookup(getattr(socket, name))))
        for owner, name, replacement in patches:
            self._saved.append((owner, name, getattr(owner, name)))
            setattr(owner, name, replacement)
        self.installed = True

    def uninstall(self) -> None:
        for owner, name, original in reversed(self._saved):
            setattr(owner, name, original)
        self._saved.clear()
        self.installed = False

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session) -> None:
        if self.outside_attempts and session.exitstatus in (
            pytest.ExitCode.OK,
            pytest.ExitCode.NO_TESTS_COLLECTED,
        ):
            session.exitstatus = pytest.ExitCode.TESTS_FAILED

    def pytest_terminal_summary(self, terminalreporter: Any) -> None:
        if self.outside_attempts:
            terminalreporter.write_sep("=", "network block", red=True, bold=True)
            terminalreporter.write_line(describe_outside_attempts(self.outside_attempts))


GUARD = NetworkGuard()
