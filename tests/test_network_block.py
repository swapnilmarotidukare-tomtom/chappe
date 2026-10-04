"""The suite must never reach the real network; loopback stays available."""

from __future__ import annotations

import contextlib
import socket

import pytest

from tests.conftest import fail_if_blocked


def test_non_loopback_connection_is_blocked(network_attempts: list[str]) -> None:
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: slack\.com"):
        socket.create_connection(("slack.com", 443), timeout=1)
    with pytest.raises(RuntimeError, match="network access is blocked in tests"):
        socket.getaddrinfo("slack.com", 443)
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock,
        pytest.raises(RuntimeError, match="network access is blocked in tests"),
    ):
        sock.connect(("93.184.216.34", 80))
    assert network_attempts[0] == "slack.com"
    network_attempts.clear()  # deliberate: the teardown check would otherwise fail this test


def test_loopback_connection_is_allowed() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            conn, _ = server.accept()
            with conn:
                client.sendall(b"x")
                assert conn.recv(1) == b"x"


def test_blocked_attempt_fails_the_test_even_if_the_error_is_swallowed(
    network_attempts: list[str],
) -> None:
    with contextlib.suppress(Exception):  # what a catch-all callback would do
        socket.create_connection(("slack.com", 443), timeout=1)
    assert network_attempts == ["slack.com"]
    # The teardown runs exactly this check, so a swallowed raise still fails the test.
    with pytest.raises(pytest.fail.Exception, match=r"slack\.com"):
        fail_if_blocked(network_attempts)
    network_attempts.clear()


def test_getaddrinfo_allows_wildcard_hosts() -> None:
    assert socket.getaddrinfo(None, 80)
    assert socket.getaddrinfo("", 80)


def test_ipv6_loopback_connection_is_allowed() -> None:
    try:
        server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        server.bind(("::1", 0))
    except OSError:
        pytest.skip("no IPv6 loopback on this host")
    with server:
        server.listen(1)
        port = server.getsockname()[1]
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as client:
            client.connect(("::1", port, 0, 0))
