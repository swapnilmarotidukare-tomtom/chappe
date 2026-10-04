"""The suite must never reach the real network; loopback stays available."""

from __future__ import annotations

import contextlib
import os
import socket
import tempfile
from pathlib import Path

import pytest

from tests.support.network import GUARD, fail_if_blocked

ROOT = Path(__file__).resolve().parents[1]


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


def test_name_lookups_are_blocked(network_attempts: list[str]) -> None:
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: slack\.com"):
        socket.gethostbyname("slack.com")
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: slack\.com"):
        socket.gethostbyname_ex("slack.com")
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: 93\.184\.216\.34"):
        socket.gethostbyaddr("93.184.216.34")
    assert network_attempts == ["slack.com", "slack.com", "93.184.216.34"]
    network_attempts.clear()


def test_loopback_name_lookups_are_allowed() -> None:
    assert socket.gethostbyname("localhost") == "127.0.0.1"
    assert socket.gethostbyname_ex("127.0.0.1")[2] == ["127.0.0.1"]


def test_datagrams_to_the_network_are_blocked(network_attempts: list[str]) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        with pytest.raises(RuntimeError, match="network access is blocked in tests"):
            sock.sendto(b"x", ("93.184.216.34", 8125))
        with pytest.raises(RuntimeError, match="network access is blocked in tests"):
            sock.sendto(b"x", 0, ("93.184.216.34", 8125))
        with pytest.raises(RuntimeError, match="network access is blocked in tests"):
            sock.sendmsg([b"x"], [], 0, ("93.184.216.34", 8125))
    assert network_attempts == ["93.184.216.34"] * 3
    network_attempts.clear()


def test_loopback_and_unix_datagrams_are_allowed() -> None:
    with (
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server,
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client,
    ):
        server.bind(("127.0.0.1", 0))
        address = server.getsockname()
        client.sendto(b"a", address)
        client.sendto(b"b", 0, address)
        client.sendmsg([b"c"], [], 0, address)
        assert [server.recv(1) for _ in range(3)] == [b"a", b"b", b"c"]
    with (
        tempfile.TemporaryDirectory() as folder,
        socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as server,
        socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as client,
    ):
        path = os.path.join(folder, "s")
        server.bind(path)
        client.sendto(b"u", path)
        assert server.recv(1) == b"u"


# Read while this module is imported, i.e. during collection.
_INSTALLED_AT_COLLECTION = GUARD.installed


def test_block_is_installed_before_collection() -> None:
    assert _INSTALLED_AT_COLLECTION


@pytest.fixture(scope="module")
def module_scope_attempts() -> list[str]:
    """A blocked lookup from a module-scoped fixture, outside the per-test window."""
    if GUARD.installed:  # never risk a real lookup if the block is missing
        with contextlib.suppress(RuntimeError):
            socket.gethostbyname("slack.invalid")
    seen = list(GUARD.outside_attempts)
    GUARD.outside_attempts.clear()  # deliberate: the session check would otherwise fail the run
    return seen


def test_block_covers_module_scoped_fixtures(module_scope_attempts: list[str]) -> None:
    assert module_scope_attempts == ["slack.invalid"]


_CHILD_TESTS = """
import contextlib
import socket

from tests.support.network import GUARD

if GUARD.installed:  # never risk a real lookup if the block is missing
    with contextlib.suppress(RuntimeError):
        socket.gethostbyname("slack.invalid")  # at collection


def test_passes():
    pass


def test_swallows_a_blocked_connection():
    assert GUARD.installed
    with contextlib.suppress(Exception):  # what a catch-all callback would do
        socket.create_connection(("slack.invalid", 443), timeout=1)
"""


def test_attempts_fail_the_test_or_the_session(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A child pytest run with this suite's conftest as a plugin."""
    monkeypatch.setenv("PYTHONPATH", str(ROOT))
    pytester.makepyfile(test_child=_CHILD_TESTS)
    result = pytester.runpytest_subprocess("-p", "tests.conftest", "-p", "no:cacheprovider")
    result.assert_outcomes(passed=2, errors=1)  # the teardown error is on a passed test
    result.stdout.fnmatch_lines(
        [
            "*network access was attempted in tests: slack.invalid*",
            "*network access was attempted outside any test (collection or a module/session "
            "fixture): slack.invalid",
        ]
    )
    assert result.ret == pytest.ExitCode.TESTS_FAILED
