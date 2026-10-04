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
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: slack\.invalid"):
        socket.create_connection(("slack.invalid", 443), timeout=1)
    with pytest.raises(RuntimeError, match="network access is blocked in tests"):
        socket.getaddrinfo("slack.invalid", 443)
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock,
        pytest.raises(RuntimeError, match="network access is blocked in tests"),
    ):
        sock.connect(("example.invalid", 80))
    assert network_attempts[0] == "slack.invalid"
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
        socket.create_connection(("slack.invalid", 443), timeout=1)
    assert network_attempts == ["slack.invalid"]
    # The teardown runs exactly this check, so a swallowed raise still fails the test.
    with pytest.raises(pytest.fail.Exception, match=r"slack\.invalid"):
        fail_if_blocked(network_attempts)
    network_attempts.clear()


@pytest.mark.parametrize("host", [None, ""])
def test_getaddrinfo_passes_wildcard_hosts_to_the_resolver(
    host: str | None, monkeypatch: pytest.MonkeyPatch, network_attempts: list[str]
) -> None:
    """The guard forwards a wildcard host unrecorded; what the OS resolver answers is its own
    business (glibc rejects "" with EAI_NONAME, macOS resolves it), so the C-level call is
    replaced by one that answers like glibc and never touches the network."""
    seen: list[str | None] = []

    def glibc_resolver(node: str | None, *args: object) -> list[object]:
        seen.append(node)
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    monkeypatch.setattr(socket._socket, "getaddrinfo", glibc_resolver)  # type: ignore[attr-defined]
    with contextlib.suppress(socket.gaierror):
        socket.getaddrinfo(host, 80)
    assert seen == [host]
    assert network_attempts == []


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


# Address-only APIs (gethostbyaddr, sendto, sendmsg) get 192.0.2.1: TEST-NET-1 (RFC 5737),
# reserved for documentation and never routed, since an .invalid name is not an address.
TEST_NET_ADDRESS = "192.0.2.1"


def test_name_lookups_are_blocked(network_attempts: list[str]) -> None:
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: slack\.invalid"):
        socket.gethostbyname("slack.invalid")
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: slack\.invalid"):
        socket.gethostbyname_ex("slack.invalid")
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: 192\.0\.2\.1"):
        socket.gethostbyaddr(TEST_NET_ADDRESS)
    assert network_attempts == ["slack.invalid", "slack.invalid", TEST_NET_ADDRESS]
    network_attempts.clear()


def test_loopback_name_lookups_are_allowed() -> None:
    assert socket.gethostbyname("localhost") == "127.0.0.1"
    assert socket.gethostbyname_ex("127.0.0.1")[2] == ["127.0.0.1"]


def test_datagrams_to_the_network_are_blocked(network_attempts: list[str]) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        with pytest.raises(RuntimeError, match="network access is blocked in tests"):
            sock.sendto(b"x", (TEST_NET_ADDRESS, 8125))
        with pytest.raises(RuntimeError, match="network access is blocked in tests"):
            sock.sendto(b"x", 0, (TEST_NET_ADDRESS, 8125))
        with pytest.raises(RuntimeError, match="network access is blocked in tests"):
            sock.sendmsg([b"x"], [], 0, (TEST_NET_ADDRESS, 8125))
    assert network_attempts == [TEST_NET_ADDRESS] * 3
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
    start = len(GUARD.outside_attempts)
    if GUARD.installed:  # never risk a real lookup if the block is missing
        with contextlib.suppress(RuntimeError):
            socket.gethostbyname("slack.invalid")
    seen = GUARD.outside_attempts[start:]
    # deliberate: remove only our own attempt; earlier ones must still fail the session
    del GUARD.outside_attempts[start:]
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
    monkeypatch.setenv("PYTHONPATH", str(ROOT), prepend=os.pathsep)
    pytester.makepyfile(test_child=_CHILD_TESTS)
    result = pytester.runpytest_subprocess("-p", "tests.conftest", "-p", "no:cacheprovider")
    result.assert_outcomes(passed=2, errors=1)  # the teardown error is on a passed test
    result.stdout.fnmatch_lines(
        [
            "*network access was attempted in tests: slack.invalid*",
            "*network access was attempted outside a test's recording window (collection, a "
            "module/session fixture, or a fixture set up before/torn down after "
            "network_attempts): slack.invalid",
        ]
    )
    assert result.ret == pytest.ExitCode.TESTS_FAILED


_CHILD_COLLECTION_ONLY = """
import contextlib
import socket

from tests.support.network import GUARD

if GUARD.installed:  # never risk a real lookup if the block is missing
    with contextlib.suppress(RuntimeError):
        socket.gethostbyname("slack.invalid")  # at collection


def test_passes():
    pass
"""


def test_an_outside_attempt_alone_fails_the_session(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PYTHONPATH", str(ROOT), prepend=os.pathsep)
    pytester.makepyfile(test_child=_CHILD_COLLECTION_ONLY)
    result = pytester.runpytest_subprocess("-p", "tests.conftest", "-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(
        [
            "*network access was attempted outside a test's recording window (collection, a "
            "module/session fixture, or a fixture set up before/torn down after "
            "network_attempts): slack.invalid",
        ]
    )
    assert result.ret == pytest.ExitCode.TESTS_FAILED


def _block_is_active() -> bool:
    return "NetworkGuard" in getattr(socket.getaddrinfo, "__qualname__", "")


def test_a_nested_install_and_uninstall_keep_the_session_block() -> None:
    """An in-process run loading tests.conftest installs and uninstalls on top of ours."""
    assert _block_is_active()
    try:
        GUARD.install()
        GUARD.uninstall()
        assert GUARD.installed
        assert _block_is_active()
    finally:
        if not _block_is_active():  # restore the session block for the remaining tests
            GUARD.installed = False
            GUARD.install()


def test_reverse_name_lookups_are_blocked(network_attempts: list[str]) -> None:
    # checked first: an unpatched getnameinfo would do a real reverse lookup
    assert "NetworkGuard" in getattr(socket.getnameinfo, "__qualname__", "")
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: 192\.0\.2\.1"):
        socket.getnameinfo((TEST_NET_ADDRESS, 80), 0)
    assert network_attempts == [TEST_NET_ADDRESS]
    network_attempts.clear()


def test_loopback_reverse_name_lookups_are_allowed() -> None:
    assert "NetworkGuard" in getattr(socket.getnameinfo, "__qualname__", "")
    flags = socket.NI_NUMERICHOST | socket.NI_NUMERICSERV
    assert socket.getnameinfo(("127.0.0.1", 80), flags) == ("127.0.0.1", "80")
