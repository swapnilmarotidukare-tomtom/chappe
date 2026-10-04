"""The suite must never reach the real network; loopback stays available."""

from __future__ import annotations

import socket

import pytest


def test_non_loopback_connection_is_blocked() -> None:
    with pytest.raises(RuntimeError, match=r"network access is blocked in tests: slack\.com"):
        socket.create_connection(("slack.com", 443), timeout=1)
    with pytest.raises(RuntimeError, match="network access is blocked in tests"):
        socket.getaddrinfo("slack.com", 443)
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock,
        pytest.raises(RuntimeError, match="network access is blocked in tests"),
    ):
        sock.connect(("93.184.216.34", 80))


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
