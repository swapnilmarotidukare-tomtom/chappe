"""Shared pytest option and fixtures."""

from __future__ import annotations

from collections.abc import Callable

import pytest

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
