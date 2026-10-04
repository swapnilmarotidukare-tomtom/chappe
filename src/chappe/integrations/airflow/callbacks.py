"""Task callbacks attached by @milestone. Filled in Task 17."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def on_step_started(context: Mapping[str, Any]) -> None:
    return None


def on_step_succeeded(context: Mapping[str, Any]) -> None:
    return None


def on_step_failed(context: Mapping[str, Any]) -> None:
    return None


def on_step_skipped(context: Mapping[str, Any]) -> None:
    return None
