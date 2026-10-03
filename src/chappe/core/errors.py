# src/chappe/core/errors.py
"""Errors Chappe raises internally. None of them may escape into Airflow."""

from __future__ import annotations


class ChappeError(Exception):
    """Base class."""


class ChappeConfigError(ChappeError):
    """Configuration is invalid; the message names the key and the fix."""


class StoreError(ChappeError):
    """The store could not read or write."""


class TransportError(ChappeError):
    def __init__(self, code: str, *, retryable: bool, retry_after: float | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after = retry_after
