"""Domain states."""

from __future__ import annotations

from enum import Enum


class ProcessState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def finished(self) -> bool:
        return self in (ProcessState.SUCCEEDED, ProcessState.FAILED)


class StepState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"

    @property
    def finished(self) -> bool:
        return self in (StepState.SUCCEEDED, StepState.FAILED, StepState.SKIPPED)
