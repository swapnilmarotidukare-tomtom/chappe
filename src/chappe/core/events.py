# src/chappe/core/events.py
"""Source-neutral events."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from chappe.core.model import ProcessState, StepState


class EventKind(str, Enum):
    STEP_STARTED = "step_started"
    STEP_FINISHED = "step_finished"
    RUN_FINISHED = "run_finished"


@dataclass(frozen=True, slots=True)
class ChappeEvent:
    kind: EventKind
    process: str
    dag_id: str
    run_id: str
    occurred_at: datetime
    step_key: str | None = None
    step_state: StepState | None = None
    process_state: ProcessState | None = None
    error: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)
