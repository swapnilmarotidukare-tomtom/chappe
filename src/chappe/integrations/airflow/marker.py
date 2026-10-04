from __future__ import annotations

from dataclasses import dataclass
from typing import Any

MARKER_ATTR = "chappe_milestone"


@dataclass(frozen=True, slots=True)
class MilestoneSpec:
    title: str | None = None
    section: str | None = None


def milestone_spec(task: Any) -> MilestoneSpec | None:
    spec = getattr(task, MARKER_ATTR, None)
    return spec if isinstance(spec, MilestoneSpec) else None
