"""DAG → sections and steps (spec 4.1.1)."""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Any

from chappe.config.models import MilestoneOverride, ProcessConfig
from chappe.integrations.airflow.marker import MilestoneSpec, milestone_spec


@dataclass(frozen=True, slots=True)
class SectionDef:
    key: str
    title: str


@dataclass(frozen=True, slots=True)
class StepDef:
    task_id: str
    title: str
    section_key: str


@dataclass(frozen=True, slots=True)
class ProcessStructure:
    sections: tuple[SectionDef, ...]
    steps: tuple[StepDef, ...]


def readable(identifier: str) -> str:
    words = [w for w in identifier.replace("-", "_").split("_") if w]
    text = " ".join(words)
    return text[:1].upper() + text[1:]


def _topological_order(dag: Any) -> list[str]:
    ids = list(dag.task_dict)
    index = {task_id: i for i, task_id in enumerate(ids)}
    indegree = {task_id: len(dag.task_dict[task_id].upstream_task_ids) for task_id in ids}
    ready = [(index[t], t) for t in ids if indegree[t] == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        _, task_id = heapq.heappop(ready)
        order.append(task_id)
        for downstream in dag.task_dict[task_id].downstream_task_ids:
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                heapq.heappush(ready, (index[downstream], downstream))
    return order


def _outermost_group(task: Any) -> Any | None:
    group = getattr(task, "task_group", None)
    if group is None or group.group_id is None:
        return None
    while group.parent_group is not None and group.parent_group.group_id is not None:
        group = group.parent_group
    return group


def _step_title(task: Any, spec: MilestoneSpec, override: MilestoneOverride | None) -> str:
    if override is not None and override.title:
        return override.title
    if spec.title:
        return spec.title
    display = getattr(task, "task_display_name", None)
    if display and display != task.task_id:
        return str(display)
    return readable(task.task_id.rsplit(".", 1)[-1])


def _section(
    task: Any, spec: MilestoneSpec, override: MilestoneOverride | None, default: SectionDef
) -> SectionDef:
    if spec.section:
        return SectionDef(spec.section, spec.section)
    if override is not None and override.section:
        return SectionDef(override.section, override.section)
    group = _outermost_group(task)
    if group is not None:
        display = getattr(group, "group_display_name", None)
        title = display if display and display != group.group_id else readable(group.group_id)
        return SectionDef(group.group_id, str(title))
    return default


def build_structure(dag: Any, process: ProcessConfig) -> ProcessStructure:
    ref = next((d for d in process.dags if d.dag_id == dag.dag_id), None)
    default = SectionDef(dag.dag_id, ref.section if ref and ref.section else readable(dag.dag_id))
    found: list[StepDef] = []
    titles: dict[str, str] = {}
    for task_id in _topological_order(dag):
        task = dag.get_task(task_id)
        spec = milestone_spec(task)
        if spec is None:
            continue
        override = process.milestones.get(task_id)
        if override is not None and override.hidden:
            continue
        section = _section(task, spec, override, default)
        titles.setdefault(section.key, section.title)
        found.append(StepDef(task_id, _step_title(task, spec, override), section.key))

    order: list[str] = []
    for step in found:
        if step.section_key not in order:
            order.append(step.section_key)
    sections = tuple(
        SectionDef(key, process.sections[key].title if key in process.sections else titles[key])
        for key in order
    )
    steps = tuple(step for key in order for step in found if step.section_key == key)
    return ProcessStructure(sections, steps)
