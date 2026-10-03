# src/chappe/config/models.py
from __future__ import annotations

import re
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jinja2 import TemplateSyntaxError
from jinja2.sandbox import SandboxedEnvironment
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CHANNEL_ID = re.compile(r"^[CG][A-Z0-9]{8,}$")
DM_ID = re.compile(r"^D[A-Z0-9]{8,}$")
CHANNEL_HINT = (
    "use the Slack channel ID (it starts with C or G, for example C0123456789); "
    "open the channel, click its name and copy the ID from the bottom of the About panel"
)
_TEMPLATES = SandboxedEnvironment()


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TransportConfig(_Model):
    type: Literal["slack"] = "slack"
    connection_id: str = "chappe_slack"


class StoreConfig(_Model):
    type: Literal["airflow_variable"] = "airflow_variable"


class ThemeConfig(_Model):
    # Phase 1 default. Task 11 switches it to "thread" when the thread theme lands.
    name: str = "plain"
    tokens: dict[str, Any] | None = None


class TimeConfig(_Model):
    timezone: str = "UTC"

    @field_validator("timezone")
    @classmethod
    def _known_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(
                f"unknown timezone {value!r}; use an IANA name such as Europe/Amsterdam"
            ) from exc
        return value


class BudgetConfig(_Model):
    event_seconds: float = Field(default=10.0, gt=0)
    final_seconds: float = Field(default=30.0, gt=0)
    final_check_delay_seconds: float = Field(default=2.0, ge=0)


class Defaults(_Model):
    transport: TransportConfig = Field(default_factory=TransportConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)
    theme: ThemeConfig = Field(default_factory=ThemeConfig)
    time: TimeConfig = Field(default_factory=TimeConfig)
    budgets: BudgetConfig = Field(default_factory=BudgetConfig)
    ui_base_url: str | None = None


class DagRef(_Model):
    dag_id: str
    section: str | None = None


class LinkConfig(_Model):
    key: str


class AlertConfig(_Model):
    mention: str | None = None
    on: Literal["final_failure"] = "final_failure"

    @model_validator(mode="before")
    @classmethod
    def _yaml_on_key(cls, data: Any) -> Any:
        # YAML 1.1 reads a bare `on:` key as True.
        if isinstance(data, dict) and any(key is True for key in data):
            return {("on" if key is True else key): value for key, value in data.items()}
        return data


class SectionOverride(_Model):
    title: str


class MilestoneOverride(_Model):
    title: str | None = None
    section: str | None = None
    hidden: bool = False


class ProcessConfig(_Model):
    enabled: bool = True
    dags: list[DagRef] = Field(min_length=1)
    link: LinkConfig | None = None
    channel: str
    title: str = "{{ dag_id }} · {{ run_id }}"
    theme: ThemeConfig | None = None
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    sections: dict[str, SectionOverride] = Field(default_factory=dict)
    milestones: dict[str, MilestoneOverride] = Field(default_factory=dict)

    @field_validator("dags")
    @classmethod
    def _one_dag(cls, value: list[DagRef]) -> list[DagRef]:
        if len(value) > 1:
            raise ValueError(
                "a process lists exactly one DAG in 0.0.1; "
                "multi-DAG processes arrive in Chappe 0.0.3"
            )
        return value

    @field_validator("channel")
    @classmethod
    def _channel_id(cls, value: str) -> str:
        if DM_ID.match(value):
            raise ValueError(
                f"{value!r} is a direct-message ID and Chappe posts to channels only; "
                f"{CHANNEL_HINT}"
            )
        if not CHANNEL_ID.match(value):
            raise ValueError(f"{value!r} is not a channel ID; {CHANNEL_HINT}")
        return value

    @field_validator("title")
    @classmethod
    def _title_template(cls, value: str) -> str:
        try:
            _TEMPLATES.parse(value)
        except TemplateSyntaxError as exc:
            raise ValueError(f"title template does not parse: {exc.message}") from exc
        return value


class ChappeSettings(_Model):
    enabled: bool = True
    defaults: Defaults = Field(default_factory=Defaults)
    processes: dict[str, ProcessConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _dag_in_one_process(self) -> ChappeSettings:
        owner: dict[str, str] = {}
        for name, process in self.processes.items():
            for ref in process.dags:
                if ref.dag_id in owner:
                    raise ValueError(
                        f"DAG {ref.dag_id!r} is listed in processes "
                        f"{owner[ref.dag_id]!r} and {name!r}"
                    )
                owner[ref.dag_id] = name
        return self

    def process_for_dag(self, dag_id: str) -> tuple[str, ProcessConfig] | None:
        for name, process in self.processes.items():
            if any(ref.dag_id == dag_id for ref in process.dags):
                return name, process
        return None

    def theme_for(self, process: ProcessConfig) -> ThemeConfig:
        return process.theme or self.defaults.theme


class ConfigFile(_Model):
    chappe: ChappeSettings
