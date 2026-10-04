# src/chappe/config/models.py
from __future__ import annotations

import re
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jinja2 import TemplateSyntaxError
from jinja2.sandbox import SandboxedEnvironment
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# used with fullmatch: `$` would accept a trailing newline
CHANNEL_ID = re.compile(r"[CG][A-Z0-9]{8,}")
DM_ID = re.compile(r"D[A-Z0-9]{8,}")
CHANNEL_HINT = (
    "use the Slack channel ID (it starts with C or G, for example C0123456789); "
    "open the channel, click its name and copy the ID from the bottom of the About panel"
)
_TEMPLATES = SandboxedEnvironment()


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TransportConfig(_Model):
    """How messages are delivered."""

    type: Literal["slack"] = Field(default="slack", description="Only `slack` in 0.0.1.")
    connection_id: str = Field(
        default="chappe_slack",
        description="Airflow connection that holds the Slack bot token in its `password` field.",
    )


class StoreConfig(_Model):
    """Where Chappe keeps what it has already sent for each run."""

    type: Literal["airflow_variable"] = Field(
        default="airflow_variable", description="Only `airflow_variable` in 0.0.1."
    )


class ThemeConfig(_Model):
    """How messages look."""

    name: str = Field(
        default="ledger",
        description="`ledger` (default), `metro`, or `plain` (the flat fallback).",
    )
    collapse_done_sections: bool = Field(
        default=False,
        description="ledger: show a fully done section as one line instead of listing its steps.",
    )
    tokens: dict[str, Any] | None = Field(
        default=None,
        description="Icon and label overrides; `extends` names the theme the tokens start from.",
    )


class TimeConfig(_Model):
    """Time display."""

    timezone: str = Field(
        default="UTC",
        description="IANA zone name for clock times in messages, e.g. `Europe/Amsterdam`.",
    )

    @field_validator("timezone")
    @classmethod
    def _known_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError, OSError) as exc:  # OSError: "America" is a dir
            raise ValueError(
                f"unknown timezone {value!r}; use an IANA name such as Europe/Amsterdam"
            ) from exc
        return value


class BudgetConfig(_Model):
    """Time limits for Chappe's own work in one callback.

    The deadline is checked before each Airflow call, but a call into Airflow, once started, is
    not bounded by it.
    """

    event_seconds: float = Field(
        default=10.0, gt=0, description="Time budget for one step or run-started event."
    )
    final_seconds: float = Field(
        default=30.0, gt=0, description="Time budget for the run-finished event."
    )
    final_check_delay_seconds: float = Field(
        default=2.0,
        ge=0,
        description="Wait before the final event reads the store back and posts its replies.",
    )


class Defaults(_Model):
    """Settings shared by all processes."""

    transport: TransportConfig = Field(
        default_factory=TransportConfig, description="Message delivery."
    )
    store: StoreConfig = Field(default_factory=StoreConfig, description="Delivery state.")
    theme: ThemeConfig = Field(
        default_factory=ThemeConfig, description="Theme for processes that set none."
    )
    time: TimeConfig = Field(default_factory=TimeConfig, description="Time display.")
    budgets: BudgetConfig = Field(default_factory=BudgetConfig, description="Time limits.")
    ui_base_url: str | None = Field(
        default=None,
        description="Airflow UI base URL for links to runs and tasks; no links when unset.",
    )


class DagRef(_Model):
    """A DAG that belongs to a process."""

    dag_id: str = Field(description="The DAG's id.")
    section: str | None = Field(
        default=None,
        description="Title of the DAG's default section (milestones outside any task group).",
    )


class LinkConfig(_Model):
    """How the runs of a process's DAGs find each other (multi-DAG processes, 0.0.3)."""

    key: str = Field(description="Template for the process key. Accepted and ignored in 0.0.1.")


class AlertConfig(_Model):
    """Alerts for a failed run."""

    mention: str | None = Field(
        default=None,
        description="Slack mention for failure alerts, in Slack syntax (`<@U…>`, `<!subteam^S…>`).",
    )
    on: Literal["final_failure"] = Field(
        default="final_failure", description="When to alert. Only `final_failure` in 0.0.1."
    )

    @model_validator(mode="before")
    @classmethod
    def _yaml_on_key(cls, data: Any) -> Any:
        # YAML 1.1 reads a bare `on:` key as True.
        if isinstance(data, dict) and any(key is True for key in data):
            return {("on" if key is True else key): value for key, value in data.items()}
        return data


class SectionOverride(_Model):
    """Override for one section."""

    title: str = Field(description="Section title.")


class MilestoneOverride(_Model):
    """Override for one milestone, without changing the DAG."""

    title: str | None = Field(default=None, description="Step title.")
    section: str | None = Field(
        default=None, description="Section for the step; `section=` on `@milestone` wins."
    )
    hidden: bool = Field(default=False, description="Leave the step out of the message.")


class ProcessConfig(_Model):
    """What one channel message represents: the DAG's run and where it is posted."""

    enabled: bool = Field(default=True, description="Per-process switch.")
    dags: list[DagRef] = Field(
        min_length=1, description="Exactly one DAG in 0.0.1 (multi-DAG processes arrive in 0.0.3)."
    )
    link: LinkConfig | None = Field(default=None, description="Accepted and ignored in 0.0.1.")
    channel: str = Field(description="Slack channel ID (`C…` or `G…`), not `#name`.")
    title: str = Field(
        default="{{ dag_id }} · {{ run_id }}",
        description=(
            "Message title, a template over `params`, `dag_id` and `run_id`. "
            "Falls back to `<dag_id> · <run_id>` when a value is missing."
        ),
    )
    theme: ThemeConfig | None = Field(
        default=None, description="Theme for this process; `defaults.theme` when unset."
    )
    alerts: AlertConfig = Field(default_factory=AlertConfig, description="Failure alerts.")
    sections: dict[str, SectionOverride] = Field(
        default_factory=dict, description="Section overrides, keyed by section key."
    )
    milestones: dict[str, MilestoneOverride] = Field(
        default_factory=dict,
        description="Milestone overrides, keyed by the full task id (e.g. `prepare.geometry`).",
    )

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
        if DM_ID.fullmatch(value):
            raise ValueError(
                f"{value!r} is a direct-message ID and Chappe posts to channels only; "
                f"{CHANNEL_HINT}"
            )
        if not CHANNEL_ID.fullmatch(value):
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
    """Everything under the top-level `chappe:` key."""

    enabled: bool = Field(
        default=True, description="Global switch. `CHAPPE_ENABLED=false` also disables Chappe."
    )
    defaults: Defaults = Field(
        default_factory=Defaults, description="Settings shared by all processes."
    )
    processes: dict[str, ProcessConfig] = Field(
        default_factory=dict,
        description="One entry per process; its name is what `ChappeNotifier(process=...)` uses.",
    )

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
    """The YAML file."""

    chappe: ChappeSettings = Field(description="All Chappe settings.")
