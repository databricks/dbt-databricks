from abc import ABC
from dataclasses import dataclass
import re
from typing import Optional
from agate import Row
from dbt.exceptions import DbtRuntimeError
from dbt.adapters.relation_configs.config_change import RelationConfigChange
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)

SCHEDULE_REGEX = re.compile(r"CRON '(.*)' AT TIME ZONE '(.*)'")


class RefreshConfig(DatabricksComponentConfig, ABC):
    pass


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class ScheduledRefreshConfig(RefreshConfig):
    cron: str
    time_zone_value: Optional[str]

    def to_sql_clause(self) -> str:
        schedule = f"SCHEDULE CRON '{self.cron}'"

        if self.time_zone_value:
            schedule += f" AT TIME ZONE '{self.time_zone_value}'"

        return schedule


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class ManualRefreshConfig(RefreshConfig):
    def to_sql_clause(self) -> str:
        return ""


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RefreshProcessor(DatabricksComponentProcessor[RefreshConfig]):
    @classmethod
    def name(cls) -> str:
        return "refresh"

    @classmethod
    def description_target(cls) -> str:
        return "Refresh Schedule"

    @classmethod
    def process_description_row_impl(cls, row: Row) -> RefreshConfig:
        if row[1] == "MANUAL":
            return ManualRefreshConfig()

        match = SCHEDULE_REGEX.match(row[1])

        if match:
            cron, time_zone_value = match.groups()
            return ScheduledRefreshConfig(cron=cron, time_zone_value=time_zone_value)
        else:
            raise DbtRuntimeError(
                f"Could not parse schedule from description: {row[1]}."
                " This is most likely a bug in the dbt-databricks adapter, so please file an issue!"
            )

    @classmethod
    def process_model_node(cls, model_node: ModelNode) -> RefreshConfig:
        schedule = model_node.config.extra.get("schedule")
        if schedule:
            if "cron" not in schedule:
                raise DbtRuntimeError(f"Schedule config must contain a 'cron' key, got {schedule}.")
            return ScheduledRefreshConfig(
                cron=schedule["cron"], time_zone_value=schedule.get("time_zone_value")
            )
        else:
            return ManualRefreshConfig()


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RefreshConfigChange(RelationConfigChange):
    context: Optional[RefreshConfig] = None

    def requires_full_refresh(self) -> bool:
        return False
