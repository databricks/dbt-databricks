from abc import ABC
from dataclasses import dataclass
import re
from typing import ClassVar, List, Optional

from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.exceptions import DbtRuntimeError
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksAlterableComponentConfig,
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)

SCHEDULE_REGEX = re.compile(r"CRON '(.*)' AT TIME ZONE '(.*)'")


class RefreshConfig(DatabricksAlterableComponentConfig, ABC):
    pass


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class ScheduledRefreshConfig(RefreshConfig):
    cron: str
    time_zone_value: Optional[str] = None
    alter: bool = False

    def to_sql_clause(self) -> str:
        schedule = f"SCHEDULE CRON '{self.cron}'"

        if self.time_zone_value:
            schedule += f" AT TIME ZONE '{self.time_zone_value}'"

        return schedule

    def to_alter_sql_clauses(self) -> List[str]:
        prefix = "ALTER " if self.alter else "ADD "

        return [prefix + self.to_sql_clause()]

    def get_diff(self, other: DatabricksComponentConfig) -> "ScheduledRefreshConfig":
        if not isinstance(other, RefreshConfig):
            raise DbtRuntimeError(
                f"Cannot diff {self.__class__.__name__} with {other.__class__.__name__}"
            )

        return ScheduledRefreshConfig(
            cron=self.cron,
            time_zone_value=self.time_zone_value,
            alter=isinstance(other, ScheduledRefreshConfig),
        )


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class ManualRefreshConfig(RefreshConfig):
    def to_sql_clause(self) -> str:
        return ""

    def to_alter_sql_clauses(self) -> List[str]:
        return ["DROP SCHEDULE"]

    def get_diff(self, other: DatabricksComponentConfig) -> "ManualRefreshConfig":
        if not isinstance(other, RefreshConfig):
            raise DbtRuntimeError(
                f"Cannot diff {self.__class__.__name__} with {other.__class__.__name__}"
            )

        return ManualRefreshConfig()


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RefreshProcessor(DatabricksComponentProcessor[RefreshConfig]):
    name: ClassVar[str] = "refresh"

    @classmethod
    def from_results(cls, results: RelationResults) -> RefreshConfig:
        table = results["describe_extended"]
        for row in table.rows:
            if row[0] == "Refresh Schedule":
                if row[1] == "MANUAL":
                    return ManualRefreshConfig()

                match = SCHEDULE_REGEX.match(row[1])

                if match:
                    cron, time_zone_value = match.groups()
                    return ScheduledRefreshConfig(cron=cron, time_zone_value=time_zone_value)

                raise DbtRuntimeError(
                    f"Could not parse schedule from description: {row[1]}."
                    " This is most likely a bug in the dbt-databricks adapter,"
                    " so please file an issue!"
                )

        raise DbtRuntimeError(
            "Could not parse schedule for table."
            " This is most likely a bug in the dbt-databricks adapter, so please file an issue!"
        )

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> RefreshConfig:
        schedule = model_node.config.extra.get("schedule")
        if schedule:
            if "cron" not in schedule:
                raise DbtRuntimeError(f"Schedule config must contain a 'cron' key, got {schedule}")
            return ScheduledRefreshConfig(
                cron=schedule["cron"], time_zone_value=schedule.get("time_zone_value")
            )
        else:
            return ManualRefreshConfig()
