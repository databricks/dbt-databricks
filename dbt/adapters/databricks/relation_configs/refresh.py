import re
from typing import ClassVar, Optional

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults

SCHEDULE_REGEX = re.compile(r"CRON '(.*)' AT TIME ZONE '(.*)'")


class RefreshConfig(DatabricksComponentConfig):
    """Component encapsulating the refresh schedule of a relation."""

    cron: Optional[str] = None
    time_zone_value: Optional[str] = None

    # Property indicating whether the schedule change should be accomplished by an ADD SCHEDULE
    # vs an ALTER SCHEDULE. This is only True when modifying an existing schedule, rather than
    # switching from manual refresh to scheduled or vice versa.
    is_altered: bool = False

    def get_diff(self, other: "RefreshConfig") -> Optional["RefreshConfig"]:
        if self != other:
            return RefreshConfig(
                cron=self.cron,
                time_zone_value=self.time_zone_value,
                is_altered=self.cron is not None and other.cron is not None,
            )
        return None


class RefreshProcessor(DatabricksComponentProcessor[RefreshConfig]):
    name: ClassVar[str] = "refresh"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> RefreshConfig:
        table = results["describe_extended"]
        for row in table.rows:
            if row[0] == "Refresh Schedule":
                if row[1] == "MANUAL":
                    return RefreshConfig()

                match = SCHEDULE_REGEX.match(row[1])

                if match:
                    cron, time_zone_value = match.groups()
                    return RefreshConfig(cron=cron, time_zone_value=time_zone_value)

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
    def from_relation_config(cls, relation_config: RelationConfig) -> RefreshConfig:
        schedule = base.get_config_value(relation_config, "schedule")
        if schedule:
            if "cron" not in schedule:
                raise DbtRuntimeError(f"Schedule config must contain a 'cron' key, got {schedule}")
            return RefreshConfig(
                cron=schedule["cron"], time_zone_value=schedule.get("time_zone_value")
            )
        else:
            return RefreshConfig()
