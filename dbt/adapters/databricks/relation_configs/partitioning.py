import itertools
from typing import ClassVar, Union

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults


class PartitionedByConfig(DatabricksComponentConfig):
    """Component encapsulating the partitioning of relations."""

    partition_by: list[str]


class PartitionedByProcessor(DatabricksComponentProcessor):
    name: ClassVar[str] = "partition_by"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> PartitionedByConfig:
        table = results["describe_extended"]
        cols = []
        rows = itertools.takewhile(
            lambda row: row[0],
            itertools.dropwhile(lambda row: row[0] != "# Partition Information", table.rows),
        )
        for row in rows:
            if not row[0].startswith("# "):
                cols.append(row[0])

        return PartitionedByConfig(partition_by=cols)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> PartitionedByConfig:
        partition_by: Union[str, list[str], None] = base.get_config_value(
            relation_config, "partition_by"
        )
        if not partition_by:
            return PartitionedByConfig(partition_by=[])
        if isinstance(partition_by, str):
            return PartitionedByConfig(partition_by=[partition_by])
        return PartitionedByConfig(partition_by=partition_by)
