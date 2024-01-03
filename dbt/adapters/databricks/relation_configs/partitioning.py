import itertools
from typing import ClassVar, List

from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.contracts.graph.nodes import ModelNode
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


class PartitionedByConfig(DatabricksComponentConfig):
    """Component encapsulating the partitioning of relations."""

    partition_by: List[str]

    @property
    def requires_full_refresh(self) -> bool:
        return True


class PartitionedByProcessor(DatabricksComponentProcessor):
    name: ClassVar[str] = "partition_by"

    @classmethod
    def from_results(cls, results: RelationResults) -> PartitionedByConfig:
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
    def from_model_node(cls, model_node: ModelNode) -> PartitionedByConfig:
        partition_by = model_node.config.extra.get("partition_by")
        if isinstance(partition_by, str):
            return PartitionedByConfig(partition_by=[partition_by])
        if not partition_by:
            return PartitionedByConfig(partition_by=[])
        return PartitionedByConfig(partition_by=partition_by)
