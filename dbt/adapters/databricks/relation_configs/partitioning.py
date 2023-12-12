from dataclasses import dataclass
import itertools
from typing import ClassVar, List, Optional

from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.adapters.relation_configs.config_change import RelationConfigChange
from dbt.contracts.graph.nodes import ModelNode
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PartitionedByConfig(DatabricksComponentConfig):
    partition_by: Optional[List[str]] = None

    def to_sql_clause(self) -> str:
        if self.partition_by:
            return f"PARTITIONED BY ({', '.join(self.partition_by)})"
        return ""


class PartitionedByProcessor(DatabricksComponentProcessor):
    name: ClassVar[str] = "partitioned_by"

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

        return PartitionedByConfig(cols)

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> PartitionedByConfig:
        partition_by = model_node.config.extra.get("partition_by")
        if isinstance(partition_by, str):
            return PartitionedByConfig([partition_by])
        return PartitionedByConfig(partition_by)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PartitionedByConfigChange(RelationConfigChange):
    context: Optional[PartitionedByConfig] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True
