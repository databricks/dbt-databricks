from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional, Set

import agate
from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksPartitionedByConfig(DatabricksRelationConfigBase, RelationConfigValidationMixin):
    """
    This config fallows the specs found here:
    https://docs.databricks.com/en/sql/language-manual/sql-ref-partition.html#partitioned-by

    The following parameters are configurable by dbt:
    - partition_column: the column identifier to be sorted
    - column_type: the type of the specified thing
    """

    partitioned_by_columns: FrozenSet[str]

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # TODO: can we reference the schema here to ensure that the columns are valid?
        pass

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config = {}
        partitioned_by = model_node.config.extra.get("partitioned_by", None)

        if partitioned_by:
            config["partitioned_by_columns"] = frozenset(partitioned_by.split(","))

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> dict:
        """
        Translate agate objects from the database into a standard dictionary.

        Note:
            This was only built for materialized views, which does not specify a sortstyle.
            Processing of `sortstyle` has been omitted here, which means it's the default (compound).

        Args:
            relation_results_entry: the description of the sortkey and sortstyle from the database in this format:

                agate.Row({
                    ...,
                    "sortkey1": "<column_name>",
                    ...
                })

        Returns: a standard dictionary describing this `DatabrickSortConfig` instance
        """
        # TODO
        pass


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksPartitionedByConfigChange(RelationConfigChange):
    context: Optional[bool] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False
