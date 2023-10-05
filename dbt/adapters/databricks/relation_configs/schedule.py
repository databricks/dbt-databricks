from dataclasses import dataclass
from typing import Optional, Set

import agate
from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksScheduleConfig(DatabricksRelationConfigBase, RelationConfigValidationMixin):
    """
    This config fallows the specs found here:
    https://docs.databricks.com/en/sql/language-manual/sql-ref-partition.html#partitioned-by

    The following parameters are configurable by dbt:
    - partition_column: the column identifier to be sorted
    - column_type: the type of the specified thing
    """

    # sortkey: Optional[FrozenSet[str]] = None

    def __post_init__(self):
        # # maintains `frozen=True` while allowing for a variable default on `sort_type`
        # if self.sortstyle is None and self.sortkey is None:
        #     object.__setattr__(self, "sortstyle", DatabrickSortStyle.default())
        # elif self.sortstyle is None:
        #     object.__setattr__(self, "sortstyle", DatabrickSortStyle.default_with_columns())
        super().__post_init__()

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # TODO

        pass

    @classmethod
    def from_dict(cls, config_dict):
        # TODO

        pass

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        # TODO
        pass

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
class DatabricksScheduleConfigChange(RelationConfigChange):
    context: Optional[bool] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False
