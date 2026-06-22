from typing import Optional

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksRelationChangeSet,
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.comment import (
    CommentProcessor,
)
from dbt.adapters.databricks.relation_configs.liquid_clustering import (
    LiquidClusteringProcessor,
)
from dbt.adapters.databricks.relation_configs.partitioning import (
    PartitionedByProcessor,
)
from dbt.adapters.databricks.relation_configs.query import DescribeQueryProcessor
from dbt.adapters.databricks.relation_configs.refresh import RefreshProcessor
from dbt.adapters.databricks.relation_configs.row_filter import RowFilterProcessor
from dbt.adapters.databricks.relation_configs.tags import TagsProcessor
from dbt.adapters.databricks.relation_configs.tblproperties import (
    TblPropertiesProcessor,
)


class StreamingTableConfig(DatabricksRelationConfigBase):
    config_components = [
        PartitionedByProcessor,
        LiquidClusteringProcessor,
        CommentProcessor,
        TblPropertiesProcessor,
        RefreshProcessor,
        TagsProcessor,
        DescribeQueryProcessor,
        RowFilterProcessor,
    ]

    def get_changeset(
        self, existing: "StreamingTableConfig"
    ) -> Optional[DatabricksRelationChangeSet]:
        """Get the changeset that must be applied to the existing relation to make it match the
        current state of the dbt project.
        """
        changes: dict[str, DatabricksComponentConfig] = {}
        requires_full_refresh = False
        has_changes = False

        for component in self.config_components:
            key = component.name
            value = self.config[key]
            diff = value.get_diff(existing.config[key])
            if diff is not None:
                has_changes = True
                if key == "partition_by":
                    requires_full_refresh = True
                changes[key] = diff
            else:
                changes[key] = value

        if not has_changes:
            return None
        return DatabricksRelationChangeSet(
            changes=changes, requires_full_refresh=requires_full_refresh
        )
