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
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig, RefreshProcessor
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
        TagsProcessor,
    ]

    def get_changeset(
        self, existing: "StreamingTableConfig"
    ) -> Optional[DatabricksRelationChangeSet]:
        """Get the changeset that must be applied to the existing relation to make it match the
        current state of the dbt project.
        """
        changes: dict[str, DatabricksComponentConfig] = {}
        requires_refresh = False
        requires_replace = False

        for component in self.config_components:
            key = component.name
            value = self.config[key]
            diff = value.get_diff(existing.config[key])
            if key == "partition_by" and diff is not None:
                requires_refresh = True
            if diff and diff != RefreshConfig():
                requires_replace = True
            diff = diff or value
            if diff != RefreshConfig():
                changes[key] = diff
        if requires_replace:
            return DatabricksRelationChangeSet(
                changes=changes, requires_full_refresh=requires_refresh
            )
        return None
