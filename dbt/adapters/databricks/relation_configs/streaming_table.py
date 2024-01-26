from typing import Optional
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksRelationChangeSet,
    DatabricksRelationConfigBase,
    RelationChange,
)
from dbt.adapters.databricks.relation_configs.comment import (
    CommentProcessor,
)
from dbt.adapters.databricks.relation_configs.partitioning import (
    PartitionedByProcessor,
)
from dbt.adapters.databricks.relation_configs.refresh import (
    RefreshProcessor,
)
from dbt.adapters.databricks.relation_configs.tblproperties import (
    TblPropertiesProcessor,
)


class StreamingTableConfig(DatabricksRelationConfigBase):
    config_components = {
        PartitionedByProcessor: True,
        CommentProcessor: False,
        TblPropertiesProcessor: False,
        RefreshProcessor: False,
    }

    def get_changeset(
        self, existing: "StreamingTableConfig"
    ) -> Optional[DatabricksRelationChangeSet]:
        """Get the changeset that must be applied to the existing relation to make it match the
        current state of the dbt project.
        """

        changes = {}

        for component, requires_refresh in self.config_components.items():
            key = component.name
            value = self.config[key]
            diff = value.get_diff(existing.config[key])
            if key == "partition_by":
                changes[key] = RelationChange(data=value, requires_full_refresh=diff is not None)
            elif key != "refresh" and not diff:
                diff = value
            if diff:
                changes[key] = RelationChange(data=diff, requires_full_refresh=requires_refresh)

        if len(changes) > 0:
            return DatabricksRelationChangeSet(changes=changes)
        return None
