from typing import Optional

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksRelationChangeSet,
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.comment import (
    CommentProcessor,
)
from dbt.adapters.databricks.relation_configs.partitioning import (
    PartitionedByProcessor,
)
from dbt.adapters.databricks.relation_configs.query import (
    QueryProcessor,
)
from dbt.adapters.databricks.relation_configs.refresh import (
    RefreshProcessor,
)
from dbt.adapters.databricks.relation_configs.tblproperties import (
    TblPropertiesProcessor,
)


class MaterializedViewConfig(DatabricksRelationConfigBase):
    config_components = [
        PartitionedByProcessor,
        CommentProcessor,
        TblPropertiesProcessor,
        RefreshProcessor,
        QueryProcessor,
    ]

    def get_changeset(
        self, existing: "MaterializedViewConfig"
    ) -> Optional[DatabricksRelationChangeSet]:
        changes: dict[str, DatabricksComponentConfig] = {}
        requires_refresh = False

        for component in self.config_components:
            key = component.name
            value = self.config[key]
            diff = value.get_diff(existing.config[key])
            if diff:
                requires_refresh = requires_refresh or key != "refresh"
                changes[key] = diff

        if len(changes) > 0:
            return DatabricksRelationChangeSet(
                changes=changes, requires_full_refresh=requires_refresh
            )
        return None
