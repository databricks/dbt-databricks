from dataclasses import dataclass
from typing import List, Optional

from dbt.exceptions import DbtRuntimeError
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksConfigChange,
    DatabricksRelationChangeSet,
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.comment import (
    CommentConfig,
    CommentProcessor,
)
from dbt.adapters.databricks.relation_configs.partitioning import (
    PartitionedByConfig,
    PartitionedByProcessor,
)
from dbt.adapters.databricks.relation_configs.query import (
    QueryConfig,
    QueryProcessor,
)
from dbt.adapters.databricks.relation_configs.refresh import (
    RefreshConfig,
    RefreshProcessor,
)
from dbt.adapters.databricks.relation_configs.tblproperties import (
    TblPropertiesConfig,
    TblPropertiesProcessor,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class MaterializedViewConfig(DatabricksRelationConfigBase):
    config_components = [
        PartitionedByProcessor,
        CommentProcessor,
        TblPropertiesProcessor,
        RefreshProcessor,
        QueryProcessor,
    ]

    partition_by: PartitionedByConfig
    comment: CommentConfig
    tblproperties: TblPropertiesConfig
    refresh: RefreshConfig
    query: QueryConfig

    def get_changeset(
        self, existing: DatabricksRelationConfigBase
    ) -> Optional[DatabricksRelationChangeSet]:
        if not isinstance(existing, MaterializedViewConfig):
            raise DbtRuntimeError(
                f"Invalid comparison between MaterializedViewConfig and {type(existing)}"
            )

        changes: List[Optional[DatabricksConfigChange]] = []
        changes.append(DatabricksConfigChange.get_change(existing.partition_by, self.partition_by))
        changes.append(
            DatabricksConfigChange.get_change(existing.tblproperties, self.tblproperties)
        )
        changes.append(DatabricksConfigChange.get_change(existing.comment, self.comment))
        changes.append(DatabricksConfigChange.get_change(existing.refresh, self.refresh))
        changes.append(DatabricksConfigChange.get_change(existing.query, self.query))

        trimmed_changes = [change for change in changes if change]
        if trimmed_changes:
            return DatabricksRelationChangeSet(trimmed_changes)
        return None
