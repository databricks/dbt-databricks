from dataclasses import dataclass
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.comment import (
    CommentConfig,
    CommentConfigChange,
    CommentProcessor,
)
from dbt.adapters.databricks.relation_configs.partitioning import (
    PartitionedByConfig,
    PartitionedByConfigChange,
    PartitionedByProcessor,
)
from dbt.adapters.databricks.relation_configs.query import (
    QueryConfig,
    QueryConfigChange,
    QueryProcessor,
)
from dbt.adapters.databricks.relation_configs.refresh import (
    RefreshConfig,
    RefreshConfigChange,
    RefreshProcessor,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class MaterializedViewConfig(DatabricksRelationConfigBase):
    config_components = [PartitionedByProcessor, CommentProcessor, RefreshProcessor, QueryProcessor]

    partition_by: PartitionedByConfig
    comment: CommentConfig
    refresh: RefreshConfig
    query: QueryConfig
