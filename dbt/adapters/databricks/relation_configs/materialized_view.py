from dataclasses import dataclass
from typing import List, Optional
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksRelationConfigBase,
    PartitionedByConfig,
    PartitionedByProcessor,
)
from dbt.adapters.databricks.relation_configs.comment import CommentConfig, CommentProcessor
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig, RefreshProcessor


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class MaterializedViewConfig(DatabricksRelationConfigBase):
    partitioned_by_processor = PartitionedByProcessor
    config_components = [CommentProcessor, RefreshProcessor]

    comment: CommentConfig
    refresh: RefreshConfig
    partition_by: PartitionedByConfig
