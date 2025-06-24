from dbt.adapters.databricks.relation_configs.base import (
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.column_comments import ColumnCommentsProcessor
from dbt.adapters.databricks.relation_configs.column_mask import ColumnMaskProcessor
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsProcessor
from dbt.adapters.databricks.relation_configs.comment import CommentProcessor
from dbt.adapters.databricks.relation_configs.constraints import ConstraintsProcessor
from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringProcessor
from dbt.adapters.databricks.relation_configs.tags import TagsProcessor
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesProcessor


class IncrementalTableConfig(DatabricksRelationConfigBase):
    config_components = [
        CommentProcessor,
        ColumnCommentsProcessor,
        ColumnMaskProcessor,
        ColumnTagsProcessor,
        ConstraintsProcessor,
        TagsProcessor,
        TblPropertiesProcessor,
        LiquidClusteringProcessor,
    ]
