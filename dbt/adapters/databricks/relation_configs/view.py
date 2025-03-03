from dbt.adapters.databricks.relation_configs.base import (
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.query import QueryProcessor
from dbt.adapters.databricks.relation_configs.tags import TagsProcessor
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesProcessor


class ViewConfig(DatabricksRelationConfigBase):
    config_components = [TagsProcessor, TblPropertiesProcessor, QueryProcessor]
