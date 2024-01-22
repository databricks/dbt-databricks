from dbt.adapters.databricks.relation_configs.base import (
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


class StreamingTableConfig(DatabricksRelationConfigBase):
    config_components = {
        PartitionedByProcessor: False,
        CommentProcessor: False,
        TblPropertiesProcessor: False,
        RefreshProcessor: False,
    }
