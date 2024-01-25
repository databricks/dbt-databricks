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


class MaterializedViewConfig(DatabricksRelationConfigBase):
    config_components = [
        PartitionedByProcessor,
        CommentProcessor,
        TblPropertiesProcessor,
        RefreshProcessor,
        QueryProcessor,
    ]
