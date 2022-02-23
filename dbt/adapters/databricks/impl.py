from dataclasses import dataclass
from typing import Optional, List, Dict, Union

from dbt.adapters.base import AdapterConfig
from dbt.adapters.databricks import DatabricksConnectionManager
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.column import DatabricksColumn

from dbt.adapters.spark.impl import SparkAdapter


@dataclass
class DatabricksConfig(AdapterConfig):
    file_format: str = "delta"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None
    tblproperties: Optional[Dict[str, str]] = None


class DatabricksAdapter(SparkAdapter):

    Relation = DatabricksRelation
    Column = DatabricksColumn

    ConnectionManager = DatabricksConnectionManager
    connections: DatabricksConnectionManager

    AdapterSpecificConfigs = DatabricksConfig

    def list_schemas(self, database: Optional[str]) -> List[str]:
        """Get a list of existing schemas in database."""
        results = self.connections.list_schemas(database=database)
        return [row[0] for row in results]

    def check_schema_exists(self, database: Optional[str], schema: str) -> bool:
        """Check if a schema exists."""
        results = self.connections.list_schemas(database=database, schema=schema)
        return schema.lower() in [row[0].lower() for row in results]
