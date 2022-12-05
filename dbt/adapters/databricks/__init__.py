from dbt.adapters.databricks.connections import DatabricksConnectionManager  # noqa
from dbt.adapters.databricks.connections import DatabricksCredentials
from dbt.adapters.databricks.relation import DatabricksRelation  # noqa
from dbt.adapters.databricks.column import DatabricksColumn  # noqa
from dbt.adapters.databricks.impl import DatabricksAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import databricks

Plugin = AdapterPlugin(
    adapter=DatabricksAdapter,  # type: ignore[arg-type]
    credentials=DatabricksCredentials,
    include_path=databricks.PACKAGE_PATH,
    dependencies=["spark"],
)
