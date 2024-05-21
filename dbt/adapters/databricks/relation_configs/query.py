from typing import ClassVar

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs.base import DatabricksComponentConfig
from dbt.adapters.databricks.relation_configs.base import DatabricksComponentProcessor
from dbt.adapters.relation_configs.config_base import RelationResults


class QueryConfig(DatabricksComponentConfig):
    """Component encapsulating the query that defines a relation."""

    query: str


class QueryProcessor(DatabricksComponentProcessor[QueryConfig]):
    name: ClassVar[str] = "query"

    @classmethod
    def from_relation_results(cls, result: RelationResults) -> QueryConfig:
        row = result["information_schema.views"]
        return QueryConfig(query=row["view_definition"])

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> QueryConfig:
        query = relation_config.compiled_code

        if query:
            return QueryConfig(query=query.strip())
        else:
            raise DbtRuntimeError(
                f"Cannot compile model {relation_config.identifier} with no SQL query"
            )
