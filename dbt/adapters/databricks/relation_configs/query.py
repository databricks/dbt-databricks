from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.handle import SqlUtils
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


class QueryConfig(DatabricksComponentConfig):
    """Component encapsulating the query that defines a relation."""

    query: str

    def get_diff(self, other: "QueryConfig") -> Optional["QueryConfig"]:
        if self.query.strip() != other.query.strip():
            return self
        return None


class QueryProcessor(DatabricksComponentProcessor[QueryConfig]):
    name: ClassVar[str] = "query"

    @classmethod
    def from_relation_results(cls, result: RelationResults) -> QueryConfig:
        view_definition_raw = result["information_schema.views"].get("view_definition") or ""
        view_definition = view_definition_raw.strip()
        if not view_definition:
            return QueryConfig(query="")
        if view_definition.startswith("(") and view_definition.endswith(")"):
            view_definition = view_definition[1:-1]
        return QueryConfig(query=SqlUtils.clean_sql(view_definition))

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> QueryConfig:
        query = relation_config.compiled_code

        if query:
            return QueryConfig(query=SqlUtils.clean_sql(query))
        else:
            raise DbtRuntimeError(
                f"Cannot compile model {relation_config.identifier} with no SQL query"
            )


class DescribeQueryProcessor(QueryProcessor):
    @classmethod
    def from_relation_results(cls, result: RelationResults) -> QueryConfig:
        table = result["describe_extended"]
        row = next((x for x in table if x[0] == "View Text"), None)
        if row is None:
            return QueryConfig(query="")
        if len(row) < 2:
            raise DbtRuntimeError(
                "Unexpected result from DESCRIBE EXTENDED: missing View Text value"
            )
        return QueryConfig(query=SqlUtils.clean_sql(row[1]))
