from dataclasses import dataclass
from typing import ClassVar
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.contracts.graph.nodes import ModelNode
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.exceptions import DbtRuntimeError


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class QueryConfig(DatabricksComponentConfig):
    query: str

    def to_sql_clause(self) -> str:
        return self.query


class QueryProcessor(DatabricksComponentProcessor[QueryConfig]):
    name: ClassVar[str] = "query"

    @classmethod
    def from_results(cls, result: RelationResults) -> QueryConfig:
        row = result["information_schema.views"]
        return QueryConfig(row["view_definition"])

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> QueryConfig:
        query = model_node.compiled_code

        if query:
            return QueryConfig(query.strip())
        else:
            raise DbtRuntimeError(f"Cannot compile model {model_node.unique_id} with no SQL query")
