import dataclasses
from typing import Optional

from dbt.adapters.databricks.relation_configs.base import DatabricksComponentConfig


@dataclasses(frozen=True, eq=True, unsafe_hash=True)
class QueryConfig(DatabricksComponentConfig):
    query: str

    def to_sql_clause(self) -> str:
        return self.query


class QueryProcessor(DatabricksComponentProcessor[CommentConfig]):
    @classmethod
    def process_description_row_impl(cls, row: Row) -> CommentConfig:
        return CommentConfig(row[1])

    @classmethod
    def process_model_node(cls, model_node: ModelNode) -> QueryConfig:
        return CommentConfig(model_node.description)
