from dataclasses import dataclass
from typing import Optional
from agate import Row
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_change import RelationConfigChange


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class CommentConfig(DatabricksComponentConfig):
    comment: Optional[str] = None

    def to_sql_clause(self) -> str:
        if self.comment:
            return f"COMMENT '{self.comment}'"
        return ""


class CommentProcessor(DatabricksComponentProcessor[CommentConfig]):
    @classmethod
    def name(cls) -> str:
        return "comment"

    @classmethod
    def description_target(cls) -> str:
        return "Comment"

    @classmethod
    def process_description_row_impl(cls, row: Row) -> CommentConfig:
        return CommentConfig(row[1])

    @classmethod
    def process_model_node(cls, model_node: ModelNode) -> CommentConfig:
        return CommentConfig(model_node.description)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class CommentConfigChange(RelationConfigChange):
    context: Optional[CommentConfig] = None

    def requires_full_refresh(self) -> bool:
        return False
