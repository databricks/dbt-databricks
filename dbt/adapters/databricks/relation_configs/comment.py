from dataclasses import dataclass
from typing import Optional, ClassVar
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.adapters.relation_configs.config_change import RelationConfigChange


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class CommentConfig(DatabricksComponentConfig):
    comment: Optional[str] = None

    def to_sql_clause(self) -> str:
        if self.comment:
            return f"COMMENT '{self.comment}'"
        return ""


class CommentProcessor(DatabricksComponentProcessor[CommentConfig]):
    name: ClassVar[str] = "comment"

    @classmethod
    def from_results(cls, results: RelationResults) -> CommentConfig:
        table = results["describe_extended"]
        for row in table.rows:
            if row[0] == "Comment":
                return CommentConfig(row[1])
        return CommentConfig()

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> CommentConfig:
        return CommentConfig(model_node.description)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class CommentConfigChange(RelationConfigChange):
    context: Optional[CommentConfig] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False
