from typing import Optional, ClassVar
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.adapters.contracts.relation import RelationConfig


class CommentConfig(DatabricksComponentConfig):
    """Component encapsulating the relation-level comment."""

    comment: Optional[str] = None


class CommentProcessor(DatabricksComponentProcessor[CommentConfig]):
    name: ClassVar[str] = "comment"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> CommentConfig:
        table = results["describe_extended"]
        for row in table.rows:
            if row[0] == "Comment":
                return CommentConfig(comment=row[1])
        return CommentConfig()

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> CommentConfig:
        return CommentConfig(comment=getattr(relation_config, "description"))
