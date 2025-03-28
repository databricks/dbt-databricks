from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults


class ColumnCommentsConfig(DatabricksComponentConfig):
    """Component encapsulating column-level comments."""

    comments: dict[str, str]
    quoted: dict[str, bool] = {}
    persist: bool = False

    def get_diff(self, other: "ColumnCommentsConfig") -> Optional["ColumnCommentsConfig"]:
        comments = {}
        if self.persist:
            for column_name, comment in self.comments.items():
                if comment != other.comments.get(column_name):
                    column_name = (
                        f"`{column_name}`" if self.quoted.get(column_name, False) else column_name
                    )
                    comments[column_name] = comment
            return ColumnCommentsConfig(comments=comments, persist=True)
        return None


class ColumnCommentsProcessor(DatabricksComponentProcessor[ColumnCommentsConfig]):
    name: ClassVar[str] = "column_comments"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> ColumnCommentsConfig:
        table = results["describe_extended"]
        comments = {}
        for row in table.rows:
            if row["col_name"].startswith("#"):
                break
            comments[row["col_name"]] = row["comment"] or ""
        return ColumnCommentsConfig(comments=comments)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> ColumnCommentsConfig:
        columns = getattr(relation_config, "columns", {})
        persist = False
        if relation_config.config:
            persist = relation_config.config.persist_docs.get("relation") or False
        comments = {}
        quoted = {}
        for column_name, column in columns.items():
            if hasattr(column, "description"):
                comments[column_name] = column.description or ""
                quoted[column_name] = column.quote or False
            else:
                comments[column_name] = column.get("description", "")
                quoted[column_name] = column.get("quote", False)
        return ColumnCommentsConfig(comments=comments, persist=persist, quoted=quoted)
