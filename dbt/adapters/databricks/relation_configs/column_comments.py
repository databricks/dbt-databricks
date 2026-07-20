from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.events.types import AdapterEventWarning
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt_common.events.functions import warn_or_error

from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


class ColumnCommentsConfig(DatabricksComponentConfig):
    """Component encapsulating column-level comments."""

    comments: dict[str, str]
    persist: bool = False

    def get_diff(self, other: "ColumnCommentsConfig") -> Optional["ColumnCommentsConfig"]:
        logger.debug(f"Getting diff for ColumnCommentsConfig: {self} and {other}")
        comments = {}
        if self.persist:
            # Create a case-insensitive lookup for other's column comments
            other_comments_lower = {k.lower(): v for k, v in other.comments.items()}

            # Warn about columns that are documented in the model's schema but are not present in
            # the relation. These are skipped below (rather than erroring on the alter), so surface
            # them to the user to catch typos and stale documentation.
            missing = [
                column_name
                for column_name in self.comments
                if column_name.lower() not in other_comments_lower
            ]
            if missing:
                warn_or_error(
                    AdapterEventWarning(
                        base_msg=(
                            "The following columns are specified in the schema but are not present "
                            "in the database and will be skipped: " + ", ".join(missing)
                        )
                    )
                )

            for column_name, comment in self.comments.items():
                # Use case-insensitive comparison for column names
                if column_name.lower() not in other_comments_lower:
                    continue
                other_comment = other_comments_lower.get(column_name.lower())
                if comment != other_comment:
                    column_name = f"`{column_name}`"
                    comments[column_name] = comment
            logger.debug(f"Comments: {comments}")
            if len(comments) > 0:
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
            comments[row["col_name"].lower()] = row["comment"] or ""
        return ColumnCommentsConfig(comments=comments)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> ColumnCommentsConfig:
        columns = getattr(relation_config, "columns", {})
        persist = False
        if relation_config.config:
            persist = relation_config.config.persist_docs.get("relation") or False
        comments = {}
        for column_name, column in columns.items():
            if hasattr(column, "description"):
                comments[column_name] = column.description or ""
            else:
                comments[column_name] = column.get("description", "")
        return ColumnCommentsConfig(comments=comments, persist=persist)
