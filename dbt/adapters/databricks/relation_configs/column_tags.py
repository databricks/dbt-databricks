from dataclasses import asdict
from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


class ColumnTagsConfig(DatabricksComponentConfig):
    """Component encapsulating column-level databricks_tags."""

    # column name -> tags config (dict of tag_name: tag_value)
    set_column_tags: dict[str, dict[str, str]]

    def get_diff(self, other: "ColumnTagsConfig") -> Optional["ColumnTagsConfig"]:
        # Column tags are now "set only" - we never unset column tags, only add or update them
        # Find columns that need to be set or updated

        # Create a case-insensitive lookup for other's column names
        other_column_tags_lower = {k.lower(): (k, v) for k, v in other.set_column_tags.items()}

        set_column_tags = {}
        for col, tags in self.set_column_tags.items():
            col_lower = col.lower()
            # Use case-insensitive comparison for column names
            if col_lower not in other_column_tags_lower:
                # Column doesn't exist in other, need to set it
                set_column_tags[col] = tags
            else:
                # Column exists, check if tags are different
                _, other_tags = other_column_tags_lower[col_lower]
                if other_tags != tags:
                    set_column_tags[col] = tags

        if set_column_tags:
            return ColumnTagsConfig(set_column_tags=set_column_tags)
        return None


class ColumnTagsProcessor(DatabricksComponentProcessor[ColumnTagsConfig]):
    name: ClassVar[str] = "column_tags"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> ColumnTagsConfig:
        column_tags_result = results.get("information_schema.column_tags")
        set_column_tags: dict[str, dict[str, str]] = {}

        if column_tags_result:
            for row in column_tags_result.rows:
                # row contains [column_name, tag_name, tag_value]
                column_name = str(row[0])
                tag_name = str(row[1])
                tag_value = str(row[2])

                if column_name not in set_column_tags:
                    set_column_tags[column_name] = {}
                set_column_tags[column_name][tag_name] = tag_value

        return ColumnTagsConfig(set_column_tags=set_column_tags)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> ColumnTagsConfig:
        # Extract config from model node
        columns = getattr(relation_config, "columns", {})
        columns = [
            {"name": name, **(col if isinstance(col, dict) else asdict(col))}
            for name, col in columns.items()
        ]

        set_column_tags = {}
        for col in columns:
            extra = col.get("_extra", {})
            databricks_tags = extra.get("databricks_tags") if extra else None
            if databricks_tags:
                if isinstance(databricks_tags, dict):
                    set_column_tags[col["name"]] = {
                        str(k): str(v) for k, v in databricks_tags.items()
                    }
                else:
                    raise DbtRuntimeError("databricks_tags must be a dictionary")

        return ColumnTagsConfig(set_column_tags=set_column_tags)
