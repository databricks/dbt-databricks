from typing import ClassVar, Optional

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults


class TagsConfig(DatabricksComponentConfig):
    """Component encapsulating the tblproperties of a relation."""

    set_tags: dict[str, str]

    def get_diff(self, other: "TagsConfig") -> Optional["TagsConfig"]:
        # Tags are now "set only" - we never unset tags, only add or update them
        if any(item not in other.set_tags.items() for item in self.set_tags.items()):
            return TagsConfig(set_tags=self.set_tags)
        return None


class TagsProcessor(DatabricksComponentProcessor[TagsConfig]):
    name: ClassVar[str] = "tags"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> TagsConfig:
        table = results.get("information_schema.tags")
        tags = dict()

        if table:
            for row in table.rows:
                tags[str(row[0])] = str(row[1])

        return TagsConfig(set_tags=tags)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> TagsConfig:
        tags = base.get_config_value(relation_config, "databricks_tags")
        if not tags:
            return TagsConfig(set_tags=dict())
        if isinstance(tags, dict):
            tags = {str(k): str(v) for k, v in tags.items()}
            return TagsConfig(set_tags=tags)
        else:
            raise DbtRuntimeError("databricks_tags must be a dictionary")
